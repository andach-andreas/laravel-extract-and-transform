<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Strategies;

use Andach\ExtractAndTransform\Dataset;
use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Models\SchemaVersion;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Models\SyncRun;
use Andach\ExtractAndTransform\Services\RowTransformer;
use Andach\ExtractAndTransform\Source;
use Andach\ExtractAndTransform\Strategies\WatermarkStrategy;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Schema;
use Mockery;

class WatermarkStrategyTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();
        Schema::create('dest_table', function ($table) {
            $table->id('__id');
            $table->string('name')->nullable();
            $table->timestamp('updated_at_source')->nullable();
            $table->unsignedBigInteger('__source_id')->nullable();
            $table->timestamp('__last_synced_at')->nullable();
            $table->boolean('__is_deleted')->default(0);
            $table->string('__content_hash')->nullable();
            $table->timestamps();
        });
    }

    public function test_it_performs_initial_sync_and_then_incremental_sync()
    {
        // Mocks
        $transformer = new RowTransformer;
        $strategy = new WatermarkStrategy($transformer);

        $sourceModel = ExtractSource::create(['name' => 'S', 'connector' => 'c', 'config' => []]);
        $profile = SyncProfile::create(['extract_source_id' => $sourceModel->id, 'dataset_identifier' => 'd', 'strategy' => 'watermark']);
        $version = SchemaVersion::create([
            'sync_profile_id' => $profile->id,
            'version_number' => 1,
            'local_table_name' => 'dest_table',
            'source_schema_hash' => 'h',
            'configuration' => ['watermark_column' => 'updated_at_source'],
        ]);
        $profile->update(['active_schema_version_id' => $version->id]);

        $dataset = Mockery::mock(Dataset::class);
        $source = Mockery::mock(Source::class);
        $source->shouldReceive('getDataset')->with('d')->andReturn($dataset);
        $source->shouldReceive('getModel')->andReturn($sourceModel);

        // 1. Initial Sync
        $run1 = SyncRun::create(['sync_profile_id' => $profile->id, 'status' => 'running']);
        $dataset->shouldReceive('getRowsWithCheckpoint')->once()->with(null, Mockery::any())->andReturn((function () {
            yield ['name' => 'Alice', 'updated_at_source' => '2023-01-01 10:00:00'];
            yield ['name' => 'Bob', 'updated_at_source' => '2023-01-01 11:00:00'];

            return ['watermark' => '2023-01-01 11:00:00'];
        })());

        $strategy->run($profile, $source, $run1);
        $this->assertDatabaseCount('dest_table', 2);
        $this->assertEquals('2023-01-01 11:00:00', $run1->fresh()->checkpoint['watermark']);

        // 2. Incremental Sync
        $run2 = SyncRun::create(['sync_profile_id' => $profile->id, 'status' => 'running', 'checkpoint' => $run1->checkpoint]);
        $dataset->shouldReceive('getRowsWithCheckpoint')->once()->with(['watermark' => '2023-01-01 11:00:00'], Mockery::any())->andReturn((function () {
            yield ['name' => 'Charlie', 'updated_at_source' => '2023-01-01 12:00:00'];

            return ['watermark' => '2023-01-01 12:00:00'];
        })());

        $strategy->run($profile, $source, $run2);
        $this->assertDatabaseCount('dest_table', 3);
        $this->assertEquals('2023-01-01 12:00:00', $run2->fresh()->checkpoint['watermark']);
    }
}
