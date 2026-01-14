<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Strategies;

use Andach\ExtractAndTransform\Dataset;
use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Models\SchemaVersion;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Models\SyncRun;
use Andach\ExtractAndTransform\Services\RowTransformer;
use Andach\ExtractAndTransform\Source;
use Andach\ExtractAndTransform\Strategies\IdDiffStrategy;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Mockery;

class IdDiffStrategyTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();
        Schema::create('dest_table', function ($table) {
            $table->id('__id');
            $table->string('id')->nullable(); // Source ID
            $table->string('name')->nullable();
            $table->unsignedBigInteger('__source_id')->nullable();
            $table->timestamp('__last_synced_at')->nullable();
            $table->boolean('__is_deleted')->default(0);
            $table->string('__content_hash')->nullable();
            $table->timestamps();
        });
    }

    public function test_it_handles_inserts_updates_and_deletes()
    {
        // Mocks
        $transformer = new RowTransformer;
        $strategy = new IdDiffStrategy($transformer);

        $sourceModel = ExtractSource::create(['name' => 'S', 'connector' => 'c', 'config' => []]);
        $profile = SyncProfile::create(['extract_source_id' => $sourceModel->id, 'dataset_identifier' => 'd', 'strategy' => 'id_diff']);
        $version = SchemaVersion::create([
            'sync_profile_id' => $profile->id,
            'version_number' => 1,
            'local_table_name' => 'dest_table',
            'source_schema_hash' => 'h',
            'configuration' => ['primary_key' => 'id'],
        ]);
        $profile->update(['active_schema_version_id' => $version->id]);

        $dataset = Mockery::mock(Dataset::class);
        $source = Mockery::mock(Source::class);
        $source->shouldReceive('getDataset')->with('d')->andReturn($dataset);
        $source->shouldReceive('getModel')->andReturn($sourceModel);

        // 1. Initial Sync (Insert)
        $run1 = SyncRun::create(['sync_profile_id' => $profile->id, 'status' => 'running']);
        $dataset->shouldReceive('getIdentities')->once()->andReturn(new \ArrayIterator([
            ['id' => '1'], ['id' => '2'],
        ]));
        $dataset->shouldReceive('getRowsByIds')->once()->with(['1', '2'], 'id')->andReturn(new \ArrayIterator([
            ['id' => '1', 'name' => 'Alice'],
            ['id' => '2', 'name' => 'Bob'],
        ]));

        $strategy->run($profile, $source, $run1);
        $this->assertDatabaseCount('dest_table', 2);
        $this->assertEquals(2, $run1->fresh()->rows_added);

        // 2. Second Sync (Alice updated, Bob deleted, Charlie added)
        $run2 = SyncRun::create(['sync_profile_id' => $profile->id, 'status' => 'running']);
        $dataset->shouldReceive('getIdentities')->once()->andReturn(new \ArrayIterator([
            ['id' => '1'], ['id' => '3'],
        ]));

        // New IDs: '3'
        $dataset->shouldReceive('getRowsByIds')->once()->with(['3'], 'id')->andReturn(new \ArrayIterator([
            ['id' => '3', 'name' => 'Charlie'],
        ]));

        // Intersect IDs: '1'
        $dataset->shouldReceive('getRowsByIds')->once()->with(['1'], 'id')->andReturn(new \ArrayIterator([
            ['id' => '1', 'name' => 'Alice Updated'],
        ]));

        $strategy->run($profile, $source, $run2);

        $this->assertEquals(1, $run2->fresh()->rows_added);
        $this->assertEquals(1, $run2->fresh()->rows_updated);
        $this->assertEquals(1, $run2->fresh()->rows_deleted);

        $this->assertDatabaseCount('dest_table', 3); // 1, 2 (soft deleted), 3

        $alice = DB::table('dest_table')->where('id', '1')->first();
        $this->assertEquals('Alice Updated', $alice->name);
        $this->assertEquals(0, $alice->__is_deleted);

        $bob = DB::table('dest_table')->where('id', '2')->first();
        $this->assertEquals(1, $bob->__is_deleted);

        $charlie = DB::table('dest_table')->where('id', '3')->first();
        $this->assertEquals('Charlie', $charlie->name);
    }
}
