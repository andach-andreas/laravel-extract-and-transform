<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Strategies;

use Andach\ExtractAndTransform\Dataset;
use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Models\SchemaVersion;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Models\SyncRun;
use Andach\ExtractAndTransform\Services\RowTransformer;
use Andach\ExtractAndTransform\Source;
use Andach\ExtractAndTransform\Strategies\ContentHashStrategy;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Mockery;

class ContentHashStrategyTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();
        Schema::create('dest_table', function ($table) {
            $table->id('__id');
            $table->string('id')->nullable();
            $table->string('name')->nullable();
            $table->unsignedBigInteger('__source_id')->nullable();
            $table->timestamp('__last_synced_at')->nullable();
            $table->boolean('__is_deleted')->default(0);
            $table->string('__content_hash')->nullable();
            $table->timestamps();
        });
    }

    public function test_it_only_updates_changed_rows()
    {
        // Mocks
        $transformer = new RowTransformer;
        $strategy = new ContentHashStrategy($transformer);

        $sourceModel = ExtractSource::create(['name' => 'S', 'connector' => 'c', 'config' => []]);
        $profile = SyncProfile::create(['extract_source_id' => $sourceModel->id, 'dataset_identifier' => 'd', 'strategy' => 'content_hash']);
        $version = SchemaVersion::create([
            'sync_profile_id' => $profile->id,
            'version_number' => 1,
            'local_table_name' => 'dest_table',
            'source_schema_hash' => 'h',
            'configuration' => ['id_column' => 'id'],
        ]);
        $profile->update(['active_schema_version_id' => $version->id]);

        $dataset = Mockery::mock(Dataset::class);
        $source = Mockery::mock(Source::class);
        $source->shouldReceive('getDataset')->with('d')->andReturn($dataset);
        $source->shouldReceive('getModel')->andReturn($sourceModel);

        // 1. Initial Sync
        $run1 = SyncRun::create(['sync_profile_id' => $profile->id, 'status' => 'running']);
        $dataset->shouldReceive('getRows')->once()->andReturn(new \ArrayIterator([
            ['id' => '1', 'name' => 'Alice'],
            ['id' => '2', 'name' => 'Bob'],
        ]));

        $strategy->run($profile, $source, $run1);
        $this->assertDatabaseCount('dest_table', 2);
        $this->assertEquals(2, $run1->fresh()->rows_added);

        // 2. Second Sync (Alice unchanged, Bob changed)
        $run2 = SyncRun::create(['sync_profile_id' => $profile->id, 'status' => 'running']);
        $dataset->shouldReceive('getRows')->once()->andReturn(new \ArrayIterator([
            ['id' => '1', 'name' => 'Alice'], // Same hash
            ['id' => '2', 'name' => 'Bob Updated'], // New hash
        ]));

        $strategy->run($profile, $source, $run2);

        // Current implementation treats update as Delete + Insert
        $this->assertEquals(1, $run2->fresh()->rows_added);
        $this->assertEquals(1, $run2->fresh()->rows_deleted);

        // Check Bob Updated exists and is active
        $bobNew = DB::table('dest_table')->where('id', '2')->where('name', 'Bob Updated')->first();
        $this->assertNotNull($bobNew);
        $this->assertEquals(0, $bobNew->__is_deleted);

        // Check old Bob is deleted (softly)
        // Note: Since we don't have a stable ID in this strategy, we can only find it by hash or content.
        // But since we inserted a new row, we have 3 rows total now.
        $this->assertDatabaseCount('dest_table', 3);

        $bobOld = DB::table('dest_table')->where('id', '2')->where('name', 'Bob')->first();
        $this->assertNotNull($bobOld);
        $this->assertEquals(1, $bobOld->__is_deleted);
    }
}
