<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Strategies;

use Andach\ExtractAndTransform\Dataset;
use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Models\SchemaVersion;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Models\SyncRun;
use Andach\ExtractAndTransform\Services\RowTransformer;
use Andach\ExtractAndTransform\Source;
use Andach\ExtractAndTransform\Strategies\FullRefreshStrategy;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Mockery;

class FullRefreshStrategyTest extends TestCase
{
    use RefreshDatabase;

    public function test_it_syncs_data_correctly()
    {
        // Setup Destination Table
        Schema::create('dest_table', function ($table) {
            $table->id('__id');
            $table->string('name')->nullable();
            $table->integer('age')->nullable();
            $table->json('meta')->nullable();
            $table->dateTime('created_at_source')->nullable();
            $table->unsignedBigInteger('__source_id')->nullable();
            $table->timestamp('__last_synced_at')->nullable();
            $table->boolean('__is_deleted')->default(0);
            $table->string('__content_hash')->nullable();
            $table->timestamps();
        });

        // Mocks
        $transformer = new RowTransformer; // Use real transformer
        $strategy = new FullRefreshStrategy($transformer);

        $sourceModel = ExtractSource::create(['name' => 'S', 'connector' => 'c', 'config' => []]);
        $profile = SyncProfile::create(['extract_source_id' => $sourceModel->id, 'dataset_identifier' => 'd', 'strategy' => 'full_refresh']);
        $version = SchemaVersion::create(['sync_profile_id' => $profile->id, 'version_number' => 1, 'local_table_name' => 'dest_table', 'source_schema_hash' => 'h']);
        $profile->update(['active_schema_version_id' => $version->id]);
        $run = SyncRun::create(['sync_profile_id' => $profile->id, 'status' => 'running']);

        $dataset = Mockery::mock(Dataset::class);
        $dataset->shouldReceive('getRows')->andReturn(new \ArrayIterator([
            ['name' => 'Alice', 'age' => 30, 'extra' => 'ignored'],
            ['name' => 'Bob', 'meta' => ['foo' => 'bar']], // Missing age
            ['name' => 'Charlie', 'created_at_source' => '/Date(1634515200000+0000)/'], // Date sanitization
        ]));

        $source = Mockery::mock(Source::class);
        $source->shouldReceive('getDataset')->with('d')->andReturn($dataset);
        $source->shouldReceive('getModel')->andReturn($sourceModel);

        // Run
        $strategy->run($profile, $source, $run);

        // Assertions
        $this->assertDatabaseCount('dest_table', 3);

        $alice = DB::table('dest_table')->where('name', 'Alice')->first();
        $this->assertEquals(30, $alice->age);
        $this->assertEquals($sourceModel->id, $alice->__source_id);
        $this->assertNotNull($alice->__last_synced_at);

        $bob = DB::table('dest_table')->where('name', 'Bob')->first();
        $this->assertNull($bob->age); // Normalized to null
        $this->assertEquals('{"foo":"bar"}', $bob->meta); // JSON encoded

        $charlie = DB::table('dest_table')->where('name', 'Charlie')->first();
        $this->assertEquals('2021-10-18 00:00:00', $charlie->created_at_source); // Date sanitized
    }
}
