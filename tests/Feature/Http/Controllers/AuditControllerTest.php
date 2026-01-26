<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Http\Controllers;

use Andach\ExtractAndTransform\Models\AuditRun;
use Andach\ExtractAndTransform\Models\Correction;
use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Models\SchemaVersion;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Models\SyncRun;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class AuditControllerTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        Schema::create('test_table', function ($table) {
            $table->id('__id'); // Use __id to match the service's expectation
            $table->string('name')->nullable();
        });
        DB::table('test_table')->insert(['__id' => 1, 'name' => 'Test']);
    }

    public function test_index_page_loads()
    {
        $source = ExtractSource::create(['name' => 'S', 'connector' => 'c', 'config' => []]);
        $profile = SyncProfile::create(['extract_source_id' => $source->id, 'dataset_identifier' => 'd', 'strategy' => 's']);
        $version = SchemaVersion::create(['sync_profile_id' => $profile->id, 'version_number' => 1, 'local_table_name' => 'test_table', 'source_schema_hash' => 'h']);
        $profile->update(['active_schema_version_id' => $version->id]);
        $run = SyncRun::create(['sync_profile_id' => $profile->id, 'status' => 'success']);

        $response = $this->get("/andach-leat/syncs/{$run->id}/audit");

        $response->assertStatus(200);
        $response->assertSee('Audit: d');
    }

    public function test_configure_page_loads()
    {
        $source = ExtractSource::create(['name' => 'S', 'connector' => 'c', 'config' => []]);
        $profile = SyncProfile::create(['extract_source_id' => $source->id, 'dataset_identifier' => 'd', 'strategy' => 's']);
        $version = SchemaVersion::create(['sync_profile_id' => $profile->id, 'version_number' => 1, 'local_table_name' => 'test_table', 'source_schema_hash' => 'h']);
        $profile->update(['active_schema_version_id' => $version->id]);
        $run = SyncRun::create(['sync_profile_id' => $profile->id, 'status' => 'success']);

        $response = $this->get("/andach-leat/syncs/{$run->id}/audit/configure");

        $response->assertStatus(200);
        $response->assertSee('Configure Audit Rules');
    }

    public function test_store_config()
    {
        $source = ExtractSource::create(['name' => 'S', 'connector' => 'c', 'config' => []]);
        $profile = SyncProfile::create(['extract_source_id' => $source->id, 'dataset_identifier' => 'd', 'strategy' => 's']);
        $version = SchemaVersion::create(['sync_profile_id' => $profile->id, 'version_number' => 1, 'local_table_name' => 'test_table', 'source_schema_hash' => 'h']);
        $profile->update(['active_schema_version_id' => $version->id]);
        $run = SyncRun::create(['sync_profile_id' => $profile->id, 'status' => 'success']);

        $response = $this->post("/andach-leat/syncs/{$run->id}/audit/config", [
            'rules' => ['name' => [['type' => 'required']]],
        ]);

        $response->assertRedirect("/andach-leat/syncs/{$run->id}/audit");

        $version->refresh();
        $this->assertEquals([['type' => 'required']], $version->configuration['audit']['name']);
    }

    public function test_run_audit()
    {
        $source = ExtractSource::create(['name' => 'S', 'connector' => 'c', 'config' => []]);
        $profile = SyncProfile::create(['extract_source_id' => $source->id, 'dataset_identifier' => 'd', 'strategy' => 's']);
        $version = SchemaVersion::create([
            'sync_profile_id' => $profile->id,
            'version_number' => 1,
            'local_table_name' => 'test_table',
            'source_schema_hash' => 'h',
            'configuration' => ['audit' => ['name' => [['type' => 'required']]]],
        ]);
        $profile->update(['active_schema_version_id' => $version->id]);
        $run = SyncRun::create(['sync_profile_id' => $profile->id, 'status' => 'success']);

        $response = $this->post("/andach-leat/syncs/{$run->id}/audit/run");

        $response->assertRedirect("/andach-leat/syncs/{$run->id}/audit");
        $this->assertDatabaseHas('andach_leat_audit_runs', ['table_name' => 'test_table']);
    }

    public function test_store_corrections()
    {
        $source = ExtractSource::create(['name' => 'S', 'connector' => 'c', 'config' => []]);
        $profile = SyncProfile::create(['extract_source_id' => $source->id, 'dataset_identifier' => 'd', 'strategy' => 's']);
        $version = SchemaVersion::create(['sync_profile_id' => $profile->id, 'version_number' => 1, 'local_table_name' => 'test_table', 'source_schema_hash' => 'h']);
        $profile->update(['active_schema_version_id' => $version->id]);
        $run = SyncRun::create(['sync_profile_id' => $profile->id, 'status' => 'success']);

        $response = $this->post("/andach-leat/syncs/{$run->id}/audit/correction", [
            'corrections' => [
                '1' => ['name' => ['value' => 'New Name', 'reason' => 'Fix']],
            ],
        ]);

        $response->assertRedirect(); // back()
        $this->assertDatabaseHas('andach_leat_corrections', [
            'table_name' => 'test_table',
            'row_identifier' => '1',
            'new_value' => 'New Name',
        ]);
    }

    public function test_reconcile_action_creates_corrected_table()
    {
        $source = ExtractSource::create(['name' => 'S', 'connector' => 'c', 'config' => []]);
        $profile = SyncProfile::create(['extract_source_id' => $source->id, 'dataset_identifier' => 'd', 'strategy' => 's']);
        $version = SchemaVersion::create(['sync_profile_id' => $profile->id, 'version_number' => 1, 'local_table_name' => 'test_table', 'source_schema_hash' => 'h']);
        $profile->update(['active_schema_version_id' => $version->id]);
        $run = SyncRun::create(['sync_profile_id' => $profile->id, 'status' => 'success']);
        AuditRun::create(['table_name' => 'test_table', 'identifier_column' => '__id', 'status' => 'success']);
        Correction::create(['table_name' => 'test_table', 'row_identifier' => 1, 'column_name' => 'name', 'new_value' => 'Corrected Name']);

        $response = $this->post("/andach-leat/syncs/{$run->id}/audit/reconcile");

        $response->assertRedirect();
        $this->assertTrue(Schema::hasTable('test_table_reconciled'));
        $this->assertDatabaseHas('test_table_reconciled', ['__id' => 1, 'name' => 'Corrected Name']);
    }
}
