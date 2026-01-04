<?php

namespace Andach\ExtractAndTransform\Tests\Unit\Models;

use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Models\SchemaVersion;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Models\SyncRun;
use Andach\ExtractAndTransform\Services\TableManager;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\HasMany;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Schema;
use Mockery\MockInterface;

class SyncProfileTest extends TestCase
{
    use RefreshDatabase;

    public function test_source_test(): void
    {
        $source = ExtractSource::factory()->create();
        $profile = SyncProfile::factory()->for($source, 'source')->create();
        $this->assertInstanceOf(BelongsTo::class, $profile->source());
        $this->assertTrue($profile->source->is($source));
    }

    public function test_runs_test(): void
    {
        $profile = SyncProfile::factory()->create();
        SyncRun::factory()->for($profile, 'profile')->create();
        $this->assertInstanceOf(HasMany::class, $profile->runs());
        $this->assertEquals(1, $profile->runs()->count());
    }

    public function test_schema_versions_test(): void
    {
        $profile = SyncProfile::factory()->create();
        SchemaVersion::factory()->for($profile, 'profile')->create();
        $this->assertInstanceOf(HasMany::class, $profile->schemaVersions());
        $this->assertEquals(1, $profile->schemaVersions()->count());
    }

    public function test_active_schema_version_test(): void
    {
        $profile = SyncProfile::factory()->create();
        $version = SchemaVersion::factory()->for($profile, 'profile')->create();
        $profile->update(['active_schema_version_id' => $version->id]);
        $this->assertInstanceOf(BelongsTo::class, $profile->activeSchemaVersion());
        $this->assertTrue($profile->activeSchemaVersion->is($version));
    }

    public function test_new_version_creates_first_version_correctly_test(): void
    {
        $source = ExtractSource::factory()->create();
        $profile = SyncProfile::factory()->for($source, 'source')->create();

        $this->mock(TableManager::class, function (MockInterface $mock) {
            $mock->shouldReceive('generateTableName')->andReturn('generated_table_v1');
        });

        $version = $profile->newVersion(['source_col' => 'local_col'], ['source_col' => 'string'], null, ['cfg' => 'val']);

        $this->assertInstanceOf(SchemaVersion::class, $version);
        $this->assertEquals(1, $version->version_number);
        $this->assertEquals('generated_table_v1', $version->local_table_name);
        $this->assertEquals(['source_col' => 'local_col'], $version->column_mapping);
        $this->assertEquals(['source_col' => 'string'], $version->schema_overrides);
        $this->assertEquals(['cfg' => 'val'], $version->configuration);
    }

    public function test_new_version_increments_version_and_copies_config_test(): void
    {
        $source = ExtractSource::factory()->create();
        $profile = SyncProfile::factory()->for($source, 'source')->create();
        $v1 = SchemaVersion::factory()->for($profile, 'profile')->create([
            'version_number' => 1,
            'local_table_name' => 'my_table_v1',
            'column_mapping' => ['id' => 'remote_id'],
        ]);
        $profile->activateVersion($v1);

        $v2 = $profile->newVersion();

        $this->assertEquals(2, $v2->version_number);
        $this->assertEquals('my_table_v2', $v2->local_table_name);
        $this->assertEquals($v1->column_mapping, $v2->column_mapping);
    }

    public function test_activate_version_swaps_active_version_id_and_refreshes_test(): void
    {
        $profile = SyncProfile::factory()->create();
        $v1 = SchemaVersion::factory()->for($profile, 'profile')->create(['local_table_name' => 'table_v1', 'version_number' => 1]);
        $v2 = SchemaVersion::factory()->for($profile, 'profile')->create(['local_table_name' => 'table_v2', 'version_number' => 2]);
        $profile->update(['active_schema_version_id' => $v1->id]);

        // Ensure the old table does not exist to test the guard clause in migrateSchema
        $this->assertFalse(Schema::hasTable('table_v1'));

        // Act
        $profile->activateVersion($v2);

        // Assert
        $this->assertEquals($v2->id, $profile->active_schema_version_id);
        $this->assertTrue($profile->activeSchemaVersion->is($v2));
    }
}
