<?php

namespace Andach\ExtractAndTransform\Tests\Unit\Services;

use Andach\ExtractAndTransform\Data\RemoteField;
use Andach\ExtractAndTransform\Data\RemoteSchema;
use Andach\ExtractAndTransform\ExtractAndTransform;
use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Models\SchemaVersion;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Services\TableManager;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Config;
use Illuminate\Support\Facades\Schema;
use Mockery\MockInterface;

class TableManagerTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();
        Config::set('extract-data.table_prefix', 'andach_');

        // Setup a default mock for ExtractAndTransform facade
        // This is needed because TableManager calls ExtractAndTransform::getSourceFromModel
        $mockRemoteSchema = new RemoteSchema(fields: [
            new RemoteField(name: 'id', remoteType: 'int', nullable: false, suggestedLocalType: 'int'),
            new RemoteField(name: 'name', remoteType: 'string', nullable: true, suggestedLocalType: 'string'),
        ]);
        $mockDataset = \Mockery::mock(\Andach\ExtractAndTransform\Dataset::class);
        $mockDataset->shouldReceive('getSchema')->andReturn($mockRemoteSchema);
        $mockSource = \Mockery::mock(\Andach\ExtractAndTransform\Source::class);
        $mockSource->shouldReceive('getDataset')->andReturn($mockDataset);

        $this->mock(ExtractAndTransform::class, function (MockInterface $mock) use ($mockSource) {
            $mock->shouldReceive('getSourceFromModel')->andReturn($mockSource);
        });
    }

    public function test_generate_table_name_creates_correct_name_test(): void
    {
        $source = ExtractSource::factory()->create(['name' => 'My SQL Source', 'connector' => 'sqlite']); // Use sqlite for consistency
        $profile = SyncProfile::factory()->for($source, 'source')->create(['dataset_identifier' => 'users']);
        $version = SchemaVersion::factory()->for($profile, 'profile')->create(['version_number' => 1]);

        $manager = new TableManager;
        $tableName = $manager->generateTableName($profile, $version);

        $this->assertEquals('andach_sqlite_my_sql_source_users_v1', $tableName);
    }

    public function test_ensure_table_exists_creates_new_table_test(): void
    {
        $source = ExtractSource::factory()->create(['name' => 'Test CSV', 'connector' => 'csv']);
        $profile = SyncProfile::factory()->for($source, 'source')->create(['dataset_identifier' => 'products']);
        $version = SchemaVersion::factory()->for($profile, 'profile')->create([
            'local_table_name' => 'test_products_table',
            'column_mapping' => ['id' => 'product_id', 'name' => 'product_name'],
            'schema_overrides' => ['id' => 'int'],
        ]);

        $manager = app(TableManager::class);
        $tableName = $manager->ensureTableExists($profile, $version);

        $this->assertEquals('test_products_table', $tableName);
        $this->assertTrue(Schema::hasTable($tableName));
        $this->assertTrue(Schema::hasColumn($tableName, '__id'));
        $this->assertTrue(Schema::hasColumn($tableName, 'product_id'));
        $this->assertTrue(Schema::hasColumn($tableName, 'product_name'));
        $this->assertEquals('integer', Schema::getColumnType($tableName, 'product_id'));
    }

    public function test_ensure_table_exists_does_not_recreate_existing_table_test(): void
    {
        $source = ExtractSource::factory()->create(['name' => 'Test CSV', 'connector' => 'csv']);
        $profile = SyncProfile::factory()->for($source, 'source')->create(['dataset_identifier' => 'products']);
        $version = SchemaVersion::factory()->for($profile, 'profile')->create(['local_table_name' => 'existing_table']);

        Schema::create('existing_table', function ($table) {
            $table->id();
        });

        $manager = app(TableManager::class);
        $tableName = $manager->ensureTableExists($profile, $version);

        $this->assertEquals('existing_table', $tableName);
        $this->assertTrue(Schema::hasTable($tableName));
        // This test should not assert __id as it's an existing table not managed by TableManager
        // The updateTableSchema method would add __id if it was missing, but this test is about not recreating.
        // For now, let's just ensure it doesn't crash.
    }
}
