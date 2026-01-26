<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Connectors\General\Sql;

use Andach\ExtractAndTransform\Connectors\ConnectorRegistry;
use Andach\ExtractAndTransform\Connectors\General\Sql\SqliteConnector;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Facades\Schema;

class SqliteConnectorTest extends TestCase
{
    private string $sqliteFilePath;

    protected function setUp(): void
    {
        parent::setUp();

        $this->sqliteFilePath = sys_get_temp_dir().'/test_db.sqlite';
        // Ensure the file exists and is empty before each test
        if (File::exists($this->sqliteFilePath)) {
            File::delete($this->sqliteFilePath);
        }
        File::put($this->sqliteFilePath, ''); // Create empty file

        // Use the test SQLite connection (file-based)
        $this->app['config']->set('database.default', 'sqlite_file_test');
        $this->app['config']->set('database.connections.sqlite_file_test.database', $this->sqliteFilePath);

        // Register the connector
        app(ConnectorRegistry::class)->register(new SqliteConnector);

        // Create a test table in the SQLite database
        Schema::connection('sqlite_file_test')->create('test_sqlite_table', function ($table) {
            $table->id();
            $table->string('name');
            $table->integer('age');
        });
        DB::connection('sqlite_file_test')->table('test_sqlite_table')->insert([
            ['name' => 'Alice', 'age' => 30],
            ['name' => 'Bob', 'age' => 25],
        ]);
    }

    protected function tearDown(): void
    {
        if (File::exists($this->sqliteFilePath)) {
            File::delete($this->sqliteFilePath);
        }
        parent::tearDown();
    }

    public function test_it_lists_datasets()
    {
        $connector = app(SqliteConnector::class);
        $config = ['database' => $this->sqliteFilePath];
        $datasets = $connector->datasets($config);

        $this->assertCount(1, $datasets);
        $this->assertEquals('test_sqlite_table', $datasets[0]->identifier);
    }

    public function test_it_infers_schema()
    {
        $connector = app(SqliteConnector::class);
        $config = ['database' => $this->sqliteFilePath];
        $dataset = new \Andach\ExtractAndTransform\Data\RemoteDataset('test_sqlite_table', 'test_sqlite_table');
        $schema = $connector->inferSchema($dataset, $config);

        $this->assertCount(3, $schema->fields); // id, name, age
        $this->assertEquals('name', $schema->fields[1]->name);
        $this->assertEquals('string', $schema->fields[1]->suggestedLocalType);
    }

    public function test_it_streams_rows()
    {
        $connector = app(SqliteConnector::class);
        $config = ['database' => $this->sqliteFilePath];
        $dataset = new \Andach\ExtractAndTransform\Data\RemoteDataset('test_sqlite_table', 'test_sqlite_table');
        $rows = iterator_to_array($connector->streamRows($dataset, $config));

        $this->assertCount(2, $rows);
        $this->assertEquals('Alice', $rows[0]['name']);
    }
}
