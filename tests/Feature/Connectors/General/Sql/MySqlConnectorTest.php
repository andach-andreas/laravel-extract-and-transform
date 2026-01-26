<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Connectors\General\Sql;

use Andach\ExtractAndTransform\Connectors\ConnectorRegistry;
use Andach\ExtractAndTransform\Connectors\General\Sql\MySqlConnector;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class MySqlConnectorTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        // Ensure MySQL connection is available
        if (! env('DB_MYSQL_HOST')) {
            $this->markTestSkipped('MySQL connection not configured.');
        }

        // Use the test MySQL connection
        $this->app['config']->set('database.default', 'mysql_test');

        // Register the connector
        app(ConnectorRegistry::class)->register(new MySqlConnector);

        // Create a test table in the MySQL database
        Schema::connection('mysql_test')->dropIfExists('test_mysql_table');
        Schema::connection('mysql_test')->create('test_mysql_table', function ($table) {
            $table->id();
            $table->string('name');
            $table->integer('age');
        });
        DB::connection('mysql_test')->table('test_mysql_table')->insert([
            ['name' => 'Alice', 'age' => 30],
            ['name' => 'Bob', 'age' => 25],
        ]);
    }

    public function test_it_lists_datasets()
    {
        $connector = app(MySqlConnector::class);
        $config = ['database' => env('DB_MYSQL_DATABASE', 'test_db')];
        $datasets = $connector->datasets($config);

        $this->assertCount(1, $datasets);
        $this->assertEquals('test_mysql_table', $datasets[0]->identifier);
    }

    public function test_it_infers_schema()
    {
        $connector = app(MySqlConnector::class);
        $config = ['database' => env('DB_MYSQL_DATABASE', 'test_db')];
        $dataset = new \Andach\ExtractAndTransform\Data\RemoteDataset('test_mysql_table', 'test_mysql_table');
        $schema = $connector->inferSchema($dataset, $config);

        $this->assertCount(3, $schema->fields); // id, name, age
        $this->assertEquals('name', $schema->fields[1]->name);
        $this->assertEquals('string', $schema->fields[1]->suggestedLocalType);
    }

    public function test_it_streams_rows()
    {
        $connector = app(MySqlConnector::class);
        $config = ['database' => env('DB_MYSQL_DATABASE', 'test_db')];
        $dataset = new \Andach\ExtractAndTransform\Data\RemoteDataset('test_mysql_table', 'test_mysql_table');
        $rows = iterator_to_array($connector->streamRows($dataset, $config));

        $this->assertCount(2, $rows);
        $this->assertEquals('Alice', $rows[0]['name']);
    }
}
