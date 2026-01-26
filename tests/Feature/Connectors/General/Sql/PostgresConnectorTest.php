<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Connectors\General\Sql;

use Andach\ExtractAndTransform\Connectors\ConnectorRegistry;
use Andach\ExtractAndTransform\Connectors\General\Sql\PostgresConnector;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class PostgresConnectorTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        // Ensure PostgreSQL connection is available
        if (!env('DB_PGSQL_HOST')) {
            $this->markTestSkipped('PostgreSQL connection not configured.');
        }

        // Use the test PostgreSQL connection
        $this->app['config']->set('database.default', 'pgsql_test');

        // Register the connector
        app(ConnectorRegistry::class)->register(new PostgresConnector());

        // Create a test table in the PostgreSQL database
        Schema::connection('pgsql_test')->dropIfExists('test_pgsql_table');
        Schema::connection('pgsql_test')->create('test_pgsql_table', function ($table) {
            $table->id();
            $table->string('name');
            $table->integer('age');
        });
        DB::connection('pgsql_test')->table('test_pgsql_table')->insert([
            ['name' => 'Alice', 'age' => 30],
            ['name' => 'Bob', 'age' => 25],
        ]);
    }

    public function test_it_lists_datasets()
    {
        $connector = app(PostgresConnector::class);
        $config = ['database' => env('DB_PGSQL_DATABASE', 'test_db')];
        $datasets = $connector->datasets($config);

        $this->assertCount(1, $datasets);
        $this->assertEquals('test_pgsql_table', $datasets[0]->identifier);
    }

    public function test_it_infers_schema()
    {
        $connector = app(PostgresConnector::class);
        $config = ['database' => env('DB_PGSQL_DATABASE', 'test_db')];
        $dataset = new \Andach\ExtractAndTransform\Data\RemoteDataset('test_pgsql_table', 'test_pgsql_table');
        $schema = $connector->inferSchema($dataset, $config);

        $this->assertCount(3, $schema->fields); // id, name, age
        $this->assertEquals('name', $schema->fields[1]->name);
        $this->assertEquals('string', $schema->fields[1]->suggestedLocalType);
    }

    public function test_it_streams_rows()
    {
        $connector = app(PostgresConnector::class);
        $config = ['database' => env('DB_PGSQL_DATABASE', 'test_db')];
        $dataset = new \Andach\ExtractAndTransform\Data\RemoteDataset('test_pgsql_table', 'test_pgsql_table');
        $rows = iterator_to_array($connector->streamRows($dataset, $config));

        $this->assertCount(2, $rows);
        $this->assertEquals('Alice', $rows[0]['name']);
    }
}
