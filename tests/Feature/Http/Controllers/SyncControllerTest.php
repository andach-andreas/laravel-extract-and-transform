<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Http\Controllers;

use Andach\ExtractAndTransform\Connectors\ConnectorRegistry;
use Andach\ExtractAndTransform\Connectors\General\Csv\CsvConnector;
use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Data\RemoteSchema;
use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Mockery;

class SyncControllerTest extends TestCase
{
    use RefreshDatabase;

    public function test_index_lists_datasets()
    {
        // Mock Connector
        $mockConnector = Mockery::mock(CsvConnector::class);
        $mockConnector->shouldReceive('datasets')->andReturn(new \ArrayIterator([
            new RemoteDataset('file.csv', 'File CSV'),
        ]));
        $mockConnector->shouldReceive('key')->andReturn('csv');
        $mockConnector->shouldReceive('label')->andReturn('CSV');
        $mockConnector->shouldReceive('getConfigDefinition')->andReturn([]);

        app(ConnectorRegistry::class)->register($mockConnector);

        $source = ExtractSource::create(['name' => 'Test Source', 'connector' => 'csv', 'config' => []]);

        $response = $this->get("/andach-leat/sources/{$source->id}/syncs");

        $response->assertStatus(200);
        $response->assertSee('File CSV');
    }

    public function test_configure_page_loads()
    {
        // Mock Connector
        $mockConnector = Mockery::mock(CsvConnector::class);
        $mockConnector->shouldReceive('datasets')->andReturn(new \ArrayIterator([
            new RemoteDataset('file.csv', 'File CSV'),
        ]));
        $mockConnector->shouldReceive('inferSchema')->andReturn(new RemoteSchema([]));
        $mockConnector->shouldReceive('key')->andReturn('csv');
        $mockConnector->shouldReceive('label')->andReturn('CSV');
        $mockConnector->shouldReceive('getConfigDefinition')->andReturn([]);

        app(ConnectorRegistry::class)->register($mockConnector);

        $source = ExtractSource::create(['name' => 'Test Source', 'connector' => 'csv', 'config' => []]);

        $response = $this->get("/andach-leat/sources/{$source->id}/syncs/configure?dataset=file.csv");

        $response->assertStatus(200);
        $response->assertSee('Configure Sync: file.csv');
    }

    public function test_store_sync_configuration()
    {
        // Mock Connector
        $mockConnector = Mockery::mock(CsvConnector::class);
        $mockConnector->shouldReceive('datasets')->andReturn(new \ArrayIterator([
            new RemoteDataset('file.csv', 'File CSV'),
        ]));
        $mockConnector->shouldReceive('inferSchema')->andReturn(new RemoteSchema([]));
        $mockConnector->shouldReceive('streamRows')->andReturn(new \ArrayIterator([]));
        $mockConnector->shouldReceive('key')->andReturn('csv');
        $mockConnector->shouldReceive('label')->andReturn('CSV');
        $mockConnector->shouldReceive('getConfigDefinition')->andReturn([]);

        app(ConnectorRegistry::class)->register($mockConnector);

        $source = ExtractSource::create(['name' => 'Test Source', 'connector' => 'csv', 'config' => []]);

        $response = $this->post("/andach-leat/sources/{$source->id}/syncs", [
            'dataset' => 'file.csv',
            'strategy' => 'full_refresh',
            'table_name' => 'dest_table',
            'mapping' => ['col1' => 'col1'],
        ]);

        $response->assertRedirect("/andach-leat/sources/{$source->id}/syncs");
        $this->assertDatabaseHas('andach_leat_sync_profiles', [
            'extract_source_id' => $source->id,
            'dataset_identifier' => 'file.csv',
            'strategy' => 'full_refresh',
        ]);
    }

    public function test_it_throws_error_for_invalid_dataset()
    {
        $mockConnector = Mockery::mock(CsvConnector::class);
        $mockConnector->shouldReceive('datasets')->andReturn(new \ArrayIterator([])); // No datasets
        $mockConnector->shouldReceive('key')->andReturn('csv');
        $mockConnector->shouldReceive('label')->andReturn('CSV');
        $mockConnector->shouldReceive('getConfigDefinition')->andReturn([]);
        app(ConnectorRegistry::class)->register($mockConnector);

        $source = ExtractSource::create(['name' => 'Test Source', 'connector' => 'csv', 'config' => []]);

        $response = $this->post("/andach-leat/sources/{$source->id}/syncs", [
            'dataset' => 'non_existent_dataset',
        ]);

        $response->assertRedirect();
        $response->assertSessionHas('error', "Sync failed: Dataset 'non_existent_dataset' could not be found for source 'Test Source'. Please check your configuration and ensure the dataset exists.");
    }
}
