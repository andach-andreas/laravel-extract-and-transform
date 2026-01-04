<?php

namespace Andach\ExtractAndTransform\Tests\Unit\Connectors\HubSpot;

use Andach\ExtractAndTransform\Connectors\HubSpot\HubSpotConnector;
use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Support\Facades\Http;

class HubSpotConnectorTest extends TestCase
{
    private HubSpotConnector $connector;

    protected function setUp(): void
    {
        parent::setUp();
        $this->connector = new HubSpotConnector;
    }

    public function test_stream_rows_handles_pagination_test(): void
    {
        // Arrange: Fake two pages of API responses
        Http::fake([
            'api.hubapi.com/crm/v3/objects/contacts?limit=100' => Http::response([
                'results' => [['id' => 1], ['id' => 2]],
                'paging' => ['next' => ['after' => 'cursor123']],
            ]),
            'api.hubapi.com/crm/v3/objects/contacts?limit=100&after=cursor123' => Http::response([
                'results' => [['id' => 3]],
                // No 'paging' key on the last page
            ]),
        ]);

        $dataset = new RemoteDataset('contacts', 'Contacts');
        $config = ['api_key' => 'test-key'];

        // Act: Stream the rows
        $rows = iterator_to_array($this->connector->streamRows($dataset, $config));

        // Assert: Check that all rows from both pages were yielded
        $this->assertCount(3, $rows);
        $this->assertEquals([['id' => 1], ['id' => 2], ['id' => 3]], $rows);

        // Assert that both API endpoints were called
        Http::assertSentCount(2);
    }

    public function test_get_config_definition_returns_correct_fields_test(): void
    {
        $definitions = $this->connector->getConfigDefinition();
        $this->assertCount(1, $definitions);
        $this->assertEquals('api_key', $definitions[0]->key);
        $this->assertEquals('password', $definitions[0]->type);
    }
}
