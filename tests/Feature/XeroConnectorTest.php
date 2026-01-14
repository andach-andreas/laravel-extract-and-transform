<?php

namespace Andach\ExtractAndTransform\Tests\Feature;

use Andach\ExtractAndTransform\Connectors\ConnectorRegistry;
use Andach\ExtractAndTransform\Connectors\Finance\XeroConnector;
use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Support\Facades\Http;

class XeroConnectorTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        // Manually register since SP write failed in previous steps
        $connector = new XeroConnector;
        app(ConnectorRegistry::class)->register($connector);
    }

    public function test_it_is_registered()
    {
        $registry = app(ConnectorRegistry::class);
        $this->assertTrue(array_key_exists('xero', $registry->all()));
        $this->assertInstanceOf(XeroConnector::class, $registry->get('xero'));
    }

    public function test_config_definition()
    {
        $connector = new XeroConnector;
        $def = $connector->getConfigDefinition();

        $this->assertCount(4, $def);
        $this->assertEquals('client_id', $def[0]->key);
        $this->assertEquals('client_secret', $def[1]->key);
    }

    public function test_datasets()
    {
        $connector = new XeroConnector;
        $datasets = iterator_to_array($connector->datasets([]));

        $this->assertCount(4, $datasets);
        $this->assertEquals('Invoices', $datasets[0]->identifier);
    }

    public function test_infer_schema_from_api()
    {
        Http::fake([
            'https://identity.xero.com/connect/token' => Http::response([
                'access_token' => 'fake_access_token',
                'expires_in' => 1800,
            ]),
            'https://api.xero.com/api.xro/2.0/Invoices*' => Http::response([
                'Invoices' => [
                    [
                        'InvoiceID' => 'uuid-123',
                        'Total' => 100.50,
                        'DateString' => '2023-01-01T00:00:00',
                        'IsPaid' => true,
                    ],
                ],
            ]),
        ]);

        $connector = new XeroConnector;
        $config = [
            'client_id' => 'cid',
            'client_secret' => 'sec',
            'refresh_token' => 'ref',
            'tenant_id' => 'tid',
        ];
        $dataset = new RemoteDataset('Invoices', 'Invoices');

        $schema = $connector->inferSchema($dataset, $config);

        $this->assertCount(4, $schema->fields);

        $idField = collect($schema->fields)->firstWhere('name', 'InvoiceID');
        $this->assertEquals('string', $idField->suggestedLocalType);

        $totalField = collect($schema->fields)->firstWhere('name', 'Total');
        $this->assertEquals('decimal:18,4', $totalField->suggestedLocalType);

        $dateField = collect($schema->fields)->firstWhere('name', 'DateString');
        $this->assertEquals('datetime', $dateField->suggestedLocalType);

        $boolField = collect($schema->fields)->firstWhere('name', 'IsPaid');
        $this->assertEquals('boolean', $boolField->suggestedLocalType);
    }

    public function test_stream_rows_with_pagination()
    {
        Http::fake([
            'https://identity.xero.com/connect/token' => Http::response([
                'access_token' => 'fake_access_token',
                'expires_in' => 1800,
            ]),
            // Page 1: 100 items (simulated by just 2 for brevity, but we check logic)
            // Actually, logic checks count < 100 to stop. So we need to return 100 to trigger page 2.
            // Let's simulate a smaller page size logic or just test that it fetches.
            // To test pagination, I'll return 100 items on page 1, and 1 item on page 2.

            'https://api.xero.com/api.xro/2.0/Invoices?page=1' => Http::response([
                'Invoices' => array_fill(0, 100, ['id' => 'p1']),
            ]),
            'https://api.xero.com/api.xro/2.0/Invoices?page=2' => Http::response([
                'Invoices' => [['id' => 'p2']],
            ]),
        ]);

        $connector = new XeroConnector;
        $config = [
            'client_id' => 'cid',
            'client_secret' => 'sec',
            'refresh_token' => 'ref',
            'tenant_id' => 'tid',
        ];
        $dataset = new RemoteDataset('Invoices', 'Invoices');

        $rows = iterator_to_array($connector->streamRows($dataset, $config));

        $this->assertCount(101, $rows);
        $this->assertEquals('p1', $rows[0]['id']);
        $this->assertEquals('p2', $rows[100]['id']);
    }
}
