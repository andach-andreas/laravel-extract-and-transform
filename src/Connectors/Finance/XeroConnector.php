<?php

namespace Andach\ExtractAndTransform\Connectors\Finance;

use Andach\ExtractAndTransform\Connectors\BaseConnector;
use Andach\ExtractAndTransform\Connectors\ConnectorConfigDefinition;
use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Data\RemoteField;
use Andach\ExtractAndTransform\Data\RemoteSchema;
use Illuminate\Support\Facades\Http;

class XeroConnector extends BaseConnector
{
    public function key(): string
    {
        return 'xero';
    }

    public function label(): string
    {
        return 'Xero';
    }

    public function getConfigDefinition(): array
    {
        return [
            new ConnectorConfigDefinition(key: 'client_id', label: 'Client ID', type: 'text', required: true),
            new ConnectorConfigDefinition(key: 'client_secret', label: 'Client Secret', type: 'password', required: true),
            new ConnectorConfigDefinition(key: 'tenant_id', label: 'Tenant ID', type: 'text', required: true),
        ];
    }

    public function test(array $config): void
    {
        // In a real implementation, we would try to get an access token or hit a test endpoint
        // For now, we assume if config is present, it's "valid" enough for this stub
        if (empty($config['client_id'])) {
            throw new \RuntimeException("Client ID is required.");
        }
    }

    public function datasets(array $config): array
    {
        return [
            new RemoteDataset('invoices', 'Invoices'),
            new RemoteDataset('contacts', 'Contacts'),
            new RemoteDataset('bank_transactions', 'Bank Transactions'),
        ];
    }

    public function streamRows(RemoteDataset $dataset, array $config, array $options = []): iterable
    {
        // This is a stub implementation. Real Xero pagination uses 'page' parameter.
        // We would use the BaseConnector's pagination logic here by implementing the hooks.
        // For simplicity in this example, we'll just yield some dummy data or use a simple loop.

        // Example of using the BaseConnector pagination hooks (if we were fully implementing it):
        return parent::streamRows($dataset, $config, $options);
    }

    protected function getInitialParameters(RemoteDataset $dataset, array $config): array
    {
        return ['page' => 1];
    }

    protected function fetchPaginatedData(array $parameters, array $config): array
    {
        // Mocking the API call
        // $response = Http::withToken(...)->get("https://api.xero.com/api.xro/2.0/{$dataset->identifier}", $parameters);
        // return $response->json();

        // Return empty to stop loop for now
        return [];
    }

    protected function extractRowsFromResponse(array $response): iterable
    {
        return $response['Invoices'] ?? [];
    }

    protected function getNextPageParameters(array $response, array $previousParameters, array $config): ?array
    {
        // If response is empty or less than page size, stop
        if (empty($response)) {
            return null;
        }
        return ['page' => $previousParameters['page'] + 1];
    }

    public function inferSchema(RemoteDataset $dataset, array $config): RemoteSchema
    {
        // Hardcoded schema for example
        if ($dataset->identifier === 'invoices') {
            return new RemoteSchema([
                new RemoteField('InvoiceID', 'guid', false, 'string'),
                new RemoteField('InvoiceNumber', 'string', false, 'string'),
                new RemoteField('Total', 'decimal', false, 'decimal:10,2'),
                new RemoteField('Date', 'datetime', false, 'date'),
            ]);
        }

        return new RemoteSchema([]);
    }
}
