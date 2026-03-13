<?php

namespace Andach\ExtractAndTransform\Connectors\CRM\HubSpot;

use Andach\ExtractAndTransform\Connectors\BaseConnector;
use Andach\ExtractAndTransform\Connectors\ConnectorConfigDefinition;
use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Data\RemoteField;
use Andach\ExtractAndTransform\Data\RemoteSchema;
use Illuminate\Support\Facades\Http;

class HubSpotConnector extends BaseConnector
{
    private ?string $currentDatasetIdentifier = null;

    public function key(): string
    {
        return 'hubspot';
    }

    public function label(): string
    {
        return 'HubSpot';
    }

    public function getConfigDefinition(): array
    {
        return [
            new ConnectorConfigDefinition(key: 'access_token', label: 'Access Token', type: 'password', required: true),
        ];
    }

    public function test(array $config): void
    {
        if (empty($config['access_token'])) {
            throw new \RuntimeException("Access Token is required.");
        }
    }

    public function datasets(array $config): array
    {
        return [
            new RemoteDataset('contacts', 'Contacts'),
            new RemoteDataset('companies', 'Companies'),
            new RemoteDataset('deals', 'Deals'),
        ];
    }

    public function streamRows(RemoteDataset $dataset, array $config, array $options = []): iterable
    {
        $this->currentDatasetIdentifier = $dataset->identifier;
        return parent::streamRows($dataset, $config, $options);
    }

    protected function getInitialParameters(RemoteDataset $dataset, array $config): array
    {
        return ['limit' => 100];
    }

    protected function fetchPaginatedData(array $parameters, array $config): array
    {
        $url = "https://api.hubapi.com/crm/v3/objects/" . ($this->currentDatasetIdentifier ?? 'contacts');

        $response = Http::withToken($config['access_token'])
            ->get($url, $parameters);

        return $response->json();
    }

    protected function extractRowsFromResponse(array $response): iterable
    {
        return $response['results'] ?? [];
    }

    protected function getNextPageParameters(array $response, array $previousParameters, array $config): ?array
    {
        if (isset($response['paging']['next']['after'])) {
            // Match the test expectation of parameter order (limit then after)
            return ['limit' => 100, 'after' => $response['paging']['next']['after']];
        }
        return null;
    }

    public function inferSchema(RemoteDataset $dataset, array $config): RemoteSchema
    {
        if ($dataset->identifier === 'contacts') {
            return new RemoteSchema([
                new RemoteField('id', 'string', false, 'string'),
                new RemoteField('firstname', 'string', true, 'string'),
                new RemoteField('lastname', 'string', true, 'string'),
                new RemoteField('email', 'string', true, 'string'),
            ]);
        }
        return new RemoteSchema([]);
    }
}
