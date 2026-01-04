<?php

namespace Andach\ExtractAndTransform\Connectors\HubSpot;

use Andach\ExtractAndTransform\Connectors\BaseConnector;
use Andach\ExtractAndTransform\Connectors\ConnectorConfigDefinition;
use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Services\RetryService;
use Illuminate\Http\Client\PendingRequest;
use Illuminate\Support\Facades\Http;

final class HubSpotConnector extends BaseConnector
{
    private RetryService $retryService;

    private string $baseUrl = 'https://api.hubapi.com';

    public function __construct()
    {
        $this->retryService = app(RetryService::class);
    }

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
            new ConnectorConfigDefinition(key: 'api_key', label: 'Private App Token', type: 'password', required: true),
        ];
    }

    public function test(array $config): void
    {
        $this->httpClient($config)->get('/crm/v3/objects/contacts', ['limit' => 1]);
    }

    public function datasets(array $config): iterable
    {
        yield new RemoteDataset('contacts', 'Contacts');
        yield new RemoteDataset('companies', 'Companies');
        yield new RemoteDataset('deals', 'Deals');
    }

    protected function getInitialParameters(RemoteDataset $dataset, array $config): array
    {
        return [
            'endpoint' => "/crm/v3/objects/{$dataset->identifier}",
            'query' => ['limit' => 100], // HubSpot's max limit
        ];
    }

    protected function fetchPaginatedData(array $parameters, array $config): array
    {
        return $this->retryService->run(
            fn () => $this->httpClient($config)->get($parameters['endpoint'], $parameters['query'])->json()
        );
    }

    protected function extractRowsFromResponse(array $response): iterable
    {
        return $response['results'] ?? [];
    }

    protected function getNextPageParameters(array $response, array $previousParameters, array $config): ?array
    {
        $nextCursor = $response['paging']['next']['after'] ?? null;

        if ($nextCursor) {
            $previousParameters['query']['after'] = $nextCursor;

            return $previousParameters;
        }

        return null;
    }

    private function httpClient(array $config): PendingRequest
    {
        return Http::baseUrl($this->baseUrl)
            ->withToken($config['api_key'])
            ->throw();
    }
}
