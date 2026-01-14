<?php

namespace Andach\ExtractAndTransform\Connectors\Finance;

use Andach\ExtractAndTransform\Connectors\BaseConnector;
use Andach\ExtractAndTransform\Connectors\ConnectorConfigDefinition;
use Andach\ExtractAndTransform\Connectors\Contracts\CanInferSchema;
use Andach\ExtractAndTransform\Connectors\Contracts\CanStreamRows;
use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Data\RemoteField;
use Andach\ExtractAndTransform\Data\RemoteSchema;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Http;

class XeroConnector extends BaseConnector implements CanInferSchema, CanStreamRows
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
            new ConnectorConfigDefinition(key: 'client_id', label: 'Client ID', required: true),
            new ConnectorConfigDefinition(key: 'client_secret', label: 'Client Secret', required: true),
            new ConnectorConfigDefinition(key: 'refresh_token', label: 'Refresh Token', required: true),
            new ConnectorConfigDefinition(key: 'tenant_id', label: 'Tenant ID', required: true),
        ];
    }

    public function test(array $config): void
    {
        $this->getAccessToken($config);
    }

    public function datasets(array $config): iterable
    {
        yield new RemoteDataset(identifier: 'Invoices', label: 'Invoices');
        yield new RemoteDataset(identifier: 'Contacts', label: 'Contacts');
        yield new RemoteDataset(identifier: 'BankTransactions', label: 'Bank Transactions');
        yield new RemoteDataset(identifier: 'Accounts', label: 'Accounts');
    }

    public function inferSchema(RemoteDataset $dataset, array $config): RemoteSchema
    {
        $accessToken = $this->getAccessToken($config);
        $tenantId = $config['tenant_id'];

        $response = Http::withToken($accessToken)
            ->withHeaders(['Xero-Tenant-Id' => $tenantId, 'Accept' => 'application/json'])
            ->get("https://api.xero.com/api.xro/2.0/{$dataset->identifier}", ['page' => 1]);

        if ($response->failed()) {
            throw new \RuntimeException('Failed to fetch data from Xero to infer schema: '.$response->body());
        }

        $data = $response->json();
        $firstRecord = $data[$dataset->identifier][0] ?? null;

        if (! $firstRecord) {
            throw new \RuntimeException("No records found in Xero dataset '{$dataset->identifier}' to infer schema from.");
        }

        $fields = [];
        foreach ($firstRecord as $key => $value) {
            $fields[] = new RemoteField(
                name: $key,
                remoteType: gettype($value),
                suggestedLocalType: $this->guessLocalType($value),
                nullable: true
            );
        }

        return new RemoteSchema($fields);
    }

    public function streamRows(RemoteDataset $dataset, array $config): iterable
    {
        $tenantId = $config['tenant_id'];
        $page = 1;
        $hasMore = true;

        while ($hasMore) {
            // Get token inside the loop in case it expires during a long sync
            $accessToken = $this->getAccessToken($config);

            $response = Http::withToken($accessToken)
                ->withHeaders(['Xero-Tenant-Id' => $tenantId, 'Accept' => 'application/json'])
                ->get("https://api.xero.com/api.xro/2.0/{$dataset->identifier}", ['page' => $page]);

            if ($response->failed()) {
                throw new \RuntimeException("Failed to fetch data from Xero (Page {$page}): ".$response->body());
            }

            $data = $response->json();
            $records = $data[$dataset->identifier] ?? [];

            if (empty($records)) {
                $hasMore = false;
                break;
            }

            foreach ($records as $record) {
                // Flatten complex objects to JSON strings if needed, or let the transformer handle it.
                // For now, we yield the raw array. The RowTransformer service handles array-to-json conversion.
                yield $record;
            }

            // Xero returns 100 records per page. If less, we are done.
            if (count($records) < 100) {
                $hasMore = false;
            } else {
                $page++;
            }
        }
    }

    private function guessLocalType(mixed $value): string
    {
        if (is_array($value)) {
            return 'json';
        }
        if (is_bool($value)) {
            return 'boolean';
        }
        if (is_int($value)) {
            return 'integer';
        }
        if (is_float($value) || is_numeric($value)) {
            return 'decimal:18,4';
        }
        if (preg_match('/^\/Date\(\d+.*\)\/$/', (string) $value) || preg_match('/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/', (string) $value)) {
            return 'datetime';
        }

        return 'string';
    }

    private function getAccessToken(array &$config): string
    {
        $cacheKey = 'xero_access_token_'.md5(json_encode($config));

        if (Cache::has($cacheKey)) {
            return Cache::get($cacheKey);
        }

        $response = Http::withBasicAuth($config['client_id'], $config['client_secret'])
            ->asForm()
            ->post('https://identity.xero.com/connect/token', [
                'grant_type' => 'refresh_token',
                'refresh_token' => $config['refresh_token'],
            ]);

        if ($response->failed()) {
            throw new \RuntimeException('Xero token refresh failed: '.$response->body());
        }

        $data = $response->json();
        $accessToken = $data['access_token'];
        $expiresIn = $data['expires_in'];

        Cache::put($cacheKey, $accessToken, $expiresIn - 30);

        if (isset($data['refresh_token']) && isset($config['__updater']) && is_callable($config['__updater'])) {
            $newConfig = $config;
            $newConfig['refresh_token'] = $data['refresh_token'];
            unset($newConfig['__updater']);

            call_user_func($config['__updater'], $newConfig);

            $config['refresh_token'] = $data['refresh_token'];
        }

        return $accessToken;
    }
}
