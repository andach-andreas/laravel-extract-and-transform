<?php

namespace Andach\ExtractAndTransform\Connectors\General\Sql;

use Andach\ExtractAndTransform\Connectors\BaseConnector;
use Andach\ExtractAndTransform\Connectors\ConnectorConfigDefinition;
use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Data\RemoteField;
use Andach\ExtractAndTransform\Data\RemoteSchema;
use Andach\ExtractAndTransform\Services\RetryService;
use Illuminate\Support\Facades\Config;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

abstract class AbstractSqlConnector extends BaseConnector
{
    protected RetryService $retryService;

    public function __construct()
    {
        $this->retryService = app(RetryService::class);
    }

    abstract public function key(): string;

    abstract public function label(): string;

    public function getConfigDefinition(): array
    {
        return [
            new ConnectorConfigDefinition(key: 'connection', label: 'Connection Name', type: 'text', required: false, help: 'An existing Laravel database connection name from config/database.php.'),
            new ConnectorConfigDefinition(key: 'host', label: 'Host', type: 'text', required: false),
            new ConnectorConfigDefinition(key: 'port', label: 'Port', type: 'text', required: false),
            new ConnectorConfigDefinition(key: 'database', label: 'Database', type: 'text', required: true),
            new ConnectorConfigDefinition(key: 'username', label: 'Username', type: 'text', required: false),
            new ConnectorConfigDefinition(key: 'password', type: 'password', label: 'Password', required: false),
            new ConnectorConfigDefinition(key: 'primary_key', label: 'Primary Key (for chunking)', type: 'text', required: false, help: 'Defaults to "id". Used for efficient chunking of large datasets.'),
            new ConnectorConfigDefinition(key: 'chunk_size', label: 'Chunk Size', type: 'number', required: false, help: 'Number of rows to fetch per query. Default: 10000.'),
        ];
    }

    public function test(array $config): void
    {
        Log::info("[SQL Connector] Testing connection for {$this->key()}...");
        $startTime = microtime(true);

        $this->retryService->run(function () use ($config) {
            $conn = $this->resolveConnectionName($config);
            DB::connection($conn)->select('select 1');
        });

        Log::info("[SQL Connector] Connection test for {$this->key()} completed in " . round(microtime(true) - $startTime, 3) . "s.");
    }

    public function datasets(array $config): array
    {
        throw new \BadMethodCallException('The datasets method must be implemented by the concrete SQL connector.');
    }

    public function streamRows(RemoteDataset $dataset, array $config): iterable
    {
        $conn = $this->resolveConnectionName($config);
        $table = $dataset->identifier;

        $primaryKey = $config['primary_key'] ?? 'id';
        $chunkSize = (int) ($config['chunk_size'] ?? 10000);

        // Check if the primary key exists to use efficient keyset pagination
        $hasPrimaryKey = DB::connection($conn)->getSchemaBuilder()->hasColumn($table, $primaryKey);

        if ($hasPrimaryKey) {
            Log::info("[SQL Connector] Streaming rows from '{$table}' using keyset pagination on '{$primaryKey}' (Chunk: {$chunkSize}).");

            $lastId = null;

            while (true) {
                $query = DB::connection($conn)->table($table)
                    ->orderBy($primaryKey)
                    ->limit($chunkSize);

                if ($lastId !== null) {
                    $query->where($primaryKey, '>', $lastId);
                }

                $rows = $query->get();

                if ($rows->isEmpty()) {
                    break;
                }

                foreach ($rows as $row) {
                    $rowArray = (array) $row;
                    $lastId = $rowArray[$primaryKey]; // Update lastId for next iteration
                    yield $rowArray;
                }

                // If we fetched fewer than chunk size, we are done
                if ($rows->count() < $chunkSize) {
                    break;
                }

                // Optional: Clear memory if needed, though 'yield' helps.
                unset($rows);
            }
        } else {
            Log::warning("[SQL Connector] Primary key '{$primaryKey}' not found in '{$table}'. Falling back to standard cursor (slower for large datasets).");
            foreach (DB::connection($conn)->table($table)->cursor() as $row) {
                yield (array) $row;
            }
        }
    }

    public function inferSchema(RemoteDataset $dataset, array $config): RemoteSchema
    {
        throw new \BadMethodCallException('The inferSchema method must be implemented by the concrete SQL connector.');
    }

    public function streamRowsWithCheckpoint(RemoteDataset $dataset, array $config, ?array $checkpoint, array $options = []): \Generator
    {
        $strategy = (string) ($options['strategy'] ?? 'full_refresh');

        return match ($strategy) {
            'time_watermark' => $this->streamByWatermark($dataset, $config, $checkpoint, $options),
            default => $this->streamFullRefresh($dataset, $config),
        };
    }

    public function listIdentities(RemoteDataset $dataset, array $config, array $identityColumns): iterable
    {
        $conn = $this->resolveConnectionName($config);
        $table = $dataset->identifier;
        if ($identityColumns === []) {
            throw new \InvalidArgumentException('identityColumns cannot be empty.');
        }
        $q = DB::connection($conn)->table($table)->select($identityColumns);
        foreach ($q->cursor() as $rowObj) {
            $row = (array) $rowObj;
            $parts = [];
            foreach ($identityColumns as $c) {
                $parts[$c] = $row[$c] ?? null;
            }
            yield $this->identityFromParts($parts);
        }
    }

    protected function streamFullRefresh(RemoteDataset $dataset, array $config): \Generator
    {
        // Reuse the efficient streamRows implementation
        foreach ($this->streamRows($dataset, $config) as $row) {
            yield $row;
        }
    }

    protected function streamByWatermark(RemoteDataset $dataset, array $config, ?array $checkpoint, array $options): \Generator
    {
        $conn = $this->resolveConnectionName($config);
        $table = $dataset->identifier;
        $watermarkCol = (string) ($options['watermark'] ?? '');
        if ($watermarkCol === '') {
            throw new \InvalidArgumentException('SQL time_watermark requires option watermark.');
        }
        $tieBreaker = (string) ($options['tie_breaker'] ?? 'id');
        $limit = isset($options['page_size']) ? (int) $options['page_size'] : 5000;
        $lastTs = $checkpoint['last']['ts'] ?? null;
        $lastTie = $checkpoint['last']['tie'] ?? null;
        $q = DB::connection($conn)->table($table);
        if ($lastTs !== null) {
            $q->where(function ($w) use ($watermarkCol, $tieBreaker, $lastTs, $lastTie) {
                $w->where($watermarkCol, '>', $lastTs);
                if ($lastTie !== null) {
                    $w->orWhere(function ($w2) use ($watermarkCol, $tieBreaker, $lastTs, $lastTie) {
                        $w2->where($watermarkCol, '=', $lastTs)->where($tieBreaker, '>', $lastTie);
                    });
                }
            });
        }
        $q->orderBy($watermarkCol)->orderBy($tieBreaker)->limit($limit);
        $nextTs = $lastTs;
        $nextTie = $lastTie;
        $yielded = 0;
        foreach ($q->cursor() as $rowObj) {
            $row = (array) $rowObj;
            $yielded++;
            $nextTs = $row[$watermarkCol] ?? $nextTs;
            $nextTie = $row[$tieBreaker] ?? $nextTie;
            yield $row;
        }
        if ($yielded === 0) {
            return $checkpoint;
        }

        return ['kind' => 'sql_watermark', 'table' => $table, 'watermark' => $watermarkCol, 'tie_breaker' => $tieBreaker, 'last' => ['ts' => $nextTs, 'tie' => $nextTie]];
    }

    protected function resolveConnectionName(array $config): string
    {
        $startTime = microtime(true);
        $explicit = $config['connection'] ?? null;
        if (is_string($explicit) && $explicit !== '') {
            Log::info("[SQL Connector] Resolving connection: Using explicit connection '{$explicit}'.");
            return $explicit;
        }
        $driver = $this->key();
        if (! is_string($driver) || $driver === '') {
            throw new \InvalidArgumentException('SQL source config must include either {"connection": "..."} or DSN keys.');
        }

        ksort($config);
        $name = 'extract_sql_'.substr(hash('sha256', json_encode($config) ?: ''), 0, 16);

        if (! Config::has("database.connections.{$name}")) {
            Config::set("database.connections.{$name}", $this->toLaravelConnectionConfig($config));
            Log::info("[SQL Connector] Configured new dynamic connection '{$name}' for driver '{$driver}'.");
        } else {
            Log::info("[SQL Connector] Reusing existing dynamic connection config for '{$name}'.");
        }

        Log::info("[SQL Connector] Resolved connection '{$name}' in " . round(microtime(true) - $startTime, 3) . "s.");
        return $name;
    }

    protected function toLaravelConnectionConfig(array $cfg): array
    {
        $driver = $this->key();
        if ($driver === 'sqlite') {
            return ['driver' => 'sqlite', 'database' => $cfg['database'] ?? ':memory:', 'prefix' => ''];
        }
        $base = ['driver' => $driver, 'host' => (string) ($cfg['host'] ?? '127.0.0.1'), 'port' => (int) ($cfg['port'] ?? ($driver === 'pgsql' ? 5432 : 3306)), 'database' => (string) ($cfg['database'] ?? ''), 'username' => (string) ($cfg['username'] ?? ''), 'password' => (string) ($cfg['password'] ?? ''), 'charset' => 'utf8mb4', 'collation' => 'utf8mb4_unicode_ci', 'prefix' => '', 'strict' => true];
        if ($driver === 'pgsql') {
            $base['schema'] = (string) ($cfg['schema'] ?? 'public');
        }

        return $base;
    }

    protected function suggestLocalType(string $remoteType): ?string
    {
        $t = strtolower($remoteType);

        return match (true) {
            str_contains($t, 'int') => 'int',
            str_contains($t, 'bool') => 'bool',
            str_contains($t, 'decimal') || str_contains($t, 'numeric') => 'decimal:18,6',
            str_contains($t, 'float') || str_contains($t, 'double') => 'float',
            str_contains($t, 'date') && ! str_contains($t, 'time') => 'date',
            str_contains($t, 'time') || str_contains($t, 'timestamp') => 'datetime',
            str_contains($t, 'json') => 'json',
            str_contains($t, 'text') => 'text',
            default => 'string',
        };
    }

    protected function identityFromParts(array $parts): string
    {
        if (count($parts) === 1) {
            $v = array_values($parts)[0];

            return (is_scalar($v) || $v === null) ? (string) $v : (json_encode($v) ?: '');
        }
        ksort($parts);
        $json = json_encode($parts, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE) ?: '';

        return hash('sha256', $json);
    }
}
