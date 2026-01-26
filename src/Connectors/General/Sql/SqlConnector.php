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

class SqlConnector extends BaseConnector
{
    private RetryService $retryService;

    public function __construct()
    {
        $this->retryService = app(RetryService::class);
    }

    public function key(): string
    {
        return 'sql';
    }

    public function label(): string
    {
        return 'SQL';
    }

    public function getConfigDefinition(): array
    {
        return [
            new ConnectorConfigDefinition(key: 'connection', label: 'Connection Name', type: 'text', required: false, help: 'An existing Laravel database connection name from config/database.php.'),
            new ConnectorConfigDefinition(key: 'driver', label: 'Driver', type: 'text', required: false, help: 'e.g., mysql, pgsql'),
            new ConnectorConfigDefinition(key: 'host', label: 'Host', type: 'text', required: false),
            new ConnectorConfigDefinition(key: 'port', label: 'Port', type: 'text', required: false),
            new ConnectorConfigDefinition(key: 'database', label: 'Database', type: 'text', required: true),
            new ConnectorConfigDefinition(key: 'username', label: 'Username', type: 'text', required: false),
            new ConnectorConfigDefinition(key: 'password', label: 'Password', type: 'password', required: false),
        ];
    }

    public function test(array $config): void
    {
        $this->retryService->run(function () use ($config) {
            $conn = $this->resolveConnectionName($config);
            DB::connection($conn)->reconnect();
            DB::connection($conn)->select('select 1');
        });
    }

    public function datasets(array $config): array
    {
        $conn = $this->resolveConnectionName($config);
        $connection = DB::connection($conn);
        $driver = $connection->getDriverName();
        $database = $config['database'] ?? $connection->getDatabaseName();

        $out = [];

        if ($driver === 'mysql') {
            // Use SHOW TABLES for performance. It's typically faster than querying information_schema.
            $tables = $connection->select("SHOW TABLES FROM `{$database}`");
            // The result key is dynamic, e.g., "Tables_in_your_db". We get the first value of the object.
            foreach ($tables as $table) {
                $tableArray = (array) $table;
                $name = reset($tableArray); // Get the first value
                $out[] = new RemoteDataset(identifier: $name, label: $name, meta: []);
            }
        } else {
            // Fallback for other drivers
            $tables = $connection->getSchemaBuilder()->getTables();
            foreach ($tables as $t) {
                $t = (array) $t;
                $name = (string) ($t['name'] ?? ($t['table_name'] ?? ''));
                if ($name === '') {
                    continue;
                }
                $out[] = new RemoteDataset(identifier: $name, label: $name, meta: []);
            }
        }

        return $out;
    }

    public function streamRows(RemoteDataset $dataset, array $config): iterable
    {
        $conn = $this->resolveConnectionName($config);
        $table = $dataset->identifier;
        foreach (DB::connection($conn)->table($table)->cursor() as $row) {
            yield (array) $row;
        }
    }

    public function inferSchema(RemoteDataset $dataset, array $config): RemoteSchema
    {
        $conn = $this->resolveConnectionName($config);
        $table = $dataset->identifier;
        $columns = DB::connection($conn)->getSchemaBuilder()->getColumns($table);
        $fields = [];
        foreach ($columns as $c) {
            $c = (array) $c;
            $name = (string) ($c['name'] ?? '');
            $type = (string) ($c['type_name'] ?? ($c['type'] ?? ''));
            $nullable = (bool) ($c['nullable'] ?? true);
            $fields[] = new RemoteField(name: $name, remoteType: $type !== '' ? $type : null, nullable: $nullable, suggestedLocalType: $this->suggestLocalType($type));
        }

        return new RemoteSchema($fields);
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

    private function streamFullRefresh(RemoteDataset $dataset, array $config): \Generator
    {
        $conn = $this->resolveConnectionName($config);
        $table = $dataset->identifier;
        foreach (DB::connection($conn)->table($table)->cursor() as $row) {
            yield (array) $row;
        }

        return null;
    }

    private function streamByWatermark(RemoteDataset $dataset, array $config, ?array $checkpoint, array $options): \Generator
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

    private function resolveConnectionName(array $config): string
    {
        $explicit = $config['connection'] ?? null;
        if (is_string($explicit) && $explicit !== '') {
            return $explicit;
        }
        $driver = $config['driver'] ?? null;
        if (! is_string($driver) || $driver === '') {
            throw new \InvalidArgumentException('SQL source config must include either {"connection": "..."} or DSN keys.');
        }
        $name = 'extract_sql_'.substr(hash('sha256', json_encode($config) ?: ''), 0, 16);
        if (! Config::has("database.connections.{$name}")) {
            Config::set("database.connections.{$name}", $this->toLaravelConnectionConfig($config));
        }
        DB::purge($name);

        return $name;
    }

    private function toLaravelConnectionConfig(array $cfg): array
    {
        $driver = (string) $cfg['driver'];
        if ($driver === 'sqlite') {
            return ['driver' => 'sqlite', 'database' => $cfg['database'] ?? ':memory:', 'prefix' => ''];
        }
        $base = ['driver' => $driver, 'host' => (string) ($cfg['host'] ?? '127.0.0.1'), 'port' => (int) ($cfg['port'] ?? ($driver === 'pgsql' ? 5432 : 3306)), 'database' => (string) ($cfg['database'] ?? ''), 'username' => (string) ($cfg['username'] ?? ''), 'password' => (string) ($cfg['password'] ?? ''), 'charset' => 'utf8mb4', 'collation' => 'utf8mb4_unicode_ci', 'prefix' => '', 'strict' => true];
        if ($driver === 'pgsql') {
            $base['schema'] = (string) ($cfg['schema'] ?? 'public');
        }

        return $base;
    }

    private function suggestLocalType(string $remoteType): ?string
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

    private function identityFromParts(array $parts): string
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
