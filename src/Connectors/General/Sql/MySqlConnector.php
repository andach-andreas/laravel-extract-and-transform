<?php

namespace Andach\ExtractAndTransform\Connectors\General\Sql;

use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Data\RemoteField;
use Andach\ExtractAndTransform\Data\RemoteSchema;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log; // Added Log facade

class MySqlConnector extends AbstractSqlConnector
{
    public function key(): string
    {
        return 'mysql';
    }

    public function label(): string
    {
        return 'MySQL / MariaDB';
    }

    public function datasets(array $config): array
    {
        Log::info("[MySQL Connector] datasets() started.");
        $startTime = microtime(true);

        $conn = $this->resolveConnectionName($config);
        $connection = DB::connection($conn);
        $database = $config['database'] ?? $connection->getDatabaseName();

        Log::info("[MySQL Connector] Executing SHOW TABLES FROM `{$database}` query...");
        $queryStartTime = microtime(true);
        $tables = $connection->select("SHOW TABLES FROM `{$database}`");
        Log::info("[MySQL Connector] SHOW TABLES query completed in " . round(microtime(true) - $queryStartTime, 3) . "s. Found " . count($tables) . " tables.");

        // Map the results to RemoteDataset objects
        $out = array_map(function ($table) {
            $tableArray = (array) $table;
            $name = reset($tableArray);
            return new RemoteDataset(identifier: $name, label: $name, meta: []);
        }, $tables);

        Log::info("[MySQL Connector] datasets() completed in " . round(microtime(true) - $startTime, 3) . "s.");
        return $out;
    }

    public function inferSchema(RemoteDataset $dataset, array $config): RemoteSchema
    {
        Log::info("[MySQL Connector] inferSchema() started for table: {$dataset->identifier}.");
        $startTime = microtime(true);

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

        Log::info("[MySQL Connector] inferSchema() completed for table: {$dataset->identifier} in " . round(microtime(true) - $startTime, 3) . "s. Found " . count($fields) . " columns.");
        return new RemoteSchema($fields);
    }
}
