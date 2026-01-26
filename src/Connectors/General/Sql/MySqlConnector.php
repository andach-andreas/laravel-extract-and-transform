<?php

namespace Andach\ExtractAndTransform\Connectors\General\Sql;

use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Data\RemoteField;
use Andach\ExtractAndTransform\Data\RemoteSchema;
use Illuminate\Support\Facades\DB;

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
        $conn = $this->resolveConnectionName($config);
        $connection = DB::connection($conn);
        $database = $config['database'] ?? $connection->getDatabaseName();

        // Use SHOW TABLES for performance. It's typically faster than querying information_schema.
        $tables = $connection->select("SHOW TABLES FROM `{$database}`");

        // Map the results to RemoteDataset objects
        $out = array_map(function ($table) {
            $tableArray = (array) $table;
            $name = reset($tableArray);
            return new RemoteDataset(identifier: $name, label: $name, meta: []);
        }, $tables);

        return $out;
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
}
