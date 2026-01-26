<?php

namespace Andach\ExtractAndTransform\Connectors\General\Sql;

use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Data\RemoteField;
use Andach\ExtractAndTransform\Data\RemoteSchema;
use Illuminate\Support\Facades\DB;

class SqliteConnector extends AbstractSqlConnector
{
    public function key(): string
    {
        return 'sqlite';
    }

    public function label(): string
    {
        return 'SQLite';
    }

    public function datasets(array $config): array
    {
        $conn = $this->resolveConnectionName($config);
        $connection = DB::connection($conn);

        $out = [];

        // SQLite lists tables in sqlite_master
        $tables = $connection->select("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%_temp'");

        foreach ($tables as $table) {
            $tableArray = (array) $table;
            $name = reset($tableArray);
            $out[] = new RemoteDataset(identifier: $name, label: $name, meta: []);
        }

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

    protected function toLaravelConnectionConfig(array $cfg): array
    {
        return ['driver' => 'sqlite', 'database' => $cfg['database'] ?? ':memory:', 'prefix' => ''];
    }
}
