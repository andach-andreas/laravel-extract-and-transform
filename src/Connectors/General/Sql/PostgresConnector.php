<?php

namespace Andach\ExtractAndTransform\Connectors\General\Sql;

use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Data\RemoteField;
use Andach\ExtractAndTransform\Data\RemoteSchema;
use Illuminate\Support\Facades\DB;

class PostgresConnector extends AbstractSqlConnector
{
    public function key(): string
    {
        return 'pgsql';
    }

    public function label(): string
    {
        return 'PostgreSQL';
    }

    public function datasets(array $config): array
    {
        $conn = $this->resolveConnectionName($config);
        $connection = DB::connection($conn);
        $database = $config['database'] ?? $connection->getDatabaseName();
        $schema = $config['schema'] ?? 'public'; // Postgres often uses schemas

        $out = [];

        // Use information_schema for PostgreSQL, filtering by schema
        $tables = $connection->select("SELECT table_name as name FROM information_schema.tables WHERE table_schema = ?", [$schema]);

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
        $base = parent::toLaravelConnectionConfig($cfg);
        $base['schema'] = (string) ($cfg['schema'] ?? 'public');
        return $base;
    }
}
