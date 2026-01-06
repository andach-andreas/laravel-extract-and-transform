<?php

namespace Andach\ExtractAndTransform\Services;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class ReconcileService
{
    public function reconcile(string $sourceTable, string $destinationTable, string|array $identifier): int
    {
        // 1. Create Destination Table (Clone)
        if (Schema::hasTable($destinationTable)) {
            Schema::drop($destinationTable);
        }

        // SQLite doesn't support CREATE TABLE AS SELECT * FROM ... in one go nicely with indexes,
        // but for raw data copy it works.
        // However, Laravel Schema builder is safer for structure, but we want data too.
        // DB::statement("CREATE TABLE $destinationTable AS SELECT * FROM $sourceTable") works in most DBs.

        DB::statement("CREATE TABLE {$destinationTable} AS SELECT * FROM {$sourceTable}");

        // 2. Find columns that have corrections
        $prefix = config('extract-data.internal_table_prefix', 'andach_leat_');
        $correctionsTable = $prefix . 'corrections';

        $columnsToCorrect = DB::table($correctionsTable)
            ->where('table_name', $sourceTable)
            ->distinct()
            ->pluck('column_name');

        $rowsAffected = 0;

        // 3. Apply updates per column
        foreach ($columnsToCorrect as $column) {
            $rowsAffected += $this->applyCorrectionForColumn(
                $sourceTable,
                $destinationTable,
                $identifier,
                $column,
                $correctionsTable
            );
        }

        return $rowsAffected;
    }

    private function applyCorrectionForColumn(string $sourceTable, string $destinationTable, string|array $identifier, string $column, string $correctionsTable): int
    {
        $identifierSql = $this->buildIdentifierSql($identifier, $destinationTable);

        // Standard SQL Update with Subquery
        // UPDATE dest SET col = (SELECT new_value FROM corrections WHERE ...)
        // WHERE EXISTS (SELECT 1 FROM corrections WHERE ...)

        // We need to handle the case where new_value is text but destination column might be int.
        // DB should handle implicit cast, or we might need casting.
        // For now, assume DB handles it.

        $sql = "
            UPDATE {$destinationTable}
            SET {$column} = (
                SELECT new_value
                FROM {$correctionsTable}
                WHERE table_name = ?
                  AND column_name = ?
                  AND row_identifier = {$identifierSql}
            )
            WHERE EXISTS (
                SELECT 1
                FROM {$correctionsTable}
                WHERE table_name = ?
                  AND column_name = ?
                  AND row_identifier = {$identifierSql}
            )
        ";

        return DB::update($sql, [$sourceTable, $column, $sourceTable, $column]);
    }

    private function buildIdentifierSql(string|array $identifier, string $tableName): string
    {
        if (is_string($identifier)) {
            return "{$tableName}.{$identifier}";
        }

        // Composite key
        $driver = DB::connection()->getDriverName();

        if ($driver === 'sqlite') {
             $cols = implode(" || '-' || ", array_map(fn($col) => "{$tableName}.{$col}", $identifier));
             return $cols;
        }

        $cols = implode(", '-', ", array_map(fn($col) => "{$tableName}.{$col}", $identifier));
        return "CONCAT({$cols})";
    }
}
