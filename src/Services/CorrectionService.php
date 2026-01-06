<?php

namespace Andach\ExtractAndTransform\Services;

use Andach\ExtractAndTransform\Models\Correction;

class CorrectionService
{
    public function add(string $tableName, string $rowIdentifier, string $column, mixed $newValue, ?string $reason = null): Correction
    {
        return Correction::updateOrCreate(
            [
                'table_name' => $tableName,
                'row_identifier' => $rowIdentifier,
                'column_name' => $column,
            ],
            [
                'new_value' => $newValue,
                'reason' => $reason,
            ]
        );
    }

    public function get(string $tableName, string $rowIdentifier, string $column): ?Correction
    {
        return Correction::where('table_name', $tableName)
            ->where('row_identifier', $rowIdentifier)
            ->where('column_name', $column)
            ->first();
    }
}
