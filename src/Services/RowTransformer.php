<?php

namespace Andach\ExtractAndTransform\Services;

use Andach\ExtractAndTransform\Data\RemoteField;

final class RowTransformer
{
    /**
     * Transforms a single row of data based on a column mapping.
     *
     * @param  array  $row  The original row from the source.
     * @param  array|null  $mapping  The column mapping configuration.
     * @return array The transformed row ready for local insertion.
     */
    public function transform(array $row, ?array $mapping): array
    {
        // If no mapping is provided, return the row as-is (1:1 mapping).
        if (empty($mapping)) {
            return $row;
        }

        $transformedRow = [];

        // Iterate through the mapping configuration.
        foreach ($mapping as $sourceKey => $localKey) {
            // If the local key is null, we explicitly exclude the column.
            if ($localKey === null) {
                continue;
            }

            // If the source key exists in the row, map it to the new local key.
            if (array_key_exists($sourceKey, $row)) {
                $transformedRow[$localKey] = $row[$sourceKey];
            }
        }

        // CRITICAL CHANGE:
        // We do NOT iterate through the remaining $row keys.
        // If a mapping is provided, it acts as an "allowlist".
        // Only columns explicitly defined in the mapping are included.
        // This prevents unmapped columns (like 'should_be_ignored') from leaking through.

        return $transformedRow;
    }

    /**
     * Filters and transforms schema fields based on a column mapping.
     *
     * @param  RemoteField[]  $fields  The original array of RemoteField objects.
     * @param  array|null  $mapping  The column mapping configuration.
     * @return RemoteField[] The filtered and transformed array of RemoteField objects.
     */
    public function filterColumns(array $fields, ?array $mapping): array
    {
        // If no mapping is provided, return all fields as-is.
        if (empty($mapping)) {
            return $fields;
        }

        $transformedFields = [];

        foreach ($fields as $field) {
            $sourceName = $field->name;

            // Only include fields that are present in the mapping
            if (array_key_exists($sourceName, $mapping)) {
                $localName = $mapping[$sourceName];

                // Exclude the field if it's mapped to null
                if ($localName === null) {
                    continue;
                }

                // Create a new field with the updated local name
                $transformedFields[] = new RemoteField(
                    name: $localName,
                    remoteType: $field->remoteType,
                    nullable: $field->nullable,
                    suggestedLocalType: $field->suggestedLocalType
                );
            }
            // Fields NOT in the mapping are implicitly excluded when a mapping exists.
        }

        return $transformedFields;
    }
}
