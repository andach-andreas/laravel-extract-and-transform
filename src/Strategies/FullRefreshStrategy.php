<?php

namespace Andach\ExtractAndTransform\Strategies;

use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Models\SyncRun;
use Andach\ExtractAndTransform\Services\RowTransformer;
use Andach\ExtractAndTransform\Source;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

final class FullRefreshStrategy implements SyncStrategy
{
    public function __construct(private readonly RowTransformer $transformer) {}

    public function run(SyncProfile $profile, Source $source, SyncRun $run): void
    {
        $activeVersion = $profile->activeSchemaVersion;
        $localTable = $activeVersion->local_table_name;
        $mapping = $activeVersion->column_mapping ?? null;

        $dataset = $source->getDataset($profile->dataset_identifier);
        if (! $dataset) {
            throw new \Exception("Dataset {$profile->dataset_identifier} not found in source.");
        }

        // Get the authoritative list of columns from the destination table
        $columns = Schema::getColumnListing($localTable);
        $expectedColumns = array_fill_keys($columns, null);

        // Remove auto-managed columns from the expected list so we don't insert NULLs
        unset($expectedColumns['__id']); // Auto-increment primary key
        unset($expectedColumns['created_at']); // Timestamp
        unset($expectedColumns['updated_at']); // Timestamp

        $sourceId = $source->getModel()->id;
        $now = now()->toDateTimeString(); // Format explicitly to avoid JSON encoding of Carbon object

        $rowsToInsert = [];
        foreach ($dataset->getRows() as $row) {
            $transformedRow = $this->transformer->transform($row, $mapping);

            $normalizedRow = array_intersect_key($transformedRow, $expectedColumns);
            $finalRow = array_merge($expectedColumns, $normalizedRow);

            // Populate internal columns
            $finalRow['__source_id'] = $sourceId;
            $finalRow['__last_synced_at'] = $now;
            $finalRow['__is_deleted'] = 0;

            // Calculate content hash (excluding internal fields)
            $contentForHash = $transformedRow;
            ksort($contentForHash);
            $finalRow['__content_hash'] = md5(json_encode($contentForHash));

            // Sanitize values
            foreach ($finalRow as $key => $value) {
                if (is_array($value) || is_object($value)) {
                    $finalRow[$key] = json_encode($value);
                } elseif (is_string($value)) {
                    $finalRow[$key] = $this->sanitizeStringValue($value);
                }
            }

            $rowsToInsert[] = $finalRow;
        }

        DB::table($localTable)->truncate();

        DB::transaction(function () use ($localTable, $rowsToInsert) {
            foreach (array_chunk($rowsToInsert, 500) as $chunk) {
                DB::table($localTable)->insert($chunk);
            }
        });

        $run->update(['rows_added' => count($rowsToInsert)]);
    }

    private function sanitizeStringValue(string $value): string
    {
        // Handle Xero / Microsoft JSON Date format: /Date(1634515200000+0000)/
        if (preg_match('/^\/Date\((\d+)([+-]\d{4})?\)\/$/', $value, $matches)) {
            $timestamp = $matches[1] / 1000; // Convert ms to seconds

            return date('Y-m-d H:i:s', $timestamp);
        }

        return $value;
    }
}
