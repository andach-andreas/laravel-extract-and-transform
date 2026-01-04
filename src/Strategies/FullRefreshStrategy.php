<?php

namespace Andach\ExtractAndTransform\Strategies;

use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Models\SyncRun;
use Andach\ExtractAndTransform\Services\RowTransformer;
use Andach\ExtractAndTransform\Source;
use Illuminate\Support\Facades\DB;

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

        $rowsToInsert = [];
        foreach ($dataset->getRows() as $row) {
            $transformedRow = $this->transformer->transform($row, $mapping);
            // Ensure we only try to insert columns that exist in the transformed row
            $insertData = [];
            foreach ($transformedRow as $key => $value) {
                $insertData[$key] = $value;
            }
            $rowsToInsert[] = $insertData;
        }

        // Truncate outside the transaction to avoid implicit commit issues
        // and to reset auto-increment IDs.
        DB::table($localTable)->truncate();

        DB::transaction(function () use ($localTable, $rowsToInsert) {
            foreach (array_chunk($rowsToInsert, 500) as $chunk) {
                DB::table($localTable)->insert($chunk);
            }
        });

        $run->update(['rows_added' => count($rowsToInsert)]);
    }
}
