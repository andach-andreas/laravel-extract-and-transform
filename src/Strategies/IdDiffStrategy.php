<?php

namespace Andach\ExtractAndTransform\Strategies;

use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Models\SyncRun;
use Andach\ExtractAndTransform\Services\RowTransformer;
use Andach\ExtractAndTransform\Source;
use Illuminate\Support\Facades\DB;

final class IdDiffStrategy implements SyncStrategy
{
    public function __construct(private readonly RowTransformer $transformer) {}

    public function run(SyncProfile $profile, Source $source, SyncRun $run): void
    {
        $activeVersion = $profile->activeSchemaVersion;
        $localTable = $activeVersion->local_table_name;
        $config = $activeVersion->configuration ?? [];
        $mapping = $activeVersion->column_mapping ?? null;
        $primaryKey = $config['primary_key'] ?? 'id';

        $dataset = $source->getDataset($profile->dataset_identifier);

        $sourceIds = collect($dataset->getIdentities([$primaryKey]));
        $localIds = DB::table($localTable)->where('__is_deleted', false)->pluck('__source_id');

        $newIds = $sourceIds->diff($localIds);
        $deletedIds = $localIds->diff($sourceIds);

        $rowsAdded = 0;
        $rowsDeleted = 0;

        DB::transaction(function () use ($dataset, $newIds, $deletedIds, $primaryKey, $localTable, $mapping, &$rowsAdded, &$rowsDeleted) {
            if ($newIds->isNotEmpty()) {
                $rowsToInsert = [];
                foreach ($dataset->getRowsByIds($newIds->all(), $primaryKey) as $row) {
                    $transformedRow = $this->transformer->transform($row, $mapping);
                    $transformedRow['__source_id'] = $row[$primaryKey];
                    $rowsToInsert[] = $transformedRow;
                }
                foreach (array_chunk($rowsToInsert, 500) as $chunk) {
                    DB::table($localTable)->insert($chunk);
                }
                $rowsAdded = count($rowsToInsert);
            }

            if ($deletedIds->isNotEmpty()) {
                DB::table($localTable)
                    ->whereIn('__source_id', $deletedIds->all())
                    ->update(['__is_deleted' => true]);
                $rowsDeleted = $deletedIds->count();
            }
        });

        $run->update([
            'rows_added' => $rowsAdded,
            'rows_deleted' => $rowsDeleted,
        ]);
    }
}
