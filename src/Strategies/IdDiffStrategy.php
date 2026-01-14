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

        $sourceIds = collect($dataset->getIdentities([$primaryKey]))->pluck($primaryKey);
        $localRows = DB::table($localTable)->where('__is_deleted', false)->get([$primaryKey, '__content_hash']);
        $localIds = $localRows->pluck($primaryKey);

        $newIds = $sourceIds->diff($localIds);
        $deletedIds = $localIds->diff($sourceIds);
        $intersectIds = $sourceIds->intersect($localIds);

        $rowsAdded = 0;
        $rowsUpdated = 0;
        $rowsDeleted = 0;

        DB::transaction(function () use ($dataset, $newIds, $deletedIds, $intersectIds, $primaryKey, $localTable, $mapping, &$rowsAdded, &$rowsUpdated, &$rowsDeleted, $localRows) {
            // Inserts
            if ($newIds->isNotEmpty()) {
                $rowsToInsert = [];
                foreach ($dataset->getRowsByIds($newIds->values()->all(), $primaryKey) as $row) {
                    $transformedRow = $this->transformer->transform($row, $mapping);
                    $transformedRow['__content_hash'] = $this->hashRow($transformedRow);
                    $rowsToInsert[] = $transformedRow;
                }
                foreach (array_chunk($rowsToInsert, 500) as $chunk) {
                    DB::table($localTable)->insert($chunk);
                }
                $rowsAdded = count($rowsToInsert);
            }

            // Deletes
            if ($deletedIds->isNotEmpty()) {
                DB::table($localTable)
                    ->whereIn($primaryKey, $deletedIds->all())
                    ->update(['__is_deleted' => true]);
                $rowsDeleted = $deletedIds->count();
            }

            // Updates
            if ($intersectIds->isNotEmpty()) {
                $localHashes = $localRows->whereIn($primaryKey, $intersectIds)->keyBy($primaryKey);

                foreach ($dataset->getRowsByIds($intersectIds->values()->all(), $primaryKey) as $row) {
                    $transformedRow = $this->transformer->transform($row, $mapping);
                    $newHash = $this->hashRow($transformedRow);
                    $id = $row[$primaryKey];

                    if ($localHashes->has($id) && $localHashes[$id]->__content_hash !== $newHash) {
                        $transformedRow['__content_hash'] = $newHash;
                        DB::table($localTable)->where($primaryKey, $id)->update($transformedRow);
                        $rowsUpdated++;
                    }
                }
            }
        });

        $run->update([
            'rows_added' => $rowsAdded,
            'rows_updated' => $rowsUpdated,
            'rows_deleted' => $rowsDeleted,
        ]);
    }

    private function hashRow(array $row): string
    {
        ksort($row);

        return hash('sha256', json_encode($row));
    }
}
