<?php

namespace Andach\ExtractAndTransform\Strategies;

use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Models\SyncRun;
use Andach\ExtractAndTransform\Services\RowTransformer;
use Andach\ExtractAndTransform\Source;
use Illuminate\Support\Facades\DB;

final class ContentHashStrategy implements SyncStrategy
{
    public function __construct(private readonly RowTransformer $transformer) {}

    public function run(SyncProfile $profile, Source $source, SyncRun $run): void
    {
        $activeVersion = $profile->activeSchemaVersion;
        $localTable = $activeVersion->local_table_name;
        $mapping = $activeVersion->column_mapping ?? null;

        $dataset = $source->getDataset($profile->dataset_identifier);

        $sourceHashes = collect($dataset->getRows())->mapWithKeys(function ($row) use ($mapping) {
            $transformedRow = $this->transformer->transform($row, $mapping);
            $hash = $this->hashRow($transformedRow);

            return [$hash => $transformedRow];
        });

        $localHashes = DB::table($localTable)->where('__is_deleted', false)->pluck('__content_hash');

        $newHashes = $sourceHashes->keys()->diff($localHashes);
        $deletedHashes = $localHashes->diff($sourceHashes->keys());

        $rowsAdded = 0;
        $rowsDeleted = 0;

        DB::transaction(function () use ($newHashes, $deletedHashes, $sourceHashes, $localTable, &$rowsAdded, &$rowsDeleted) {
            if ($newHashes->isNotEmpty()) {
                $rowsToInsert = [];
                foreach ($newHashes as $hash) {
                    $row = $sourceHashes[$hash];
                    $row['__content_hash'] = $hash;
                    $rowsToInsert[] = $row;
                }
                foreach (array_chunk($rowsToInsert, 500) as $chunk) {
                    DB::table($localTable)->insert($chunk);
                }
                $rowsAdded = count($rowsToInsert);
            }

            if ($deletedHashes->isNotEmpty()) {
                DB::table($localTable)
                    ->whereIn('__content_hash', $deletedHashes->all())
                    ->update(['__is_deleted' => true]);
                $rowsDeleted = $deletedHashes->count();
            }
        });

        $run->update([
            'rows_added' => $rowsAdded,
            'rows_deleted' => $rowsDeleted,
        ]);
    }

    private function hashRow(array $row): string
    {
        ksort($row);

        return hash('sha256', json_encode($row));
    }
}
