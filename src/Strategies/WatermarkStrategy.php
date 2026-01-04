<?php

namespace Andach\ExtractAndTransform\Strategies;

use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Models\SyncRun;
use Andach\ExtractAndTransform\Services\RowTransformer;
use Andach\ExtractAndTransform\Source;
use Illuminate\Support\Facades\DB;

final class WatermarkStrategy implements SyncStrategy
{
    public function __construct(private readonly RowTransformer $transformer) {}

    public function run(SyncProfile $profile, Source $source, SyncRun $run): void
    {
        $activeVersion = $profile->activeSchemaVersion;
        $localTable = $activeVersion->local_table_name;
        $mapping = $activeVersion->column_mapping ?? null;
        $config = $activeVersion->configuration ?? [];
        $mode = $config['mode'] ?? 'append_only';
        $watermarkColumn = $config['watermark_column'] ?? 'id';
        $primaryKey = $config['primary_key'] ?? 'id';

        $lastRun = $profile->runs()->where('status', 'success')->latest('finished_at')->first();
        $checkpoint = $lastRun ? $lastRun->checkpoint : null;

        $dataset = $source->getDataset($profile->dataset_identifier);
        if (! $dataset) {
            throw new \Exception("Dataset {$profile->dataset_identifier} not found in source.");
        }

        $rowsAdded = 0;
        $rowsUpdated = 0;

        $generator = $dataset->getRowsWithCheckpoint($checkpoint, [
            'strategy' => 'time_watermark',
            'watermark' => $watermarkColumn,
        ]);

        DB::transaction(function () use ($generator, $mode, $localTable, $primaryKey, $mapping, &$rowsAdded, &$rowsUpdated) {
            foreach ($generator as $row) {
                $transformedRow = $this->transformer->transform($row, $mapping);

                if ($mode === 'append_only') {
                    DB::table($localTable)->insert($transformedRow);
                    $rowsAdded++;
                } elseif ($mode === 'upsert') {
                    DB::table($localTable)->upsert(
                        $transformedRow,
                        [$primaryKey],
                        array_keys($transformedRow)
                    );
                    $rowsUpdated++;
                }
            }
        });

        $newCheckpoint = $generator->getReturn();

        $run->update([
            'rows_added' => $rowsAdded,
            'rows_updated' => $rowsUpdated,
            'checkpoint' => $newCheckpoint,
        ]);
    }
}
