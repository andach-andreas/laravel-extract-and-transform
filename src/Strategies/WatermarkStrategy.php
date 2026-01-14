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
        $config = $activeVersion->configuration ?? [];
        $mapping = $activeVersion->column_mapping ?? null;
        $watermarkColumn = $config['watermark_column'] ?? 'updated_at';

        $dataset = $source->getDataset($profile->dataset_identifier);
        $checkpoint = $run->checkpoint;
        $options = ['watermark_column' => $watermarkColumn];

        $generator = $dataset->getRowsWithCheckpoint($checkpoint, $options);

        $rowsToInsert = [];
        while ($generator->valid()) {
            $row = $generator->current();
            $transformedRow = $this->transformer->transform($row, $mapping);
            $rowsToInsert[] = $transformedRow;
            $generator->next();
        }

        $newCheckpoint = $generator->getReturn();

        if (! empty($rowsToInsert)) {
            DB::transaction(function () use ($localTable, $rowsToInsert) {
                foreach (array_chunk($rowsToInsert, 500) as $chunk) {
                    DB::table($localTable)->insert($chunk);
                }
            });
        }

        $run->update([
            'rows_added' => count($rowsToInsert),
            'checkpoint' => $newCheckpoint,
        ]);
    }
}
