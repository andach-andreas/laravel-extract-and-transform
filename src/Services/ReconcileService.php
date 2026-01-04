<?php

namespace Andach\ExtractAndTransform\Services;

use Andach\ExtractAndTransform\Connectors\ConnectorRegistry;
use Andach\ExtractAndTransform\Connectors\Contracts\CanListIdentities;
use Andach\ExtractAndTransform\Data\RemoteDataset as RemoteDatasetDto;
use Andach\ExtractAndTransform\Models\ExtractDataset;
use Andach\ExtractAndTransform\Models\ExtractSchemaVersion;
use Andach\ExtractAndTransform\Services\Dto\ReconcileOptions;
use Andach\ExtractAndTransform\Services\Dto\ReconcileResult;
use Carbon\CarbonImmutable;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

final class ReconcileService
{
    public function __construct(
        private readonly ConnectorRegistry $registry,
    ) {}

    public function reconcile(ExtractDataset $dataset, ReconcileOptions $opts): ReconcileResult
    {
        $dataset->loadMissing('source');

        if (! $dataset->source) {
            throw new \RuntimeException('Dataset has no source.');
        }
        if (! $dataset->active_schema_version_id) {
            throw new \RuntimeException("Dataset [{$dataset->slug}] has no active schema version.");
        }

        /** @var ExtractSchemaVersion $sv */
        $sv = ExtractSchemaVersion::query()->findOrFail($dataset->active_schema_version_id);

        $targetConn = $opts->connectionOrDefault();
        $targetTable = $sv->target_table;

        $connector = $this->registry->get($dataset->source->connector);
        if (! $connector instanceof CanListIdentities) {
            throw new \RuntimeException("Connector [{$dataset->source->connector}] does not support identity listing.");
        }

        $remoteDataset = new RemoteDatasetDto(
            identifier: $dataset->identifier,
            label: $dataset->slug,
            meta: ['path' => $dataset->identifier]
        );

        $temp = 'extract_seen_'.uniqid();
        $this->createTempSeenTable($targetConn, $temp);

        $scanned = 0;

        try {
            $buffer = [];
            $chunk = 1000;

            foreach ($connector->listIdentities($remoteDataset, $dataset->source->config, $opts->identityColumns) as $identity) {
                $scanned++;
                $buffer[] = ['identity' => (string) $identity];

                if (count($buffer) >= $chunk) {
                    DB::connection($targetConn)->table($temp)->insertOrIgnore($buffer);
                    $buffer = [];
                }
            }

            if ($buffer !== []) {
                DB::connection($targetConn)->table($temp)->insertOrIgnore($buffer);
            }

            $missing = DB::connection($targetConn)->table('extract_identities as ei')
                ->leftJoin($temp.' as s', 's.identity', '=', 'ei.identity')
                ->where('ei.extract_dataset_id', $dataset->id)
                ->whereNull('ei.deleted_at')
                ->whereNull('s.identity')
                ->pluck('ei.identity')
                ->all();

            $deletedCount = count($missing);

            if (! $opts->dryRun && $deletedCount > 0) {
                $this->writeTombstones(
                    targetConn: $targetConn,
                    targetTable: $targetTable,
                    datasetId: (int) $dataset->id,
                    identities: $missing
                );
            }

            return new ReconcileResult(
                identitiesScanned: $scanned,
                tombstonesWritten: $opts->dryRun ? 0 : $deletedCount
            );
        } finally {
            $this->dropTempSeenTable($targetConn, $temp);
        }
    }

    private function createTempSeenTable(string $targetConn, string $tempTable): void
    {
        $schema = Schema::connection($targetConn);
        $schema->dropIfExists($tempTable);

        $schema->create($tempTable, function (Blueprint $t) {
            $t->string('identity')->primary();
        });
    }

    private function dropTempSeenTable(string $targetConn, string $tempTable): void
    {
        Schema::connection($targetConn)->dropIfExists($tempTable);
    }

    /**
     * @param  array<int,string>  $identities
     */
    private function writeTombstones(string $targetConn, string $targetTable, int $datasetId, array $identities): void
    {
        $now = CarbonImmutable::now()->toDateTimeString();

        $rows = [];
        foreach ($identities as $id) {
            $rows[] = [
                '__identity' => (string) $id,
                '__op' => 'delete',
                '__row_hash' => null,
                '__source_updated_at' => null,
                '__extracted_at' => $now,
                '__raw' => json_encode(['deleted' => true], JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
            ];
        }

        DB::connection($targetConn)->table($targetTable)->insert($rows);

        DB::connection($targetConn)->table('extract_identities')
            ->where('extract_dataset_id', $datasetId)
            ->whereIn('identity', $identities)
            ->update([
                'deleted_at' => $now,
                'updated_at' => $now,
            ]);
    }
}
