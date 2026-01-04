<?php

namespace Andach\ExtractAndTransform\Services;

use Andach\ExtractAndTransform\Connectors\ConnectorRegistry;
use Andach\ExtractAndTransform\Connectors\Contracts\CanStreamRows;
use Andach\ExtractAndTransform\Connectors\Contracts\CanStreamRowsWithCheckpoint;
use Andach\ExtractAndTransform\Data\RemoteDataset as RemoteDatasetDto;
use Andach\ExtractAndTransform\Models\ExtractDataset;
use Andach\ExtractAndTransform\Models\ExtractSchemaVersion;
use Andach\ExtractAndTransform\Services\Dto\ImportOptions;
use Andach\ExtractAndTransform\Services\Dto\ImportResult;
use Andach\ExtractAndTransform\Services\Support\IdentityBuilder;
use Andach\ExtractAndTransform\Services\Support\RowHasher;
use Carbon\CarbonImmutable;
use Illuminate\Support\Facades\DB;

final class ImportService
{
    public function __construct(
        private readonly ConnectorRegistry $registry,
        private readonly IdentityBuilder $identities,
        private readonly RowHasher $hasher,
    ) {}

    public function import(ExtractDataset $dataset, ImportOptions $opts): ImportResult
    {
        $dataset->loadMissing('source');

        if (! $dataset->source) {
            throw new \RuntimeException('Dataset has no source.');
        }
        if (! $dataset->active_schema_version_id) {
            throw new \RuntimeException("Dataset [{$dataset->slug}] has no active schema version. Run setup first.");
        }

        /** @var ExtractSchemaVersion $sv */
        $sv = ExtractSchemaVersion::query()->findOrFail($dataset->active_schema_version_id);

        $targetConn = $opts->connectionOrDefault();
        $targetTable = $sv->target_table;

        $columns = DB::table('extract_columns')
            ->where('extract_schema_version_id', $sv->id)
            ->where('selected', 1)
            ->orderBy('position')
            ->get(['remote_name', 'local_name'])
            ->all();

        $map = [];
        foreach ($columns as $c) {
            $map[(string) $c->remote_name] = (string) $c->local_name;
        }

        $connector = $this->registry->get($dataset->source->connector);

        $remoteDataset = new RemoteDatasetDto(
            identifier: $dataset->identifier,
            label: $dataset->slug,
            meta: ['path' => $dataset->identifier]
        );

        if (! $connector instanceof CanStreamRows) {
            throw new \RuntimeException("Connector [{$dataset->source->connector}] cannot stream rows.");
        }

        $now = CarbonImmutable::now();
        $chunkSize = (int) config('extract-data.chunk_size', 1000);

        if ($opts->strategy === 'full_refresh' && ! $opts->dryRun) {
            DB::connection($targetConn)->table($targetTable)->truncate();
        }

        $checkpoint = $this->loadCheckpoint((int) $dataset->id, $opts->strategy);

        $rowsRead = 0;
        $rowsWritten = 0;
        $rowsSkipped = 0;

        $buffer = [];
        $bufferIdentities = [];

        $gen = null;
        $iterable = null;

        $connectorOptions = $opts->connectorOptions();

        if ($connector instanceof CanStreamRowsWithCheckpoint) {
            $gen = $connector->streamRowsWithCheckpoint($remoteDataset, $dataset->source->config, $checkpoint, $connectorOptions);
            $iterable = $gen;
        } else {
            $iterable = $connector->streamRows($remoteDataset, $dataset->source->config);
        }

        foreach ($iterable as $row) {
            $rowsRead++;

            $rowArr = is_array($row) ? $row : (array) $row;
            $mapped = $this->mapRow($rowArr, $map);

            $identity = $opts->hasIdentity()
                ? $this->identities->fromRow($opts->identityColumns, $rowArr)
                : null;

            $rowHash = $this->hasher->hash($this->canonicalForHash($rowArr, $map));

            if ($opts->strategy === 'content_hash' && $identity !== null) {
                $exists = DB::connection($targetConn)->table($targetTable)
                    ->where('__identity', $identity)
                    ->where('__row_hash', $rowHash)
                    ->exists();

                if ($exists) {
                    $rowsSkipped++;

                    continue;
                }
            }

            $op = $opts->strategy === 'full_refresh' ? 'snapshot' : 'upsert';

            $record = $mapped + [
                '__identity' => $identity,
                '__op' => $op,
                '__row_hash' => $rowHash,
                '__source_updated_at' => $this->resolveSourceUpdatedAt($rowArr, $opts),
                '__extracted_at' => $now->toDateTimeString(),
                '__raw' => json_encode($rowArr, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
            ];

            $buffer[] = $record;

            if ($identity !== null) {
                $bufferIdentities[] = $identity;
            }

            if (count($buffer) >= $chunkSize) {
                if (! $opts->dryRun) {
                    DB::connection($targetConn)->table($targetTable)->insert($buffer);
                    $rowsWritten += count($buffer);

                    $this->touchIdentities((int) $dataset->id, $bufferIdentities, $now);
                }

                $buffer = [];
                $bufferIdentities = [];
            }
        }

        if ($buffer !== []) {
            if (! $opts->dryRun) {
                DB::connection($targetConn)->table($targetTable)->insert($buffer);
                $rowsWritten += count($buffer);

                $this->touchIdentities((int) $dataset->id, $bufferIdentities, $now);
            }
        }

        $newCheckpoint = $checkpoint;
        if ($gen instanceof \Generator) {
            $ret = $gen->getReturn();
            if (is_array($ret)) {
                $newCheckpoint = $ret;
            }
        }

        if (! $opts->dryRun) {
            $this->storeCheckpoint((int) $dataset->id, $opts->strategy, $newCheckpoint);
        }

        return new ImportResult(
            rowsRead: $rowsRead,
            rowsWritten: $rowsWritten,
            rowsSkipped: $rowsSkipped
        );
    }

    /**
     * @param  array<string,mixed>  $row
     * @param  array<string,string>  $map  remote_name => local_name
     * @return array<string,mixed>
     */
    private function mapRow(array $row, array $map): array
    {
        $out = [];
        foreach ($map as $remote => $local) {
            $out[$local] = $row[$remote] ?? null;
        }

        return $out;
    }

    /**
     * @param  array<string,mixed>  $row
     * @param  array<string,string>  $map
     * @return array<string,mixed>
     */
    private function canonicalForHash(array $row, array $map): array
    {
        $out = [];
        foreach ($map as $remote => $local) {
            $out[$local] = $row[$remote] ?? null;
        }
        ksort($out);

        return $out;
    }

    private function resolveSourceUpdatedAt(array $row, ImportOptions $opts): ?string
    {
        if (! is_string($opts->sourceUpdatedAt) || trim($opts->sourceUpdatedAt) === '') {
            return null;
        }

        $v = $row[$opts->sourceUpdatedAt] ?? null;
        if ($v === null) {
            return null;
        }
        if (is_string($v)) {
            return $v;
        }
        if ($v instanceof \DateTimeInterface) {
            return $v->format('Y-m-d H:i:s');
        }

        return (string) $v;
    }

    private function loadCheckpoint(int $datasetId, string $strategy): ?array
    {
        $row = DB::table('extract_checkpoints')
            ->where('extract_dataset_id', $datasetId)
            ->where('strategy', $strategy)
            ->first();

        if (! $row) {
            return null;
        }

        $json = $row->checkpoint_json ?? null;
        if (! is_string($json) || trim($json) === '') {
            return null;
        }

        $decoded = json_decode($json, true);

        return is_array($decoded) ? $decoded : null;
    }

    private function storeCheckpoint(int $datasetId, string $strategy, ?array $checkpoint): void
    {
        $json = $checkpoint ? json_encode($checkpoint, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE) : null;
        $now = now()->toDateTimeString();

        $exists = DB::table('extract_checkpoints')
            ->where('extract_dataset_id', $datasetId)
            ->where('strategy', $strategy)
            ->exists();

        if ($exists) {
            DB::table('extract_checkpoints')
                ->where('extract_dataset_id', $datasetId)
                ->where('strategy', $strategy)
                ->update([
                    'checkpoint_json' => $json,
                    'updated_at' => $now,
                ]);
        } else {
            DB::table('extract_checkpoints')->insert([
                'extract_dataset_id' => $datasetId,
                'strategy' => $strategy,
                'checkpoint_json' => $json,
                'created_at' => $now,
                'updated_at' => $now,
            ]);
        }
    }

    /**
     * @param  array<int,string>  $identities
     */
    private function touchIdentities(int $datasetId, array $identities, CarbonImmutable $now): void
    {
        if ($identities === []) {
            return;
        }

        $nowStr = $now->toDateTimeString();

        $rows = [];
        foreach (array_values(array_unique($identities)) as $id) {
            $rows[] = [
                'extract_dataset_id' => $datasetId,
                'identity' => $id,
                'first_seen_at' => $nowStr,
                'last_seen_at' => $nowStr,
                'deleted_at' => null,
                'created_at' => $nowStr,
                'updated_at' => $nowStr,
            ];
        }

        DB::table('extract_identities')->insertOrIgnore($rows);

        DB::table('extract_identities')
            ->where('extract_dataset_id', $datasetId)
            ->whereIn('identity', array_values(array_unique($identities)))
            ->update([
                'last_seen_at' => $nowStr,
                'deleted_at' => null,
                'updated_at' => $nowStr,
            ]);
    }
}
