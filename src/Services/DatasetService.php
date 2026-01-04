<?php

namespace Andach\ExtractAndTransform\Services;

use Andach\ExtractAndTransform\Connectors\ConnectorRegistry;
use Andach\ExtractAndTransform\Connectors\Contracts\CanInferSchema;
use Andach\ExtractAndTransform\Data\RemoteDataset as RemoteDatasetDto;
use Andach\ExtractAndTransform\Data\RemoteSchema;
use Andach\ExtractAndTransform\Models\ExtractColumn;
use Andach\ExtractAndTransform\Models\ExtractDataset;
use Andach\ExtractAndTransform\Models\ExtractSchemaVersion;
use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Services\Dto\SetupOptions;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Str;

final class DatasetService
{
    public function __construct(
        private readonly ConnectorRegistry $registry,
        private readonly SchemaHasher $hasher,
        private readonly TableNamer $namer,
        private readonly TableCreator $creator,
    ) {}

    public function getBySlug(string $slug): ExtractDataset
    {
        /** @var ExtractDataset|null $ds */
        $ds = ExtractDataset::query()->with('source', 'schemaVersions')->where('slug', $slug)->first();
        if (! $ds || ! $ds->source) {
            throw new \RuntimeException("Dataset not found: {$slug}");
        }

        return $ds;
    }

    public function createOrGet(ExtractSource $source, string $identifier, ?string $slug = null): ExtractDataset
    {
        $slug = $slug ?: $this->defaultSlug($identifier);

        /** @var ExtractDataset $ds */
        $ds = ExtractDataset::query()->firstOrCreate(
            ['slug' => $slug],
            [
                'extract_source_id' => $source->id,
                'identifier' => $identifier,
                'active_schema_version_id' => null,
            ]
        );

        if ((int) $ds->extract_source_id !== (int) $source->id) {
            throw new \RuntimeException("Dataset slug [{$slug}] already exists but belongs to a different source.");
        }

        return $ds->fresh(['source', 'schemaVersions']);
    }

    public function inspect(ExtractDataset $dataset): RemoteSchema
    {
        $dataset->loadMissing('source');

        $connector = $this->registry->get($dataset->source->connector);
        if (! $connector instanceof CanInferSchema) {
            throw new \RuntimeException("Connector [{$dataset->source->connector}] does not support schema inference.");
        }

        $remoteDataset = new RemoteDatasetDto(
            identifier: $dataset->identifier,
            label: $dataset->slug,
            meta: ['path' => $dataset->identifier]
        );

        return $connector->inferSchema($remoteDataset, $dataset->source->config);
    }

    public function setup(ExtractDataset $dataset, SetupOptions $opts): ExtractSchemaVersion
    {
        $dataset->loadMissing('source', 'schemaVersions');

        if (! $dataset->source) {
            throw new \RuntimeException('Dataset has no source.');
        }

        $remoteSchema = $this->inspect($dataset);
        $remoteHash = $this->hasher->remoteSchemaHash($remoteSchema);
        $nextVersion = $this->nextVersion($dataset);

        $conn = $opts->connectionOrDefault();

        [$targetTable, $baseTable, $prefixUsed] = $this->resolveTargetTable($dataset, $nextVersion, $opts);

        [$mapping, $tableColumns] = $this->buildMappingAndColumns($remoteSchema->fields, $opts->columns, $opts->types);
        $mappingHash = $this->hasher->mappingHash($mapping);

        if ($opts->target === 'versioned') {
            if (DB::connection($conn)->getSchemaBuilder()->hasTable($targetTable)) {
                throw new \RuntimeException("Extract table already exists: {$targetTable}");
            }
            $this->creator->create($targetTable, $tableColumns, $conn);
        } else {
            if (! DB::connection($conn)->getSchemaBuilder()->hasTable($targetTable)) {
                throw new \RuntimeException("Existing target table does not exist: {$targetTable}");
            }
        }

        /** @var ExtractSchemaVersion $sv */
        $sv = DB::transaction(function () use ($dataset, $nextVersion, $opts, $targetTable, $baseTable, $prefixUsed, $remoteHash, $mappingHash, $mapping) {
            $sv = ExtractSchemaVersion::query()->create([
                'extract_dataset_id' => $dataset->id,
                'version' => $nextVersion,
                'target_mode' => $opts->target,
                'target_table' => $targetTable,
                'base_table' => $baseTable,
                'prefix_used' => $prefixUsed,
                'remote_schema_hash' => $remoteHash,
                'mapping_hash' => $mappingHash,
            ]);

            foreach ($mapping as $m) {
                ExtractColumn::query()->create([
                    'extract_schema_version_id' => $sv->id,
                    'remote_name' => $m['remote_name'],
                    'remote_type' => $m['remote_type'],
                    'local_name' => $m['local_name'],
                    'local_type' => $m['local_type'],
                    'selected' => true,
                    'nullable' => (bool) $m['nullable'],
                    'position' => (int) $m['position'],
                ]);
            }

            $dataset->active_schema_version_id = $sv->id;
            $dataset->save();

            return $sv;
        });

        return $sv;
    }

    private function defaultSlug(string $identifier): string
    {
        $s = Str::of($identifier)->lower();
        $s = $s->replaceMatches('/[^a-z0-9]+/i', '_');

        return trim((string) $s, '_') ?: 'dataset';
    }

    private function nextVersion(ExtractDataset $dataset): int
    {
        $max = (int) $dataset->schemaVersions->max('version');
        $next = $max + 1;

        return $next < 1 ? 1 : $next;
    }

    /**
     * @return array{0:string,1:?string,2:?string}
     */
    private function resolveTargetTable(ExtractDataset $dataset, int $version, SetupOptions $opts): array
    {
        if ($opts->target === 'existing') {
            $table = $opts->tableOrFail();

            return [$table, null, null];
        }

        $prefix = $opts->prefixOrConfig();
        $base = $opts->baseTableOrFallback($dataset->slug);

        $table = $this->namer->versioned(prefix: $prefix, baseTable: $base, version: $version);

        return [$table, $base, $prefix];
    }

    /**
     * @param  array<int,object>  $fields
     * @param  array<int,string>  $selected
     * @param  array<string,string>  $typeOverrides
     * @return array{0:array<int,array<string,mixed>>,1:array<int,array{local_name:string,local_type:string,nullable:bool}>}
     */
    private function buildMappingAndColumns(array $fields, array $selected, array $typeOverrides): array
    {
        $mapping = [];
        $tableColumns = [];

        foreach ($fields as $field) {
            if (! in_array($field->name, $selected, true)) {
                continue;
            }

            $localType = $typeOverrides[$field->name] ?? $field->suggestedLocalType ?? 'string';
            $localName = (string) $field->name;

            $mapping[] = [
                'remote_name' => $field->name,
                'remote_type' => $field->remoteType,
                'local_name' => $localName,
                'local_type' => $localType,
                'nullable' => (bool) $field->nullable,
                'selected' => true,
                'position' => count($mapping),
            ];

            $tableColumns[] = [
                'local_name' => $localName,
                'local_type' => $localType,
                'nullable' => (bool) $field->nullable,
            ];
        }

        return [$mapping, $tableColumns];
    }
}
