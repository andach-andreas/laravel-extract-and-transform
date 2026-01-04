<?php

namespace Andach\ExtractAndTransform;

use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Models\SyncRun;

final class Sync
{
    private array $mapping = [];

    private array $overrides = [];

    private array $strategyConfig = [];

    private ?string $tableName = null;

    public function __construct(
        private readonly SyncProfile $profile,
        private readonly ExtractAndTransform $extractor
    ) {
        if ($activeVersion = $this->profile->activeSchemaVersion) {
            $this->mapping = $activeVersion->column_mapping ?? [];
            $this->overrides = $activeVersion->schema_overrides ?? [];
            $this->strategyConfig = $activeVersion->configuration ?? [];
            // We do NOT load the table name here. If we did, we would lock the next version
            // to the old table name unless the user explicitly called toTable() again.
            // By leaving it null, we allow SyncProfile::newVersion to auto-generate
            // a new incremented table name (e.g. _v2) if a schema change is detected.
        }
    }

    public function withStrategy(string $strategy, array $config = []): self
    {
        $this->profile->strategy = $strategy;
        $this->strategyConfig = $config;

        return $this;
    }

    public function mapColumns(array $mapping): self
    {
        $this->mapping = $mapping;

        return $this;
    }

    public function overrideSchema(array $overrides): self
    {
        $this->overrides = $overrides;

        return $this;
    }

    public function toTable(string $tableName): self
    {
        $this->tableName = $tableName;

        return $this;
    }

    public function run(): SyncRun
    {
        $this->profile->save();
        $this->prepareAndActivateVersion();

        return $this->profile->run();
    }

    private function prepareAndActivateVersion(): void
    {
        $source = $this->extractor->getSourceFromModel($this->profile->source);
        $dataset = $source->getDataset($this->profile->dataset_identifier);
        $liveSchema = $dataset->getSchema();
        $liveSchemaHash = hash('sha256', json_encode($liveSchema->fields));

        $configHash = hash('sha256', json_encode([
            'mapping' => $this->mapping,
            'overrides' => $this->overrides,
            'strategy_config' => $this->strategyConfig,
            'strategy' => $this->profile->strategy,
        ]));

        $existingVersion = $this->profile->schemaVersions()
            ->where('source_schema_hash', $liveSchemaHash)
            ->where('config_hash', $configHash)
            ->first();

        if ($existingVersion) {
            if ($this->profile->active_schema_version_id !== $existingVersion->id) {
                $this->profile->activateVersion($existingVersion);
            }

            return;
        }

        $newVersion = $this->profile->newVersion(
            $this->mapping,
            $this->overrides,
            $this->tableName,
            $this->strategyConfig
        );

        $newVersion->update([
            'source_schema_hash' => $liveSchemaHash,
            'config_hash' => $configHash,
        ]);

        $this->profile->activateVersion($newVersion);
    }
}
