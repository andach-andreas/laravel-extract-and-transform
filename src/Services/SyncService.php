<?php

namespace Andach\ExtractAndTransform\Services;

use Andach\ExtractAndTransform\Events\SyncFailed;
use Andach\ExtractAndTransform\Events\SyncStarting;
use Andach\ExtractAndTransform\Events\SyncSucceeded;
use Andach\ExtractAndTransform\ExtractAndTransform;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Models\SyncRun;
use Andach\ExtractAndTransform\Strategies\StrategyRegistry;
use Exception;
use Illuminate\Database\QueryException;
use Throwable;

final class SyncService
{
    public function __construct(
        private readonly StrategyRegistry $registry,
        private readonly ExtractAndTransform $extractor,
        private readonly TableManager $tableManager
    ) {}

    public function run(SyncProfile $profile): SyncRun
    {
        event(new SyncStarting($profile));

        $run = $profile->runs()->create([
            'status' => 'running',
            'started_at' => now(),
        ]);

        try {
            // Force reload the active schema version to ensure we have the latest one
            // This is crucial because Sync::run() might have just updated the active version ID
            // but the $profile instance might still have the old relation cached.
            $activeVersion = $profile->activeSchemaVersion()->first();

            if (! $activeVersion) {
                throw new Exception('Profile has no active schema version. Please create and activate a version first.');
            }

            // Use the helper to get the Source object from the model
            $source = $this->extractor->getSourceFromModel($profile->source);
            $dataset = $source->getDataset($profile->dataset_identifier);

            if (! $dataset) {
                throw new Exception("Dataset '{$profile->dataset_identifier}' not found in source '{$profile->source->name}'.");
            }

            $liveSchema = $dataset->getSchema();
            $liveSchemaHash = hash('sha256', json_encode($liveSchema->fields));

            if ($activeVersion->source_schema_hash !== $liveSchemaHash) {
                throw new Exception('Source schema has changed and no longer matches the active mapping. A new schema version must be created.');
            }

            $localTable = $this->tableManager->ensureTableExists($profile, $activeVersion);
            if ($activeVersion->local_table_name !== $localTable) {
                $activeVersion->update(['local_table_name' => $localTable]);
            }

            $strategy = $this->registry->get($profile->strategy);

            // Ensure the strategy also uses the fresh active version if it accesses it via the profile
            // We can't easily force the strategy to reload, but we can ensure the profile has the correct relation loaded
            $profile->setRelation('activeSchemaVersion', $activeVersion);

            $strategy->run($profile, $source, $run);

            $run->update(['status' => 'success', 'finished_at' => now()]);
            event(new SyncSucceeded($run));

        } catch (Throwable $e) {
            // Enhance error message for common schema mismatch scenarios
            if ($e instanceof QueryException && $e->getCode() === '42S22') {
                $msg = "Column mismatch detected. This usually happens when you add columns to the mapping but force the sync to use an existing table that lacks these columns.\n";
                $msg .= "Original Error: " . $e->getMessage();
                $e = new Exception($msg, 0, $e);
            }

            $run->update(['status' => 'failed', 'finished_at' => now(), 'log_message' => $e->getMessage()]);
            event(new SyncFailed($run, $profile));
            throw $e;
        }

        return $run;
    }
}
