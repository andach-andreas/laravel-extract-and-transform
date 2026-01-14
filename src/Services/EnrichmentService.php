<?php

namespace Andach\ExtractAndTransform\Services;

use Andach\ExtractAndTransform\Enrichment\Contracts\CanPreprocessIdentifier;
use Andach\ExtractAndTransform\Enrichment\EnrichmentRegistry;
use Andach\ExtractAndTransform\Models\EnrichmentProfile;
use Andach\ExtractAndTransform\Models\EnrichmentRun;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Throwable;

class EnrichmentService
{
    public function __construct(
        private readonly EnrichmentRegistry $registry
    ) {}

    public function run(EnrichmentProfile $profile): EnrichmentRun
    {
        $run = $profile->runs()->create(['status' => 'running', 'started_at' => now()]);

        try {
            $provider = $this->registry->get($profile->provider);
            $sourceTable = $profile->source_table;
            $sourceColumn = $profile->source_column;
            $destinationTable = $profile->destination_table;

            $this->ensureDestinationTableExists($destinationTable, $sourceColumn);

            $sourceIds = DB::table($sourceTable)->pluck($sourceColumn);
            $cachedIds = DB::table($destinationTable)->pluck($sourceColumn);
            $idsToEnrich = $sourceIds->filter()->diff($cachedIds);

            if ($idsToEnrich->isEmpty()) {
                $run->update(['status' => 'success', 'finished_at' => now(), 'log_message' => 'No new records to enrich.']);

                return $run;
            }

            $rowsAdded = 0;
            $rowsToInsert = [];
            foreach ($idsToEnrich as $id) {
                $processedId = $id;
                if ($provider instanceof CanPreprocessIdentifier) {
                    $processedId = $provider->preprocessIdentifier($id);
                }

                $enrichedData = $provider->enrich($processedId, $profile->config);
                if ($enrichedData) {
                    // Use the original ID for the cache table identifier, not the processed one
                    $enrichedData[$sourceColumn] = $id;
                    $rowsToInsert[] = $enrichedData;
                    $rowsAdded++;
                }
            }

            if (! empty($rowsToInsert)) {
                foreach (array_chunk($rowsToInsert, 200) as $chunk) {
                    DB::table($destinationTable)->insert($chunk);
                }
            }

            $run->update([
                'status' => 'success',
                'finished_at' => now(),
                'rows_added' => $rowsAdded,
            ]);

        } catch (Throwable $e) {
            $run->update([
                'status' => 'failed',
                'finished_at' => now(),
                'log_message' => $e->getMessage(),
            ]);
            throw $e;
        }

        return $run;
    }

    private function ensureDestinationTableExists(string $tableName, string $identifierColumn): void
    {
        if (Schema::hasTable($tableName)) {
            return;
        }

        Schema::create($tableName, function ($table) use ($identifierColumn) {
            $table->string($identifierColumn)->primary();
            $table->string('company_name')->nullable();
            $table->string('company_status')->nullable();
            $table->json('registered_office_address')->nullable();
            $table->date('date_of_creation')->nullable();
            $table->json('sic_codes')->nullable();
            $table->timestamps();
        });
    }
}
