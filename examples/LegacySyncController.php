<?php

namespace App\Http\Controllers;

use Andach\ExtractAndTransform\ExtractAndTransform;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;

class LegacySyncController extends Controller
{
    /**
     * Handle a sync request for a legacy system using the 'id_diff' strategy.
     *
     * This strategy is ideal for remote tables that have a reliable primary key
     * but lack an 'updated_at' timestamp, making it difficult to detect changes
     * incrementally. It detects additions and deletions.
     */
    public function __invoke(Request $request, ExtractAndTransform $extractor): JsonResponse
    {
        // 1. Define the Source
        // Assume 'legacy_db' is configured in config/database.php
        $source = $extractor->source('sql', ['connection' => 'legacy_db']);
        $source->save('Legacy Customer DB');

        // 2. Create a Sync Profile using 'id_diff' strategy
        $profile = SyncProfile::updateOrCreate(
            [
                'extract_source_id' => $source->getModel()->id,
                'dataset_identifier' => 'customers_no_timestamps', // Remote table name
            ],
            [
                'strategy' => 'id_diff',
                'configuration' => [
                    // The primary key column in the remote table
                    'primary_key' => 'customer_id',

                    // Example mapping: rename and exclude some columns
                    'column_mapping' => [
                        'customer_id' => 'legacy_customer_id',
                        'customer_name' => 'name',
                        'customer_address' => 'address',
                        'internal_legacy_field' => null, // Exclude
                    ],
                ],
            ]
        );

        // 3. Run the Sync
        try {
            $run = $profile->run();

            return response()->json([
                'message' => 'Legacy sync completed.',
                'status' => $run->status,
                'stats' => [
                    'rows_added' => $run->rows_added,
                    'rows_deleted' => $run->rows_deleted, // id_diff detects deletions!
                    'local_table' => $profile->fresh()->local_table_name,
                ],
            ]);

        } catch (\Throwable $e) {
            return response()->json(['error' => $e->getMessage()], 500);
        }
    }
}
