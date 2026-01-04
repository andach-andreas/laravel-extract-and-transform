<?php

namespace App\Http\Controllers;

use Andach\ExtractAndTransform\ExtractAndTransform;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;

class SqlSyncController extends Controller
{
    /**
     * Handle the incoming request to sync a SQL table.
     *
     * This example demonstrates the 'watermark' strategy, which is efficient
     * for large datasets as it only fetches records that have changed since
     * the last run.
     */
    public function __invoke(Request $request, ExtractAndTransform $extractor): JsonResponse
    {
        // --- 1. Define the Source ---
        // We connect to a remote database using a connection defined in config/database.php
        $source = $extractor->source('sql', ['connection' => 'mysql_read_replica']);
        $source->save('Production Read Replica');

        // --- 2. Create or Update the Sync Profile ---
        // We are syncing the 'users' table.
        $profile = SyncProfile::updateOrCreate(
            [
                'extract_source_id' => $source->getModel()->id,
                'dataset_identifier' => 'users',
            ],
            [
                'strategy' => 'watermark',
                'configuration' => [
                    // --- Watermark Settings ---
                    'watermark_column' => 'updated_at', // Track changes via this column
                    'mode' => 'upsert',               // Update existing rows, insert new ones
                    'primary_key' => 'id',            // Use 'id' to match rows

                    // --- Mapping Settings ---
                    'column_mapping' => [
                        'user_email' => 'email',
                        'full_name' => 'name',
                        'password' => null, // Security: Never sync passwords!
                    ],
                    'schema_overrides' => [
                        'id' => 'string', // Treat ID as string locally
                    ],
                ],
            ]
        );

        // --- 3. Run the Sync ---
        try {
            $run = $profile->run();

            return response()->json([
                'message' => 'Incremental sync completed.',
                'status' => $run->status,
                'stats' => [
                    'rows_added' => $run->rows_added,
                    'rows_updated' => $run->rows_updated,
                    'last_checkpoint' => $run->checkpoint,
                ],
            ]);

        } catch (\Throwable $e) {
            return response()->json([
                'message' => 'Sync failed.',
                'error' => $e->getMessage(),
            ], 500);
        }
    }
}
