<?php

namespace App\Http\Controllers;

use Andach\ExtractAndTransform\ExtractAndTransform;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;

class LogStreamController extends Controller
{
    /**
     * Handle a sync request for an immutable log stream.
     *
     * This example demonstrates the 'watermark' strategy in 'append_only' mode.
     * This is highly optimized for data that is never updated or deleted, such as
     * audit logs, transaction history, or sensor data.
     */
    public function __invoke(Request $request, ExtractAndTransform $extractor): JsonResponse
    {
        // 1. Define the Source
        $source = $extractor->source('sql', ['connection' => 'audit_db']);
        $source->save('Audit Logs');

        // 2. Create a Sync Profile
        $profile = SyncProfile::updateOrCreate(
            [
                'extract_source_id' => $source->getModel()->id,
                'dataset_identifier' => 'system_events',
            ],
            [
                'strategy' => 'watermark',
                'configuration' => [
                    // We track progress using the auto-incrementing ID.
                    'watermark_column' => 'id',

                    // CRITICAL: 'append_only' mode.
                    // The package will NOT check if rows exist locally.
                    // It will simply INSERT every row where `id > last_synced_id`.
                    // This is much faster than 'upsert' mode.
                    'mode' => 'append_only',

                    // We can still map columns if we want.
                    'column_mapping' => [
                        'id' => 'remote_event_id',
                        'event_type' => 'type',
                        'payload' => 'data',
                        'created_at' => 'occurred_at',
                    ],
                ],
            ]
        );

        // 3. Run the Sync
        try {
            $run = $profile->run();

            return response()->json([
                'message' => 'Log stream sync completed.',
                'status' => $run->status,
                'stats' => [
                    'new_logs_imported' => $run->rows_added,
                    'last_checkpoint' => $run->checkpoint,
                ],
            ]);

        } catch (\Throwable $e) {
            return response()->json(['error' => $e->getMessage()], 500);
        }
    }
}
