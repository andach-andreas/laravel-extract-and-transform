<?php

namespace App\Http\Controllers;

use Andach\ExtractAndTransform\ExtractAndTransform;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;

class ZeroConfigSyncController extends Controller
{
    /**
     * Handle a "Zero Config" sync request.
     *
     * This example demonstrates how the package behaves when you provide the absolute
     * minimum configuration. It relies heavily on the package's intelligent defaults.
     */
    public function __invoke(Request $request, ExtractAndTransform $extractor): JsonResponse
    {
        // 1. Define a simple SQL source
        $source = $extractor->createSource('My Database', 'sql', ['connection' => 'mysql']);

        // 2. Create a minimal Sync Profile
        $profile = SyncProfile::updateOrCreate(
            [
                'extract_source_id' => $source->getModel()->id,
                'dataset_identifier' => 'products', // The remote table name
            ],
            [
                // We choose the simplest strategy.
                'strategy' => 'full_refresh',

                // NOTICE: We are NOT providing a 'configuration' array here.
                // We are also NOT providing a 'local_table_name'.
                //
                // Here is what happens by default:
                //
                // 1. Local Table Name:
                //    Since we didn't specify one, the package generates a unique name:
                //    Format: andach_{connector}_{source_name}_{dataset}_{version}
                //    Result: andach_sql_my_database_products_v1
                //
                // 2. Column Mapping:
                //    Since 'column_mapping' is missing, the package assumes a 1:1 map.
                //    It will import ALL columns from the source using their original names.
                //
                // 3. Schema Inference:
                //    Since 'schema_overrides' is missing, the package inspects the source.
                //    - If source is 'INT', local becomes 'integer'.
                //    - If source is 'VARCHAR', local becomes 'string'.
                //    - If source is 'DATETIME', local becomes 'datetime'.
                //
                // 4. Metadata:
                //    The package always adds these columns automatically:
                //    - __id (Local Primary Key)
                //    - __source_id (The original ID, if applicable)
                //    - __content_hash (For change detection)
                //    - __is_deleted (For soft deletes)
                //    - __last_synced_at
            ]
        );

        // 3. Run the Sync
        try {
            $run = $profile->run();

            return response()->json([
                'message' => 'Zero-config sync successful.',
                'auto_generated_table' => $profile->fresh()->local_table_name,
                'rows_imported' => $run->rows_added,
            ]);

        } catch (\Throwable $e) {
            return response()->json(['error' => $e->getMessage()], 500);
        }
    }
}
