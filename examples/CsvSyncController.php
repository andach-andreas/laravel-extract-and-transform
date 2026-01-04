<?php

namespace App\Http\Controllers;

use Andach\ExtractAndTransform\ExtractAndTransform;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Illuminate\Http\JsonResponse;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\File;

class CsvSyncController extends Controller
{
    /**
     * Handle the incoming request to sync a CSV file.
     *
     * This example demonstrates a 'full_refresh' strategy, which is ideal for
     * smaller datasets where you want to completely replace the local data
     * with the latest version from the file.
     */
    public function __invoke(Request $request, ExtractAndTransform $extractor): JsonResponse
    {
        // --- 1. Define the Source ---
        // In a real app, this path might come from a file upload or config.
        // For this example, we'll ensure a dummy file exists.
        $csvPath = storage_path('app/example_products.csv');
        $this->ensureDummyCsvExists($csvPath);

        // Create or retrieve the source definition.
        $source = $extractor->source('csv', ['path' => $csvPath]);
        $source->save('Example CSV Products');

        // --- 2. Create or Update the Sync Profile ---
        // The profile acts as the configuration for this specific sync job.
        $profile = SyncProfile::updateOrCreate(
            [
                'extract_source_id' => $source->getModel()->id,
                'dataset_identifier' => $csvPath,
            ],
            [
                'strategy' => 'full_refresh',
                'configuration' => [
                    // Map source columns to friendly local names
                    'column_mapping' => [
                        'product_id' => 'sku',
                        'product_name' => 'name',
                        'stock_count' => 'inventory_level',
                        'internal_code' => null, // Exclude this column
                    ],
                    // Force specific data types for the local table
                    'schema_overrides' => [
                        'stock_count' => 'int',
                    ],
                ],
            ]
        );

        // --- 3. Run the Sync ---
        try {
            // The run() method executes the sync within a database transaction.
            $run = $profile->run();

            return response()->json([
                'message' => 'Sync completed successfully.',
                'status' => $run->status,
                'stats' => [
                    'rows_added' => $run->rows_added,
                    'local_table' => $profile->fresh()->local_table_name,
                ],
            ]);

        } catch (\Throwable $e) {
            return response()->json([
                'message' => 'Sync failed.',
                'error' => $e->getMessage(),
            ], 500);
        }
    }

    private function ensureDummyCsvExists(string $path): void
    {
        if (! File::exists(dirname($path))) {
            File::makeDirectory(dirname($path), 0755, true);
        }
        if (! File::exists($path)) {
            File::put($path, "product_id,product_name,stock_count,internal_code\n101,Blue Widget,50,BW-01\n102,Red Widget,35,RW-02");
        }
    }
}
