<?php

/**
 * This example provides a complete, end-to-end workflow for using the package.
 * It demonstrates connecting to a source, creating a versioned sync profile,
 * running the sync, and then creating a new version to handle a schema change.
 *
 * You can adapt this logic into a controller, service, or command in your application.
 */

use Andach\ExtractAndTransform\ExtractAndTransform;
use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Illuminate\Support\Facades\File;

// --- 1. Setup: Get the main package service and create a dummy source file ---
$extractor = app(ExtractAndTransform::class);

if (! File::exists(storage_path('app'))) {
    File::makeDirectory(storage_path('app'));
}
$csvPath = storage_path('app/workflow_products.csv');
File::put($csvPath, "id,name,price\n1,Blue Widget,10.00\n2,Red Widget,12.50");

// --- 2. Define and Save the Data Source ---
// This tells the package where to find the data. We give it a name to reference later.
$source = ExtractSource::firstOrCreate(
    ['name' => 'My CSV Warehouse'],
    ['connector' => 'csv', 'config' => ['path' => $csvPath]]
);

// --- 3. Create the Sync Profile ---
// This is the main "recipe" for the sync job. It defines the source, dataset, and strategy.
$profile = SyncProfile::create([
    'extract_source_id' => $source->id,
    'dataset_identifier' => $csvPath,
    'strategy' => 'full_refresh',
]);

// --- 4. Create and Activate the First Schema Version (v1) ---
// A version is an immutable snapshot of the schema and mapping.
// You must create and activate a version before you can run a sync.

// First, get the live schema from the source to create a "fingerprint" hash.
$liveSchema = $extractor->source($source->connector, $source->config)->getDataset($csvPath)->getSchema();
$liveSchemaHash = hash('sha256', json_encode($liveSchema->fields));

// Create the new version with your desired mapping.
$v1 = $profile->newVersion(
    // Column Mapping: Rename source columns to local names. `null` excludes a column.
    ['id' => 'sku', 'name' => 'product_name', 'price' => 'cost'],
    // Schema Overrides: Force a specific data type for a column.
    ['price' => 'decimal:8,2']
);

// "Lock in" the schema hash and activate the version.
$v1->update(['source_schema_hash' => $liveSchemaHash]);
$profile->activateVersion($v1);

// --- 5. Run the First Sync ---
// The package will now create a table named 'andach_csv_my_csv_warehouse_workflow_products_v1'
// and import the data according to your v1 mapping.
try {
    $run = $profile->run();
    echo "V1 Sync Succeeded. Rows added: {$run->rows_added}\n";
} catch (\Throwable $e) {
    echo 'V1 Sync Failed: '.$e->getMessage()."\n";
}

// --- 6. Handle a Source Schema Change ---
// Some time later, the source file is updated with a new column.
File::put($csvPath, "id,name,price,stock_level\n1,Blue Widget,10.50,100\n3,Green Widget,15.00,50");

// The next sync run will now fail with an exception because the file has changed.
try {
    $profile->run();
} catch (\Throwable $e) {
    echo 'Sync correctly failed as expected: '.$e->getMessage()."\n";

    // To fix this, we create and activate a new version (v2).
    $newLiveSchema = $extractor->source($source->connector, $source->config)->getDataset($csvPath)->getSchema();
    $newLiveSchemaHash = hash('sha256', json_encode($newLiveSchema->fields));

    $v2 = $profile->newVersion(
        // We update our mapping to include the new 'stock_level' column.
        ['id' => 'sku', 'name' => 'product_name', 'price' => 'cost', 'stock_level' => 'stock'],
        ['price' => 'decimal:8,2', 'stock_level' => 'int']
    );

    $v2->update(['source_schema_hash' => $newLiveSchemaHash]);
    $profile->activateVersion($v2);

    echo "Created and activated v2 to handle schema change.\n";
}

// --- 7. Run the Sync Again ---
// This run will now succeed. The package will create a *new* table for v2
// ('..._v2') and import the data using the new mapping. The old v1 table is left untouched.
try {
    $run = $profile->run();
    echo "V2 Sync Succeeded. Rows added: {$run->rows_added}\n";
    echo 'Data imported into new table: '.$profile->activeSchemaVersion->local_table_name."\n";
} catch (\Throwable $e) {
    echo 'V2 Sync Failed: '.$e->getMessage()."\n";
}
