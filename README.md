# Andach's Laravel Extract Data Package

This is a package to extract and copy data from various sources, including CSVs and SQL tables. It provides a simple, programmatic API for connecting to data sources, inspecting their datasets, and synchronizing them to local database tables.

The package is built with robustness in mind, featuring:
- **Schema Versioning:** Safely manage changes to your source data or local mappings over time. The package can detect schema drift and prevent data corruption.
- **Transactional Syncs:** Guarantees that a sync operation either completes fully or fails completely, preventing partially-synced, inconsistent data.
- **Resilient Connections:** Automatically retries connections to external sources with exponential backoff, making syncs resilient to temporary network issues.
- **Flexible Schema Mapping:** Full control over how source columns are named, typed, or excluded in your local database.
- **Extensible Strategies:** A strategy pattern for handling different sync scenarios.

## Installation

You can install the package via composer:

```bash
composer require andach/extract-and-transform
```

You can publish and run the migrations with:

```bash
php artisan vendor:publish --provider="Andach\ExtractAndTransform\ExtractAndTransformServiceProvider"
php artisan migrate
```

## Usage

The following example demonstrates the complete workflow: defining a source, creating a versioned sync profile, running a sync, and handling a schema change by creating a new version.

### Simple CSV Synchronisation

This example shows a connection to a CSV file. It will automatically search for and create columns unless mapColumns() is specified

```php
use Andach\ExtractAndTransform\Facades\ExtractAndTransform;

$path = storage_path('app/products.csv');
$source = ExtractAndTransform::createSource('My Source', 'csv', ['path' => $path]);

$run = $source->sync($path)
    ->withStrategy('full_refresh')
    ->mapColumns(['id' => 'remote_id', 'name' => 'product_name'])   // Optional
    ->toTable('products_v1')                                        // Optional
    ->run();
```

### SQL Database Synchronisation

This example demonstrates connecting to an external MySQL database and syncing two different tables (`users` and `orders`) from that single source.

```php
use Andach\ExtractAndTransform\Facades\ExtractAndTransform;

// 1. Create the Source (Connection) once
$source = ExtractAndTransform::createSource('Legacy DB', 'sql', [
    'driver' => 'mysql',
    'host' => '192.168.1.50',
    'database' => 'legacy_app',
    'username' => 'readonly_user',
    'password' => 'secret',
]);

// 2. Sync the 'users' table
$source->sync('users')
    ->withStrategy('full_refresh')
    ->toTable('legacy_users')
    ->run();

// 3. Sync the 'orders' table
$source->sync('orders')
    ->withStrategy('full_refresh')
    ->mapColumns(['order_id' => 'id', 'total_amount' => 'amount'])
    ->toTable('legacy_orders')
    ->run();
```

## Data Transformations

The package includes a powerful, database-agnostic DSL for transforming your data *after* it has been extracted. This allows you to perform complex operations like joins (lookups), string manipulation, and math entirely within your database, ensuring high performance.

### Basic Usage

Use the `transform()` method to define a transformation pipeline. The `Expr` class provides a fluent API for building expressions.

```php
use Andach\ExtractAndTransform\Transform\Expr;

ExtractAndTransform::transform('Clean Products')
    ->from('raw_products')
    ->select([
        // Simple Rename
        'sku' => 'remote_id',
        
        // Concatenation
        'full_name' => Expr::concat(Expr::col('brand'), ' ', Expr::col('name')),
        
        // Static Mapping (Case Statement)
        'is_active' => Expr::map('status', ['live' => 1])->default(0),
        
        // Math Operations
        'tax_amount' => Expr::col('price')->multiply(0.2),
        
        // Lookups (Left Joins)
        'category_name' => Expr::lookup('categories', 'cat_id', 'id', 'name'),
    ])
    ->toTable('clean_products')
    ->run();
```

### Chaining Transformations

You can chain multiple operations together to build complex logic.

```php
ExtractAndTransform::transform('Advanced Transform')
    ->from('raw_products')
    ->select([
        // Chain math: (price * 1.2) + 5
        'final_price' => Expr::col('price')->multiply(1.2)->add(5),

        // Chain string functions: LOWER(REPLACE(name, ' ', '-'))
        'slug' => Expr::col('name')->replace(' ', '-')->lower(),

        // Combine concatenation and string functions
        'report_name' => Expr::concat(Expr::col('brand'), ': ', Expr::col('name'))->upper(),
    ])
    ->toTable('advanced_products')
    ->run();
```

### Available Expressions

*   **`Expr::col(string $column)`**: Selects a column.
*   **`Expr::concat(...$parts)`**: Concatenates columns and strings.
*   **`Expr::map(string $column, array $mapping)`**: Creates a `CASE WHEN` statement.
*   **`Expr::lookup(string $table, string $localKey, string $foreignKey, string $targetColumn)`**: Performs a `LEFT JOIN` to fetch a value from another table.

### Chainable Methods

*   **Numeric:** `add()`, `subtract()`, `multiply()`, `divide()`
*   **String:** `upper()`, `lower()`, `trim()`, `replace($search, $replace)`

### Data-Driven Transformations (GUI Support)

The transformation engine is designed to be fully serializable, making it easy to build a GUI on top of it. You can save the transformation configuration as a JSON object in the database and execute it later without writing any PHP code.

**Example JSON Configuration:**
This JSON corresponds to a transformation that lowercases a reference code and calculates a total.

```json
{
    "selects": {
        "ref": {
            "type": "string_function",
            "function": "LOWER",
            "column": {
                "type": "column",
                "column": "order_ref"
            },
            "arguments": []
        },
        "total": {
            "type": "math",
            "operator": "+",
            "left": {
                "type": "column",
                "column": "subtotal"
            },
            "right": {
                "type": "column",
                "column": "tax"
            }
        },
        "is_paid": {
            "type": "map",
            "column": "status",
            "mapping": { "paid": 1 },
            "default": 0
        }
    }
}
```

**Executing the Saved Transformation:**

```php
use Andach\ExtractAndTransform\Models\Transformation;
use Andach\ExtractAndTransform\Services\TransformationService;

// 1. Load the transformation model (which contains the JSON config)
$transformation = Transformation::where('name', 'My Saved Transform')->first();

// 2. Run it
app(TransformationService::class)->run($transformation);
```

The service automatically rehydrates the JSON configuration into executable expressions.

## Core Concepts

Understanding the distinction between a **Source** and a **Sync** is key to using this package effectively.

### 1. Source (The Connection)
A **Source** represents the configuration required to connect to an external system.
*   **Examples:** A MySQL database connection, a Shopify API credential, or a path to a CSV file.
*   **Role:** It knows *how* to connect, but not *what* to fetch.

### 2. Sync (The Dataset)
A **Sync** represents a specific dataset retrieved from a Source.
*   **Examples:** The `users` table from a database, the `orders` endpoint from an API, or the contents of a CSV file.
*   **Role:** It holds the state (mappings, schema versions, run history) and the strategy for that specific dataset.

### Sync Strategies

The package supports multiple strategies for synchronizing data, allowing you to choose the best approach for your specific use case.

*   **`full_refresh`**: The simplest strategy. It truncates the local table and re-imports all data from the source. Best for small datasets or when the source does not support incremental updates.
*   **`watermark`**: Efficient for large, append-only or mutable datasets. It uses a "watermark" column (e.g., `updated_at` or `id`) to fetch only rows that have changed or been added since the last sync.
    *   **Modes:** Supports `append_only` (inserts new rows) and `upsert` (updates existing rows based on primary key).
*   **`content_hash`**: Useful when the source lacks a reliable `updated_at` column. It calculates a hash of the row's content to detect changes. It can also detect and handle deletions (soft deletes) by comparing source and local hashes.
*   **`id_diff`**: Fetches all IDs from the source and compares them with local IDs. It then fetches full rows only for new IDs. Useful for detecting new records and deletions when fetching the full dataset is too expensive, but fetching a list of IDs is cheap.

### Metadata Columns

The package automatically adds several reserved metadata columns to your local tables to manage synchronization state and history. You should **not** use these names for your own mapped columns.

*   **`__id`**: The local primary key (BigInteger).
*   **`__source_id`**: The original ID from the source (nullable).
*   **`__content_hash`**: A hash of the row's content, used by the `content_hash` strategy.
*   **`__is_deleted`**: A boolean flag indicating if the row has been deleted in the source (used by `content_hash` and `id_diff` strategies).
*   **`__last_synced_at`**: The timestamp when the row was last synced/updated in the local table.

### Simple vs. Complex Workflows

*   **Simple (1-to-1):** For a single CSV file, the Source and Sync might seem redundant. You define the Source (file path) and Sync (file content) together.
*   **Complex (1-to-Many):** For a Database, you define the **Source** once (the connection). You then define multiple **Syncs**—one for each table you want to extract. This keeps your credentials centralized while allowing independent management of each table's sync schedule and schema.

## Handling Schema Changes

When you change your column mapping (e.g., adding a new column), the package automatically detects this change and creates a **new schema version**.

### Automatic Table Naming (Recommended)
If you do **not** explicitly specify a table name using `toTable()`, the package will automatically handle the table creation for you.
*   **Version 1:** Creates table `csv_my_source_products_v1`
*   **Version 2 (Schema Change):** Automatically creates `csv_my_source_products_v2`

This ensures that your sync never fails due to missing columns, and you retain the old data in the `v1` table.

### Explicit Table Naming
If you explicitly specify a table name using `toTable('products_v1')`, you must be careful when changing the schema.

If you add a column to `mapColumns` but keep `toTable('products_v1')`, the sync will fail with a **Column not found** error because the package will try to insert the new column into the existing `products_v1` table (which doesn't have it).

**Correct Approach:**
When changing the schema, you should either:
1.  **Remove `toTable()`** to let the package auto-generate the next version name.
2.  **Update the table name** manually (e.g., change to `toTable('products_v2')`).

## Testing

```bash
docker compose exec -e XDEBUG_MODE=coverage app ./vendor/bin/phpunit --coverage-html phpunit_reports
```
