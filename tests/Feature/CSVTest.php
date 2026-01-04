<?php

namespace Andach\ExtractAndTransform\Tests\Feature;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Facades\Storage;

class CSVTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        Storage::fake('local');
    }

    public function test_it_can_sync_from_a_local_csv_file_using_absolute_path()
    {
        // Create a temporary CSV file
        $path = sys_get_temp_dir().'/test_products.csv';
        $content = "id,name,price\n1,Widget,10.50\n2,Gadget,20.00";
        file_put_contents($path, $content);

        try {
            ExtractAndTransform::createSource('Local CSV', 'csv', ['path' => $path]);

            ExtractAndTransform::source('Local CSV')
                ->sync($path)
                ->withStrategy('full_refresh')
                ->toTable('products_local')
                ->run();

            $this->assertTrue(Schema::hasTable('products_local'));
            $this->assertDatabaseCount('products_local', 2);

            $first = DB::table('products_local')->where('id', 1)->first();
            $this->assertEquals('Widget', $first->name);
            $this->assertEquals(10.50, $first->price);
        } finally {
            @unlink($path);
        }
    }

    public function test_it_can_sync_from_a_csv_on_a_storage_disk()
    {
        // Setup fake storage
        Storage::fake('s3');
        $path = 'exports/products.csv';
        $content = "sku,product_name,stock\nA100,Hammer,50\nB200,Drill,15";

        Storage::disk('s3')->put($path, $content);

        ExtractAndTransform::createSource('S3 CSV', 'csv', [
            'disk' => 's3',
            'path' => $path,
        ]);

        ExtractAndTransform::source('S3 CSV')
            ->sync($path)
            ->withStrategy('full_refresh')
            ->toTable('products_s3')
            ->run();

        $this->assertTrue(Schema::hasTable('products_s3'));
        $this->assertDatabaseCount('products_s3', 2);

        $hammer = DB::table('products_s3')->where('sku', 'A100')->first();
        $this->assertEquals('Hammer', $hammer->product_name);
        $this->assertEquals(50, $hammer->stock);
    }

    public function test_it_handles_missing_files_gracefully()
    {
        Storage::fake('sftp');
        $path = 'missing.csv';

        ExtractAndTransform::createSource('Missing CSV', 'csv', [
            'disk' => 'sftp',
            'path' => $path,
        ]);

        $this->expectException(\RuntimeException::class);
        // The message might vary slightly depending on where it fails (test vs streamRows)
        // but our code throws "Failed to open stream..." in streamRows
        $this->expectExceptionMessage('Failed to open stream for CSV file on disk [sftp]: missing.csv');

        ExtractAndTransform::source('Missing CSV')
            ->sync($path)
            ->withStrategy('full_refresh')
            ->toTable('products_missing')
            ->run();
    }
}
