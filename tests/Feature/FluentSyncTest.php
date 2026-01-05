<?php

namespace Andach\ExtractAndTransform\Tests\Feature;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Facades\Schema;

class FluentSyncTest extends TestCase
{
    use RefreshDatabase;

    private string $fixturesPath;

    protected function setUp(): void
    {
        parent::setUp();
        $this->fixturesPath = __DIR__.'/../fixtures';
        if (! is_dir($this->fixturesPath)) {
            mkdir($this->fixturesPath, 0777, true);
        }
    }

    protected function tearDown(): void
    {
        if (is_dir($this->fixturesPath)) {
            File::deleteDirectory($this->fixturesPath);
        }
        parent::tearDown();
    }

    public function test_it_runs_a_full_fluent_workflow(): void
    {
        // 1. Create a source and a dummy file
        $path = $this->fixturesPath.'/test.csv';
        File::put($path, "id,name,email\n1,test1,test1@example.com");
        $source = ExtractAndTransform::createSource('My Source', 'csv', ['path' => $path]);

        // 2. Run the first sync with initial mapping
        $run1 = $source->sync($path)
            ->withStrategy('full_refresh')
            ->mapColumns(['id' => 'remote_id', 'name' => 'product_name'])
            ->toTable('products_v1')
            ->run();

        $this->assertEquals('success', $run1->status);
        $this->assertTrue(Schema::hasTable('products_v1'));
        $this->assertTrue(Schema::hasColumn('products_v1', 'product_name'));
        $this->assertFalse(Schema::hasColumn('products_v1', 'email')); // Was not in mapping

        // 3. Change the mapping and run again
        $run2 = $source->sync($path)
            ->withStrategy('full_refresh')
            ->mapColumns(['id' => 'remote_id', 'name' => 'name', 'email' => 'contact_email'])
            ->toTable('products_v2')
            ->run();

        $this->assertEquals('success', $run2->status);
        $this->assertTrue(Schema::hasTable('products_v2'));
        $this->assertTrue(Schema::hasColumn('products_v2', 'name'));
        $this->assertTrue(Schema::hasColumn('products_v2', 'contact_email'));
        $this->assertFalse(Schema::hasColumn('products_v2', 'product_name'));
    }

    public function test_it_handles_schema_evolution_without_explicit_table(): void
    {
        $path = $this->fixturesPath.'/evolution.csv';
        File::put($path, "id,name,price\n1,Widget,10.00");
        $source = ExtractAndTransform::createSource('Evolution Source', 'csv', ['path' => $path]);

        // Run 1: Only map id and name
        $source->sync($path)
            ->withStrategy('full_refresh')
            ->mapColumns(['id' => 'remote_id', 'name' => 'product_name'])
            ->toTable('evolution_v1')
            ->run();

        $this->assertTrue(Schema::hasTable('evolution_v1'));
        $this->assertFalse(Schema::hasColumn('evolution_v1', 'price'));

        // Run 2: Add price, NO toTable call
        // Should auto-generate evolution_v2
        $run2 = $source->sync($path)
            ->mapColumns(['id' => 'remote_id', 'name' => 'product_name', 'price' => 'price'])
            ->run();

        $this->assertEquals('success', $run2->status);
        $this->assertTrue(Schema::hasTable('evolution_v2'));
        $this->assertTrue(Schema::hasColumn('evolution_v2', 'price'));
    }

    public function test_it_handles_schema_evolution_with_explicit_table(): void
    {
        $path = $this->fixturesPath.'/explicit.csv';
        File::put($path, "id,name,price\n1,Widget,10.00");
        $source = ExtractAndTransform::createSource('Explicit Source', 'csv', ['path' => $path]);

        // Run 1
        $source->sync($path)
            ->withStrategy('full_refresh')
            ->mapColumns(['id' => 'remote_id', 'name' => 'product_name'])
            ->toTable('explicit_v1')
            ->run();

        // Run 2: Add price, Explicit toTable
        $run2 = $source->sync($path)
            ->mapColumns(['id' => 'remote_id', 'name' => 'product_name', 'price' => 'price'])
            ->toTable('explicit_v2')
            ->run();

        $this->assertEquals('success', $run2->status);
        $this->assertTrue(Schema::hasTable('explicit_v2'));
        $this->assertTrue(Schema::hasColumn('explicit_v2', 'price'));
    }

    public function test_source_exposes_underlying_model(): void
    {
        $path = $this->fixturesPath.'/model_test.csv';
        File::put($path, "id\n1");
        $source = ExtractAndTransform::createSource('Model Test Source', 'csv', ['path' => $path]);

        $this->assertNotNull($source->getModel());
        $this->assertNotNull($source->getModel()->id);
        $this->assertEquals('Model Test Source', $source->getModel()->name);
    }
}
