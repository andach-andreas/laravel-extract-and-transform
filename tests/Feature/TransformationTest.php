<?php

namespace Andach\ExtractAndTransform\Tests\Feature;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Tests\TestCase;
use Andach\ExtractAndTransform\Transform\Expr;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class TransformationTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        // Setup source tables
        Schema::create('raw_products', function ($table) {
            $table->id();
            $table->string('remote_id');
            $table->string('brand');
            $table->string('name');
            $table->string('status');
            $table->decimal('price', 10, 2);
            $table->unsignedBigInteger('cat_id');
        });

        Schema::create('categories', function ($table) {
            $table->id();
            $table->string('name');
        });

        // Seed data
        DB::table('categories')->insert([
            ['id' => 1, 'name' => 'Electronics'],
            ['id' => 2, 'name' => 'Home Goods'],
        ]);

        DB::table('raw_products')->insert([
            [
                'remote_id' => 'P001',
                'brand' => 'Acme',
                'name' => 'Widget',
                'status' => 'live',
                'price' => 100.00,
                'cat_id' => 1
            ],
            [
                'remote_id' => 'P002',
                'brand' => 'Beta',
                'name' => 'Chair',
                'status' => 'draft',
                'price' => 50.00,
                'cat_id' => 2
            ],
        ]);
    }

    public function testItCanRunASimpleTransformation()
    {
        $run = ExtractAndTransform::transform('Simple Transform')
            ->from('raw_products')
            ->select([
                'sku' => 'remote_id',
                'full_name' => Expr::concat(Expr::col('brand'), ' ', Expr::col('name')),
                'is_active' => Expr::map('status', ['live' => 1])->default(0),
                'category_name' => Expr::lookup('categories', 'cat_id', 'id', 'name'),
            ])
            ->toTable('clean_products')
            ->run();

        $this->assertEquals('success', $run->status);
        $this->assertEquals('clean_products_v1', $run->destination_table);
        $this->assertEquals(2, $run->rows_affected);

        $this->assertTrue(Schema::hasTable('clean_products_v1'));

        $p1 = DB::table('clean_products_v1')->where('sku', 'P001')->first();
        $this->assertEquals('Acme Widget', $p1->full_name);
        $this->assertEquals(1, $p1->is_active);
        $this->assertEquals('Electronics', $p1->category_name);
    }

    public function testItCanChainTransformations()
    {
        $run = ExtractAndTransform::transform('Chained Transform')
            ->from('raw_products')
            ->select([
                'sku' => Expr::col('remote_id')->lower(),
                'final_price' => Expr::col('price')->multiply(1.2)->add(5),
                'report_name' => Expr::concat(Expr::col('brand'), ': ', Expr::col('name'))->upper(),
                'category_slug' => Expr::lookup('categories', 'cat_id', 'id', 'name')
                                        ->lower()
                                        ->replace(' ', '-'),
            ])
            ->toTable('chained_products')
            ->run();

        $this->assertEquals('success', $run->status);
        $this->assertTrue(Schema::hasTable('chained_products_v1'));

        $p1 = DB::table('chained_products_v1')->where('sku', 'p001')->first();
        $this->assertNotNull($p1);
        $this->assertEquals(125.00, $p1->final_price); // (100 * 1.2) + 5
        $this->assertEquals('ACME: WIDGET', $p1->report_name);
        $this->assertEquals('electronics', $p1->category_slug);

        $p2 = DB::table('chained_products_v1')->where('sku', 'p002')->first();
        $this->assertNotNull($p2);
        $this->assertEquals(65.00, $p2->final_price); // (50 * 1.2) + 5
        $this->assertEquals('BETA: CHAIR', $p2->report_name);
        $this->assertEquals('home-goods', $p2->category_slug);
    }
}
