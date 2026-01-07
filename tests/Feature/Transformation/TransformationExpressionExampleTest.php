<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Transformation;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Tests\TestCase;
use Andach\ExtractAndTransform\Transform\Expr;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class TransformationExpressionExampleTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        Schema::create('example_source', function ($table) {
            $table->id();
            $table->string('name')->nullable();
            $table->integer('quantity')->nullable();
            $table->decimal('price', 8, 2)->nullable();
            $table->string('status_code')->nullable();
            $table->unsignedBigInteger('category_id')->nullable();
            $table->string('sku')->nullable(); // For split test
        });

        Schema::create('categories', function ($table) {
            $table->id();
            $table->string('title');
            $table->unsignedBigInteger('parent_id')->nullable();
        });

        Schema::create('parent_categories', function ($table) {
            $table->id();
            $table->string('name');
        });

        DB::table('categories')->insert([
            ['id' => 10, 'title' => 'Electronics', 'parent_id' => 100],
        ]);

        DB::table('parent_categories')->insert([
            ['id' => 100, 'name' => 'Goods'],
        ]);

        DB::table('example_source')->insert([
            [
                'name' => ' Widget ',
                'quantity' => 5,
                'price' => 10.55,
                'status_code' => 'A',
                'category_id' => 10,
                'sku' => 'ABC-12345',
            ],
        ]);
    }

    public function test_column_selection()
    {
        $run = ExtractAndTransform::transform('Expr Col')
            ->from('example_source')
            ->select([
                'item_name' => Expr::col('name'),
            ])
            ->toTable('res_col')
            ->run();

        $row = DB::table('res_col_v1')->first();
        $this->assertEquals(' Widget ', $row->item_name);
    }

    public function test_concatenation()
    {
        $run = ExtractAndTransform::transform('Expr Concat')
            ->from('example_source')
            ->select([
                'full_desc' => Expr::concat('Item: ', Expr::col('name'), ' (', Expr::col('status_code'), ')'),
            ])
            ->toTable('res_concat')
            ->run();

        $row = DB::table('res_concat_v1')->first();
        $this->assertEquals('Item:  Widget  (A)', $row->full_desc);
    }

    public function test_mapping()
    {
        $run = ExtractAndTransform::transform('Expr Map')
            ->from('example_source')
            ->select([
                'status' => Expr::map('status_code', ['A' => 'Active', 'I' => 'Inactive'])->default('Unknown'),
            ])
            ->toTable('res_map')
            ->run();

        $row = DB::table('res_map_v1')->first();
        $this->assertEquals('Active', $row->status);
    }

    public function test_lookup()
    {
        $run = ExtractAndTransform::transform('Expr Lookup')
            ->from('example_source')
            ->select([
                'cat_title' => Expr::lookup('categories', 'category_id', 'id', 'title'),
            ])
            ->toTable('res_lookup')
            ->run();

        $row = DB::table('res_lookup_v1')->first();
        $this->assertEquals('Electronics', $row->cat_title);
    }

    public function test_chained_lookup()
    {
        $run = ExtractAndTransform::transform('Expr Chained Lookup')
            ->from('example_source')
            ->select([
                'root_cat' => Expr::lookup('categories', 'category_id', 'id', 'parent_id')
                    ->then('parent_categories', 'id', 'name'),
            ])
            ->toTable('res_chained_lookup')
            ->run();

        $row = DB::table('res_chained_lookup_v1')->first();
        $this->assertEquals('Goods', $row->root_cat);
    }

    public function test_math_operations()
    {
        $run = ExtractAndTransform::transform('Expr Math')
            ->from('example_source')
            ->select([
                'total' => Expr::col('quantity')->multiply(Expr::col('price')),
                'discounted' => Expr::col('price')->subtract(2),
                'incremented' => Expr::col('quantity')->add(1),
                'halved' => Expr::col('price')->divide(2),
            ])
            ->toTable('res_math')
            ->run();

        $row = DB::table('res_math_v1')->first();
        $this->assertEquals(52.75, $row->total);
        $this->assertEquals(8.55, $row->discounted);
        $this->assertEquals(6, $row->incremented);
        $this->assertEquals(5.275, $row->halved);
    }

    public function test_string_functions()
    {
        $run = ExtractAndTransform::transform('Expr String')
            ->from('example_source')
            ->select([
                'upper_name' => Expr::col('name')->upper(),
                'lower_name' => Expr::col('name')->lower(),
                'trimmed_name' => Expr::col('name')->trim(),
                'replaced_name' => Expr::col('name')->replace('Widget', 'Gadget'),
                'sku_part_1' => Expr::col('sku')->split('-', 0),
                'sku_part_2' => Expr::col('sku')->split('-', 1),
            ])
            ->toTable('res_string')
            ->run();

        $row = DB::table('res_string_v1')->first();
        $this->assertEquals(' WIDGET ', $row->upper_name);
        $this->assertEquals(' widget ', $row->lower_name);
        $this->assertEquals('Widget', $row->trimmed_name);
        $this->assertEquals(' Gadget ', $row->replaced_name);
        $this->assertEquals('ABC', $row->sku_part_1);
        $this->assertEquals('12345', $row->sku_part_2);
    }

    public function test_split_missing_delimiter()
    {
        DB::table('example_source')->insert([
            [
                'name' => 'No Delim',
                'sku' => 'NODELIM',
            ],
        ]);

        $run = ExtractAndTransform::transform('Expr Split Missing')
            ->from('example_source')
            ->select([
                'part_0' => Expr::col('sku')->split('-', 0),
                'part_1' => Expr::col('sku')->split('-', 1),
            ])
            ->toTable('res_split_missing')
            ->run();

        $row = DB::table('res_split_missing_v1')->where('part_0', 'NODELIM')->first();

        $this->assertEquals('NODELIM', $row->part_0); // Whole string if index 0
        $this->assertNull($row->part_1); // Null if index 1 not found
    }

    public function test_complex_combination()
    {
        // (Price * Quantity) formatted as "Total: VALUE"
        $run = ExtractAndTransform::transform('Expr Complex')
            ->from('example_source')
            ->select([
                'report' => Expr::concat(
                    'Total: ',
                    Expr::col('quantity')->multiply(Expr::col('price'))
                ),
            ])
            ->toTable('res_complex')
            ->run();

        $row = DB::table('res_complex_v1')->first();
        // Note: DB concatenation of numbers usually works implicitly
        $this->assertEquals('Total: 52.75', $row->report);
    }

    public function test_case_expression()
    {
        $run = ExtractAndTransform::transform('Expr Case')
            ->from('example_source')
            ->select([
                'stock_status' => Expr::when('quantity', '<=', 0)->then('Out of Stock')->else('In Stock'),
                'is_expensive' => Expr::when('price', '>', 5)->then(1)->else(0),
            ])
            ->toTable('res_case')
            ->run();

        $row = DB::table('res_case_v1')->first();
        $this->assertEquals('In Stock', $row->stock_status);
        $this->assertEquals(1, $row->is_expensive);
    }

    public function test_numeric_functions()
    {
        $run = ExtractAndTransform::transform('Expr Numeric')
            ->from('example_source')
            ->select([
                'rounded' => Expr::col('price')->round(1),
                'rounded_int' => Expr::col('price')->round(0),
            ])
            ->toTable('res_numeric')
            ->run();

        $row = DB::table('res_numeric_v1')->first();
        $this->assertEquals(10.6, $row->rounded);
        $this->assertEquals(11, $row->rounded_int);
    }
}
