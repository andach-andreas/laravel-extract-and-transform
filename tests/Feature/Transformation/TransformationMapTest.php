<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Transformation;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Models\Transformation;
use Andach\ExtractAndTransform\Tests\TestCase;
use Andach\ExtractAndTransform\Transform\Expr;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class TransformationMapTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        Schema::create('map_source', function ($table) {
            $table->id();
            $table->string('status_code')->nullable();
            $table->integer('type_id')->nullable();
            $table->string('category')->nullable();
        });

        DB::table('map_source')->insert([
            ['status_code' => 'A', 'type_id' => 1, 'category' => 'cat1'],
            ['status_code' => 'I', 'type_id' => 2, 'category' => 'cat2'],
            ['status_code' => 'X', 'type_id' => 3, 'category' => 'cat3'], // Unknown status
            ['status_code' => null, 'type_id' => 99, 'category' => null],
        ]);
    }

    public function test_map_string_to_string()
    {
        $run = ExtractAndTransform::transform('Map String String')
            ->from('map_source')
            ->select([
                'original' => 'status_code',
                'status_label' => Expr::map('status_code', [
                    'A' => 'Active',
                    'I' => 'Inactive',
                ])->default('Unknown'),
            ])
            ->toTable('map_result_1')
            ->run();

        $this->assertEquals('success', $run->status);

        $rows = DB::table('map_result_1_v1')->get();

        $this->assertEquals('Active', $rows[0]->status_label);
        $this->assertEquals('Inactive', $rows[1]->status_label);
        $this->assertEquals('Unknown', $rows[2]->status_label); // 'X' -> default
        $this->assertEquals('Unknown', $rows[3]->status_label); // null -> default
    }

    public function test_map_string_to_int()
    {
        $run = ExtractAndTransform::transform('Map String Int')
            ->from('map_source')
            ->select([
                'original' => 'status_code',
                'is_active' => Expr::map('status_code', [
                    'A' => 1,
                    'I' => 0,
                ])->default(-1),
            ])
            ->toTable('map_result_2')
            ->run();

        $this->assertEquals('success', $run->status);

        $rows = DB::table('map_result_2_v1')->get();

        $this->assertEquals(1, $rows[0]->is_active);
        $this->assertEquals(0, $rows[1]->is_active);
        $this->assertEquals(-1, $rows[2]->is_active);
    }

    public function test_map_numeric_keys()
    {
        // Note: In PHP arrays, integer-like string keys are cast to integers.
        // But Expr::map implementation quotes keys.
        // Let's see if it handles integer columns correctly against string-quoted keys in SQL.
        // Usually `CASE type_id WHEN '1' ...` works for integer columns in MySQL/SQLite.

        $run = ExtractAndTransform::transform('Map Numeric Keys')
            ->from('map_source')
            ->select([
                'original' => 'type_id',
                'type_name' => Expr::map('type_id', [
                    1 => 'Type One',
                    2 => 'Type Two',
                ])->default('Other'),
            ])
            ->toTable('map_result_3')
            ->run();

        $this->assertEquals('success', $run->status);

        $rows = DB::table('map_result_3_v1')->get();

        $this->assertEquals('Type One', $rows[0]->type_name);
        $this->assertEquals('Type Two', $rows[1]->type_name);
        $this->assertEquals('Other', $rows[2]->type_name); // 3 -> Other
    }

    public function test_map_without_default_returns_null()
    {
        $run = ExtractAndTransform::transform('Map No Default')
            ->from('map_source')
            ->select([
                'original' => 'status_code',
                'mapped' => Expr::map('status_code', [
                    'A' => 'Active',
                ]), // No default() called
            ])
            ->toTable('map_result_4')
            ->run();

        $this->assertEquals('success', $run->status);

        $rows = DB::table('map_result_4_v1')->get();

        $this->assertEquals('Active', $rows[0]->mapped);
        $this->assertNull($rows[1]->mapped); // 'I' not in map -> NULL
        $this->assertNull($rows[2]->mapped); // 'X' not in map -> NULL
    }

    public function test_map_mixed_types_in_values()
    {
        // Mapping to different types in the same column (e.g. string and int)
        // usually results in the column being a string type in the DB.

        $run = ExtractAndTransform::transform('Map Mixed Values')
            ->from('map_source')
            ->select([
                'original' => 'status_code',
                'mixed_val' => Expr::map('status_code', [
                    'A' => 100,
                    'I' => 'Inactive',
                ])->default(null),
            ])
            ->toTable('map_result_5')
            ->run();

        $this->assertEquals('success', $run->status);

        $rows = DB::table('map_result_5_v1')->get();

        // SQLite/MySQL might return '100' as string or 100 as int depending on driver and column type inference.
        // Since one value is 'Inactive', the column likely becomes TEXT/VARCHAR.
        $this->assertEquals(100, $rows[0]->mixed_val);
        $this->assertEquals('Inactive', $rows[1]->mixed_val);
    }

    public function test_map_from_json_config()
    {
        $config = [
            'selects' => [
                'original' => [
                    'type' => 'column',
                    'column' => 'status_code',
                ],
                'status_label' => [
                    'type' => 'map',
                    'column' => 'status_code',
                    'mapping' => [
                        'A' => 'Active',
                        'I' => 'Inactive',
                    ],
                    'default' => 'Unknown',
                ],
            ],
            'wheres' => [],
        ];

        $transformation = Transformation::create([
            'name' => 'JSON Map Transform',
            'source_table' => 'map_source',
            'destination_table_pattern' => 'map_result_json',
            'configuration' => $config,
        ]);

        $run = $transformation->run();

        $this->assertEquals('success', $run->status);

        $rows = DB::table('map_result_json_v1')->get();

        $this->assertEquals('Active', $rows[0]->status_label);
        $this->assertEquals('Inactive', $rows[1]->status_label);
        $this->assertEquals('Unknown', $rows[2]->status_label);
    }
}
