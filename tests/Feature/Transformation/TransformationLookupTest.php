<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Transformation;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Models\Transformation;
use Andach\ExtractAndTransform\Tests\TestCase;
use Andach\ExtractAndTransform\Transform\Expr;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class TransformationLookupTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        // 1. Source Table (Orders)
        Schema::create('orders', function ($table) {
            $table->id();
            $table->string('order_ref');
            $table->unsignedBigInteger('customer_id')->nullable();
            $table->string('status_code')->nullable();
        });

        // 2. Lookup Table 1 (Customers)
        Schema::create('customers', function ($table) {
            $table->id();
            $table->string('name');
            $table->string('email');
            $table->unsignedBigInteger('city_id')->nullable();
        });

        // 3. Lookup Table 2 (Statuses) - using string key
        Schema::create('statuses', function ($table) {
            $table->string('code')->primary();
            $table->string('label');
            $table->string('category');
        });

        // 4. Lookup Table 3 (Cities)
        Schema::create('cities', function ($table) {
            $table->id();
            $table->string('name');
            $table->unsignedBigInteger('country_id')->nullable();
        });

        // 5. Lookup Table 4 (Countries)
        Schema::create('countries', function ($table) {
            $table->id();
            $table->string('name');
        });

        // Seed Data
        DB::table('countries')->insert([
            ['id' => 1, 'name' => 'USA'],
            ['id' => 2, 'name' => 'UK'],
        ]);

        DB::table('cities')->insert([
            ['id' => 10, 'name' => 'New York', 'country_id' => 1],
            ['id' => 20, 'name' => 'London', 'country_id' => 2],
            ['id' => 30, 'name' => 'Lost City', 'country_id' => 99], // Invalid Country
        ]);

        DB::table('customers')->insert([
            ['id' => 101, 'name' => 'Alice', 'email' => 'alice@example.com', 'city_id' => 10],
            ['id' => 102, 'name' => 'Bob', 'email' => 'bob@example.com', 'city_id' => 20],
            ['id' => 103, 'name' => 'Charlie', 'email' => 'charlie@example.com', 'city_id' => 99], // Invalid City
            ['id' => 104, 'name' => 'Dave', 'email' => 'dave@example.com', 'city_id' => 30], // Valid City, Invalid Country
            ['id' => 105, 'name' => 'Eve', 'email' => 'eve@example.com', 'city_id' => null], // No City
        ]);

        DB::table('statuses')->insert([
            ['code' => 'P', 'label' => 'Pending', 'category' => 'Open'],
            ['code' => 'C', 'label' => 'Completed', 'category' => 'Closed'],
            ['code' => 'X', 'label' => 'Cancelled', 'category' => 'Closed'],
        ]);

        DB::table('orders')->insert([
            ['order_ref' => 'ORD-001', 'customer_id' => 101, 'status_code' => 'P'],
            ['order_ref' => 'ORD-002', 'customer_id' => 102, 'status_code' => 'C'],
            ['order_ref' => 'ORD-003', 'customer_id' => 999, 'status_code' => 'P'], // Unknown customer
            ['order_ref' => 'ORD-004', 'customer_id' => 101, 'status_code' => 'Z'], // Unknown status
            ['order_ref' => 'ORD-005', 'customer_id' => null, 'status_code' => null], // Nulls
            ['order_ref' => 'ORD-006', 'customer_id' => 103, 'status_code' => 'P'], // Charlie (Invalid City)
            ['order_ref' => 'ORD-007', 'customer_id' => 104, 'status_code' => 'P'], // Dave (Invalid Country)
            ['order_ref' => 'ORD-008', 'customer_id' => 105, 'status_code' => 'P'], // Eve (Null City)
        ]);
    }

    public function test_lookup_by_integer_id()
    {
        // Lookup customer name from customer_id
        $run = ExtractAndTransform::transform('Lookup Customer')
            ->from('orders')
            ->select([
                'ref' => 'order_ref',
                'customer_name' => Expr::lookup('customers', 'customer_id', 'id', 'name'),
            ])
            ->toTable('lookup_result_1')
            ->run();

        $this->assertEquals('success', $run->status);

        $rows = DB::table('lookup_result_1_v1')->orderBy('ref')->get();

        $this->assertEquals('Alice', $rows[0]->customer_name); // ORD-001
        $this->assertEquals('Bob', $rows[1]->customer_name);   // ORD-002
        $this->assertNull($rows[2]->customer_name);            // ORD-003 (999 not found)
        $this->assertEquals('Alice', $rows[3]->customer_name); // ORD-004
        $this->assertNull($rows[4]->customer_name);            // ORD-005 (null source)
    }

    public function test_lookup_by_string_code()
    {
        // Lookup status label from status_code
        $run = ExtractAndTransform::transform('Lookup Status')
            ->from('orders')
            ->select([
                'ref' => 'order_ref',
                'status_label' => Expr::lookup('statuses', 'status_code', 'code', 'label'),
            ])
            ->toTable('lookup_result_2')
            ->run();

        $this->assertEquals('success', $run->status);

        $rows = DB::table('lookup_result_2_v1')->orderBy('ref')->get();

        $this->assertEquals('Pending', $rows[0]->status_label);   // ORD-001 (P)
        $this->assertEquals('Completed', $rows[1]->status_label); // ORD-002 (C)
        $this->assertEquals('Pending', $rows[2]->status_label);   // ORD-003 (P)
        $this->assertNull($rows[3]->status_label);                // ORD-004 (Z not found)
        $this->assertNull($rows[4]->status_label);                // ORD-005 (null)
    }

    public function test_multiple_lookups_on_same_row()
    {
        // Lookup both customer email and status category
        $run = ExtractAndTransform::transform('Multi Lookup')
            ->from('orders')
            ->select([
                'ref' => 'order_ref',
                'cust_email' => Expr::lookup('customers', 'customer_id', 'id', 'email'),
                'stat_cat' => Expr::lookup('statuses', 'status_code', 'code', 'category'),
            ])
            ->toTable('lookup_result_3')
            ->run();

        $this->assertEquals('success', $run->status);

        $rows = DB::table('lookup_result_3_v1')->orderBy('ref')->get();

        // ORD-001: Alice (101), Pending (P) -> Open
        $this->assertEquals('alice@example.com', $rows[0]->cust_email);
        $this->assertEquals('Open', $rows[0]->stat_cat);

        // ORD-002: Bob (102), Completed (C) -> Closed
        $this->assertEquals('bob@example.com', $rows[1]->cust_email);
        $this->assertEquals('Closed', $rows[1]->stat_cat);
    }

    public function test_chained_lookup_operations()
    {
        // Lookup status label and then uppercase it
        $run = ExtractAndTransform::transform('Chained Lookup')
            ->from('orders')
            ->select([
                'ref' => 'order_ref',
                'upper_status' => Expr::lookup('statuses', 'status_code', 'code', 'label')->upper(),
            ])
            ->toTable('lookup_result_4')
            ->run();

        $this->assertEquals('success', $run->status);

        $rows = DB::table('lookup_result_4_v1')->orderBy('ref')->get();

        $this->assertEquals('PENDING', $rows[0]->upper_status);
        $this->assertEquals('COMPLETED', $rows[1]->upper_status);
    }

    public function test_lookup_from_json_config()
    {
        $config = [
            'selects' => [
                'ref' => [
                    'type' => 'column',
                    'column' => 'order_ref',
                ],
                'customer_name' => [
                    'type' => 'lookup',
                    'target_table' => 'customers',
                    'local_key' => 'customer_id',
                    'foreign_key' => 'id',
                    'target_column' => 'name',
                ],
            ],
            'wheres' => [],
        ];

        $transformation = Transformation::create([
            'name' => 'JSON Lookup Transform',
            'source_table' => 'orders',
            'destination_table_pattern' => 'lookup_result_json',
            'configuration' => $config,
        ]);

        $run = $transformation->run();

        $this->assertEquals('success', $run->status);

        $rows = DB::table('lookup_result_json_v1')->orderBy('ref')->get();

        $this->assertEquals('Alice', $rows[0]->customer_name);
        $this->assertEquals('Bob', $rows[1]->customer_name);
    }

    public function test_chained_lookup_via_then_method()
    {
        // Order -> Customer -> City -> Country
        $run = ExtractAndTransform::transform('Grandfather Lookup')
            ->from('orders')
            ->select([
                'ref' => 'order_ref',
                'country_name' => Expr::lookup('customers', 'customer_id', 'id', 'city_id')
                    ->then('cities', 'id', 'country_id')
                    ->then('countries', 'id', 'name'),
            ])
            ->toTable('lookup_result_chain')
            ->run();

        $this->assertEquals('success', $run->status);

        $rows = DB::table('lookup_result_chain_v1')->orderBy('ref')->get();

        // Alice -> New York -> USA
        $this->assertEquals('USA', $rows[0]->country_name);
        // Bob -> London -> UK
        $this->assertEquals('UK', $rows[1]->country_name);
    }

    public function test_chained_lookup_from_json_config()
    {
        $config = [
            'selects' => [
                'ref' => [
                    'type' => 'column',
                    'column' => 'order_ref',
                ],
                'country_name' => [
                    'type' => 'lookup',
                    'steps' => [
                        [
                            'table' => 'customers',
                            'local' => 'customer_id',
                            'foreign' => 'id',
                            'target' => 'city_id',
                        ],
                        [
                            'table' => 'cities',
                            'local' => 'city_id', // Implied from previous target, but stored explicitly in steps
                            'foreign' => 'id',
                            'target' => 'country_id',
                        ],
                        [
                            'table' => 'countries',
                            'local' => 'country_id',
                            'foreign' => 'id',
                            'target' => 'name',
                        ],
                    ],
                ],
            ],
            'wheres' => [],
        ];

        $transformation = Transformation::create([
            'name' => 'JSON Chain Transform',
            'source_table' => 'orders',
            'destination_table_pattern' => 'lookup_result_json_chain',
            'configuration' => $config,
        ]);

        $run = $transformation->run();

        $this->assertEquals('success', $run->status);

        $rows = DB::table('lookup_result_json_chain_v1')->orderBy('ref')->get();

        $this->assertEquals('USA', $rows[0]->country_name);
        $this->assertEquals('UK', $rows[1]->country_name);
    }

    public function test_chained_lookup_returns_null_on_missing_link()
    {
        // Order -> Customer -> City -> Country
        $run = ExtractAndTransform::transform('Broken Chain Lookup')
            ->from('orders')
            ->select([
                'ref' => 'order_ref',
                'country_name' => Expr::lookup('customers', 'customer_id', 'id', 'city_id')
                    ->then('cities', 'id', 'country_id')
                    ->then('countries', 'id', 'name'),
            ])
            ->toTable('lookup_result_broken')
            ->run();

        $this->assertEquals('success', $run->status);

        $rows = DB::table('lookup_result_broken_v1')->orderBy('ref')->get();

        // ORD-006: Charlie -> City 99 (Missing) -> NULL
        $rowCharlie = $rows->firstWhere('ref', 'ORD-006');
        $this->assertNull($rowCharlie->country_name);

        // ORD-007: Dave -> City 30 (Lost City) -> Country 99 (Missing) -> NULL
        $rowDave = $rows->firstWhere('ref', 'ORD-007');
        $this->assertNull($rowDave->country_name);

        // ORD-008: Eve -> City NULL -> NULL
        $rowEve = $rows->firstWhere('ref', 'ORD-008');
        $this->assertNull($rowEve->country_name);
    }

    public function test_combining_lookups_with_concat_and_string_functions()
    {
        // Format: "CITY (COUNTRY)"
        $run = ExtractAndTransform::transform('Complex Lookup')
            ->from('orders')
            ->select([
                'ref' => 'order_ref',
                'location' => Expr::concat(
                    Expr::lookup('customers', 'customer_id', 'id', 'city_id')
                        ->then('cities', 'id', 'name'),
                    ' (',
                    Expr::lookup('customers', 'customer_id', 'id', 'city_id')
                        ->then('cities', 'id', 'country_id')
                        ->then('countries', 'id', 'name'),
                    ')'
                )->upper(),
            ])
            ->toTable('lookup_result_complex')
            ->run();

        $this->assertEquals('success', $run->status);

        $rows = DB::table('lookup_result_complex_v1')->orderBy('ref')->get();

        // Alice -> New York (USA) -> NEW YORK (USA)
        $this->assertEquals('NEW YORK (USA)', $rows[0]->location);

        // Bob -> London (UK) -> LONDON (UK)
        $this->assertEquals('LONDON (UK)', $rows[1]->location);
    }
}
