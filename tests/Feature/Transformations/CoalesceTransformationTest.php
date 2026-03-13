<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Transformations;

use Andach\ExtractAndTransform\Models\Transformation;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class CoalesceTransformationTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        Schema::create('source_customers', function ($table) {
            $table->id();
            $table->string('name');
            $table->string('telephone_no')->nullable();
            $table->string('mobile_no')->nullable();
        });

        DB::table('source_customers')->insert([
            // Case 1: mobile_no is valid
            ['name' => 'Alice', 'telephone_no' => '111', 'mobile_no' => '999'],
            // Case 2: mobile_no is '0', fallback to telephone_no
            ['name' => 'Bob', 'telephone_no' => '222', 'mobile_no' => '0'],
            // Case 3: mobile_no is null, fallback to telephone_no
            ['name' => 'Charlie', 'telephone_no' => '333', 'mobile_no' => null],
            // Case 4: Both are invalid, should result in null
            ['name' => 'David', 'telephone_no' => '0', 'mobile_no' => ''],
            // Case 5: Only telephone_no is valid
            ['name' => 'Eve', 'telephone_no' => '555', 'mobile_no' => null],
        ]);
    }

    public function test_it_coalesces_columns_correctly()
    {
        $config = [
            'source' => 'source_customers',
            'destination' => 'transformed_customers',
            'columns' => [
                'name' => ['type' => 'column', 'column' => 'name'],
                'cellphone' => [
                    'type' => 'coalesce',
                    'expressions' => [
                        [
                            'type' => 'case',
                            'when' => [
                                'column' => 'mobile_no',
                                'operator' => 'NOT IN',
                                'value' => ['', '0'], // NULL is handled by NOT IN
                            ],
                            'then' => ['type' => 'column', 'column' => 'mobile_no'],
                        ],
                        [
                            'type' => 'case',
                            'when' => [
                                'column' => 'telephone_no',
                                'operator' => 'NOT IN',
                                'value' => ['', '0'],
                            ],
                            'then' => ['type' => 'column', 'column' => 'telephone_no'],
                        ],
                    ],
                ],
            ],
        ];

        $transformation = Transformation::create([
            'name' => 'Coalesce Phone Numbers',
            'source_table' => 'source_customers',
            'destination_table_pattern' => 'transformed_customers',
            'configuration' => $config,
        ]);

        $transformation->run();

        $this->assertTrue(Schema::hasTable('transformed_customers_v1'));

        $results = DB::table('transformed_customers_v1')->orderBy('name')->get();

        $this->assertEquals('999', $results[0]->cellphone); // Alice
        $this->assertEquals('222', $results[1]->cellphone); // Bob
        $this->assertEquals('333', $results[2]->cellphone); // Charlie
        $this->assertNull($results[3]->cellphone);      // David
        $this->assertEquals('555', $results[4]->cellphone); // Eve
    }
}
