<?php

namespace Andach\ExtractAndTransform\Tests\Feature;

use Andach\ExtractAndTransform\Models\Transformation;
use Andach\ExtractAndTransform\Services\TransformationService;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class TransformationFromFactoryTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        // Setup source tables
        Schema::create('raw_orders', function ($table) {
            $table->id();
            $table->string('order_ref');
            $table->decimal('subtotal', 10, 2);
            $table->decimal('tax', 10, 2);
            $table->string('status');
        });

        DB::table('raw_orders')->insert([
            ['order_ref' => 'ORD-001', 'subtotal' => 100.00, 'tax' => 20.00, 'status' => 'paid'],
            ['order_ref' => 'ORD-002', 'subtotal' => 50.00, 'tax' => 5.00, 'status' => 'pending'],
        ]);
    }

    public function testItCanRunTransformationFromStoredJsonConfig()
    {
        // Construct the JSON configuration manually to simulate what a GUI would save
        $config = [
            'selects' => [
                'ref' => [
                    'type' => 'string_function',
                    'function' => 'LOWER',
                    'column' => [
                        'type' => 'column',
                        'column' => 'order_ref'
                    ],
                    'arguments' => []
                ],
                'total' => [
                    'type' => 'math',
                    'operator' => '+',
                    'left' => [
                        'type' => 'column',
                        'column' => 'subtotal'
                    ],
                    'right' => [
                        'type' => 'column',
                        'column' => 'tax'
                    ]
                ],
                'is_paid' => [
                    'type' => 'map',
                    'column' => 'status',
                    'mapping' => ['paid' => 1],
                    'default' => 0
                ]
            ]
        ];

        // Create the Transformation model directly
        $transformation = Transformation::create([
            'name' => 'Factory Test Transform',
            'source_table' => 'raw_orders',
            'destination_table_pattern' => 'processed_orders',
            'configuration' => $config,
            'active_version' => 0
        ]);

        // Run the service without passing explicit selects
        $run = app(TransformationService::class)->run($transformation);

        $this->assertEquals('success', $run->status);
        $this->assertEquals('processed_orders_v1', $run->destination_table);

        $this->assertTrue(Schema::hasTable('processed_orders_v1'));

        $o1 = DB::table('processed_orders_v1')->where('ref', 'ord-001')->first();
        $this->assertNotNull($o1);
        $this->assertEquals(120.00, $o1->total);
        $this->assertEquals(1, $o1->is_paid);

        $o2 = DB::table('processed_orders_v1')->where('ref', 'ord-002')->first();
        $this->assertNotNull($o2);
        $this->assertEquals(55.00, $o2->total);
        $this->assertEquals(0, $o2->is_paid);
    }
}
