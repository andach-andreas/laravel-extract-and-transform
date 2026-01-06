<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Audit;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class AuditCrossColumnTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        Schema::create('cross_column_source', function ($table) {
            $table->id();
            $table->decimal('price', 8, 2)->nullable();
            $table->decimal('cost_price', 8, 2)->nullable();
            $table->timestamp('shipped_at')->nullable();
            $table->timestamp('ordered_at')->nullable();
            $table->string('password')->nullable();
            $table->string('password_confirmation')->nullable();
        });
    }

    public function test_greater_than_column()
    {
        DB::table('cross_column_source')->insert([
            ['price' => 100, 'cost_price' => 80], // Valid
            ['price' => 100, 'cost_price' => 120], // Invalid
        ]);

        $run = ExtractAndTransform::audit('cross_column_source')
            ->identifiedBy('id')
            ->check([
                'price' => fn($r) => $r->greaterThanColumn('cost_price'),
            ])
            ->run();

        $this->assertEquals(1, $run->total_violations);
        $this->assertEquals('2', $run->logs->first()->row_identifier);
    }

    public function test_less_than_column()
    {
        DB::table('cross_column_source')->insert([
            ['shipped_at' => '2023-01-05', 'ordered_at' => '2023-01-10'], // Invalid
            ['shipped_at' => '2023-01-10', 'ordered_at' => '2023-01-05'], // Valid
        ]);

        $run = ExtractAndTransform::audit('cross_column_source')
            ->identifiedBy('id')
            ->check([
                'ordered_at' => fn($r) => $r->lessThanColumn('shipped_at'),
            ])
            ->run();

        $this->assertEquals(1, $run->total_violations);
        $this->assertEquals('1', $run->logs->first()->row_identifier);
    }

    public function test_equal_to_column()
    {
        DB::table('cross_column_source')->insert([
            ['password' => 'secret', 'password_confirmation' => 'secret'], // Valid
            ['password' => 'secret', 'password_confirmation' => 'different'], // Invalid
        ]);

        $run = ExtractAndTransform::audit('cross_column_source')
            ->identifiedBy('id')
            ->check([
                'password' => fn($r) => $r->equalToColumn('password_confirmation'),
            ])
            ->run();

        $this->assertEquals(1, $run->total_violations);
        $this->assertEquals('2', $run->logs->first()->row_identifier);
    }

    public function test_not_equal_to_column()
    {
        DB::table('cross_column_source')->insert([
            ['price' => 100, 'cost_price' => 80], // Valid
            ['price' => 100, 'cost_price' => 100], // Invalid
        ]);

        $run = ExtractAndTransform::audit('cross_column_source')
            ->identifiedBy('id')
            ->check([
                'price' => fn($r) => $r->notEqualToColumn('cost_price'),
            ])
            ->run();

        $this->assertEquals(1, $run->total_violations);
        $this->assertEquals('2', $run->logs->first()->row_identifier);
    }
}
