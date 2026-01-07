<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Audit;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class AuditForeignKeyExistsTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        Schema::create('users', function ($table) {
            $table->id();
            $table->string('name');
        });

        Schema::create('orders', function ($table) {
            $table->id();
            $table->unsignedBigInteger('user_id')->nullable();
        });

        DB::table('users')->insert([
            ['id' => 10, 'name' => 'Alice'],
            ['id' => 20, 'name' => 'Bob'],
        ]);

        DB::table('orders')->insert([
            ['user_id' => 10],      // Valid
            ['user_id' => 99],      // Invalid (Violation)
            ['user_id' => null],    // Valid (Rule doesn't apply to nulls)
        ]);
    }

    public function test_exists_in_rule()
    {
        $run = ExtractAndTransform::audit('orders')
            ->identifiedBy('id')
            ->check([
                'user_id' => fn ($rule) => $rule->existsIn('users', 'id'),
            ])
            ->run();

        $this->assertEquals('success', $run->status);
        $this->assertEquals(1, $run->total_violations);

        $log = $run->logs->first();
        $this->assertEquals('2', $log->row_identifier); // The order with user_id 99
        $this->assertEquals('user_id', $log->column_name);
        $this->assertEquals('exists_in', $log->rule_name);
    }
}
