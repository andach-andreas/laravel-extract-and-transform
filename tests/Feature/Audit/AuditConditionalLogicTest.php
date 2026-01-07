<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Audit;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class AuditConditionalLogicTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        Schema::create('conditional_source', function ($table) {
            $table->id();
            $table->string('country_code');
            $table->string('postcode')->nullable();
        });

        DB::table('conditional_source')->insert([
            ['country_code' => 'US', 'postcode' => '12345'],      // Valid US
            ['country_code' => 'US', 'postcode' => '12345-6789'], // Valid US
            ['country_code' => 'US', 'postcode' => 'ABCDE'],      // Invalid US (Violation)
            ['country_code' => 'UK', 'postcode' => 'SW1A 1AA'],   // Valid UK (Rule doesn't apply)
            ['country_code' => 'UK', 'postcode' => '12345'],      // Valid UK (Rule doesn't apply)
        ]);
    }

    public function test_conditional_rule_is_applied()
    {
        $run = ExtractAndTransform::audit('conditional_source')
            ->identifiedBy('id')
            ->check([
                'postcode' => fn ($rule) => $rule->when('country_code', '=', 'US', function ($subRule) {
                    $subRule->regex('/^\d{5}(-\d{4})?$/');
                }),
            ])
            ->run();

        $this->assertEquals('success', $run->status);
        $this->assertEquals(1, $run->total_violations);

        $log = $run->logs->first();
        $this->assertEquals('3', $log->row_identifier); // The 'ABCDE' row
        $this->assertEquals('postcode', $log->column_name);
        $this->assertEquals('when', $log->rule_name);
    }
}
