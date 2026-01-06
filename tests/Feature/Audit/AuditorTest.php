<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Audit;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class AuditorTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        Schema::create('audit_source', function ($table) {
            $table->id();
            $table->string('name')->nullable();
            $table->string('code')->nullable();
            $table->string('status')->nullable();
            $table->integer('age')->nullable();
            $table->string('description')->nullable();
        });
    }

    public function test_it_can_audit_required_fields()
    {
        DB::table('audit_source')->insert([
            ['name' => 'Valid Item', 'code' => 'A1'],
            ['name' => null, 'code' => 'B2'], // Violation 1
            ['name' => 'Another Item', 'code' => null], // Violation 2
        ]);

        $run = ExtractAndTransform::audit('audit_source')
            ->identifiedBy('id')
            ->check([
                'name' => fn($rule) => $rule->required(),
                'code' => fn($rule) => $rule->required(),
            ])
            ->run();

        $this->assertEquals('success', $run->status);
        $this->assertEquals(2, $run->total_violations);
    }

    public function test_it_can_audit_in_list()
    {
        DB::table('audit_source')->insert([
            ['status' => 'active'],
            ['status' => 'pending'], // Violation
        ]);

        $run = ExtractAndTransform::audit('audit_source')
            ->identifiedBy('id')
            ->check([
                'status' => fn($rule) => $rule->in(['active', 'inactive']),
            ])
            ->run();

        $this->assertEquals(1, $run->total_violations);
        $log = $run->logs->first();
        $this->assertEquals('2', $log->row_identifier);
    }

    public function test_it_can_audit_not_in_list()
    {
        DB::table('audit_source')->insert([
            ['status' => 'active'],
            ['status' => 'invalid'], // Violation
        ]);

        $run = ExtractAndTransform::audit('audit_source')
            ->identifiedBy('id')
            ->check([
                'status' => fn($rule) => $rule->notIn(['invalid', 'pending']),
            ])
            ->run();

        $this->assertEquals(1, $run->total_violations);
    }

    public function test_it_can_audit_min_length()
    {
        DB::table('audit_source')->insert([
            ['description' => 'long enough'],
            ['description' => 'short'], // Violation
        ]);

        $run = ExtractAndTransform::audit('audit_source')
            ->identifiedBy('id')
            ->check([
                'description' => fn($rule) => $rule->minLength(10),
            ])
            ->run();

        $this->assertEquals(1, $run->total_violations);
        $log = $run->logs->first();
        $this->assertEquals('2', $log->row_identifier);
    }

    public function test_it_can_audit_max_length()
    {
        DB::table('audit_source')->insert([
            ['description' => 'short'],
            ['description' => 'this is way too long'], // Violation
        ]);

        $run = ExtractAndTransform::audit('audit_source')
            ->identifiedBy('id')
            ->check([
                'description' => fn($rule) => $rule->maxLength(10),
            ])
            ->run();

        $this->assertEquals(1, $run->total_violations);
        $log = $run->logs->first();
        $this->assertEquals('2', $log->row_identifier);
    }

    public function test_it_can_audit_greater_than()
    {
        DB::table('audit_source')->insert([
            ['age' => 20],
            ['age' => 10], // Violation
        ]);

        $run = ExtractAndTransform::audit('audit_source')
            ->identifiedBy('id')
            ->check([
                'age' => fn($rule) => $rule->greaterThan(18),
            ])
            ->run();

        $this->assertEquals(1, $run->total_violations);
        $log = $run->logs->first();
        $this->assertEquals('2', $log->row_identifier);
    }

    public function test_it_can_audit_less_than()
    {
        DB::table('audit_source')->insert([
            ['age' => 90],
            ['age' => 100], // Violation
        ]);

        $run = ExtractAndTransform::audit('audit_source')
            ->identifiedBy('id')
            ->check([
                'age' => fn($rule) => $rule->lessThan(99),
            ])
            ->run();

        $this->assertEquals(1, $run->total_violations);
        $log = $run->logs->first();
        $this->assertEquals('2', $log->row_identifier);
    }

    public function test_it_can_audit_regex_via_php_fallback()
    {
        DB::table('audit_source')->insert([
            ['code' => 'A1'],
            ['code' => 'bad-code'], // Violation
        ]);

        // This will run in PHP because SQLite doesn't support REGEXP
        $run = ExtractAndTransform::audit('audit_source')
            ->identifiedBy('id')
            ->check([
                'code' => fn($rule) => $rule->regex('/^[A-Z][0-9]$/'),
            ])
            ->run();

        $this->assertEquals(1, $run->total_violations);
        $log = $run->logs->first();
        $this->assertEquals('2', $log->row_identifier);
    }

    public function test_it_can_audit_custom_php_rule()
    {
        DB::table('audit_source')->insert([
            ['name' => 'Good Name'],
            ['name' => 'Bad Code'], // Violation
        ]);

        $run = ExtractAndTransform::audit('audit_source')
            ->identifiedBy('id')
            ->check([
                'name' => fn($rule) => $rule->custom(fn($val) => $val !== 'Bad Code'),
            ])
            ->run();

        $this->assertEquals(1, $run->total_violations);
        $log = $run->logs->first();
        $this->assertEquals('2', $log->row_identifier);
    }
}
