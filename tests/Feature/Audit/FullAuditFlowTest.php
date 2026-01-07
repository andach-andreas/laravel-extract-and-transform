<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Audit;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class FullAuditFlowTest extends TestCase
{
    use RefreshDatabase;

    public function test_full_flow_with_integer_key()
    {
        // 1. Setup Source
        Schema::create('audit_flow_int', function ($table) {
            $table->id();
            $table->string('name')->nullable();
            $table->string('email')->nullable();
        });

        DB::table('audit_flow_int')->insert([
            ['name' => 'Alice', 'email' => 'alice@example.com'],
            ['name' => null, 'email' => 'bob@example.com'], // Violation 1: Name required
            ['name' => 'Charlie', 'email' => null], // Violation 2: Email required
        ]);

        // 2. Run Audit
        $run = ExtractAndTransform::audit('audit_flow_int')
            ->identifiedBy('id')
            ->check([
                'name' => fn ($r) => $r->required(),
                'email' => fn ($r) => $r->required(),
            ])
            ->run();

        $this->assertEquals(2, $run->total_violations);

        // 3. Apply Corrections
        // Fix Bob's name (ID 2)
        ExtractAndTransform::addCorrection('audit_flow_int', '2', 'name', 'Bob');
        // Fix Charlie's email (ID 3)
        ExtractAndTransform::addCorrection('audit_flow_int', '3', 'email', 'charlie@example.com');

        // 4. Reconcile
        ExtractAndTransform::reconcile('audit_flow_int', 'audit_flow_int_clean', 'id');

        // 5. Verify Output
        $bob = DB::table('audit_flow_int_clean')->where('id', 2)->first();
        $this->assertEquals('Bob', $bob->name);

        $charlie = DB::table('audit_flow_int_clean')->where('id', 3)->first();
        $this->assertEquals('charlie@example.com', $charlie->email);
    }

    public function test_full_flow_with_string_key()
    {
        // 1. Setup Source
        Schema::create('audit_flow_str', function ($table) {
            $table->string('sku')->primary();
            $table->string('desc')->nullable();
        });

        DB::table('audit_flow_str')->insert([
            ['sku' => 'A1', 'desc' => 'Item A'],
            ['sku' => 'B2', 'desc' => null], // Violation
        ]);

        // 2. Run Audit
        $run = ExtractAndTransform::audit('audit_flow_str')
            ->identifiedBy('sku')
            ->check([
                'desc' => fn ($r) => $r->required(),
            ])
            ->run();

        $this->assertEquals(1, $run->total_violations);

        // 3. Apply Correction
        ExtractAndTransform::addCorrection('audit_flow_str', 'B2', 'desc', 'Item B');

        // 4. Reconcile
        ExtractAndTransform::reconcile('audit_flow_str', 'audit_flow_str_clean', 'sku');

        // 5. Verify
        $itemB = DB::table('audit_flow_str_clean')->where('sku', 'B2')->first();
        $this->assertEquals('Item B', $itemB->desc);
    }

    public function test_full_flow_with_composite_key()
    {
        // 1. Setup Source
        Schema::create('audit_flow_comp', function ($table) {
            $table->string('cat');
            $table->string('code');
            $table->string('val')->nullable();
        });

        DB::table('audit_flow_comp')->insert([
            ['cat' => 'X', 'code' => '1', 'val' => 'ok'],
            ['cat' => 'Y', 'code' => '2', 'val' => null], // Violation
        ]);

        // 2. Run Audit
        $run = ExtractAndTransform::audit('audit_flow_comp')
            ->identifiedBy(['cat', 'code'])
            ->check([
                'val' => fn ($r) => $r->required(),
            ])
            ->run();

        $this->assertEquals(1, $run->total_violations);

        // 3. Apply Correction
        // Identifier is "Y-2" (imploded with -)
        ExtractAndTransform::addCorrection('audit_flow_comp', 'Y-2', 'val', 'fixed');

        // 4. Reconcile
        ExtractAndTransform::reconcile('audit_flow_comp', 'audit_flow_comp_clean', ['cat', 'code']);

        // 5. Verify
        $row = DB::table('audit_flow_comp_clean')->where('cat', 'Y')->where('code', '2')->first();
        $this->assertEquals('fixed', $row->val);
    }
}
