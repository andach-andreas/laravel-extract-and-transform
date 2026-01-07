<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Audit;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class AuditResultHelpersTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        Schema::create('helper_source', function ($table) {
            $table->id();
            $table->string('name')->nullable();
            $table->string('email')->nullable();
        });

        DB::table('helper_source')->insert([
            ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com'],
            ['id' => 2, 'name' => null, 'email' => 'bob@example.com'], // Fail name
            ['id' => 3, 'name' => 'Charlie', 'email' => null], // Fail email
            ['id' => 4, 'name' => null, 'email' => null], // Fail both
        ]);
    }

    public function test_group_by_helpers()
    {
        $run = ExtractAndTransform::audit('helper_source')
            ->identifiedBy('id')
            ->check([
                'name' => fn ($r) => $r->required(),
                'email' => fn ($r) => $r->required(),
            ])
            ->run();

        $byRow = $run->getViolationsByRow();
        $this->assertCount(3, $byRow); // Rows 2, 3, 4
        $this->assertCount(1, $byRow['2']);
        $this->assertCount(1, $byRow['3']);
        $this->assertCount(2, $byRow['4']);

        $byCol = $run->getViolationsByColumn();
        $this->assertCount(2, $byCol); // name, email
        $this->assertCount(2, $byCol['name']); // Rows 2, 4
        $this->assertCount(2, $byCol['email']); // Rows 3, 4
    }

    public function test_get_failed_rows_single_key()
    {
        $run = ExtractAndTransform::audit('helper_source')
            ->identifiedBy('id')
            ->check(['name' => fn ($r) => $r->required()])
            ->run();

        $failedRows = $run->getFailedRows();

        $this->assertCount(2, $failedRows); // Rows 2 and 4
        $this->assertEquals(2, $failedRows->firstWhere('id', 2)->id);
        $this->assertEquals(4, $failedRows->firstWhere('id', 4)->id);
    }

    public function test_get_failed_rows_composite_key()
    {
        Schema::create('comp_helper_source', function ($table) {
            $table->string('a');
            $table->string('b');
            $table->string('val')->nullable();
        });

        DB::table('comp_helper_source')->insert([
            ['a' => 'X', 'b' => '1', 'val' => 'ok'],
            ['a' => 'Y', 'b' => '2', 'val' => null], // Fail
        ]);

        $run = ExtractAndTransform::audit('comp_helper_source')
            ->identifiedBy(['a', 'b'])
            ->check(['val' => fn ($r) => $r->required()])
            ->run();

        $failedRows = $run->getFailedRows(['a', 'b']);

        $this->assertCount(1, $failedRows);
        $this->assertEquals('Y', $failedRows[0]->a);
        $this->assertEquals('2', $failedRows[0]->b);
    }

    public function test_override_with()
    {
        $run = ExtractAndTransform::audit('helper_source')
            ->identifiedBy('id')
            ->check(['name' => fn ($r) => $r->required()])
            ->run();

        $log = $run->logs->firstWhere('row_identifier', '2');

        $correction = $log->overrideWith('Bob', 'Fixed via helper');

        $this->assertDatabaseHas('andach_leat_corrections', [
            'table_name' => 'helper_source',
            'row_identifier' => '2',
            'column_name' => 'name',
            'new_value' => 'Bob',
            'reason' => 'Fixed via helper',
        ]);
    }

    public function test_override_with_updates_existing_correction()
    {
        $run = ExtractAndTransform::audit('helper_source')
            ->identifiedBy('id')
            ->check(['name' => fn ($r) => $r->required()])
            ->run();

        $log = $run->logs->firstWhere('row_identifier', '2');

        // First override
        $log->overrideWith('Bob', 'First fix');

        // Second override (should update)
        $log->overrideWith('Bobby', 'Better fix');

        $this->assertDatabaseCount('andach_leat_corrections', 1);
        $this->assertDatabaseHas('andach_leat_corrections', [
            'table_name' => 'helper_source',
            'row_identifier' => '2',
            'column_name' => 'name',
            'new_value' => 'Bobby',
            'reason' => 'Better fix',
        ]);
    }
}
