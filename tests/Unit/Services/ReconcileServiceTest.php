<?php

namespace Andach\ExtractAndTransform\Tests\Unit\Services;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Services\ReconcileService;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class ReconcileServiceTest extends TestCase
{
    use RefreshDatabase;

    private ReconcileService $service;

    protected function setUp(): void
    {
        parent::setUp();
        $this->service = app(ReconcileService::class);

        Schema::create('unit_reconcile_source', function ($table) {
            $table->id();
            $table->string('name');
            $table->string('code');
        });

        DB::table('unit_reconcile_source')->insert([
            ['id' => 1, 'name' => 'Original Name', 'code' => 'A1'],
            ['id' => 2, 'name' => 'Another Name', 'code' => 'B2'],
        ]);
    }

    public function test_it_copies_table_if_no_corrections()
    {
        $this->service->reconcile('unit_reconcile_source', 'unit_reconcile_dest', 'id');

        $this->assertTrue(Schema::hasTable('unit_reconcile_dest'));
        $this->assertDatabaseCount('unit_reconcile_dest', 2);

        $row = DB::table('unit_reconcile_dest')->where('id', 1)->first();
        $this->assertEquals('Original Name', $row->name);
    }

    public function test_it_applies_corrections()
    {
        ExtractAndTransform::addCorrection('unit_reconcile_source', '1', 'name', 'Corrected Name');
        ExtractAndTransform::addCorrection('unit_reconcile_source', '2', 'code', 'C3');

        $affected = $this->service->reconcile('unit_reconcile_source', 'unit_reconcile_dest', 'id');

        $this->assertTrue($affected >= 2); // At least 2 updates (one per column per row logic)

        $row1 = DB::table('unit_reconcile_dest')->where('id', 1)->first();
        $this->assertEquals('Corrected Name', $row1->name);
        $this->assertEquals('A1', $row1->code); // Unchanged

        $row2 = DB::table('unit_reconcile_dest')->where('id', 2)->first();
        $this->assertEquals('Another Name', $row2->name); // Unchanged
        $this->assertEquals('C3', $row2->code);
    }

    public function test_it_handles_composite_keys()
    {
        Schema::create('composite_reconcile_source', function ($table) {
            $table->string('p1');
            $table->string('p2');
            $table->string('val');
        });

        DB::table('composite_reconcile_source')->insert([
            ['p1' => 'A', 'p2' => '1', 'val' => 'Wrong'],
        ]);

        // Identifier is "A-1"
        ExtractAndTransform::addCorrection('composite_reconcile_source', 'A-1', 'val', 'Right');

        $this->service->reconcile('composite_reconcile_source', 'composite_reconcile_dest', ['p1', 'p2']);

        $row = DB::table('composite_reconcile_dest')->first();
        $this->assertEquals('Right', $row->val);
    }

    public function test_it_handles_multiple_corrections_on_same_row()
    {
        ExtractAndTransform::addCorrection('unit_reconcile_source', '1', 'name', 'New Name');
        ExtractAndTransform::addCorrection('unit_reconcile_source', '1', 'code', 'New Code');

        $this->service->reconcile('unit_reconcile_source', 'unit_reconcile_dest', 'id');

        $row = DB::table('unit_reconcile_dest')->where('id', 1)->first();
        $this->assertEquals('New Name', $row->name);
        $this->assertEquals('New Code', $row->code);
    }

    public function test_it_ignores_corrections_for_other_tables()
    {
        ExtractAndTransform::addCorrection('other_table', '1', 'name', 'Wrong');
        ExtractAndTransform::addCorrection('unit_reconcile_source', '1', 'name', 'Right');

        $this->service->reconcile('unit_reconcile_source', 'unit_reconcile_dest', 'id');

        $row = DB::table('unit_reconcile_dest')->where('id', 1)->first();
        $this->assertEquals('Right', $row->name);
    }
}
