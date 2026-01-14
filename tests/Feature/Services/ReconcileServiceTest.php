<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Services;

use Andach\ExtractAndTransform\Models\Correction;
use Andach\ExtractAndTransform\Services\ReconcileService;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class ReconcileServiceTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();
        Schema::create('source_table', function ($table) {
            $table->id();
            $table->string('name')->nullable();
            $table->string('email')->nullable();
        });
        DB::table('source_table')->insert([
            ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com'],
            ['id' => 2, 'name' => 'Bob', 'email' => 'bob@wrong.com'],
        ]);
    }

    public function test_it_reconciles_data()
    {
        Correction::create([
            'table_name' => 'source_table',
            'row_identifier' => '2',
            'column_name' => 'email',
            'new_value' => 'bob@correct.com',
        ]);

        $service = app(ReconcileService::class);
        $rowsAffected = $service->reconcile('source_table', 'reconciled_table', 'id');

        $this->assertEquals(2, $rowsAffected);
        $this->assertTrue(Schema::hasTable('reconciled_table'));
        $this->assertDatabaseHas('reconciled_table', [
            'id' => 1,
            'name' => 'Alice',
            'email' => 'alice@example.com',
        ]);
        $this->assertDatabaseHas('reconciled_table', [
            'id' => 2,
            'name' => 'Bob',
            'email' => 'bob@correct.com', // Corrected value
        ]);
    }
}
