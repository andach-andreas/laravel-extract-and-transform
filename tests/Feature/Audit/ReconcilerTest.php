<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Audit;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class ReconcilerTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        Schema::create('reconcile_source', function ($table) {
            $table->id();
            $table->string('name');
            $table->integer('age');
        });

        DB::table('reconcile_source')->insert([
            ['id' => 100, 'name' => 'John', 'age' => 30],
            ['id' => 101, 'name' => 'Jane', 'age' => 25],
        ]);
    }

    public function test_it_can_reconcile_via_facade()
    {
        // Add correction
        ExtractAndTransform::addCorrection('reconcile_source', '100', 'age', 31, 'Birthday');

        // Run reconcile
        $affected = ExtractAndTransform::reconcile('reconcile_source', 'reconciled_users', 'id');

        $this->assertGreaterThan(0, $affected);

        $john = DB::table('reconciled_users')->where('id', 100)->first();
        $this->assertEquals(31, $john->age);
        $this->assertEquals('John', $john->name);

        $jane = DB::table('reconciled_users')->where('id', 101)->first();
        $this->assertEquals(25, $jane->age);
    }
}
