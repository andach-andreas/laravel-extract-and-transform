<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Http\Controllers;

use Andach\ExtractAndTransform\Models\Transformation;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class TransformationControllerTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        Schema::create('source_table', function ($table) {
            $table->string('name');
        });
        DB::table('source_table')->insert(['name' => 'Test']);
    }

    public function test_index_page_loads()
    {
        Transformation::create(['name' => 'T1', 'source_table' => 's', 'destination_table_pattern' => 'd', 'configuration' => []]);
        $response = $this->get('/andach-leat/transformations');
        $response->assertStatus(200)->assertSee('T1');
    }

    public function test_create_page_loads()
    {
        $response = $this->get('/andach-leat/transformations/create');
        $response->assertStatus(200)->assertSee('Create Transformation');
    }

    public function test_store_transformation()
    {
        $config = [
            'source' => 'source_table',
            'destination' => 'dest_table',
            'columns' => ['col1' => ['type' => 'column', 'column' => 'col1']],
        ];

        $response = $this->post('/andach-leat/transformations', [
            'name' => 'New Transform',
            'configuration' => json_encode($config),
        ]);

        $response->assertRedirect('/andach-leat/transformations');
        $this->assertDatabaseHas('andach_leat_transformations', [
            'name' => 'New Transform',
            'source_table' => 'source_table',
        ]);
    }

    public function test_edit_page_loads()
    {
        $t = Transformation::create(['name' => 'T1', 'source_table' => 's', 'destination_table_pattern' => 'd', 'configuration' => []]);
        $response = $this->get("/andach-leat/transformations/{$t->id}/edit");
        $response->assertStatus(200)->assertSee('Edit Transformation');
    }

    public function test_update_transformation()
    {
        $t = Transformation::create(['name' => 'T1', 'source_table' => 's', 'destination_table_pattern' => 'd', 'configuration' => []]);

        $config = [
            'source' => 'new_source',
            'destination' => 'new_dest',
            'columns' => ['col1' => ['type' => 'column', 'column' => 'col1']],
        ];

        $response = $this->put("/andach-leat/transformations/{$t->id}", [
            'name' => 'Updated Name',
            'configuration' => json_encode($config),
        ]);

        $response->assertRedirect('/andach-leat/transformations');
        $this->assertDatabaseHas('andach_leat_transformations', [
            'id' => $t->id,
            'name' => 'Updated Name',
            'source_table' => 'new_source',
        ]);
    }

    public function test_destroy_transformation()
    {
        $t = Transformation::create(['name' => 'T1', 'source_table' => 's', 'destination_table_pattern' => 'd', 'configuration' => []]);

        $response = $this->delete("/andach-leat/transformations/{$t->id}");

        $response->assertRedirect('/andach-leat/transformations');
        $this->assertDatabaseMissing('andach_leat_transformations', ['id' => $t->id]);
    }

    public function test_run_transformation()
    {
        $config = [
            'source' => 'source_table',
            'destination' => 'dest_table',
            'columns' => ['name' => ['type' => 'column', 'column' => 'name']],
        ];
        $t = Transformation::create(['name' => 'T1', 'source_table' => 'source_table', 'destination_table_pattern' => 'dest_table', 'configuration' => $config]);

        $response = $this->post("/andach-leat/transformations/{$t->id}/run");

        $response->assertRedirect();
        $this->assertTrue(Schema::hasTable('dest_table_v1'));
        $this->assertDatabaseHas('dest_table_v1', ['name' => 'Test']);
    }
}
