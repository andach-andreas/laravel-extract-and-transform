<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Http\Controllers;

use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;

class SourceControllerTest extends TestCase
{
    use RefreshDatabase;

    public function test_index_page_loads()
    {
        ExtractSource::create(['name' => 'Test Source', 'connector' => 'csv', 'config' => []]);

        $response = $this->get('/andach-leat/sources');

        $response->assertStatus(200);
        $response->assertSee('Test Source');
        $response->assertSee('Sources');
    }

    public function test_create_page_loads()
    {
        $response = $this->get('/andach-leat/sources/create');

        $response->assertStatus(200);
        $response->assertSee('Create Source');
        $response->assertSee('csv');
    }

    public function test_store_source()
    {
        $response = $this->post('/andach-leat/sources', [
            'name' => 'New Source',
            'connector' => 'csv',
            'config' => ['path' => '/tmp/test.csv'],
        ]);

        $response->assertRedirect('/andach-leat/sources');
        $this->assertDatabaseHas('andach_leat_extract_sources', [
            'name' => 'New Source',
            'connector' => 'csv',
        ]);
    }

    public function test_edit_page_loads()
    {
        $source = ExtractSource::create(['name' => 'Edit Me', 'connector' => 'csv', 'config' => []]);

        $response = $this->get("/andach-leat/sources/{$source->id}/edit");

        $response->assertStatus(200);
        $response->assertSee('Edit Source: Edit Me');
        $response->assertSee($source->name);
    }

    public function test_update_source()
    {
        $source = ExtractSource::create(['name' => 'Old Name', 'connector' => 'csv', 'config' => []]);

        $response = $this->put("/andach-leat/sources/{$source->id}", [
            'name' => 'Updated Name',
            'connector' => 'mysql', // Use mysql connector
            'config' => ['database' => 'test_db'],
        ]);

        $response->assertRedirect('/andach-leat/sources');
        $this->assertDatabaseHas('andach_leat_extract_sources', [
            'id' => $source->id,
            'name' => 'Updated Name',
            'connector' => 'mysql',
        ]);
    }

    public function test_destroy_source()
    {
        $source = ExtractSource::create(['name' => 'Delete Me', 'connector' => 'csv', 'config' => []]);

        $response = $this->delete("/andach-leat/sources/{$source->id}");

        $response->assertRedirect('/andach-leat/sources');
        $this->assertDatabaseMissing('andach_leat_extract_sources', ['id' => $source->id]);
    }
}
