<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Http\Controllers\Api;

use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Schema;

class TableControllerTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        Schema::create('test_api_table', function ($table) {
            $table->id();
            $table->string('name');
            $table->integer('age');
        });
    }

    public function test_it_returns_columns_for_valid_table()
    {
        $response = $this->get('/andach-leat/api/tables/test_api_table/columns');

        $response->assertStatus(200);
        $response->assertJson(['id', 'name', 'age']);
    }

    public function test_it_returns_404_for_invalid_table()
    {
        $response = $this->get('/andach-leat/api/tables/non_existent_table/columns');

        $response->assertStatus(404);
        $response->assertJson(['error' => 'Table not found']);
    }
}
