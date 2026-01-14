<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Http\Controllers;

use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;

class GlobalSyncControllerTest extends TestCase
{
    use RefreshDatabase;

    public function test_index_page_loads()
    {
        $source = ExtractSource::create(['name' => 'S', 'connector' => 'c', 'config' => []]);
        SyncProfile::create(['extract_source_id' => $source->id, 'dataset_identifier' => 'd', 'strategy' => 's']);

        $response = $this->get('/andach-leat/syncs');

        $response->assertStatus(200);
        $response->assertSee('S');
        $response->assertSee('d');
    }
}
