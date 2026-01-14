<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Http\Controllers;

use Andach\ExtractAndTransform\Enrichment\Connectors\CompaniesHouseConnector;
use Andach\ExtractAndTransform\Enrichment\EnrichmentRegistry;
use Andach\ExtractAndTransform\Models\EnrichmentProfile;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;

class EnrichmentControllerTest extends TestCase
{
    use RefreshDatabase;

    protected function setUp(): void
    {
        parent::setUp();

        app(EnrichmentRegistry::class)->register(new CompaniesHouseConnector);
    }

    public function test_index_page_loads()
    {
        EnrichmentProfile::create(['name' => 'E1', 'provider' => 'p', 'source_table' => 's', 'source_column' => 'sc', 'destination_table' => 'd', 'config' => []]);
        $response = $this->get('/andach-leat/enrichments');
        $response->assertStatus(200)->assertSee('E1');
    }

    public function test_create_page_loads()
    {
        $response = $this->get('/andach-leat/enrichments/create');
        $response->assertStatus(200)->assertSee('Create Enrichment Profile');
    }

    public function test_store_enrichment()
    {
        $response = $this->post('/andach-leat/enrichments', [
            'name' => 'New Enrichment',
            'provider' => 'companies_house',
            'source_table' => 'companies',
            'source_column' => 'company_number',
            'destination_table' => 'cache',
            'config' => ['api_key' => 'key'],
        ]);

        $response->assertRedirect('/andach-leat/enrichments');
        $this->assertDatabaseHas('andach_leat_enrichment_profiles', [
            'name' => 'New Enrichment',
            'provider' => 'companies_house',
        ]);
    }
}
