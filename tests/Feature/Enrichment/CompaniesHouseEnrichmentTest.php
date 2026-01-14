<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Enrichment;

use Andach\ExtractAndTransform\Models\EnrichmentProfile;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Schema;

class CompaniesHouseEnrichmentTest extends TestCase
{
    use RefreshDatabase;

    public function test_it_enriches_a_source_table_into_a_cache_table()
    {
        // 1. ARRANGE: Start with a source table containing company numbers.
        Schema::create('companies', function ($table) {
            $table->id();
            $table->string('company_number');
            $table->string('name');
        });
        DB::table('companies')->insert([
            ['company_number' => '12345678', 'name' => 'Company A'],
            ['company_number' => '87654321', 'name' => 'Company B'],
            ['company_number' => ' 2097644 ', 'name' => 'Company C (Malformed)'], // Needs trim and padding
        ]);

        // Mock the external API to avoid real network calls.
        Http::fake([
            'api.company-information.service.gov.uk/company/12345678' => Http::response([
                'company_name' => 'COMPANY A LTD',
                'company_status' => 'active',
                'date_of_creation' => '2020-01-01',
            ]),
            'api.company-information.service.gov.uk/company/87654321' => Http::response([
                'company_name' => 'COMPANY B LTD',
                'company_status' => 'dissolved',
                'date_of_creation' => '2021-02-02',
            ]),
            // Expect the preprocessed ID (trimmed and padded)
            'api.company-information.service.gov.uk/company/02097644' => Http::response([
                'company_name' => 'COMPANY C LTD',
                'company_status' => 'active',
                'date_of_creation' => '1990-01-01',
            ]),
        ]);

        // Define the enrichment process.
        $profile = EnrichmentProfile::create([
            'name' => 'Enrich UK Companies',
            'provider' => 'companies_house',
            'source_table' => 'companies',
            'source_column' => 'company_number',
            'destination_table' => 'companies_house_cache',
            'config' => ['api_key' => 'fake_key'],
        ]);

        // 2. ACT: Run the enrichment process for the first time.
        $firstRun = $profile->run();

        // 3. ASSERT: Check that the cache table was created and populated correctly.
        $this->assertEquals('success', $firstRun->status);
        $this->assertEquals(3, $firstRun->rows_added);
        $this->assertTrue(Schema::hasTable('companies_house_cache'));
        $this->assertDatabaseCount('companies_house_cache', 3);

        $this->assertDatabaseHas('companies_house_cache', [
            'company_number' => '12345678',
            'company_name' => 'COMPANY A LTD',
        ]);

        // Verify the malformed ID was enriched correctly but stored with original ID
        $this->assertDatabaseHas('companies_house_cache', [
            'company_number' => ' 2097644 ',
            'company_name' => 'COMPANY C LTD',
        ]);

        // 4. ACT (AGAIN): Run the same enrichment process a second time.
        $secondRun = $profile->run();

        // 5. ASSERT (AGAIN): Check that no new API calls were made and no new rows were added.
        $this->assertEquals('success', $secondRun->status);
        $this->assertEquals(0, $secondRun->rows_added, 'The enrichment should not add rows that are already cached.');
        $this->assertDatabaseCount('companies_house_cache', 3);
    }
}
