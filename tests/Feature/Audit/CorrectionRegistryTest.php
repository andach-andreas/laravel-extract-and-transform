<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Audit;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Services\CorrectionService;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;

class CorrectionRegistryTest extends TestCase
{
    use RefreshDatabase;

    public function test_it_can_add_a_correction()
    {
        $correction = ExtractAndTransform::addCorrection(
            'raw_sites',
            '105',
            'postcode',
            'AB1 2CD',
            'Manual fix'
        );

        $this->assertDatabaseHas('andach_leat_corrections', [
            'table_name' => 'raw_sites',
            'row_identifier' => '105',
            'column_name' => 'postcode',
            'new_value' => 'AB1 2CD',
            'reason' => 'Manual fix',
        ]);
    }

    public function test_it_updates_existing_correction()
    {
        ExtractAndTransform::addCorrection('raw_sites', '105', 'postcode', 'OLD', 'Old reason');

        // Update
        ExtractAndTransform::addCorrection('raw_sites', '105', 'postcode', 'NEW', 'New reason');

        $this->assertDatabaseCount('andach_leat_corrections', 1);
        $this->assertDatabaseHas('andach_leat_corrections', [
            'table_name' => 'raw_sites',
            'row_identifier' => '105',
            'column_name' => 'postcode',
            'new_value' => 'NEW',
            'reason' => 'New reason',
        ]);
    }

    public function test_it_can_retrieve_correction()
    {
        ExtractAndTransform::addCorrection('raw_sites', '105', 'postcode', 'AB1 2CD');

        $service = app(CorrectionService::class);
        $correction = $service->get('raw_sites', '105', 'postcode');

        $this->assertNotNull($correction);
        $this->assertEquals('AB1 2CD', $correction->new_value);
    }
}
