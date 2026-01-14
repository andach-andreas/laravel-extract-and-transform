<?php

namespace Andach\ExtractAndTransform\Tests\Feature;

use Andach\ExtractAndTransform\Connectors\General\Excel\LegacyExcelConnector;
use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Tests\TestCase;
use PhpOffice\PhpSpreadsheet\Spreadsheet;
use PhpOffice\PhpSpreadsheet\Writer\Xls;

class LegacyExcelConnectorTest extends TestCase
{
    private string $filePath;

    protected function setUp(): void
    {
        parent::setUp();

        $this->filePath = sys_get_temp_dir().'/test_legacy_multi.xls';

        $spreadsheet = new Spreadsheet;

        // Sheet 1: Products
        $sheet1 = $spreadsheet->getActiveSheet();
        $sheet1->setTitle('Products');
        $sheet1->setCellValue('A1', 'ID');
        $sheet1->setCellValue('B1', 'Name');
        $sheet1->setCellValue('A2', 1);
        $sheet1->setCellValue('B2', 'Legacy Item');

        // Sheet 2: Orders
        $sheet2 = $spreadsheet->createSheet();
        $sheet2->setTitle('Orders');
        $sheet2->setCellValue('A1', 'OrderID');
        $sheet2->setCellValue('B1', 'Total');
        $sheet2->setCellValue('A2', 999);
        $sheet2->setCellValue('B2', 50.00);

        $writer = new Xls($spreadsheet);
        $writer->save($this->filePath);
    }

    protected function tearDown(): void
    {
        if (file_exists($this->filePath)) {
            unlink($this->filePath);
        }
        parent::tearDown();
    }

    public function test_it_lists_all_sheets()
    {
        $connector = new LegacyExcelConnector;
        $datasets = iterator_to_array($connector->datasets(['path' => $this->filePath]));

        $this->assertCount(2, $datasets);
        $this->assertEquals('Products', $datasets[0]->identifier);
        $this->assertEquals('Orders', $datasets[1]->identifier);
    }

    public function test_it_streams_from_specific_sheet()
    {
        $connector = new LegacyExcelConnector;

        // Test Sheet 1
        $dataset1 = new RemoteDataset('Products', 'Products');
        $rows1 = iterator_to_array($connector->streamRows($dataset1, ['path' => $this->filePath]));

        $this->assertCount(1, $rows1);
        $this->assertEquals('Legacy Item', $rows1[0]['Name']);

        // Test Sheet 2
        $dataset2 = new RemoteDataset('Orders', 'Orders');
        $rows2 = iterator_to_array($connector->streamRows($dataset2, ['path' => $this->filePath]));

        $this->assertCount(1, $rows2);
        $this->assertEquals(999, $rows2[0]['OrderID']);
        $this->assertEquals(50.00, $rows2[0]['Total']);
    }
}
