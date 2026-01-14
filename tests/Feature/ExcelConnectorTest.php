<?php

namespace Andach\ExtractAndTransform\Tests\Feature;

use Andach\ExtractAndTransform\Connectors\General\Excel\ExcelConnector;
use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Tests\TestCase;
use OpenSpout\Common\Entity\Row;
use OpenSpout\Writer\XLSX\Writer;

class ExcelConnectorTest extends TestCase
{
    private string $filePath;

    protected function setUp(): void
    {
        parent::setUp();

        $this->filePath = sys_get_temp_dir().'/test_multi.xlsx';

        $writer = new Writer;
        $writer->openToFile($this->filePath);

        // Sheet 1: Products (Default)
        $writer->addRow(Row::fromValues(['ID', 'Name', 'Price']));
        $writer->addRow(Row::fromValues([1, 'Item A', 10.5]));

        // Sheet 2: Customers
        $writer->addNewSheetAndMakeItCurrent();
        $writer->addRow(Row::fromValues(['CustID', 'Email']));
        $writer->addRow(Row::fromValues([100, 'bob@example.com']));
        $writer->addRow(Row::fromValues([101, 'alice@example.com']));

        $writer->close();
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
        $connector = new ExcelConnector;
        $datasets = iterator_to_array($connector->datasets(['path' => $this->filePath]));

        $this->assertCount(2, $datasets);
        $this->assertEquals('Sheet1', $datasets[0]->identifier);
        $this->assertEquals('Sheet2', $datasets[1]->identifier);
    }

    public function test_it_streams_from_specific_sheet()
    {
        $connector = new ExcelConnector;

        // Test Sheet 1 (Products)
        $dataset1 = new RemoteDataset('Sheet1', 'Sheet1');
        $rows1 = iterator_to_array($connector->streamRows($dataset1, ['path' => $this->filePath]));

        $this->assertCount(1, $rows1);
        $this->assertEquals('Item A', $rows1[0]['Name']);
        $this->assertArrayHasKey('Price', $rows1[0]);

        // Test Sheet 2 (Customers)
        $dataset2 = new RemoteDataset('Sheet2', 'Sheet2');
        $rows2 = iterator_to_array($connector->streamRows($dataset2, ['path' => $this->filePath]));

        $this->assertCount(2, $rows2);
        $this->assertEquals('bob@example.com', $rows2[0]['Email']);
        $this->assertEquals('alice@example.com', $rows2[1]['Email']);
        $this->assertArrayNotHasKey('Price', $rows2[0]); // Ensure we aren't reading Sheet 1
    }
}
