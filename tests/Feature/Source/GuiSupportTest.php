<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Source;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Services\TransformationService;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Facades\Schema;

class GuiSupportTest extends TestCase
{
    use RefreshDatabase;

    private string $fixturesPath;

    protected function setUp(): void
    {
        parent::setUp();
        $this->fixturesPath = __DIR__.'/../fixtures';
        if (! is_dir($this->fixturesPath)) {
            mkdir($this->fixturesPath, 0777, true);
        }
    }

    protected function tearDown(): void
    {
        if (is_dir($this->fixturesPath)) {
            File::deleteDirectory($this->fixturesPath);
        }
        parent::tearDown();
    }

    public function test_can_list_available_connectors()
    {
        $connectors = ExtractAndTransform::getConnectors();

        $this->assertIsArray($connectors);
        $this->assertArrayHasKey('csv', $connectors);
        $this->assertArrayHasKey('sql', $connectors);
        $this->assertEquals('CSV', $connectors['csv']);
    }

    public function test_can_get_connector_config_schema()
    {
        $schema = ExtractAndTransform::getConnectorConfigSchema('csv');

        $this->assertIsArray($schema);
        // Assuming CsvConnector defines 'path'
        $hasPath = false;
        foreach ($schema as $field) {
            if ($field->key === 'path') {
                $hasPath = true;
                break;
            }
        }
        $this->assertTrue($hasPath, 'CSV connector should have a path config field');
    }

    public function test_can_list_datasets_from_source()
    {
        // Create a dummy CSV
        $path = $this->fixturesPath.'/list_test.csv';
        File::put($path, "id,name\n1,Test");

        $source = ExtractAndTransform::createSource('List Test', 'csv', ['path' => $path]);

        $datasets = iterator_to_array($source->listDatasets());

        $this->assertNotEmpty($datasets);
        $this->assertEquals($path, $datasets[0]->getIdentifier());
    }

    public function test_can_preview_dataset()
    {
        $path = $this->fixturesPath.'/preview_test.csv';
        File::put($path, "id,name\n1,Row1\n2,Row2\n3,Row3");

        $source = ExtractAndTransform::createSource('Preview Test', 'csv', ['path' => $path]);
        $dataset = $source->getDataset($path);

        $preview = $dataset->preview(2);

        $this->assertCount(2, $preview);
        $this->assertEquals('Row1', $preview[0]['name']);
        $this->assertEquals('Row2', $preview[1]['name']);
    }

    public function test_can_preview_transformation()
    {
        // Setup source table
        Schema::create('preview_source', function ($table) {
            $table->id();
            $table->string('name');
            $table->integer('score');
        });

        DB::table('preview_source')->insert([
            ['name' => 'Alice', 'score' => 10],
            ['name' => 'Bob', 'score' => 20],
            ['name' => 'Charlie', 'score' => 30],
        ]);

        $config = [
            'source_table' => 'preview_source',
            'selects' => [
                'upper_name' => [
                    'type' => 'string_function',
                    'function' => 'UPPER',
                    'column' => ['type' => 'column', 'column' => 'name'],
                    'arguments' => [],
                ],
                'double_score' => [
                    'type' => 'math',
                    'operator' => '*',
                    'left' => ['type' => 'column', 'column' => 'score'],
                    'right' => 2,
                ],
            ],
        ];

        $service = app(TransformationService::class);
        $preview = $service->preview($config, 2);

        $this->assertCount(2, $preview);

        $this->assertEquals('ALICE', $preview[0]['upper_name']);
        $this->assertEquals(20, $preview[0]['double_score']);

        $this->assertEquals('BOB', $preview[1]['upper_name']);
        $this->assertEquals(40, $preview[1]['double_score']);
    }
}
