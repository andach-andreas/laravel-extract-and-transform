<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Source;

use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Source;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Facades\Schema;

class ExtractSourceModelTest extends TestCase
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

    public function test_can_connect_from_model_and_run_sync()
    {
        // 1. Create the source using standard Eloquent
        $path = $this->fixturesPath.'/model_connect.csv';
        File::put($path, "id,name\n1,Test Item");

        $model = ExtractSource::create([
            'name' => 'Eloquent Source',
            'connector' => 'csv',
            'config' => ['path' => $path],
        ]);

        // 2. Use the bridge method to get the worker
        $source = $model->connect();

        $this->assertInstanceOf(Source::class, $source);
        $this->assertEquals($model->id, $source->getModel()->id);

        // 3. Run a sync using the worker
        $run = $source->sync($path)
            ->toTable('model_connect_result')
            ->run();

        $this->assertEquals('success', $run->status);

        // When toTable is used explicitly, the package uses the name exactly as provided.
        // It does NOT append _v1 automatically.
        $this->assertTrue(Schema::hasTable('model_connect_result'));

        $rows = \Illuminate\Support\Facades\DB::table('model_connect_result')->get();
        $this->assertEquals('Test Item', $rows[0]->name);
    }
}
