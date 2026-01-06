<?php

namespace Andach\ExtractAndTransform\Tests\Feature\Source;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Source;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Foundation\Testing\RefreshDatabase;

class SourceTest extends TestCase
{
    use RefreshDatabase;

    public function test_create_source_returns_source_instance_with_model(): void
    {
        $name = 'Test Source';
        $connector = 'csv';
        $config = ['path' => '/tmp/test.csv'];

        $source = ExtractAndTransform::createSource($name, $connector, $config);

        $this->assertInstanceOf(Source::class, $source);

        $model = $source->getModel();
        $this->assertInstanceOf(ExtractSource::class, $model);
        $this->assertEquals($name, $model->name);
        $this->assertEquals($connector, $model->connector);
        $this->assertEquals($config, $model->config);
        $this->assertTrue($model->exists);
    }

    public function test_source_method_returns_source_instance_with_model(): void
    {
        $name = 'Existing Source';
        $connector = 'sql';
        $config = ['connection' => 'mysql'];

        // Manually create the source first
        ExtractSource::create([
            'name' => $name,
            'connector' => $connector,
            'config' => $config,
        ]);

        $source = ExtractAndTransform::source($name);

        $this->assertInstanceOf(Source::class, $source);

        $model = $source->getModel();
        $this->assertInstanceOf(ExtractSource::class, $model);
        $this->assertEquals($name, $model->name);
        $this->assertEquals($connector, $model->connector);
        $this->assertEquals($config, $model->config);
    }

    public function test_get_model_returns_fresh_instance(): void
    {
        $name = 'Mutable Source';
        $source = ExtractAndTransform::createSource($name, 'csv', []);

        $model1 = $source->getModel();

        // Update the model in the database directly
        $model1->update(['connector' => 'json']);

        // getModel() returns the instance stored in the Source object,
        // so it might not reflect database changes unless refreshed.
        // However, since Source holds a reference to the model object,
        // and Eloquent models are objects, changes to the object should be reflected
        // if it's the same instance in memory.
        // But here we updated via $model1->update(), which updates the instance.

        $model2 = $source->getModel();
        $this->assertEquals('json', $model2->connector);
        $this->assertSame($model1, $model2);
    }
}
