<?php

namespace Andach\ExtractAndTransform;

use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Models\Transformation;
use Andach\ExtractAndTransform\Transform\TransformationBuilder;

class ExtractAndTransform
{
    public function source(string $name): Source
    {
        $sourceModel = ExtractSource::where('name', $name)->first();

        return new Source($sourceModel, $this);
    }

    public function createSource(string $name, string $connector, array $config): Source
    {
        $sourceModel = ExtractSource::updateOrCreate(
            ['name' => $name],
            ['connector' => $connector, 'config' => $config]
        );

        return new Source($sourceModel, $this);
    }

    public function getSourceFromModel(ExtractSource $sourceModel): Source
    {
        return new Source($sourceModel, $this);
    }

    public function transform(string $name): TransformationBuilder
    {
        return new TransformationBuilder($name);
    }

    public function getTransformation(string $name): ?Transformation
    {
        return Transformation::where('name', $name)->first();
    }
}
