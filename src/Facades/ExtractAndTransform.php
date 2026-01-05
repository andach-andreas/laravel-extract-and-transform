<?php

namespace Andach\ExtractAndTransform\Facades;

use Illuminate\Support\Facades\Facade;

/**
 * @method static \Andach\ExtractAndTransform\Source source(string $name)
 * @method static \Andach\ExtractAndTransform\Source createSource(string $name, string $connector, array $config)
 * @method static \Andach\ExtractAndTransform\Transform\TransformationBuilder transform(string $name)
 * @method static \Andach\ExtractAndTransform\Models\Transformation|null getTransformation(string $name)
 *
 * @see \Andach\ExtractAndTransform\ExtractAndTransform
 */
class ExtractAndTransform extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        return \Andach\ExtractAndTransform\ExtractAndTransform::class;
    }
}
