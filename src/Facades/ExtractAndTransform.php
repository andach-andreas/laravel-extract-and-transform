<?php

namespace Andach\ExtractAndTransform\Facades;

use Illuminate\Support\Facades\Facade;

/**
 * @method static \Andach\ExtractAndTransform\Source source(string $name)
 * @method static \Andach\ExtractAndTransform\Source createSource(string $name, string $connector, array $config)
 * @method static \Andach\ExtractAndTransform\Transform\TransformationBuilder transform(string $name)
 * @method static \Andach\ExtractAndTransform\Models\Transformation|null getTransformation(string $name)
 * @method static array getConnectors()
 * @method static array getConnectorConfigSchema(string $key)
 * @method static \Andach\ExtractAndTransform\Audit\AuditBuilder audit(string $tableName)
 * @method static \Andach\ExtractAndTransform\Models\Correction addCorrection(string $tableName, string $rowIdentifier, string $column, mixed $newValue, ?string $reason = null)
 * @method static int reconcile(string $sourceTable, string $destinationTable, string|array $identifier)
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
