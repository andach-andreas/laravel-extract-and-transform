<?php

namespace Andach\ExtractAndTransform;

use Andach\ExtractAndTransform\Connectors\ConnectorRegistry;
use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Models\Transformation;
use Andach\ExtractAndTransform\Transform\TransformationBuilder;

class ExtractAndTransform
{
    public function __construct(
        private readonly ConnectorRegistry $registry
    ) {}

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

    public function getConnectors(): array
    {
        $connectors = [];
        foreach ($this->registry->all() as $connector) {
            $connectors[$connector->key()] = $connector->label();
        }

        return $connectors;
    }

    public function getConnectorConfigSchema(string $key): array
    {
        return $this->registry->get($key)->getConfigDefinition();
    }

    public function audit(string $tableName): \Andach\ExtractAndTransform\Audit\AuditBuilder
    {
        return new \Andach\ExtractAndTransform\Audit\AuditBuilder($tableName);
    }

    public function addCorrection(string $tableName, string $rowIdentifier, string $column, mixed $newValue, ?string $reason = null): \Andach\ExtractAndTransform\Models\Correction
    {
        return app(\Andach\ExtractAndTransform\Services\CorrectionService::class)->add($tableName, $rowIdentifier, $column, $newValue, $reason);
    }

    public function reconcile(string $sourceTable, string $destinationTable, string|array $identifier): int
    {
        return app(\Andach\ExtractAndTransform\Services\ReconcileService::class)->reconcile($sourceTable, $destinationTable, $identifier);
    }
}
