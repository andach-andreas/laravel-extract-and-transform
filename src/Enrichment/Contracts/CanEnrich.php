<?php

namespace Andach\ExtractAndTransform\Enrichment\Contracts;

use Andach\ExtractAndTransform\Connectors\ConnectorConfigDefinition;

interface CanEnrich
{
    public function key(): string;

    public function label(): string;

    /**
     * @return ConnectorConfigDefinition[]
     */
    public function getConfigDefinition(): array;

    public function enrich(string|int $identifier, array $config): ?array;
}
