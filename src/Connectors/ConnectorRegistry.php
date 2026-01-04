<?php

namespace Andach\ExtractAndTransform\Connectors;

use Andach\ExtractAndTransform\Connectors\Contracts\Connector;
use InvalidArgumentException;

final class ConnectorRegistry
{
    /** @var array<string, Connector> */
    private array $connectors = [];

    public function register(Connector $connector): void
    {
        $this->connectors[$connector->key()] = $connector;
    }

    public function get(string $key): Connector
    {
        if (! isset($this->connectors[$key])) {
            throw new InvalidArgumentException("Connector [$key] is not registered.");
        }

        return $this->connectors[$key];
    }

    /**
     * @return array<string, Connector>
     */
    public function all(): array
    {
        return $this->connectors;
    }
}
