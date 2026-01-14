<?php

namespace Andach\ExtractAndTransform\Enrichment;

use Andach\ExtractAndTransform\Enrichment\Contracts\CanEnrich;
use InvalidArgumentException;

class EnrichmentRegistry
{
    /** @var array<string, CanEnrich> */
    private array $providers = [];

    public function register(CanEnrich $provider): void
    {
        $this->providers[$provider->key()] = $provider;
    }

    public function get(string $key): CanEnrich
    {
        if (! isset($this->providers[$key])) {
            throw new InvalidArgumentException("Enrichment provider [{$key}] is not registered.");
        }

        return $this->providers[$key];
    }

    public function all(): array
    {
        return $this->providers;
    }
}
