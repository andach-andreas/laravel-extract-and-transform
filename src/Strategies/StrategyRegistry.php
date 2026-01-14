<?php

namespace Andach\ExtractAndTransform\Strategies;

use InvalidArgumentException;

final class StrategyRegistry
{
    /** @var array<string, SyncStrategy> */
    private array $strategies = [];

    public function register(string $key, SyncStrategy $strategy): void
    {
        $this->strategies[$key] = $strategy;
    }

    public function get(string $key): SyncStrategy
    {
        if (! isset($this->strategies[$key])) {
            throw new InvalidArgumentException("Strategy [{$key}] is not registered.");
        }

        return $this->strategies[$key];
    }

    /**
     * @return array<string, SyncStrategy>
     */
    public function all(): array
    {
        return $this->strategies;
    }
}
