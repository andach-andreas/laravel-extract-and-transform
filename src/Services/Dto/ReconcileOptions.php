<?php

namespace Andach\ExtractAndTransform\Services\Dto;

final class ReconcileOptions
{
    /**
     * @param  array<int,string>  $identityColumns  single or composite identity columns (remote column names)
     */
    public function __construct(
        public readonly array $identityColumns = [],
        public readonly ?string $connection = null, // target DB connection
        public readonly bool $dryRun = false,
    ) {
        if ($this->identityColumns === []) {
            throw new \InvalidArgumentException('ReconcileOptions requires identityColumns.');
        }

        foreach ($this->identityColumns as $c) {
            if (! is_string($c) || trim($c) === '') {
                throw new \InvalidArgumentException('ReconcileOptions identityColumns must be non-empty strings.');
            }
        }
    }

    public function connectionOrDefault(): string
    {
        return (is_string($this->connection) && trim($this->connection) !== '')
            ? $this->connection
            : (string) config('database.default');
    }
}
