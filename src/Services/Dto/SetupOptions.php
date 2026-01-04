<?php

namespace Andach\ExtractAndTransform\Services\Dto;

final class SetupOptions
{
    /**
     * @param  array<int,string>  $columns  Remote column names to include
     * @param  array<string,string>  $types  remoteName => localType
     */
    public function __construct(
        public readonly string $target = 'versioned', // versioned|existing
        public readonly ?string $prefix = null,
        public readonly ?string $baseTable = null,
        public readonly ?string $table = null, // required for existing
        public readonly array $columns = [],
        public readonly array $types = [],
        public readonly ?string $connection = null, // target DB connection
    ) {
        $allowed = ['versioned', 'existing'];
        if (! in_array($this->target, $allowed, true)) {
            throw new \InvalidArgumentException('SetupOptions target must be one of: '.implode(', ', $allowed));
        }

        if ($this->target === 'existing' && (! is_string($this->table) || trim($this->table) === '')) {
            throw new \InvalidArgumentException('SetupOptions table is required when target=existing.');
        }

        if ($this->columns === []) {
            throw new \InvalidArgumentException('SetupOptions columns cannot be empty.');
        }

        foreach ($this->columns as $c) {
            if (! is_string($c) || trim($c) === '') {
                throw new \InvalidArgumentException('SetupOptions columns must be non-empty strings.');
            }
        }

        foreach ($this->types as $k => $v) {
            if (! is_string($k) || trim($k) === '' || ! is_string($v) || trim($v) === '') {
                throw new \InvalidArgumentException('SetupOptions types must be a map of non-empty string => non-empty string.');
            }
        }
    }

    public function connectionOrDefault(): string
    {
        return (is_string($this->connection) && trim($this->connection) !== '')
            ? $this->connection
            : (string) config('database.default');
    }

    public function prefixOrConfig(): string
    {
        return (is_string($this->prefix) && trim($this->prefix) !== '')
            ? $this->prefix
            : (string) config('extract-data.table_prefix', 'andach_');
    }

    public function baseTableOrFallback(string $fallback): string
    {
        return (is_string($this->baseTable) && trim($this->baseTable) !== '')
            ? $this->baseTable
            : $fallback;
    }

    public function tableOrFail(): string
    {
        if ($this->target !== 'existing') {
            throw new \LogicException('SetupOptions::tableOrFail called when target is not existing.');
        }

        $t = (string) $this->table;
        if (trim($t) === '') {
            throw new \InvalidArgumentException('SetupOptions table is required when target=existing.');
        }

        return $t;
    }
}
