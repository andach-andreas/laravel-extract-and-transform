<?php

namespace Andach\ExtractAndTransform\Services\Dto;

final class ImportOptions
{
    /**
     * @param  array<int,string>  $identityColumns  single or composite identity columns (remote column names)
     */
    public function __construct(
        public readonly string $strategy = 'full_refresh', // full_refresh|time_watermark|cursor_token|content_hash
        public readonly array $identityColumns = [],
        public readonly ?string $connection = null, // target DB connection
        public readonly ?string $watermark = null, // time_watermark
        public readonly ?string $tieBreaker = null, // time_watermark
        public readonly ?string $key = null, // cursor_token
        public readonly ?int $pageSize = null,
        public readonly ?string $sourceUpdatedAt = null, // source column to copy into __source_updated_at
        public readonly bool $dryRun = false,
    ) {
        $allowed = ['full_refresh', 'time_watermark', 'cursor_token', 'content_hash'];
        if (! in_array($this->strategy, $allowed, true)) {
            throw new \InvalidArgumentException('ImportOptions strategy must be one of: '.implode(', ', $allowed));
        }

        foreach ($this->identityColumns as $c) {
            if (! is_string($c) || trim($c) === '') {
                throw new \InvalidArgumentException('ImportOptions identityColumns must be non-empty strings.');
            }
        }

        if ($this->strategy === 'time_watermark') {
            if (! is_string($this->watermark) || trim($this->watermark) === '') {
                throw new \InvalidArgumentException('time_watermark requires watermark.');
            }
            if (! is_string($this->tieBreaker) || trim($this->tieBreaker) === '') {
                throw new \InvalidArgumentException('time_watermark requires tieBreaker.');
            }
        }

        if ($this->strategy === 'cursor_token') {
            if (! is_string($this->key) || trim($this->key) === '') {
                throw new \InvalidArgumentException('cursor_token requires key.');
            }
        }

        if ($this->pageSize !== null && $this->pageSize < 1) {
            throw new \InvalidArgumentException('pageSize must be >= 1 when provided.');
        }
    }

    public function connectionOrDefault(): string
    {
        return (is_string($this->connection) && trim($this->connection) !== '')
            ? $this->connection
            : (string) config('database.default');
    }

    /**
     * Options passed through to connectors that support checkpoints.
     *
     * @return array<string,mixed>
     */
    public function connectorOptions(): array
    {
        $out = ['strategy' => $this->strategy];

        if (is_string($this->watermark) && trim($this->watermark) !== '') {
            $out['watermark'] = $this->watermark;
        }
        if (is_string($this->tieBreaker) && trim($this->tieBreaker) !== '') {
            $out['tie_breaker'] = $this->tieBreaker;
        }
        if (is_string($this->key) && trim($this->key) !== '') {
            $out['key'] = $this->key;
        }
        if ($this->pageSize !== null) {
            $out['page_size'] = $this->pageSize;
        }

        return $out;
    }

    public function hasIdentity(): bool
    {
        return $this->identityColumns !== [];
    }
}
