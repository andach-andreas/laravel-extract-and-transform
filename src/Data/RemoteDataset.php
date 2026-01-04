<?php

namespace Andach\ExtractAndTransform\Data;

final class RemoteDataset
{
    public function __construct(
        public readonly string $identifier, // e.g. table name, or file path
        public readonly string $label,      // display label
        public readonly array $meta = [],   // connector-specific metadata
    ) {}
}
