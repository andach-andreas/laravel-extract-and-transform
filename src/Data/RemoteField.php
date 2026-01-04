<?php

namespace Andach\ExtractAndTransform\Data;

final class RemoteField
{
    public function __construct(
        public readonly string $name,
        public readonly ?string $remoteType = null,
        public readonly bool $nullable = true,
        public readonly ?string $suggestedLocalType = null,
    ) {}
}
