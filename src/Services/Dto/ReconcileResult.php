<?php

namespace Andach\ExtractAndTransform\Services\Dto;

final class ReconcileResult
{
    public function __construct(
        public readonly int $identitiesScanned,
        public readonly int $tombstonesWritten,
    ) {}
}
