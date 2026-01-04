<?php

namespace Andach\ExtractAndTransform\Services\Dto;

final class ImportResult
{
    public function __construct(
        public readonly int $rowsRead,
        public readonly int $rowsWritten,
        public readonly int $rowsSkipped,
    ) {}
}
