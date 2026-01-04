<?php

namespace Andach\ExtractAndTransform\Connectors\Contracts;

use Andach\ExtractAndTransform\Data\RemoteDataset;

interface CanStreamRows
{
    /**
     * Yield associative rows keyed by remote column name.
     *
     * @return iterable<array<string, mixed>>
     */
    public function streamRows(RemoteDataset $dataset, array $config): iterable;
}
