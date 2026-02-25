<?php

namespace Andach\ExtractAndTransform\Connectors\Contracts;

use Andach\ExtractAndTransform\Data\RemoteDataset;

interface CanStreamRows
{
    /**
     * Yield associative rows keyed by remote column name.
     *
     * @param array $options Runtime options (e.g. chunk_size, primary_key)
     * @return iterable<array<string, mixed>>
     */
    public function streamRows(RemoteDataset $dataset, array $config, array $options = []): iterable;
}
