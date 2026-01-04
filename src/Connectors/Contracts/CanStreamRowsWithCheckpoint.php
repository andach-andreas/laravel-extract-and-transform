<?php

namespace Andach\ExtractAndTransform\Connectors\Contracts;

use Andach\ExtractAndTransform\Data\RemoteDataset;

interface CanStreamRowsWithCheckpoint
{
    /**
     * Yield associative rows keyed by remote column name.
     * The generator RETURN value must be the next checkpoint array (JSON-serializable), or null.
     *
     * @param  array<string,mixed>  $options
     * @return \Generator<array<string,mixed>, void, void, array<string,mixed>|null>
     */
    public function streamRowsWithCheckpoint(RemoteDataset $dataset, array $config, ?array $checkpoint, array $options = []): \Generator;
}
