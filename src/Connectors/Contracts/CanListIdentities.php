<?php

namespace Andach\ExtractAndTransform\Connectors\Contracts;

use Andach\ExtractAndTransform\Data\RemoteDataset;

interface CanListIdentities
{
    /**
     * Return identities for the dataset using the provided identity columns.
     *
     * @param  array<int,string>  $identityColumns
     * @return iterable<string>
     */
    public function listIdentities(RemoteDataset $dataset, array $config, array $identityColumns): iterable;
}
