<?php

namespace Andach\ExtractAndTransform\Connectors\Contracts;

use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Data\RemoteSchema;

interface CanInferSchema
{
    public function inferSchema(RemoteDataset $dataset, array $config): RemoteSchema;
}
