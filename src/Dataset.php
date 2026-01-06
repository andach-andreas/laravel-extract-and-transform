<?php

namespace Andach\ExtractAndTransform;

use Andach\ExtractAndTransform\Connectors\Contracts\CanInferSchema;
use Andach\ExtractAndTransform\Connectors\Contracts\CanListIdentities;
use Andach\ExtractAndTransform\Connectors\Contracts\CanStreamRows;
use Andach\ExtractAndTransform\Connectors\Contracts\CanStreamRowsWithCheckpoint;
use Andach\ExtractAndTransform\Connectors\Contracts\Connector;
use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Data\RemoteSchema;

class Dataset
{
    public function __construct(
        private readonly Connector $connector,
        private readonly array $config,
        private readonly RemoteDataset $remoteDataset
    ) {}

    public function getIdentifier(): string
    {
        return $this->remoteDataset->identifier;
    }

    public function getLabel(): string
    {
        return $this->remoteDataset->label;
    }

    public function getMeta(): array
    {
        return $this->remoteDataset->meta;
    }

    public function getRows(): iterable
    {
        if ($this->connector instanceof CanStreamRows) {
            return $this->connector->streamRows($this->remoteDataset, $this->config);
        }

        throw new \Exception('This connector does not support streaming rows.');
    }

    public function getRowsWithCheckpoint(?array $checkpoint, array $options = []): \Generator
    {
        if ($this->connector instanceof CanStreamRowsWithCheckpoint) {
            return $this->connector->streamRowsWithCheckpoint($this->remoteDataset, $this->config, $checkpoint, $options);
        }

        throw new \Exception('This connector does not support streaming rows with checkpoint.');
    }

    public function getIdentities(array $identityColumns): iterable
    {
        if ($this->connector instanceof CanListIdentities) {
            return $this->connector->listIdentities($this->remoteDataset, $this->config, $identityColumns);
        }

        throw new \Exception('This connector does not support listing identities.');
    }

    public function getRowsByIds(array $ids, string $primaryKey): iterable
    {
        // This is a naive implementation. Ideally, the connector should support fetching by IDs.
        // For now, we'll stream all rows and filter.
        foreach ($this->getRows() as $row) {
            if (in_array($row[$primaryKey], $ids)) {
                yield $row;
            }
        }
    }

    public function getSchema(): RemoteSchema
    {
        if ($this->connector instanceof CanInferSchema) {
            return $this->connector->inferSchema($this->remoteDataset, $this->config);
        }

        throw new \Exception('This connector does not support inferring a schema.');
    }

    public function preview(int $limit = 5): array
    {
        $rows = [];
        $count = 0;
        foreach ($this->getRows() as $row) {
            $rows[] = $row;
            $count++;
            if ($count >= $limit) {
                break;
            }
        }

        return $rows;
    }
}
