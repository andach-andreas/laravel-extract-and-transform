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
    /** @var callable|null */
    private $configUpdater;

    public function __construct(
        private readonly Connector $connector,
        private readonly array $config,
        private readonly RemoteDataset $remoteDataset,
        ?callable $configUpdater = null
    ) {
        $this->configUpdater = $configUpdater;
    }

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
        $config = $this->config;
        if ($this->configUpdater) {
            $config['__updater'] = $this->configUpdater;
        }

        if ($this->connector instanceof CanStreamRows) {
            return $this->connector->streamRows($this->remoteDataset, $config);
        }

        throw new \Exception('This connector does not support streaming rows.');
    }

    public function getRowsWithCheckpoint(?array $checkpoint, array $options = []): \Generator
    {
        $config = $this->config;
        if ($this->configUpdater) {
            $config['__updater'] = $this->configUpdater;
        }

        if ($this->connector instanceof CanStreamRowsWithCheckpoint) {
            return $this->connector->streamRowsWithCheckpoint($this->remoteDataset, $config, $checkpoint, $options);
        }

        throw new \Exception('This connector does not support streaming rows with checkpoint.');
    }

    public function getIdentities(array $identityColumns): iterable
    {
        $config = $this->config;
        if ($this->configUpdater) {
            $config['__updater'] = $this->configUpdater;
        }

        if ($this->connector instanceof CanListIdentities) {
            return $this->connector->listIdentities($this->remoteDataset, $config, $identityColumns);
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
        $config = $this->config;
        if ($this->configUpdater) {
            $config['__updater'] = $this->configUpdater;
        }

        if ($this->connector instanceof CanInferSchema) {
            return $this->connector->inferSchema($this->remoteDataset, $config);
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
