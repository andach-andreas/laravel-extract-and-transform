<?php

namespace Andach\ExtractAndTransform\Connectors;

use Andach\ExtractAndTransform\Connectors\Contracts\CanInferSchema;
use Andach\ExtractAndTransform\Connectors\Contracts\CanListIdentities;
use Andach\ExtractAndTransform\Connectors\Contracts\CanStreamRows;
use Andach\ExtractAndTransform\Connectors\Contracts\CanStreamRowsWithCheckpoint;
use Andach\ExtractAndTransform\Connectors\Contracts\Connector;
use Andach\ExtractAndTransform\Data\RemoteDataset;
use Andach\ExtractAndTransform\Data\RemoteSchema;
use LogicException;

abstract class BaseConnector implements CanInferSchema, CanListIdentities, CanStreamRows, CanStreamRowsWithCheckpoint, Connector
{
    abstract public function key(): string;

    abstract public function label(): string;

    abstract public function getConfigDefinition(): array;

    abstract public function test(array $config): void;

    abstract public function datasets(array $config): iterable;

    /**
     * Contains the generic pagination loop for API-based connectors.
     * Connectors that do not use pagination (like CSV or SQL) should override this method.
     */
    public function streamRows(RemoteDataset $dataset, array $config): iterable
    {
        $parameters = $this->getInitialParameters($dataset, $config);

        do {
            $response = $this->fetchPaginatedData($parameters, $config);

            foreach ($this->extractRowsFromResponse($response) as $row) {
                yield $row;
            }

            $parameters = $this->getNextPageParameters($response, $parameters, $config);

        } while ($parameters !== null);
    }

    // --- Abstract "Hook" Methods for API Connectors ---

    /**
     * Get the initial parameters for the first API request.
     */
    protected function getInitialParameters(RemoteDataset $dataset, array $config): array
    {
        return [];
    }

    /**
     * Make a single API call to fetch a page of data.
     */
    protected function fetchPaginatedData(array $parameters, array $config): array
    {
        throw new LogicException(static::class.' does not support API pagination.');
    }

    /**
     * Extract the data records from the raw API response.
     */
    protected function extractRowsFromResponse(array $response): iterable
    {
        throw new LogicException(static::class.' does not support API pagination.');
    }

    /**
     * Determine the parameters for the next page request from the current response.
     * Must return null when there are no more pages.
     */
    protected function getNextPageParameters(array $response, array $previousParameters, array $config): ?array
    {
        throw new LogicException(static::class.' does not support API pagination.');
    }

    // --- Default "Not Implemented" for other optional capabilities ---

    public function inferSchema(RemoteDataset $dataset, array $config): RemoteSchema
    {
        throw new LogicException(static::class.' does not support inferring schema.');
    }

    public function listIdentities(RemoteDataset $dataset, array $config, array $identityColumns): iterable
    {
        throw new LogicException(static::class.' does not support listing identities.');
    }

    public function streamRowsWithCheckpoint(RemoteDataset $dataset, array $config, ?array $checkpoint, array $options = []): \Generator
    {
        throw new LogicException(static::class.' does not support streaming rows with checkpoint.');
    }
}
