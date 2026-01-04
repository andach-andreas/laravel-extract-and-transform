<?php

namespace Andach\ExtractAndTransform\Connectors\Contracts;

use Andach\ExtractAndTransform\Data\RemoteDataset;

interface Connector
{
    /**
     * Unique key used in config/DB, e.g. "csv" or "sql".
     */
    public function key(): string;

    /**
     * Human label, used in CLI output.
     */
    public function label(): string;

    /**
     * Defines the configuration fields required by this connector.
     *
     * @return \Andach\ExtractAndTransform\Connectors\ConnectorConfigDefinition[]
     */
    public function getConfigDefinition(): array;

    /**
     * Validate and test the connection. Throw an exception on failure.
     */
    public function test(array $config): void;

    /**
     * List available datasets (CSV: usually one per configured path; SQL: tables/views).
     *
     * @return iterable<RemoteDataset>
     */
    public function datasets(array $config): iterable;
}
