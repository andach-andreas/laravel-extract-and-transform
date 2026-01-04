<?php

namespace Andach\ExtractAndTransform\Services;

use Andach\ExtractAndTransform\Connectors\ConnectorRegistry;
use Andach\ExtractAndTransform\Data\RemoteDataset as RemoteDatasetDto;
use Andach\ExtractAndTransform\Models\ExtractSource;

final class SourceService
{
    public function __construct(
        private readonly ConnectorRegistry $registry,
    ) {}

    public function getByName(string $name): ExtractSource
    {
        /** @var ExtractSource|null $src */
        $src = ExtractSource::query()->where('name', $name)->first();
        if (! $src) {
            throw new \RuntimeException("Source not found: {$name}");
        }

        return $src;
    }

    /**
     * @return array<int, RemoteDatasetDto>
     */
    public function remoteDatasets(ExtractSource $source): array
    {
        $connector = $this->registry->get($source->connector);

        return $connector->datasets($source->config);
    }
}
