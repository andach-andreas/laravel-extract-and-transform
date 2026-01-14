<?php

namespace Andach\ExtractAndTransform;

use Andach\ExtractAndTransform\Models\ExtractSource;
use Illuminate\Support\Facades\App;

class Source
{
    public function __construct(
        private readonly ExtractSource $model,
        private readonly ExtractAndTransform $extractor
    ) {}

    public function getModel(): ExtractSource
    {
        return $this->model;
    }

    public function sync(string $datasetIdentifier): Sync
    {
        $profile = $this->model->syncProfiles()->firstOrNew(
            ['dataset_identifier' => $datasetIdentifier]
        );

        if (! $profile->exists) {
            $profile->strategy = 'full_refresh'; // Default strategy
        }

        return new Sync($profile, $this->extractor);
    }

    public function run(): array
    {
        return $this->model->syncProfiles->map(function ($profile) {
            return $profile->run();
        })->all();
    }

    public function getDataset(string $identifier): ?Dataset
    {
        $connector = App::make(Connectors\ConnectorRegistry::class)->get($this->model->connector);

        $config = $this->model->config;
        $updater = function (array $newConfig) {
            $this->model->update(['config' => $newConfig]);
        };
        $config['__updater'] = $updater;

        $remoteDataset = collect($connector->datasets($config))
            ->first(fn ($d) => $d->identifier === $identifier);

        if (! $remoteDataset) {
            return null;
        }

        return new Dataset($connector, $this->model->config, $remoteDataset, $updater);
    }

    public function listDatasets(): iterable
    {
        $connector = App::make(Connectors\ConnectorRegistry::class)->get($this->model->connector);

        $config = $this->model->config;
        $updater = function (array $newConfig) {
            $this->model->update(['config' => $newConfig]);
        };
        $config['__updater'] = $updater;

        $remoteDatasets = $connector->datasets($config);

        foreach ($remoteDatasets as $remoteDataset) {
            yield new Dataset($connector, $this->model->config, $remoteDataset, $updater);
        }
    }
}
