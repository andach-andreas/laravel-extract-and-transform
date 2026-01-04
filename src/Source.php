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
        $remoteDataset = collect($connector->datasets($this->model->config))
            ->first(fn ($d) => $d->identifier === $identifier);

        if (! $remoteDataset) {
            return null;
        }

        return new Dataset($connector, $this->model->config, $remoteDataset);
    }
}
