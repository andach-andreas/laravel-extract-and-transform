<?php

namespace Andach\ExtractAndTransform\Database\Factories;

use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Illuminate\Database\Eloquent\Factories\Factory;

class SyncProfileFactory extends Factory
{
    protected $model = SyncProfile::class;

    public function definition(): array
    {
        return [
            'extract_source_id' => ExtractSource::factory(),
            'dataset_identifier' => $this->faker->word(),
            'strategy' => $this->faker->randomElement(['full_refresh', 'watermark']),
        ];
    }
}
