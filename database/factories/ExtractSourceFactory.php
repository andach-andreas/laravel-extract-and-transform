<?php

namespace Andach\ExtractAndTransform\Database\Factories;

use Andach\ExtractAndTransform\Models\ExtractSource;
use Illuminate\Database\Eloquent\Factories\Factory;

class ExtractSourceFactory extends Factory
{
    protected $model = ExtractSource::class;

    public function definition(): array
    {
        return [
            'name' => $this->faker->unique()->word(),
            'connector' => $this->faker->randomElement(['csv', 'sql']),
            'config' => ['path' => $this->faker->filePath()],
        ];
    }
}
