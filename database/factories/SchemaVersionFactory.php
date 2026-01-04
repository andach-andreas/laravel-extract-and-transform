<?php

namespace Andach\ExtractAndTransform\Database\Factories;

use Andach\ExtractAndTransform\Models\SchemaVersion;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Illuminate\Database\Eloquent\Factories\Factory;

class SchemaVersionFactory extends Factory
{
    protected $model = SchemaVersion::class;

    public function definition(): array
    {
        return [
            'sync_profile_id' => SyncProfile::factory(),
            'version_number' => 1,
            'local_table_name' => $this->faker->word().'_v1',
            'source_schema_hash' => $this->faker->sha256(),
        ];
    }
}
