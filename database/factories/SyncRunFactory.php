<?php

namespace Andach\ExtractAndTransform\Database\Factories;

use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Models\SyncRun;
use Illuminate\Database\Eloquent\Factories\Factory;

class SyncRunFactory extends Factory
{
    protected $model = SyncRun::class;

    public function definition(): array
    {
        return [
            'sync_profile_id' => SyncProfile::factory(),
            'status' => 'success',
            'started_at' => now(),
            'finished_at' => now(),
        ];
    }
}
