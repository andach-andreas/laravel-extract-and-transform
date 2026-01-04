<?php

namespace Andach\ExtractAndTransform\Tests\Unit\Models;

use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Models\SyncRun;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Foundation\Testing\RefreshDatabase;

class SyncRunTest extends TestCase
{
    use RefreshDatabase;

    public function test_profile_test(): void
    {
        $profile = SyncProfile::factory()->create();
        $run = SyncRun::factory()->for($profile, 'profile')->create(); // Specify relationship name

        $this->assertInstanceOf(BelongsTo::class, $run->profile());
        $this->assertTrue($run->profile->is($profile));
    }
}
