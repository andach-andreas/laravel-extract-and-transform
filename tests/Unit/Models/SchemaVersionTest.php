<?php

namespace Andach\ExtractAndTransform\Tests\Unit\Models;

use Andach\ExtractAndTransform\Models\SchemaVersion;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Foundation\Testing\RefreshDatabase;

class SchemaVersionTest extends TestCase
{
    use RefreshDatabase;

    public function test_profile_test(): void
    {
        $profile = SyncProfile::factory()->create();
        $version = SchemaVersion::factory()->for($profile, 'profile')->create();

        $this->assertInstanceOf(BelongsTo::class, $version->profile());
        $this->assertTrue($version->profile->is($profile));
    }
}
