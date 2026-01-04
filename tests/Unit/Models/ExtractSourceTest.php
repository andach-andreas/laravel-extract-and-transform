<?php

namespace Andach\ExtractAndTransform\Tests\Unit\Models;

use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Database\Eloquent\Relations\HasMany;
use Illuminate\Foundation\Testing\RefreshDatabase;

class ExtractSourceTest extends TestCase
{
    use RefreshDatabase;

    public function test_sync_profiles_test(): void
    {
        $source = ExtractSource::factory()->create();
        SyncProfile::factory()->for($source, 'source')->create();

        $this->assertInstanceOf(HasMany::class, $source->syncProfiles());
        $this->assertEquals(1, $source->syncProfiles()->count());
    }
}
