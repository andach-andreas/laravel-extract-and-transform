<?php

namespace Andach\ExtractAndTransform\Tests\Feature;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Tests\TestCase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class SqlTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        // Create a source table in the default testing database
        Schema::create('source_users', function ($table) {
            $table->id();
            $table->string('name');
            $table->string('email');
            $table->timestamps();
        });

        DB::table('source_users')->insert([
            ['name' => 'Alice', 'email' => 'alice@example.com', 'created_at' => now(), 'updated_at' => now()],
            ['name' => 'Bob', 'email' => 'bob@example.com', 'created_at' => now(), 'updated_at' => now()],
        ]);
    }

    public function testItCanSyncFromASqlTable()
    {
        // We use the 'testing' connection which is already set up in TestCase
        ExtractAndTransform::createSource('Local SQL', 'sql', [
            'connection' => 'testing'
        ]);

        ExtractAndTransform::source('Local SQL')
            ->sync('source_users')
            ->withStrategy('full_refresh')
            ->toTable('synced_users')
            ->run();

        $this->assertTrue(Schema::hasTable('synced_users'));
        $this->assertDatabaseCount('synced_users', 2);

        $alice = DB::table('synced_users')->where('name', 'Alice')->first();
        $this->assertNotNull($alice);
        $this->assertEquals('alice@example.com', $alice->email);
    }

    public function testItCanSyncWithColumnMapping()
    {
        ExtractAndTransform::createSource('Local SQL', 'sql', [
            'connection' => 'testing'
        ]);

        ExtractAndTransform::source('Local SQL')
            ->sync('source_users')
            ->withStrategy('full_refresh')
            ->mapColumns([
                'name' => 'full_name',
                'email' => 'contact_email'
            ])
            ->toTable('mapped_users')
            ->run();

        $this->assertTrue(Schema::hasTable('mapped_users'));

        $user = DB::table('mapped_users')->first();
        $this->assertTrue(property_exists($user, 'full_name'));
        $this->assertTrue(property_exists($user, 'contact_email'));
        $this->assertFalse(property_exists($user, 'name'));
    }
}
