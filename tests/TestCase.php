<?php

namespace Andach\ExtractAndTransform\Tests;

use Andach\ExtractAndTransform\ExtractAndTransformServiceProvider;
use Orchestra\Testbench\TestCase as Orchestra;

abstract class TestCase extends Orchestra
{
    protected function getPackageProviders($app)
    {
        return [
            ExtractAndTransformServiceProvider::class,
        ];
    }

    protected function getEnvironmentSetUp($app)
    {
        // Use an in-memory SQLite database for testing
        $app['config']->set('database.default', 'testing');
        $app['config']->set('database.connections.testing', [
            'driver' => 'sqlite',
            'database' => ':memory:',
            'prefix' => '',
        ]);

        // Set a default app key for encryption
        $app['config']->set('app.key', 'base64:'.base64_encode(random_bytes(32)));
    }

    /**
     * Define database migrations.
     *
     * @return void
     */
    protected function defineDatabaseMigrations()
    {
        // Load the migration stub directly.
        // This is necessary because we are using a stub file for publishing,
        // but we need to run it as a real migration in the test environment.
        $migration1 = include __DIR__.'/../database/migrations/create_andach_laravel_extract_data_tables.php.stub';
        $migration1->up();

        $migration2 = include __DIR__.'/../database/migrations/create_andach_laravel_extract_data_audit_tables.php.stub';
        $migration2->up();

        $migration3 = include __DIR__.'/../database/migrations/create_andach_laravel_extract_data_correction_tables.php.stub';
        $migration3->up();

        $migration4 = include __DIR__.'/../database/migrations/create_andach_laravel_extract_data_enrichment_tables.php.stub';
        $migration4->up();
    }
}
