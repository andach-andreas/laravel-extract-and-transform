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
        $migration = include __DIR__.'/../database/migrations/create_andach_laravel_extract_data_tables.php.stub';
        $migration->up();
    }
}
