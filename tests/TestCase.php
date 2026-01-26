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
        // Use an in-memory SQLite database for default testing
        $app['config']->set('database.default', 'testing');
        $app['config']->set('database.connections.testing', [
            'driver' => 'sqlite',
            'database' => ':memory:',
            'prefix' => '',
        ]);

        // Add connections for MySQL, PostgreSQL, and SQLite for specific tests
        $app['config']->set('database.connections.mysql_test', [
            'driver' => 'mysql',
            'host' => env('DB_MYSQL_HOST', '127.0.0.1'),
            'port' => env('DB_MYSQL_PORT', '3306'),
            'database' => env('DB_MYSQL_DATABASE', 'test_db'),
            'username' => env('DB_MYSQL_USERNAME', 'root'),
            'password' => env('DB_MYSQL_PASSWORD', 'password'),
            'charset' => 'utf8mb4',
            'collation' => 'utf8mb4_unicode_ci',
            'prefix' => '',
            'strict' => true,
            'engine' => null,
        ]);

        $app['config']->set('database.connections.pgsql_test', [
            'driver' => 'pgsql',
            'host' => env('DB_PGSQL_HOST', '127.0.0.1'),
            'port' => env('DB_PGSQL_PORT', '5432'),
            'database' => env('DB_PGSQL_DATABASE', 'test_db'),
            'username' => env('DB_PGSQL_USERNAME', 'postgres'),
            'password' => env('DB_PGSQL_PASSWORD', 'password'),
            'charset' => 'utf8',
            'prefix' => '',
            'schema' => 'public',
            'sslmode' => 'prefer',
        ]);

        // Use a temporary file for SQLite file tests
        $app['config']->set('database.connections.sqlite_file_test', [
            'driver' => 'sqlite',
            'database' => sys_get_temp_dir().'/test_db.sqlite',
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
