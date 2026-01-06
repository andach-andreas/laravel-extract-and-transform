<?php

namespace Andach\ExtractAndTransform;

use Andach\ExtractAndTransform\Connectors\ConnectorRegistry;
use Andach\ExtractAndTransform\Connectors\Csv\CsvConnector;
use Andach\ExtractAndTransform\Connectors\HubSpot\HubSpotConnector;
use Andach\ExtractAndTransform\Connectors\Sql\SqlConnector;
use Andach\ExtractAndTransform\Services\RetryService;
use Andach\ExtractAndTransform\Services\RowTransformer;
use Andach\ExtractAndTransform\Services\TableManager;
use Andach\ExtractAndTransform\Strategies\ContentHashStrategy;
use Andach\ExtractAndTransform\Strategies\FullRefreshStrategy;
use Andach\ExtractAndTransform\Strategies\IdDiffStrategy;
use Andach\ExtractAndTransform\Strategies\StrategyRegistry;
use Andach\ExtractAndTransform\Strategies\WatermarkStrategy;
use Illuminate\Foundation\AliasLoader;
use Spatie\LaravelPackageTools\Package;
use Spatie\LaravelPackageTools\PackageServiceProvider;

final class ExtractAndTransformServiceProvider extends PackageServiceProvider
{
    public function configurePackage(Package $package): void
    {
        $package
            ->name('extract-and-transform')
            ->hasConfigFile('extract-data')
            ->hasMigration('create_andach_laravel_extract_data_tables')
            ->hasMigration('create_andach_laravel_extract_data_audit_tables')
            ->hasMigration('create_andach_laravel_extract_data_correction_tables');
        // ->hasAlias() method removed as it is not supported in this version.
    }

    public function packageRegistered(): void
    {
        // Manually register the Facade alias.
        $loader = AliasLoader::getInstance();
        $loader->alias('ExtractAndTransform', \Andach\ExtractAndTransform\Facades\ExtractAndTransform::class);

        // Register services into the container.
        $this->app->singleton(ConnectorRegistry::class);
        $this->app->singleton(StrategyRegistry::class);
        $this->app->singleton(ExtractAndTransform::class);
        $this->app->singleton(TableManager::class);
        $this->app->singleton(RowTransformer::class);
        $this->app->singleton(RetryService::class);

        $this->app->afterResolving(
            ConnectorRegistry::class,
            function (ConnectorRegistry $registry) {
                $registry->register(app(CsvConnector::class));
                $registry->register(app(SqlConnector::class));
                $registry->register(app(HubSpotConnector::class));
            }
        );

        $this->app->afterResolving(
            StrategyRegistry::class,
            function (StrategyRegistry $registry) {
                $registry->register('full_refresh', app(FullRefreshStrategy::class));
                $registry->register('watermark', app(WatermarkStrategy::class));
                $registry->register('id_diff', app(IdDiffStrategy::class));
                $registry->register('content_hash', app(ContentHashStrategy::class));
            }
        );
    }
}
