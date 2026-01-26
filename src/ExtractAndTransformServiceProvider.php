<?php

namespace Andach\ExtractAndTransform;

use Andach\ExtractAndTransform\Connectors\ConnectorRegistry;
use Andach\ExtractAndTransform\Connectors\CRM\HubSpot\HubSpotConnector;
use Andach\ExtractAndTransform\Connectors\Finance\XeroConnector;
use Andach\ExtractAndTransform\Connectors\General\Csv\CsvConnector;
use Andach\ExtractAndTransform\Connectors\General\Excel\ExcelConnector;
use Andach\ExtractAndTransform\Connectors\General\Excel\LegacyExcelConnector;
use Andach\ExtractAndTransform\Connectors\General\Sql\MySqlConnector;
use Andach\ExtractAndTransform\Connectors\General\Sql\PostgresConnector;
use Andach\ExtractAndTransform\Connectors\General\Sql\SqliteConnector;
use Andach\ExtractAndTransform\Enrichment\Connectors\CompaniesHouseConnector;
use Andach\ExtractAndTransform\Enrichment\EnrichmentRegistry;
use Andach\ExtractAndTransform\Services\EnrichmentService;
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
            ->hasMigration('create_andach_laravel_extract_data_correction_tables')
            ->hasMigration('create_andach_laravel_extract_data_enrichment_tables')
            ->hasViews('extract-data')
            ->hasRoute('web');
    }

    public function packageRegistered(): void
    {
        $loader = AliasLoader::getInstance();
        $loader->alias('ExtractAndTransform', \Andach\ExtractAndTransform\Facades\ExtractAndTransform::class);

        // Core Services
        $this->app->singleton(ExtractAndTransform::class);
        $this->app->singleton(TableManager::class);
        $this->app->singleton(RowTransformer::class);
        $this->app->singleton(RetryService::class);

        // Sync Services
        $this->app->singleton(ConnectorRegistry::class);
        $this->app->singleton(StrategyRegistry::class);

        // Enrichment Services
        $this->app->singleton(EnrichmentRegistry::class);
        $this->app->singleton(EnrichmentService::class);

        $this->app->afterResolving(
            ConnectorRegistry::class,
            function (ConnectorRegistry $registry) {
                $registry->register(app(CsvConnector::class));
                $registry->register(app(MySqlConnector::class));
                $registry->register(app(PostgresConnector::class));
                $registry->register(app(SqliteConnector::class));
                $registry->register(app(HubSpotConnector::class));
                $registry->register(app(XeroConnector::class));
                $registry->register(app(ExcelConnector::class));
                $registry->register(app(LegacyExcelConnector::class));
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

        $this->app->afterResolving(
            EnrichmentRegistry::class,
            function (EnrichmentRegistry $registry) {
                $registry->register(app(CompaniesHouseConnector::class));
            }
        );
    }

    public function packageBooted(): void
    {
        try {
            $connection = $this->app['db']->connection();
            if ($connection->getDriverName() === 'sqlite') {
                $connection->getPdo()->sqliteCreateFunction('SPLIT_PART', function ($string, $delimiter, $position) {
                    if ($string === null) {
                        return null;
                    }
                    $parts = explode($delimiter, $string);
                    $index = $position - 1;

                    return $parts[$index] ?? null;
                }, 3);
            }
        } catch (\Throwable $e) {
            // DB might not be configured or ready, ignore.
        }
    }
}
