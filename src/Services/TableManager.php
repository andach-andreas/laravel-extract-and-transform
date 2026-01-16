<?php

namespace Andach\ExtractAndTransform\Services;

use Andach\ExtractAndTransform\Models\SchemaVersion;
use Andach\ExtractAndTransform\Models\SyncProfile;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Str;

class TableManager
{
    public function __construct() {}

    public function ensureTableExists(SyncProfile $profile, SchemaVersion $version): string
    {
        $tableName = $version->local_table_name ?? $this->generateTableName($profile, $version);

        if (! Schema::hasTable($tableName)) {
            $this->createTable($tableName, $profile, $version);
        } else {
            $this->updateTableSchema($tableName, $profile, $version);
        }

        return $tableName;
    }

    public function generateTableName(SyncProfile $profile, SchemaVersion $version): string
    {
        $prefix = config('extract-data.table_prefix', 'andach_');
        $source = $profile->source;
        $sourceType = $source->connector;
        $sourceName = Str::slug($source->name, '_');
        $datasetName = Str::slug(pathinfo($profile->dataset_identifier, PATHINFO_FILENAME), '_');
        $versionNumber = 'v'.$version->version_number;

        return "{$prefix}{$sourceType}_{$sourceName}_{$datasetName}_{$versionNumber}";
    }

    private function createTable(string $tableName, SyncProfile $profile, SchemaVersion $version): void
    {
        $mapping = $version->column_mapping ?? [];
        $overrides = $version->schema_overrides ?? [];

        Schema::create($tableName, function (Blueprint $table) use ($profile, $mapping, $overrides) {
            $table->id('__id');
            $table->string('__source_id')->nullable()->index();
            $table->string('__content_hash')->nullable()->index();
            $table->boolean('__is_deleted')->default(false);
            $table->timestamp('__last_synced_at')->useCurrent();

            $source = app(\Andach\ExtractAndTransform\ExtractAndTransform::class)->getSourceFromModel($profile->source);
            $dataset = $source->getDataset($profile->dataset_identifier);
            $schema = $dataset->getSchema();

            foreach ($schema->fields as $field) {
                $sourceName = $field->name;

                // If a mapping is provided, it acts as an allowlist.
                if (! empty($mapping)) {
                    if (array_key_exists($sourceName, $mapping)) {
                        $localName = $mapping[$sourceName];

                        // Explicitly excluded
                        if ($localName === null) {
                            continue;
                        }
                    } else {
                        // Not in mapping, so exclude it
                        continue;
                    }
                } else {
                    // No mapping provided, include everything 1:1
                    $localName = $sourceName;
                }

                $type = $overrides[$sourceName] ?? $field->suggestedLocalType;

                $this->addColumn($table, $localName, $type, true);
            }
        });
    }

    private function updateTableSchema(string $tableName, SyncProfile $profile, SchemaVersion $version): void
    {
        $mapping = $version->column_mapping ?? [];
        $overrides = $version->schema_overrides ?? [];

        $source = app(\Andach\ExtractAndTransform\ExtractAndTransform::class)->getSourceFromModel($profile->source);
        $dataset = $source->getDataset($profile->dataset_identifier);
        $schema = $dataset->getSchema();

        Schema::table($tableName, function (Blueprint $table) use ($tableName, $schema, $mapping, $overrides) {
            foreach ($schema->fields as $field) {
                $sourceName = $field->name;

                if (! empty($mapping)) {
                    if (array_key_exists($sourceName, $mapping)) {
                        $localName = $mapping[$sourceName];
                        if ($localName === null) continue;
                    } else {
                        continue;
                    }
                } else {
                    $localName = $sourceName;
                }

                if (! Schema::hasColumn($tableName, $localName)) {
                    $type = $overrides[$sourceName] ?? $field->suggestedLocalType;
                    $this->addColumn($table, $localName, $type, true);
                }
            }
        });
    }

    private function addColumn(Blueprint $table, string $name, ?string $type, bool $nullable): void
    {
        $column = null;
        $type = $type ?? 'string';

        switch ($type) {
            case 'int':
                $column = $table->integer($name);
                break;
            case 'float':
                $column = $table->float($name);
                break;
            case 'bool':
                $column = $table->boolean($name);
                break;
            case 'date':
                $column = $table->date($name);
                break;
            case 'datetime':
                $column = $table->dateTime($name);
                break;
            case 'json':
                $column = $table->json($name);
                break;
            case 'text':
                $column = $table->text($name);
                break;
            default:
                if (str_starts_with($type, 'decimal')) {
                    [, $precision, $scale] = explode(':', $type) + [null, 18, 6];
                    $column = $table->decimal($name, (int) $precision, (int) $scale);
                } else {
                    $column = $table->string($name);
                }
                break;
        }

        if ($nullable) {
            $column->nullable();
        }
    }
}
