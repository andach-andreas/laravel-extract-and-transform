<?php

namespace Andach\ExtractAndTransform\Models;

use Andach\ExtractAndTransform\Database\Factories\SyncProfileFactory;
use Andach\ExtractAndTransform\Services\SyncService;
use Andach\ExtractAndTransform\Services\TableManager;
use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\HasMany;

final class SyncProfile extends Model
{
    use HasFactory;

    protected $guarded = [];

    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->setTable(config('extract-data.internal_table_prefix', 'andach_led_').'sync_profiles');
    }

    protected static function newFactory(): SyncProfileFactory
    {
        return SyncProfileFactory::new();
    }

    public function source(): BelongsTo
    {
        return $this->belongsTo(ExtractSource::class, 'extract_source_id');
    }

    public function runs(): HasMany
    {
        return $this->hasMany(SyncRun::class);
    }

    public function schemaVersions(): HasMany
    {
        return $this->hasMany(SchemaVersion::class);
    }

    public function activeSchemaVersion(): BelongsTo
    {
        return $this->belongsTo(SchemaVersion::class, 'active_schema_version_id');
    }

    public function newVersion(array $mapping = [], array $overrides = [], ?string $tableName = null, array $config = []): SchemaVersion
    {
        $activeVersion = $this->activeSchemaVersion;
        $nextVersionNumber = ($activeVersion ? $activeVersion->version_number : 0) + 1;

        if (! $tableName) {
            if ($activeVersion && $activeVersion->local_table_name) {
                $tableName = preg_replace('/_v\d+$/', '', $activeVersion->local_table_name).'_v'.$nextVersionNumber;
            } else {
                $dummyVersion = new SchemaVersion(['version_number' => $nextVersionNumber]);
                $tableName = app(TableManager::class)->generateTableName($this, $dummyVersion);
            }
        }

        return $this->schemaVersions()->create([
            'version_number' => $nextVersionNumber,
            'local_table_name' => $tableName,
            'column_mapping' => $mapping ?: ($activeVersion ? $activeVersion->column_mapping : []),
            'schema_overrides' => $overrides ?: ($activeVersion ? $activeVersion->schema_overrides : []),
            'configuration' => $config ?: ($activeVersion ? $activeVersion->configuration : []),
            'source_schema_hash' => '',
        ]);
    }

    public function activateVersion(SchemaVersion $version): void
    {
        $this->update(['active_schema_version_id' => $version->id]);
        $this->refresh();
    }

    public function run(): SyncRun
    {
        return app(SyncService::class)->run($this);
    }
}
