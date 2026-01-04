<?php

namespace Andach\ExtractAndTransform\Models;

use Andach\ExtractAndTransform\Services\DatasetService;
use Andach\ExtractAndTransform\Services\Dto\ImportOptions;
use Andach\ExtractAndTransform\Services\Dto\ImportResult;
use Andach\ExtractAndTransform\Services\Dto\ReconcileOptions;
use Andach\ExtractAndTransform\Services\Dto\ReconcileResult;
use Andach\ExtractAndTransform\Services\Dto\SetupOptions;
use Andach\ExtractAndTransform\Services\ImportService;
use Andach\ExtractAndTransform\Services\ReconcileService;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\HasMany;

final class ExtractDataset extends Model
{
    protected $table = 'extract_datasets';

    protected $guarded = [];

    public function source(): BelongsTo
    {
        return $this->belongsTo(ExtractSource::class, 'extract_source_id');
    }

    public function schemaVersions(): HasMany
    {
        return $this->hasMany(ExtractSchemaVersion::class, 'extract_dataset_id');
    }

    public function inspect()
    {
        return app(DatasetService::class)->inspect($this);
    }

    public function setup(SetupOptions $opts): ExtractSchemaVersion
    {
        return app(DatasetService::class)->setup($this, $opts);
    }

    public function import(ImportOptions $opts): ImportResult
    {
        return app(ImportService::class)->import($this, $opts);
    }

    public function reconcile(ReconcileOptions $opts): ReconcileResult
    {
        return app(ReconcileService::class)->reconcile($this, $opts);
    }
}
