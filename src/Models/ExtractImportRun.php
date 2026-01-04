<?php

namespace Andach\ExtractAndTransform\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

final class ExtractImportRun extends Model
{
    protected $table = 'extract_import_runs';

    protected $fillable = [
        'extract_dataset_id',
        'extract_schema_version_id',
        'strategy',
        'run_type',
        'status',
        'started_at',
        'finished_at',
        'rows_read',
        'rows_written',
        'checkpoint',
        'error',
    ];

    protected $casts = [
        'started_at' => 'datetime',
        'finished_at' => 'datetime',
    ];

    public function dataset(): BelongsTo
    {
        return $this->belongsTo(ExtractDataset::class, 'extract_dataset_id');
    }

    public function schemaVersion(): BelongsTo
    {
        return $this->belongsTo(ExtractSchemaVersion::class, 'extract_schema_version_id');
    }
}
