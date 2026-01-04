<?php

namespace Andach\ExtractAndTransform\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\HasMany;

final class ExtractSchemaVersion extends Model
{
    protected $table = 'extract_schema_versions';

    protected $fillable = [
        'extract_dataset_id',
        'version',
        'target_mode',
        'target_table',
        'base_table',
        'prefix_used',
        'remote_schema_hash',
        'mapping_hash',
    ];

    public function dataset(): BelongsTo
    {
        return $this->belongsTo(ExtractDataset::class, 'extract_dataset_id');
    }

    public function columns(): HasMany
    {
        return $this->hasMany(ExtractColumn::class, 'extract_schema_version_id')->orderBy('position');
    }
}
