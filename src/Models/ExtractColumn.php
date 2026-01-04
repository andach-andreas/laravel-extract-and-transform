<?php

namespace Andach\ExtractAndTransform\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

final class ExtractColumn extends Model
{
    protected $table = 'extract_columns';

    protected $fillable = [
        'extract_schema_version_id',
        'remote_name',
        'remote_type',
        'local_name',
        'local_type',
        'selected',
        'nullable',
        'position',
    ];

    public function schemaVersion(): BelongsTo
    {
        return $this->belongsTo(ExtractSchemaVersion::class, 'extract_schema_version_id');
    }
}
