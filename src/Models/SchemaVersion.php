<?php

namespace Andach\ExtractAndTransform\Models;

use Andach\ExtractAndTransform\Database\Factories\SchemaVersionFactory;
use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

final class SchemaVersion extends Model
{
    use HasFactory;

    protected $guarded = [];

    protected $casts = [
        'configuration' => 'array',
        'column_mapping' => 'array',
        'schema_overrides' => 'array',
    ];

    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->setTable(config('extract-data.internal_table_prefix', 'andach_led_').'schema_versions');
    }

    protected static function newFactory(): SchemaVersionFactory
    {
        return SchemaVersionFactory::new();
    }

    public function profile(): BelongsTo
    {
        return $this->belongsTo(SyncProfile::class, 'sync_profile_id');
    }
}
