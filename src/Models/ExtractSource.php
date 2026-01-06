<?php

namespace Andach\ExtractAndTransform\Models;

use Andach\ExtractAndTransform\Database\Factories\ExtractSourceFactory;
use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasMany;

final class ExtractSource extends Model
{
    use HasFactory;

    protected $guarded = [];

    protected $casts = [
        'config' => 'array',
    ];

    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->setTable(config('extract-data.internal_table_prefix', 'andach_leat_').'extract_sources');
    }

    protected static function newFactory(): ExtractSourceFactory
    {
        return ExtractSourceFactory::new();
    }

    public function syncProfiles(): HasMany
    {
        return $this->hasMany(SyncProfile::class);
    }

    public function connect(): \Andach\ExtractAndTransform\Source
    {
        return new \Andach\ExtractAndTransform\Source(
            $this,
            app(\Andach\ExtractAndTransform\ExtractAndTransform::class)
        );
    }
}
