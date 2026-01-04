<?php

namespace Andach\ExtractAndTransform\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasMany;

class Transformation extends Model
{
    protected $guarded = [];

    protected $casts = [
        'configuration' => 'array',
    ];

    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->setTable(config('extract-data.internal_table_prefix', 'andach_led_').'transformations');
    }

    public function runs(): HasMany
    {
        return $this->hasMany(TransformationRun::class);
    }
}
