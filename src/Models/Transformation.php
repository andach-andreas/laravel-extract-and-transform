<?php

namespace Andach\ExtractAndTransform\Models;

use Andach\ExtractAndTransform\Services\TransformationService;
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
        $this->setTable(config('extract-data.internal_table_prefix', 'andach_leat_').'transformations');
    }

    public function runs(): HasMany
    {
        return $this->hasMany(TransformationRun::class);
    }

    public function run(): TransformationRun
    {
        return app(TransformationService::class)->run($this);
    }
}
