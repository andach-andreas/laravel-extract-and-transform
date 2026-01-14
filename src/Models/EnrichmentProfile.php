<?php

namespace Andach\ExtractAndTransform\Models;

use Andach\ExtractAndTransform\Services\EnrichmentService;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasMany;

class EnrichmentProfile extends Model
{
    protected $guarded = [];

    protected $casts = [
        'config' => 'array',
    ];

    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->setTable(config('extract-data.internal_table_prefix', 'andach_leat_').'enrichment_profiles');
    }

    public function runs(): HasMany
    {
        return $this->hasMany(EnrichmentRun::class);
    }

    public function run(): EnrichmentRun
    {
        return app(EnrichmentService::class)->run($this);
    }
}
