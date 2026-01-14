<?php

namespace Andach\ExtractAndTransform\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

class EnrichmentRun extends Model
{
    protected $guarded = [];

    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->setTable(config('extract-data.internal_table_prefix', 'andach_leat_').'enrichment_runs');
    }

    public function profile(): BelongsTo
    {
        return $this->belongsTo(EnrichmentProfile::class, 'enrichment_profile_id');
    }
}
