<?php

namespace Andach\ExtractAndTransform\Models;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

class AuditLog extends Model
{
    protected $guarded = [];

    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->setTable(config('extract-data.internal_table_prefix', 'andach_leat_').'audit_logs');
    }

    public function run(): BelongsTo
    {
        return $this->belongsTo(AuditRun::class, 'audit_run_id');
    }

    public function overrideWith(mixed $value, ?string $reason = null): Correction
    {
        // We need the table name from the parent run
        $tableName = $this->run->table_name;

        return ExtractAndTransform::addCorrection(
            $tableName,
            $this->row_identifier,
            $this->column_name,
            $value,
            $reason ?? "Correction for rule '{$this->rule_name}' failure"
        );
    }
}
