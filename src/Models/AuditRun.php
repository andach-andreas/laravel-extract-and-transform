<?php

namespace Andach\ExtractAndTransform\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasMany;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\DB;

class AuditRun extends Model
{
    protected $guarded = [];

    protected $casts = [
        'identifier_column' => 'array',
    ];

    public function __construct(array $attributes = [])
    {
        parent::__construct($attributes);
        $this->setTable(config('extract-data.internal_table_prefix', 'andach_leat_').'audit_runs');
    }

    public function logs(): HasMany
    {
        return $this->hasMany(AuditLog::class);
    }

    public function getViolationsByRow(): Collection
    {
        return $this->logs->groupBy('row_identifier');
    }

    public function getViolationsByColumn(): Collection
    {
        return $this->logs->groupBy('column_name');
    }

    public function getFailedRows(): Collection
    {
        $failedIds = $this->logs()->distinct()->pluck('row_identifier')->toArray();

        if (empty($failedIds)) {
            return collect();
        }

        $query = DB::table($this->table_name);
        $identifierColumn = $this->identifier_column;

        // If it was stored as a single string in JSON (e.g. "id"), cast handles it?
        // If I save it as `['id']` or `"id"`, array cast might behave differently.
        // I'll ensure AuditorService saves it as an array or I handle both here.
        // Ideally, AuditorService should normalize it to array before saving.

        $cols = is_array($identifierColumn) ? $identifierColumn : [$identifierColumn];

        if (count($cols) === 1) {
            $query->whereIn($cols[0], $failedIds);
        } else {
            // Composite key handling
            $driver = DB::connection()->getDriverName();
            if ($driver === 'sqlite') {
                $concatSql = implode(" || '-' || ", array_map(fn ($col) => "`{$col}`", $cols));
            } else {
                $concatSql = 'CONCAT('.implode(", '-', ", array_map(fn ($col) => "`{$col}`", $cols)).')';
            }

            $query->whereIn(DB::raw($concatSql), $failedIds);
        }

        return $query->get();
    }
}
