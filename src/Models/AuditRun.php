<?php

namespace Andach\ExtractAndTransform\Models;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\HasMany;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\DB;

class AuditRun extends Model
{
    protected $guarded = [];

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

    public function getFailedRows(string|array $identifierColumn): Collection
    {
        $failedIds = $this->logs()->distinct()->pluck('row_identifier')->toArray();

        if (empty($failedIds)) {
            return collect();
        }

        $query = DB::table($this->table_name);

        if (is_string($identifierColumn)) {
            $query->whereIn($identifierColumn, $failedIds);
        } else {
            // Composite key handling
            // We reconstruct the CONCAT logic to match against the stored identifiers
            $driver = DB::connection()->getDriverName();
            if ($driver === 'sqlite') {
                $concatSql = implode(" || '-' || ", array_map(fn($col) => "`{$col}`", $identifierColumn));
            } else {
                $concatSql = "CONCAT(" . implode(", '-', ", array_map(fn($col) => "`{$col}`", $identifierColumn)) . ")";
            }

            $query->whereIn(DB::raw($concatSql), $failedIds);
        }

        return $query->get();
    }
}
