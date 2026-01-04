<?php

namespace Andach\ExtractAndTransform\Transform\Expressions;

use Andach\ExtractAndTransform\Transform\Expression;
use Andach\ExtractAndTransform\Transform\Traits\HasStringFunctions;
use Illuminate\Database\Query\Builder;
use Illuminate\Support\Facades\DB;

class LookupExpression implements Expression
{
    use HasStringFunctions;

    public function __construct(
        private string $targetTable,
        private string $localKey,
        private string $foreignKey,
        private string $targetColumn
    ) {}

    public function compile(Builder $query): mixed
    {
        // Deterministic alias to allow multiple lookups to the same table/key combo
        $alias = 'lkp_'.substr(md5($this->targetTable.$this->localKey.$this->foreignKey), 0, 8);

        // Get the main table from the query to fully qualify the local key
        $sourceTable = $query->from;

        // Check if this join is already added to avoid duplication
        $alreadyJoined = false;
        if ($query->joins) {
            foreach ($query->joins as $join) {
                // Laravel stores table as "table as alias" or just "table"
                if (str_contains($join->table, " as {$alias}")) {
                    $alreadyJoined = true;
                    break;
                }
            }
        }

        if (! $alreadyJoined) {
            $query->leftJoin("{$this->targetTable} as {$alias}", "{$sourceTable}.{$this->localKey}", '=', "{$alias}.{$this->foreignKey}");
        }

        $grammar = $query->getGrammar();

        return DB::raw($grammar->wrap("{$alias}.{$this->targetColumn}"));
    }

    public function toArray(): array
    {
        return [
            'type' => 'lookup',
            'target_table' => $this->targetTable,
            'local_key' => $this->localKey,
            'foreign_key' => $this->foreignKey,
            'target_column' => $this->targetColumn,
        ];
    }
}
