<?php

namespace Andach\ExtractAndTransform\Transform\Expressions;

use Andach\ExtractAndTransform\Transform\Expression;
use Andach\ExtractAndTransform\Transform\Traits\HasStringFunctions;
use Illuminate\Database\Query\Builder;
use Illuminate\Support\Facades\DB;

class LookupExpression implements Expression
{
    use HasStringFunctions;

    private array $steps = [];

    public function __construct(
        string $targetTable,
        string $localKey,
        string $foreignKey,
        string $targetColumn
    ) {
        $this->steps[] = [
            'table' => $targetTable,
            'local' => $localKey,
            'foreign' => $foreignKey,
            'target' => $targetColumn,
        ];
    }

    public function then(string $targetTable, string $foreignKey, string $targetColumn): self
    {
        $previousStep = end($this->steps);

        $this->steps[] = [
            'table' => $targetTable,
            'local' => $previousStep['target'], // Connect to previous result
            'foreign' => $foreignKey,
            'target' => $targetColumn,
        ];

        return $this;
    }

    public function compile(Builder $query): mixed
    {
        $sourceTable = $query->from;
        $grammar = $query->getGrammar();

        foreach ($this->steps as $step) {
            // Deterministic alias to allow multiple lookups to the same table/key combo
            // We include the sourceTable in the hash to ensure uniqueness if the same lookup chain is used multiple times
            // but starting from different points (though usually sourceTable is static for a query).
            // More importantly, we need to ensure that if we have multiple lookups, they get different aliases.
            // The previous implementation used md5($targetTable.$localKey.$foreignKey).
            // Here we should include the step details.

            $alias = 'lkp_'.substr(md5(json_encode($step).$sourceTable), 0, 10);

            // Check if this join is already added to avoid duplication
            $alreadyJoined = false;
            if ($query->joins) {
                foreach ($query->joins as $join) {
                    if (str_contains($join->table, " as {$alias}")) {
                        $alreadyJoined = true;
                        break;
                    }
                }
            }

            if (! $alreadyJoined) {
                $query->leftJoin(
                    "{$step['table']} as {$alias}",
                    "{$sourceTable}.{$step['local']}",
                    '=',
                    "{$alias}.{$step['foreign']}"
                );
            }

            // The next join will hang off this alias
            $sourceTable = $alias;
        }

        $lastStep = end($this->steps);

        return DB::raw($grammar->wrap("{$sourceTable}.{$lastStep['target']}"));
    }

    public function toArray(): array
    {
        return [
            'type' => 'lookup',
            'steps' => $this->steps,
        ];
    }
}
