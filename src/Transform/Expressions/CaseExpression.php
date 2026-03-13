<?php

namespace Andach\ExtractAndTransform\Transform\Expressions;

use Andach\ExtractAndTransform\Transform\Expression;
use Illuminate\Database\Query\Builder;
use Illuminate\Support\Facades\DB;

class CaseExpression implements Expression
{
    private array $when;
    private Expression $then;

    public function __construct(array $when, Expression $then)
    {
        $this->when = $when;
        $this->then = $then;
    }

    public function compile(Builder $query): string
    {
        $column = $this->when['column'];
        $operator = $this->when['operator'];
        $value = $this->when['value'];

        $thenSql = $this->then->compile($query);

        $sql = "CASE WHEN ";

        if ($operator === 'IN' || $operator === 'NOT IN') {
            $placeholders = implode(',', array_fill(0, count($value), '?'));
            $sql .= "`{$column}` {$operator} ({$placeholders})";
            foreach ($value as $v) {
                $query->addBinding($v, 'where');
            }
        } else {
            $sql .= "`{$column}` {$operator} ?";
            $query->addBinding($value, 'where');
        }

        $sql .= " THEN {$thenSql} ELSE NULL END";

        return DB::raw($sql)->getValue($query->getGrammar());
    }

    public function toArray(): array
    {
        return [
            'type' => 'case',
            'when' => $this->when,
            'then' => $this->then->toArray(),
        ];
    }
}
