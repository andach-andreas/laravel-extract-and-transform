<?php

namespace Andach\ExtractAndTransform\Transform\Expressions;

use Andach\ExtractAndTransform\Transform\Expression;
use Illuminate\Database\Query\Builder;
use Illuminate\Support\Facades\DB;

class CaseExpression implements Expression
{
    private array $when;
    private Expression $then;
    private ?Expression $else;

    public function __construct(array $when, Expression $then, ?Expression $else = null)
    {
        $this->when = $when;
        $this->then = $then;
        $this->else = $else;
    }

    public function compile(Builder $query): mixed
    {
        $column = $this->when['column'];
        $operator = $this->when['operator'];
        $value = $this->when['value'];

        $thenSql = $this->then->compile($query);
        if ($thenSql instanceof \Illuminate\Database\Query\Expression) {
            $thenSql = $thenSql->getValue($query->getGrammar());
        }

        $elseSql = 'NULL';
        if ($this->else) {
            $elseSql = $this->else->compile($query);
            if ($elseSql instanceof \Illuminate\Database\Query\Expression) {
                $elseSql = $elseSql->getValue($query->getGrammar());
            }
        }

        $sql = "CASE WHEN ";

        if ($column instanceof Expression) {
            $columnSql = $column->compile($query);
            if ($columnSql instanceof \Illuminate\Database\Query\Expression) {
                $columnSql = $columnSql->getValue($query->getGrammar());
            }
        } else {
            $columnSql = "`{$column}`";
        }

        if ($operator === 'IN' || $operator === 'NOT IN') {
            $quotedValues = array_map(fn($v) => DB::getPdo()->quote($v), (array) $value);
            $placeholders = implode(',', $quotedValues);
            $sql .= "{$columnSql} {$operator} ({$placeholders})";
        } else {
            $quotedValue = DB::getPdo()->quote($value);
            $sql .= "{$columnSql} {$operator} {$quotedValue}";
        }

        $sql .= " THEN {$thenSql} ELSE {$elseSql} END";

        return DB::raw($sql);
    }

    public function toArray(): array
    {
        return [
            'type' => 'case',
            'when' => $this->when,
            'then' => $this->then->toArray(),
            'else' => $this->else ? $this->else->toArray() : null,
        ];
    }
}
