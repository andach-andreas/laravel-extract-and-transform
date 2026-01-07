<?php

namespace Andach\ExtractAndTransform\Transform\Expressions;

use Andach\ExtractAndTransform\Transform\Expression;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar;
use Illuminate\Support\Facades\DB;

class CaseExpression implements Expression
{
    private mixed $trueValue = null;
    private mixed $falseValue = null;

    public function __construct(
        private Expression $column,
        private string $operator,
        private mixed $value
    ) {}

    public function then(mixed $value): self
    {
        $this->trueValue = $value;
        return $this;
    }

    public function else(mixed $value): self
    {
        $this->falseValue = $value;
        return $this;
    }

    public function compile(Builder $query): mixed
    {
        $grammar = $query->getGrammar();

        $colSql = $this->unwrapRaw($this->column->compile($query), $grammar);

        $valSql = $this->value instanceof Expression
            ? $this->unwrapRaw($this->value->compile($query), $grammar)
            : $grammar->quoteString((string)$this->value); // Basic quoting, might need type check

        // Handle numeric literals correctly (don't quote if int/float)
        if (!($this->value instanceof Expression) && is_numeric($this->value)) {
            $valSql = $this->value;
        }

        $trueSql = $this->compileValue($this->trueValue, $query, $grammar);
        $falseSql = $this->compileValue($this->falseValue, $query, $grammar);

        return DB::raw("CASE WHEN {$colSql} {$this->operator} {$valSql} THEN {$trueSql} ELSE {$falseSql} END");
    }

    private function compileValue(mixed $value, Builder $query, Grammar $grammar): string
    {
        if ($value instanceof Expression) {
            return $this->unwrapRaw($value->compile($query), $grammar);
        }
        if (is_numeric($value)) {
            return (string)$value;
        }
        return $grammar->quoteString((string)$value);
    }

    public function toArray(): array
    {
        return [
            'type' => 'case',
            'column' => $this->column->toArray(),
            'operator' => $this->operator,
            'value' => $this->value instanceof Expression ? $this->value->toArray() : $this->value,
            'true_value' => $this->trueValue instanceof Expression ? $this->trueValue->toArray() : $this->trueValue,
            'false_value' => $this->falseValue instanceof Expression ? $this->falseValue->toArray() : $this->falseValue,
        ];
    }

    private function unwrapRaw($value, Grammar $grammar)
    {
        if ($value instanceof \Illuminate\Database\Query\Expression) {
            return $value->getValue($grammar);
        }
        return $value;
    }
}
