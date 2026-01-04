<?php

namespace Andach\ExtractAndTransform\Transform\Expressions;

use Andach\ExtractAndTransform\Transform\Expression;
use Andach\ExtractAndTransform\Transform\Traits\HasNumericFunctions;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar;
use Illuminate\Support\Facades\DB;

class MathExpression implements Expression
{
    use HasNumericFunctions;

    public function __construct(
        private Expression $left,
        private string $operator,
        private float|int|Expression $right
    ) {}

    public function compile(Builder $query): mixed
    {
        $grammar = $query->getGrammar();

        $leftSql = $this->unwrapRaw($this->left->compile($query), $grammar);

        $rightSql = $this->right;
        if ($this->right instanceof Expression) {
            $rightSql = $this->unwrapRaw($this->right->compile($query), $grammar);
        }

        return DB::raw("({$leftSql} {$this->operator} {$rightSql})");
    }

    public function toArray(): array
    {
        return [
            'type' => 'math',
            'left' => $this->left->toArray(),
            'operator' => $this->operator,
            'right' => $this->right instanceof Expression ? $this->right->toArray() : $this->right,
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
