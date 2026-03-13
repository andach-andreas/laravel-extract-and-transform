<?php

namespace Andach\ExtractAndTransform\Transform\Expressions;

use Andach\ExtractAndTransform\Transform\Expression;
use Illuminate\Database\Query\Builder;
use Illuminate\Support\Facades\DB;

class CoalesceExpression implements Expression
{
    /** @var Expression[] */
    private array $expressions;

    public function __construct(array $expressions)
    {
        $this->expressions = $expressions;
    }

    public function compile(Builder $query): mixed
    {
        $grammar = $query->getGrammar();
        $compiledParts = array_map(function (Expression $expr) use ($query, $grammar) {
            $compiled = $expr->compile($query);
            if ($compiled instanceof \Illuminate\Database\Query\Expression) {
                return $compiled->getValue($grammar);
            }
            return $compiled;
        }, $this->expressions);

        $sql = 'COALESCE(' . implode(', ', $compiledParts) . ')';

        return DB::raw($sql);
    }

    public function toArray(): array
    {
        return [
            'type' => 'coalesce',
            'expressions' => array_map(fn (Expression $expr) => $expr->toArray(), $this->expressions),
        ];
    }
}
