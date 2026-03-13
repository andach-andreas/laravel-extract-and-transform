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

    public function compile(Builder $query): string
    {
        $compiledParts = array_map(function (Expression $expr) use ($query) {
            return $expr->compile($query);
        }, $this->expressions);

        $sql = 'COALESCE(' . implode(', ', $compiledParts) . ')';

        return DB::raw($sql)->getValue($query->getGrammar());
    }

    public function toArray(): array
    {
        return [
            'type' => 'coalesce',
            'expressions' => array_map(fn (Expression $expr) => $expr->toArray(), $this->expressions),
        ];
    }
}
