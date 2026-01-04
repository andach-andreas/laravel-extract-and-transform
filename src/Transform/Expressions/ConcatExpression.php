<?php

namespace Andach\ExtractAndTransform\Transform\Expressions;

use Andach\ExtractAndTransform\Transform\Expression;
use Andach\ExtractAndTransform\Transform\Traits\HasStringFunctions;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar;
use Illuminate\Support\Facades\DB;

class ConcatExpression implements Expression
{
    use HasStringFunctions;

    public function __construct(
        private array $parts
    ) {}

    public function compile(Builder $query): mixed
    {
        $grammar = $query->getGrammar();

        $compiledParts = array_map(function ($part) use ($query, $grammar) {
            if ($part instanceof Expression) {
                // It's an expression object (e.g., Expr::col('brand')), compile it
                return $this->unwrapRaw($part->compile($query), $grammar);
            }
            // It's a raw string, treat it as a literal
            return $grammar->quoteString((string) $part);
        }, $this->parts);

        $driver = $query->getConnection()->getDriverName();

        if ($driver === 'sqlite') {
             return DB::raw(implode(' || ', $compiledParts));
        }

        // Standard SQL CONCAT for MySQL, Postgres, etc.
        return DB::raw("CONCAT(" . implode(', ', $compiledParts) . ")");
    }

    public function toArray(): array
    {
        return [
            'type' => 'concat',
            'parts' => array_map(fn($p) => $p instanceof Expression ? $p->toArray() : $p, $this->parts),
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
