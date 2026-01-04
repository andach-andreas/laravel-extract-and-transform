<?php

namespace Andach\ExtractAndTransform\Transform\Expressions;

use Andach\ExtractAndTransform\Transform\Expression;
use Andach\ExtractAndTransform\Transform\Traits\HasStringFunctions;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar;
use Illuminate\Support\Facades\DB;

class StringFunctionExpression implements Expression
{
    use HasStringFunctions;

    public function __construct(
        private string $function,
        private Expression $column,
        private array $arguments = []
    ) {}

    public function compile(Builder $query): mixed
    {
        $grammar = $query->getGrammar();
        $columnSql = $this->unwrapRaw($this->column->compile($query), $grammar);

        $args = array_map(fn($arg) => $grammar->quoteString($arg), $this->arguments);
        $allArgs = implode(', ', array_merge([$columnSql], $args));

        return DB::raw("{$this->function}({$allArgs})");
    }

    public function toArray(): array
    {
        return [
            'type' => 'string_function',
            'function' => $this->function,
            'column' => $this->column->toArray(),
            'arguments' => $this->arguments,
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
