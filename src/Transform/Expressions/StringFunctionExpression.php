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
        $driver = $query->getConnection()->getDriverName();

        if ($this->function === 'SPLIT_PART') {
            $delimiter = $grammar->quoteString($this->arguments[0]);
            $index = (int) $this->arguments[1];

            if ($driver === 'mysql') {
                // MySQL logic to return NULL if the part doesn't exist.
                // We check if the number of delimiters is sufficient.
                // count = (length(str) - length(replace(str, delim, ''))) / length(delim)
                // We need count >= index.

                $countExpr = "(CHAR_LENGTH({$columnSql}) - CHAR_LENGTH(REPLACE({$columnSql}, {$delimiter}, ''))) / CHAR_LENGTH({$delimiter})";

                $extractExpr = "";
                $count = $index + 1;
                if ($index === 0) {
                    $extractExpr = "SUBSTRING_INDEX({$columnSql}, {$delimiter}, 1)";
                } else {
                    $extractExpr = "SUBSTRING_INDEX(SUBSTRING_INDEX({$columnSql}, {$delimiter}, {$count}), {$delimiter}, -1)";
                }

                return DB::raw("IF({$countExpr} >= {$index}, {$extractExpr}, NULL)");
            }

            if ($driver === 'pgsql') {
                // Postgres SPLIT_PART returns empty string if out of bounds.
                // We wrap it in NULLIF to return NULL instead.
                $pgIndex = $index + 1;
                return DB::raw("NULLIF(SPLIT_PART({$columnSql}, {$delimiter}, {$pgIndex}), '')");
            }

            if ($driver === 'sqlite') {
                // SQLite polyfill (registered in ServiceProvider) returns NULL if out of bounds.
                $pgIndex = $index + 1;
                return DB::raw("SPLIT_PART({$columnSql}, {$delimiter}, {$pgIndex})");
            }
        }

        $args = array_map(fn ($arg) => $grammar->quoteString($arg), $this->arguments);
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
