<?php

namespace Andach\ExtractAndTransform\Transform\Expressions;

use Andach\ExtractAndTransform\Transform\Expression;
use Andach\ExtractAndTransform\Transform\Traits\HasNumericFunctions;
use Andach\ExtractAndTransform\Transform\Traits\HasStringFunctions;
use Illuminate\Database\Query\Builder;
use Illuminate\Support\Facades\DB;

class ColumnExpression implements Expression
{
    use HasNumericFunctions;
    use HasStringFunctions;

    public function __construct(
        private string $column
    ) {}

    public function compile(Builder $query): mixed
    {
        $grammar = $query->getGrammar();
        $sourceTable = $query->from;

        // Qualify the column with the main source table to avoid ambiguity in joins
        return DB::raw($grammar->wrap("{$sourceTable}.{$this->column}"));
    }

    public function toArray(): array
    {
        return [
            'type' => 'column',
            'column' => $this->column,
        ];
    }
}
