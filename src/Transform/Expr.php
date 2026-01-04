<?php

namespace Andach\ExtractAndTransform\Transform;

use Andach\ExtractAndTransform\Transform\Expressions\ColumnExpression;
use Andach\ExtractAndTransform\Transform\Expressions\ConcatExpression;
use Andach\ExtractAndTransform\Transform\Expressions\LookupExpression;
use Andach\ExtractAndTransform\Transform\Expressions\MapExpression;

class Expr
{
    public static function col(string $column): ColumnExpression
    {
        return new ColumnExpression($column);
    }

    public static function concat(...$parts): ConcatExpression
    {
        return new ConcatExpression($parts);
    }

    public static function map(string $column, array $mapping): MapExpression
    {
        return new MapExpression($column, $mapping);
    }

    public static function lookup(string $targetTable, string $localKey, string $foreignKey, string $targetColumn): LookupExpression
    {
        return new LookupExpression($targetTable, $localKey, $foreignKey, $targetColumn);
    }
}
