<?php

namespace Andach\ExtractAndTransform\Transform;

use Andach\ExtractAndTransform\Transform\Expressions\ColumnExpression;
use Andach\ExtractAndTransform\Transform\Expressions\ConcatExpression;
use Andach\ExtractAndTransform\Transform\Expressions\LookupExpression;
use Andach\ExtractAndTransform\Transform\Expressions\MapExpression;
use Andach\ExtractAndTransform\Transform\Expressions\MathExpression;
use Andach\ExtractAndTransform\Transform\Expressions\StringFunctionExpression;
use InvalidArgumentException;

class ExpressionFactory
{
    public static function make(mixed $config): mixed
    {
        // If it's not an array with a 'type', it's a literal value (string/int/float)
        if (! is_array($config) || ! isset($config['type'])) {
            return $config;
        }

        return match ($config['type']) {
            'column' => new ColumnExpression($config['column']),
            'concat' => new ConcatExpression(
                array_map(fn ($part) => self::make($part), $config['parts'])
            ),
            'map' => self::makeMapExpression($config),
            'lookup' => new LookupExpression(
                $config['target_table'],
                $config['local_key'],
                $config['foreign_key'],
                $config['target_column']
            ),
            'math' => new MathExpression(
                self::make($config['left']),
                $config['operator'],
                self::make($config['right'])
            ),
            'string_function' => new StringFunctionExpression(
                $config['function'],
                self::make($config['column']),
                $config['arguments'] ?? []
            ),
            default => throw new InvalidArgumentException("Unknown expression type: {$config['type']}"),
        };
    }

    private static function makeMapExpression(array $config): MapExpression
    {
        $expr = new MapExpression($config['column'], $config['mapping']);
        if (array_key_exists('default', $config)) {
            $expr->default($config['default']);
        }
        return $expr;
    }
}
