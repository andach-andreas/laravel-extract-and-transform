<?php

namespace Andach\ExtractAndTransform\Transform\Expressions;

use Andach\ExtractAndTransform\Transform\Expression;
use Illuminate\Database\Query\Builder;

class CaseBuilder implements Expression
{
    private string|Expression $column;
    private string $operator;
    private mixed $value;
    private mixed $then = null;
    private mixed $else = null;

    public function __construct(string|Expression $column, string $operator, mixed $value)
    {
        $this->column = $column;
        $this->operator = $operator;
        $this->value = $value;
    }

    public function then(mixed $then): self
    {
        $this->then = $then instanceof Expression ? $then : new LiteralExpression($then);
        return $this;
    }

    public function else(mixed $else): self
    {
        $this->else = $else instanceof Expression ? $else : new LiteralExpression($else);
        return $this;
    }

    public function compile(Builder $query): mixed
    {
        $when = [
            'column' => $this->column,
            'operator' => $this->operator,
            'value' => $this->value,
        ];

        $thenExpr = $this->then ?? new LiteralExpression(null);

        $expression = new CaseExpression($when, $thenExpr, $this->else);

        return $expression->compile($query);
    }

    public function toArray(): array
    {
        return [
            'type' => 'case',
            'when' => [
                'column' => $this->column instanceof Expression ? $this->column->toArray() : $this->column,
                'operator' => $this->operator,
                'value' => $this->value,
            ],
            'then' => $this->then ? $this->then->toArray() : null,
            'else' => $this->else ? $this->else->toArray() : null,
        ];
    }
}
