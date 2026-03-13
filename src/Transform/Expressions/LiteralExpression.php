<?php

namespace Andach\ExtractAndTransform\Transform\Expressions;

use Andach\ExtractAndTransform\Transform\Expression;
use Illuminate\Database\Query\Builder;
use Illuminate\Support\Facades\DB;

class LiteralExpression implements Expression
{
    private mixed $value;

    public function __construct(mixed $value)
    {
        $this->value = $value;
    }

    public function compile(Builder $query): mixed
    {
        if ($this->value === null) {
            return 'NULL';
        }
        if (is_bool($this->value)) {
            return $this->value ? 'TRUE' : 'FALSE';
        }
        if (is_numeric($this->value)) {
            return $this->value;
        }

        // For strings, quoting is safest to avoid binding context issues in complex expressions
        return DB::getPdo()->quote((string) $this->value);
    }

    public function toArray(): array
    {
        return [
            'type' => 'literal',
            'value' => $this->value,
        ];
    }
}
