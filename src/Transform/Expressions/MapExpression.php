<?php

namespace Andach\ExtractAndTransform\Transform\Expressions;

use Andach\ExtractAndTransform\Transform\Expression;
use Illuminate\Database\Query\Builder;
use Illuminate\Support\Facades\DB;

class MapExpression implements Expression
{
    private mixed $defaultValue = null;

    public function __construct(
        private string $column,
        private array $mapping
    ) {}

    public function default(mixed $value): self
    {
        $this->defaultValue = $value;
        return $this;
    }

    public function compile(Builder $query): mixed
    {
        $grammar = $query->getGrammar();
        $sourceTable = $query->from;
        $qualifiedColumn = $grammar->wrap("{$sourceTable}.{$this->column}");

        $sql = "CASE {$qualifiedColumn}";
        foreach ($this->mapping as $key => $value) {
            $k = $grammar->quoteString($key);
            $v = is_numeric($value) ? $value : $grammar->quoteString($value);
            $sql .= " WHEN {$k} THEN {$v}";
        }

        if ($this->defaultValue !== null) {
            $d = is_numeric($this->defaultValue) ? $this->defaultValue : $grammar->quoteString($this->defaultValue);
            $sql .= " ELSE {$d}";
        } else {
            $sql .= " ELSE NULL";
        }

        $sql .= " END";

        return DB::raw($sql);
    }

    public function toArray(): array
    {
        return [
            'type' => 'map',
            'column' => $this->column,
            'mapping' => $this->mapping,
            'default' => $this->defaultValue,
        ];
    }
}
