<?php

namespace Andach\ExtractAndTransform\Transform\Traits;

use Andach\ExtractAndTransform\Transform\Expressions\StringFunctionExpression;

trait HasStringFunctions
{
    public function upper(): StringFunctionExpression
    {
        return new StringFunctionExpression('UPPER', $this);
    }

    public function lower(): StringFunctionExpression
    {
        return new StringFunctionExpression('LOWER', $this);
    }

    public function trim(): StringFunctionExpression
    {
        return new StringFunctionExpression('TRIM', $this);
    }

    public function replace(string $search, string $replace): StringFunctionExpression
    {
        return new StringFunctionExpression('REPLACE', $this, [$search, $replace]);
    }

    public function split(string $delimiter, int $index): StringFunctionExpression
    {
        return new StringFunctionExpression('SPLIT_PART', $this, [$delimiter, $index]);
    }
}
