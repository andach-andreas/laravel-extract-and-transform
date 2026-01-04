<?php

namespace Andach\ExtractAndTransform\Transform\Traits;

use Andach\ExtractAndTransform\Transform\Expression;
use Andach\ExtractAndTransform\Transform\Expressions\MathExpression;

trait HasNumericFunctions
{
    public function add(float|int|Expression $value): MathExpression
    {
        return new MathExpression($this, '+', $value);
    }

    public function subtract(float|int|Expression $value): MathExpression
    {
        return new MathExpression($this, '-', $value);
    }

    public function multiply(float|int|Expression $value): MathExpression
    {
        return new MathExpression($this, '*', $value);
    }

    public function divide(float|int|Expression $value): MathExpression
    {
        return new MathExpression($this, '/', $value);
    }
}
