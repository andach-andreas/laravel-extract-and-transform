<?php

namespace Andach\ExtractAndTransform\Transform;

use Illuminate\Database\Query\Builder;

interface Expression
{
    /**
     * Compile the expression into a raw SQL string or a Laravel DB::raw object.
     *
     * @param Builder $query The query builder context (for joins, etc.)
     * @return mixed
     */
    public function compile(Builder $query): mixed;

    /**
     * Serialize the expression configuration for storage.
     *
     * @return array
     */
    public function toArray(): array;
}
