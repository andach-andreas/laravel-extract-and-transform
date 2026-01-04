<?php

namespace Andach\ExtractAndTransform\Data;

class RemoteSchema
{
    /**
     * @param  RemoteField[]  $fields
     */
    public function __construct(
        public readonly array $fields,
    ) {}
}
