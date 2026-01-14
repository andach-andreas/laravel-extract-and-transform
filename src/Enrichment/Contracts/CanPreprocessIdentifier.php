<?php

namespace Andach\ExtractAndTransform\Enrichment\Contracts;

interface CanPreprocessIdentifier
{
    /**
     * Preprocesses a raw identifier into the format expected by the API.
     *
     * @param  string|int  $identifier  The raw identifier from the source table.
     * @return string|int The cleaned and formatted identifier.
     */
    public function preprocessIdentifier(string|int $identifier): string|int;
}
