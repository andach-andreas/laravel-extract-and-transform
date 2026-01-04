<?php

namespace Andach\ExtractAndTransform\Services\Support;

final class RowHasher
{
    /**
     * Stable hash for dedupe: ksort + json_encode + sha256
     *
     * @param  array<string,mixed>  $row
     */
    public function hash(array $row): string
    {
        $copy = $row;
        ksort($copy);

        $json = json_encode($copy, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE) ?: '';

        return hash('sha256', $json);
    }
}
