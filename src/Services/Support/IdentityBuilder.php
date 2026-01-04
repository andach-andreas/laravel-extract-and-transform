<?php

namespace Andach\ExtractAndTransform\Services\Support;

final class IdentityBuilder
{
    /**
     * @param  array<string,mixed>  $parts
     */
    public function fromParts(array $parts): string
    {
        if (count($parts) === 1) {
            $v = array_values($parts)[0];

            return (is_scalar($v) || $v === null) ? (string) $v : (json_encode($v) ?: '');
        }

        ksort($parts);
        $json = json_encode($parts, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE) ?: '';

        return hash('sha256', $json);
    }

    /**
     * @param  array<int,string>  $columns
     * @param  array<string,mixed>  $row
     */
    public function fromRow(array $columns, array $row): string
    {
        if ($columns === []) {
            return '';
        }

        $parts = [];
        foreach ($columns as $c) {
            $parts[$c] = $row[$c] ?? null;
        }

        return $this->fromParts($parts);
    }
}
