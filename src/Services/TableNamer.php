<?php

namespace Andach\ExtractAndTransform\Services;

use Illuminate\Support\Str;

final class TableNamer
{
    public function versionedTable(
        string $prefix,
        string $sourceType,
        string $sourceName,
        string $baseTable,
        int $version
    ): string {
        $prefix = $prefix ?? '';

        // keep it safe for SQL identifiers
        $sourceType = $this->clean($sourceType);
        $sourceName = $this->clean($sourceName);
        $baseTable = $this->clean($baseTable);

        return $prefix."{$sourceType}_{$sourceName}_{$baseTable}_v{$version}";
    }

    private function clean(string $s): string
    {
        return (string) Str::of($s)
            ->lower()
            ->replace('\\', '_')
            ->replace('/', '_')
            ->replace('.', '_')
            ->replace('-', '_')
            ->replace(' ', '_')
            ->replaceMatches('/[^a-z0-9_]/', '_')
            ->replaceMatches('/_+/', '_')
            ->trim('_');
    }
}
