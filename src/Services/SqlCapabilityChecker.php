<?php

namespace Andach\ExtractAndTransform\Services;

use Illuminate\Support\Facades\DB;

class SqlCapabilityChecker
{
    private string $driver;

    public function __construct()
    {
        $this->driver = DB::connection()->getDriverName();
    }

    public function canRunInSql(array $constraint): bool
    {
        return match ($constraint['type']) {
            // These rules are simple and universally supported
            'required',
            'in',
            'not_in',
            'min_length',
            'max_length',
            'greater_than',
            'less_than',
            'greater_than_column',
            'less_than_column',
            'equal_to_column',
            'not_equal_to_column',
            'exists_in',
            'starts_with',
            'ends_with'
             => true,

            // Regex is only supported by default in MySQL/Postgres
            'regex' => in_array($this->driver, ['mysql', 'pgsql']),

            // Numeric/Integer checks use specific syntax that we've tailored for SQLite/MySQL
            'numeric', 'integer' => in_array($this->driver, ['mysql', 'sqlite']),

            // All other convenience rules and custom rules are PHP-only
            default => false,
        };
    }
}
