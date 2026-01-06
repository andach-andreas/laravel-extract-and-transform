<?php

namespace Andach\ExtractAndTransform\Services;

use Andach\ExtractAndTransform\Audit\RuleBuilder;
use Andach\ExtractAndTransform\Models\AuditLog;
use Andach\ExtractAndTransform\Models\AuditRun;
use DateTime;
use Illuminate\Support\Facades\DB;
use Throwable;

class AuditorService
{
    public function __construct(
        private readonly SqlCapabilityChecker $capabilityChecker
    ) {}

    public function run(string $tableName, string|array $identifier, array $rules): AuditRun
    {
        $run = AuditRun::create([
            'table_name' => $tableName,
            'status' => 'running',
            'started_at' => now(),
        ]);

        try {
            $totalRows = DB::table($tableName)->count();
            $run->update(['total_rows_scanned' => $totalRows]);

            $sqlRules = [];
            $phpRules = [];

            // 1. Partition rules
            foreach ($rules as $column => $ruleClosure) {
                $ruleBuilder = new RuleBuilder();
                $ruleClosure($ruleBuilder);
                foreach ($ruleBuilder->getConstraints() as $constraint) {
                    if ($this->capabilityChecker->canRunInSql($constraint)) {
                        $sqlRules[$column][] = $constraint;
                    } else {
                        $phpRules[$column][] = $constraint;
                    }
                }
            }

            // 2. Execute rules
            $sqlViolations = $this->executeSqlRules($run, $tableName, $identifier, $sqlRules);
            $phpViolations = $this->executePhpRules($run, $tableName, $identifier, $phpRules);

            $run->update([
                'status' => 'success',
                'finished_at' => now(),
                'total_violations' => $sqlViolations + $phpViolations,
            ]);

        } catch (Throwable $e) {
            $run->update([
                'status' => 'failed',
                'finished_at' => now(),
                'log_message' => $e->getMessage(),
            ]);
            throw $e;
        }

        return $run;
    }

    private function executeSqlRules(AuditRun $run, string $tableName, string|array $identifier, array $sqlRules): int
    {
        $violations = 0;
        foreach ($sqlRules as $column => $constraints) {
            foreach ($constraints as $constraint) {
                $violations += $this->applySqlConstraint($run, $tableName, $identifier, $column, $constraint);
            }
        }
        return $violations;
    }

    private function executePhpRules(AuditRun $run, string $tableName, string|array $identifier, array $phpRules): int
    {
        if (empty($phpRules)) {
            return 0;
        }

        $conditionalColumns = [];
        foreach ($phpRules as $column => $constraints) {
            foreach ($constraints as $constraint) {
                if ($constraint['type'] === 'when') {
                    $conditionalColumns[] = $constraint['condition']['column'];
                }
            }
        }

        $columnsToSelect = array_unique(array_merge(
            (array) $identifier,
            array_keys($phpRules),
            $conditionalColumns
        ));

        $violations = [];
        $now = now();

        DB::table($tableName)->select($columnsToSelect)->chunkById(500, function ($rows) use ($phpRules, $identifier, $run, $now, &$violations) {
            foreach ($rows as $row) {
                foreach ($phpRules as $column => $constraints) {
                    foreach ($constraints as $constraint) {
                        if (! $this->checkValueInPhp($row->{$column}, $constraint, $row)) {
                            $violations[] = [
                                'audit_run_id' => $run->id,
                                'row_identifier' => $this->buildIdentifierForRow($row, $identifier),
                                'column_name' => $column,
                                'rule_name' => $constraint['type'],
                                'severity' => 'error',
                                'message' => "Rule '{$constraint['type']}' failed for column '{$column}'",
                                'created_at' => $now,
                                'updated_at' => $now,
                            ];
                        }
                    }
                }
            }
        });

        if (!empty($violations)) {
            AuditLog::insert($violations);
        }

        return count($violations);
    }

    private function checkValueInPhp($value, array $constraint, \stdClass $row): bool
    {
        if ($constraint['type'] === 'when') {
            $condition = $constraint['condition'];
            $conditionMet = false;
            switch ($condition['operator']) {
                case '=':
                    $conditionMet = $row->{$condition['column']} == $condition['value'];
                    break;
            }

            if ($conditionMet) {
                foreach ($constraint['sub_constraints'] as $subConstraint) {
                    if (! $this->checkValueInPhp($value, $subConstraint, $row)) {
                        return false;
                    }
                }
            }
            return true;
        }

        if (is_null($value) || $value === '') {
            return $constraint['type'] === 'required' ? false : true;
        }

        return match ($constraint['type']) {
            'email' => filter_var($value, FILTER_VALIDATE_EMAIL) !== false,
            'uuid' => preg_match('/^[a-f\d]{8}(-[a-f\d]{4}){3}-[a-f\d]{12}$/i', $value) === 1,
            'url' => filter_var($value, FILTER_VALIDATE_URL) !== false,
            'ip' => filter_var($value, FILTER_VALIDATE_IP) !== false,
            'ipv4' => filter_var($value, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4) !== false,
            'ipv6' => filter_var($value, FILTER_VALIDATE_IP, FILTER_FLAG_IPV6) !== false,
            'credit_card' => $this->validateLuhn($value),
            'isbn' => $this->validateIsbn($value),
            'currency_code' => in_array(strtoupper($value), $this->getCurrencyCodes()),
            'latitude' => is_numeric($value) && $value >= -90 && $value <= 90,
            'longitude' => is_numeric($value) && $value >= -180 && $value <= 180,
            'date_format' => $this->validateDateFormat($value, $constraint['format']),
            'timezone' => in_array($value, timezone_identifiers_list()),
            'alpha' => ctype_alpha($value),
            'alpha_num' => ctype_alnum($value),
            'alpha_dash' => preg_match('/^[a-zA-Z0-9_-]+$/', $value) === 1,
            'json' => json_decode($value) !== null,
            'regex' => preg_match($constraint['pattern'], $value) === 1,
            'custom' => $constraint['callback']($value),
            'required' => true,
            'numeric' => is_numeric($value),
            'integer' => is_numeric($value) && floor($value) == $value,
            'in' => in_array($value, $constraint['values']),
            'not_in' => !in_array($value, $constraint['values']),
            'min_length' => mb_strlen($value) >= $constraint['length'],
            'max_length' => mb_strlen($value) <= $constraint['length'],
            'greater_than' => is_numeric($value) && $value > $constraint['value'],
            'less_than' => is_numeric($value) && $value < $constraint['value'],
            default => true,
        };
    }

    private function buildIdentifierForRow(\stdClass $row, string|array $identifier): string
    {
        if (is_string($identifier)) {
            return $row->{$identifier};
        }

        $parts = [];
        foreach ($identifier as $col) {
            $parts[] = $row->{$col};
        }
        return implode('-', $parts);
    }

    private function applySqlConstraint(AuditRun $run, string $tableName, string|array $identifier, string $column, array $constraint): int
    {
        $identifierSql = $this->buildIdentifierSql($identifier);

        $query = DB::table($tableName);
        $ruleName = $constraint['type'];
        $message = "Rule '{$ruleName}' failed for column '{$column}'";

        if ($ruleName !== 'required') {
            $query->whereNotNull($column);
        }

        switch ($constraint['type']) {
            case 'required':
                $query->whereNull($column);
                break;
            case 'numeric':
                $query->whereRaw("{$column} != 0 AND {$column} * 1.0 != {$column}");
                break;
            case 'integer':
                $query->whereRaw("CAST({$column} AS TEXT) GLOB '*[.]*'");
                break;
            case 'in':
                $query->whereNotIn($column, $constraint['values']);
                break;
            case 'not_in':
                $query->whereIn($column, $constraint['values']);
                break;
            case 'min_length':
                $query->whereRaw("LENGTH({$column}) < ?", [$constraint['length']]);
                break;
            case 'max_length':
                $query->whereRaw("LENGTH({$column}) > ?", [$constraint['length']]);
                break;
            case 'greater_than':
                $query->whereRaw("CAST({$column} AS REAL) <= ?", [$constraint['value']]);
                break;
            case 'less_than':
                $query->whereRaw("CAST({$column} AS REAL) >= ?", [$constraint['value']]);
                break;
            case 'greater_than_column':
                $query->whereColumn($column, '<=', $constraint['column']);
                break;
            case 'less_than_column':
                $query->whereColumn($column, '>=', $constraint['column']);
                break;
            case 'equal_to_column':
                $query->whereColumn($column, '!=', $constraint['column']);
                break;
            case 'not_equal_to_column':
                $query->whereColumn($column, '=', $constraint['column']);
                break;
            case 'exists_in':
                $query->whereNotExists(function ($subQuery) use ($constraint, $tableName, $column) {
                    $subQuery->select(DB::raw(1))
                        ->from($constraint['table'])
                        ->whereColumn("{$tableName}.{$column}", '=', "{$constraint['table']}.{$constraint['column']}");
                });
                break;
            case 'starts_with':
                $query->where($column, 'not like', $constraint['prefix'] . '%');
                break;
            case 'ends_with':
                $query->where($column, 'not like', '%' . $constraint['suffix']);
                break;
            default:
                return 0;
        }

        $now = now();
        $subQuery = $query->selectRaw(
            "{$run->id}, {$identifierSql}, ?, ?, ?, ?, ?, ?",
            [$column, $ruleName, 'error', $message, $now, $now]
        );

        $prefix = config('extract-data.internal_table_prefix', 'andach_leat_');
        $logTable = $prefix . 'audit_logs';

        $sql = "INSERT INTO {$logTable} (audit_run_id, row_identifier, column_name, rule_name, severity, message, created_at, updated_at) ";
        $sql .= $subQuery->toSql();

        DB::connection()->insert($sql, $subQuery->getBindings());

        return $query->count();
    }

    private function buildIdentifierSql(string|array $identifier): string
    {
        if (is_string($identifier)) {
            return "`{$identifier}`";
        }

        $driver = DB::connection()->getDriverName();

        if ($driver === 'sqlite') {
             $cols = implode(" || '-' || ", array_map(fn($col) => "`{$col}`", $identifier));
             return $cols;
        }

        $cols = implode(", '-', ", array_map(fn($col) => "`{$col}`", $identifier));
        return "CONCAT({$cols})";
    }

    // --- Helper methods for PHP validation ---

    private function validateLuhn(string $number): bool
    {
        $number = preg_replace('/\D/', '', $number);
        $sum = 0;
        $numDigits = strlen($number);
        $parity = $numDigits % 2;

        for ($i = 0; $i < $numDigits; $i++) {
            $digit = $number[$i];
            if ($i % 2 === $parity) {
                $digit *= 2;
                if ($digit > 9) {
                    $digit -= 9;
                }
            }
            $sum += $digit;
        }

        return ($sum % 10) === 0;
    }

    private function validateIsbn(string $isbn): bool
    {
        $isbn = str_replace('-', '', $isbn);
        if (strlen($isbn) === 10) {
            $sum = 0;
            for ($i = 0; $i < 9; $i++) {
                $sum += (int)$isbn[$i] * (10 - $i);
            }
            $check = 11 - ($sum % 11);
            if ($check === 11) $check = 0;
            return ($check == 10 && strtoupper($isbn[9]) == 'X') || ($check == $isbn[9]);
        } elseif (strlen($isbn) === 13) {
            $sum = 0;
            for ($i = 0; $i < 12; $i++) {
                $sum += (int)$isbn[$i] * (($i % 2 === 0) ? 1 : 3);
            }
            $check = (10 - ($sum % 10)) % 10;
            return $check == $isbn[12];
        }
        return false;
    }

    private function getCurrencyCodes(): array
    {
        // In a real app, this might be cached or come from a config
        return ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD'];
    }

    private function validateDateFormat(string $value, string $format): bool
    {
        $d = DateTime::createFromFormat($format, $value);
        return $d && $d->format($format) === $value;
    }
}
