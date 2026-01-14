<?php

namespace Andach\ExtractAndTransform\Services;

use Andach\ExtractAndTransform\Models\Transformation;
use Andach\ExtractAndTransform\Models\TransformationRun;
use Andach\ExtractAndTransform\Transform\Expr;
use Andach\ExtractAndTransform\Transform\Expression;
use Andach\ExtractAndTransform\Transform\ExpressionFactory;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Throwable;

class TransformationService
{
    public function run(Transformation $transformation, array $selects = []): TransformationRun
    {
        $run = $transformation->runs()->create([
            'status' => 'running',
            'started_at' => now(),
        ]);

        try {
            // Hydrate configuration if selects are empty
            if (empty($selects)) {
                $config = $transformation->configuration;
                // Support both 'columns' (new GUI) and 'selects' (legacy/manual) keys
                $columns = $config['columns'] ?? $config['selects'] ?? [];

                foreach ($columns as $alias => $exprConfig) {
                    $selects[$alias] = ExpressionFactory::make($exprConfig);
                }
            }

            if (empty($selects)) {
                throw new \Exception('No columns defined for transformation.');
            }

            // 1. Determine Destination Table Name
            $currentConfigHash = md5(json_encode($transformation->configuration));

            // Ensure version is treated as integer, defaulting to 0 if null
            $version = (int) $transformation->active_version;

            if ($version === 0) {
                $version = 1;
                $transformation->update(['active_version' => 1]);
            }

            $destTable = $transformation->destination_table_pattern.'_v'.$version;

            // 2. Build the Select Query
            $query = DB::table($transformation->source_table);

            $selectSqls = [];
            foreach ($selects as $alias => $expr) {
                if (! $expr instanceof Expression) {
                    // Convert string literals/columns to ColumnExpression
                    $expr = Expr::col($expr);
                }

                $compiled = $expr->compile($query);
                // If it's a DB::raw, get the value.
                // We need to alias it.
                $sqlFragment = $this->unwrapRaw($compiled);
                $selectSqls[] = "$sqlFragment as `$alias`";
            }

            $query->select(DB::raw(implode(', ', $selectSqls)));

            // 3. Create Table if not exists (Auto-Schema)
            if (! Schema::hasTable($destTable)) {
                // Create table using CTAS (Create Table As Select) pattern
                $structureQuery = clone $query;
                $structureQuery->whereRaw('1 = 0');

                $sql = $structureQuery->toSql();
                $bindings = $structureQuery->getBindings();

                $createSql = "CREATE TABLE `$destTable` AS $sql";
                DB::statement($createSql, $bindings);
            }

            // 4. Truncate and Insert
            DB::table($destTable)->truncate();

            // INSERT INTO dest SELECT ...
            $bindings = $query->getBindings();
            $selectSql = $query->toSql();

            // Fix: Removed parentheses around $selectSql
            $insertSql = "INSERT INTO `$destTable` $selectSql";

            DB::statement($insertSql, $bindings);

            $count = DB::table($destTable)->count();

            $run->update([
                'status' => 'success',
                'destination_table' => $destTable,
                'finished_at' => now(),
                'rows_affected' => $count,
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

    public function preview(array $config, int $limit = 5): array
    {
        $sourceTable = $config['source_table'] ?? null;
        if (! $sourceTable) {
            throw new \InvalidArgumentException('Source table is required for preview.');
        }

        $selects = [];
        $columns = $config['columns'] ?? $config['selects'] ?? [];
        foreach ($columns as $alias => $exprConfig) {
            $selects[$alias] = ExpressionFactory::make($exprConfig);
        }

        $query = DB::table($sourceTable);

        $selectSqls = [];
        foreach ($selects as $alias => $expr) {
            if (! $expr instanceof Expression) {
                $expr = Expr::col($expr);
            }
            $compiled = $expr->compile($query);
            $sqlFragment = $this->unwrapRaw($compiled);
            $selectSqls[] = "$sqlFragment as `$alias`";
        }

        if (empty($selectSqls)) {
            $query->select('*');
        } else {
            $query->select(DB::raw(implode(', ', $selectSqls)));
        }

        return $query->limit($limit)->get()->map(fn ($row) => (array) $row)->toArray();
    }

    private function unwrapRaw($value)
    {
        if ($value instanceof \Illuminate\Database\Query\Expression) {
            return $value->getValue($this->grammar());
        }

        return $value;
    }

    private function grammar()
    {
        return DB::connection()->getQueryGrammar();
    }
}
