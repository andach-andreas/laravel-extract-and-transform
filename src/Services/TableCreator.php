<?php

namespace Andach\ExtractAndTransform\Services;

use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;
use InvalidArgumentException;

final class TableCreator
{
    /**
     * @param  array<int, array{local_name:string, local_type:string, nullable:bool}>  $columns
     */
    public function create(string $table, array $columns, ?string $connection = null): void
    {
        $connection = $connection ?: (string) config('database.default');
        $schema = Schema::connection($connection);

        $schema->create($table, function (Blueprint $t) use ($columns) {
            $t->bigIncrements('__id');

            // identity + change tracking
            $t->string('__identity')->nullable()->index();
            $t->string('__op')->default('upsert')->index(); // upsert|delete|snapshot
            $t->string('__row_hash', 64)->nullable()->index();
            $t->dateTime('__source_updated_at')->nullable()->index();

            $reserved = [
                '__id', '__identity', '__op', '__row_hash', '__source_updated_at',
                '__extracted_at', '__raw',
            ];

            foreach ($columns as $col) {
                $name = $col['local_name'];
                $type = $col['local_type'];

                if (in_array($name, $reserved, true)) {
                    throw new InvalidArgumentException("Column name [{$name}] is reserved.");
                }

                // IMPORTANT: extract tables must allow tombstones and partial rows
                // so all mapped columns are nullable regardless of source nullability.
                $this->addColumn($t, $name, $type)->nullable();
            }

            $t->dateTime('__extracted_at')->index();
            $t->longText('__raw')->nullable();
        });
    }

    private function addColumn(Blueprint $t, string $name, string $type)
    {
        if (str_starts_with($type, 'decimal:')) {
            $spec = substr($type, 8);
            $parts = array_map('trim', explode(',', $spec));
            $p = isset($parts[0]) ? (int) $parts[0] : 18;
            $s = isset($parts[1]) ? (int) $parts[1] : 6;

            return $t->decimal($name, $p, $s);
        }

        return match ($type) {
            'string' => $t->string($name),
            'text' => $t->text($name),
            'int' => $t->integer($name),
            'bigint' => $t->bigInteger($name),
            'float' => $t->double($name),
            'bool' => $t->boolean($name),
            'date' => $t->date($name),
            'datetime' => $t->dateTime($name),
            'json' => $t->json($name),
            default => $t->string($name),
        };
    }
}
