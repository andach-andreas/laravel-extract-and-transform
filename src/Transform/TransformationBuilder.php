<?php

namespace Andach\ExtractAndTransform\Transform;

use Andach\ExtractAndTransform\Models\Transformation;
use Andach\ExtractAndTransform\Services\TransformationService;

class TransformationBuilder
{
    private string $name;
    private string $sourceTable;
    private string $destinationTablePattern;
    private array $selects = [];
    private array $wheres = [];

    public function __construct(string $name)
    {
        $this->name = $name;
    }

    public function from(string $table): self
    {
        $this->sourceTable = $table;
        return $this;
    }

    public function select(array $columns): self
    {
        $this->selects = $columns;
        return $this;
    }

    public function toTable(string $pattern): self
    {
        $this->destinationTablePattern = $pattern;
        return $this;
    }

    public function run()
    {
        // Persist configuration
        $config = [
            'selects' => array_map(fn($expr) => $expr instanceof Expression ? $expr->toArray() : $expr, $this->selects),
            'wheres' => $this->wheres,
        ];

        $transformation = Transformation::updateOrCreate(
            ['name' => $this->name],
            [
                'source_table' => $this->sourceTable,
                'destination_table_pattern' => $this->destinationTablePattern,
                'configuration' => $config,
            ]
        );

        return app(TransformationService::class)->run($transformation, $this->selects);
    }
}
