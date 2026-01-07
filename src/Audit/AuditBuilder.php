<?php

namespace Andach\ExtractAndTransform\Audit;

use Andach\ExtractAndTransform\Services\AuditorService;

class AuditBuilder
{
    private string $tableName;

    private string|array|null $identifier = null;

    private array $rules = [];

    public function __construct(string $tableName)
    {
        $this->tableName = $tableName;
    }

    public function identifiedBy(string|array $identifier): self
    {
        $this->identifier = $identifier;

        return $this;
    }

    public function check(array $rules): self
    {
        $this->rules = $rules;

        return $this;
    }

    public function run()
    {
        if ($this->identifier === null) {
            throw new \InvalidArgumentException('Audit must have an identifier defined via identifiedBy().');
        }

        return app(AuditorService::class)->run(
            $this->tableName,
            $this->identifier,
            $this->rules
        );
    }
}
