<?php

namespace Andach\ExtractAndTransform\Audit;

class RuleBuilder
{
    private array $constraints = [];

    // Basic Rules
    public function required(): self
    {
        $this->constraints[] = ['type' => 'required'];

        return $this;
    }

    public function numeric(): self
    {
        $this->constraints[] = ['type' => 'numeric'];

        return $this;
    }

    public function integer(): self
    {
        $this->constraints[] = ['type' => 'integer'];

        return $this;
    }

    public function string(): self
    {
        $this->constraints[] = ['type' => 'string'];

        return $this;
    }

    public function in(array $values): self
    {
        $this->constraints[] = ['type' => 'in', 'values' => $values];

        return $this;
    }

    public function notIn(array $values): self
    {
        $this->constraints[] = ['type' => 'not_in', 'values' => $values];

        return $this;
    }

    public function minLength(int $length): self
    {
        $this->constraints[] = ['type' => 'min_length', 'length' => $length];

        return $this;
    }

    public function maxLength(int $length): self
    {
        $this->constraints[] = ['type' => 'max_length', 'length' => $length];

        return $this;
    }

    public function greaterThan(int|float $value): self
    {
        $this->constraints[] = ['type' => 'greater_than', 'value' => $value];

        return $this;
    }

    public function lessThan(int|float $value): self
    {
        $this->constraints[] = ['type' => 'less_than', 'value' => $value];

        return $this;
    }

    // Cross-Column Rules
    public function greaterThanColumn(string $otherColumn): self
    {
        $this->constraints[] = ['type' => 'greater_than_column', 'column' => $otherColumn];

        return $this;
    }

    public function lessThanColumn(string $otherColumn): self
    {
        $this->constraints[] = ['type' => 'less_than_column', 'column' => $otherColumn];

        return $this;
    }

    public function equalToColumn(string $otherColumn): self
    {
        $this->constraints[] = ['type' => 'equal_to_column', 'column' => $otherColumn];

        return $this;
    }

    public function notEqualToColumn(string $otherColumn): self
    {
        $this->constraints[] = ['type' => 'not_equal_to_column', 'column' => $otherColumn];

        return $this;
    }

    // Foreign Key
    public function existsIn(string $foreignTable, string $foreignColumn): self
    {
        $this->constraints[] = ['type' => 'exists_in', 'table' => $foreignTable, 'column' => $foreignColumn];

        return $this;
    }

    // Conditional
    public function when(string $otherColumn, string $operator, mixed $value, callable $subRules): self
    {
        $subRuleBuilder = new RuleBuilder;
        $subRules($subRuleBuilder);
        $this->constraints[] = ['type' => 'when', 'condition' => ['column' => $otherColumn, 'operator' => $operator, 'value' => $value], 'sub_constraints' => $subRuleBuilder->getConstraints()];

        return $this;
    }

    // Custom
    public function custom(callable $callback): self
    {
        $this->constraints[] = ['type' => 'custom', 'callback' => $callback];

        return $this;
    }

    // Convenience Rules
    public function email(): self
    {
        $this->constraints[] = ['type' => 'email'];

        return $this;
    }

    public function uuid(): self
    {
        $this->constraints[] = ['type' => 'uuid'];

        return $this;
    }

    public function url(): self
    {
        $this->constraints[] = ['type' => 'url'];

        return $this;
    }

    public function ip(): self
    {
        $this->constraints[] = ['type' => 'ip'];

        return $this;
    }

    public function ipv4(): self
    {
        $this->constraints[] = ['type' => 'ipv4'];

        return $this;
    }

    public function ipv6(): self
    {
        $this->constraints[] = ['type' => 'ipv6'];

        return $this;
    }

    public function creditCard(): self
    {
        $this->constraints[] = ['type' => 'credit_card'];

        return $this;
    }

    public function isbn(): self
    {
        $this->constraints[] = ['type' => 'isbn'];

        return $this;
    }

    public function currencyCode(): self
    {
        $this->constraints[] = ['type' => 'currency_code'];

        return $this;
    }

    public function latitude(): self
    {
        $this->constraints[] = ['type' => 'latitude'];

        return $this;
    }

    public function longitude(): self
    {
        $this->constraints[] = ['type' => 'longitude'];

        return $this;
    }

    public function dateFormat(string $format): self
    {
        $this->constraints[] = ['type' => 'date_format', 'format' => $format];

        return $this;
    }

    public function timezone(): self
    {
        $this->constraints[] = ['type' => 'timezone'];

        return $this;
    }

    public function alpha(): self
    {
        $this->constraints[] = ['type' => 'alpha'];

        return $this;
    }

    public function alphaNum(): self
    {
        $this->constraints[] = ['type' => 'alpha_num'];

        return $this;
    }

    public function alphaDash(): self
    {
        $this->constraints[] = ['type' => 'alpha_dash'];

        return $this;
    }

    public function json(): self
    {
        $this->constraints[] = ['type' => 'json'];

        return $this;
    }

    public function startsWith(string $prefix): self
    {
        $this->constraints[] = ['type' => 'starts_with', 'prefix' => $prefix];

        return $this;
    }

    public function endsWith(string $suffix): self
    {
        $this->constraints[] = ['type' => 'ends_with', 'suffix' => $suffix];

        return $this;
    }

    public function regex(string $pattern): self
    {
        $this->constraints[] = ['type' => 'regex', 'pattern' => $pattern];

        return $this;
    }

    public function getConstraints(): array
    {
        return $this->constraints;
    }
}
