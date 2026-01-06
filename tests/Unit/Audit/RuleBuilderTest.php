<?php

namespace Andach\ExtractAndTransform\Tests\Unit\Audit;

use Andach\ExtractAndTransform\Audit\RuleBuilder;
use Andach\ExtractAndTransform\Tests\TestCase;

class RuleBuilderTest extends TestCase
{
    public function test_it_builds_required_rule()
    {
        $builder = new RuleBuilder();
        $builder->required();

        $constraints = $builder->getConstraints();
        $this->assertCount(1, $constraints);
        $this->assertEquals(['type' => 'required'], $constraints[0]);
    }

    public function test_it_builds_regex_rule()
    {
        $builder = new RuleBuilder();
        $builder->regex('/^[A-Z]+$/');

        $constraints = $builder->getConstraints();
        $this->assertCount(1, $constraints);
        $this->assertEquals(['type' => 'regex', 'pattern' => '/^[A-Z]+$/'], $constraints[0]);
    }

    public function test_it_builds_multiple_rules()
    {
        $builder = new RuleBuilder();
        $builder->required()->numeric()->integer();

        $constraints = $builder->getConstraints();
        $this->assertCount(3, $constraints);
        $this->assertEquals('required', $constraints[0]['type']);
        $this->assertEquals('numeric', $constraints[1]['type']);
        $this->assertEquals('integer', $constraints[2]['type']);
    }

    public function test_it_builds_in_rule()
    {
        $builder = new RuleBuilder();
        $builder->in(['A', 'B']);

        $constraints = $builder->getConstraints();
        $this->assertCount(1, $constraints);
        $this->assertEquals(['type' => 'in', 'values' => ['A', 'B']], $constraints[0]);
    }

    public function test_it_builds_custom_rule()
    {
        $callback = fn($val) => true;
        $builder = new RuleBuilder();
        $builder->custom($callback);

        $constraints = $builder->getConstraints();
        $this->assertCount(1, $constraints);
        $this->assertEquals('custom', $constraints[0]['type']);
        $this->assertSame($callback, $constraints[0]['callback']);
    }
}
