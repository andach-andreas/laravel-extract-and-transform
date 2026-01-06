<?php

namespace Andach\ExtractAndTransform\Tests\Unit\Audit;

use Andach\ExtractAndTransform\Audit\AuditBuilder;
use Andach\ExtractAndTransform\Models\AuditRun;
use Andach\ExtractAndTransform\Services\AuditorService;
use Andach\ExtractAndTransform\Tests\TestCase;
use Mockery;

class AuditBuilderTest extends TestCase
{
    public function test_it_stores_configuration()
    {
        $builder = new AuditBuilder('test_table');
        $builder->identifiedBy('id');
        $builder->check(['col' => fn($r) => $r->required()]);

        // Use reflection to check private properties since there are no getters
        $reflection = new \ReflectionClass($builder);

        $tableProp = $reflection->getProperty('tableName');
        $tableProp->setAccessible(true);
        $this->assertEquals('test_table', $tableProp->getValue($builder));

        $idProp = $reflection->getProperty('identifier');
        $idProp->setAccessible(true);
        $this->assertEquals('id', $idProp->getValue($builder));

        $rulesProp = $reflection->getProperty('rules');
        $rulesProp->setAccessible(true);
        $this->assertIsArray($rulesProp->getValue($builder));
    }

    public function test_it_throws_exception_if_identifier_missing()
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Audit must have an identifier defined via identifiedBy().');

        $builder = new AuditBuilder('test_table');
        $builder->run();
    }

    public function test_it_delegates_to_service_on_run()
    {
        $mockService = Mockery::mock(AuditorService::class);
        $mockService->shouldReceive('run')
            ->once()
            ->with('test_table', 'id', ['col' => []])
            ->andReturn(new AuditRun());

        $this->app->instance(AuditorService::class, $mockService);

        $builder = new AuditBuilder('test_table');
        $builder->identifiedBy('id');
        $builder->check(['col' => []]);
        $builder->run();
    }
}
