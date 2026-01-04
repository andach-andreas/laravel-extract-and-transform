<?php

namespace Andach\ExtractAndTransform\Tests\Feature;

use Andach\ExtractAndTransform\Services\RetryService;
use Andach\ExtractAndTransform\Tests\TestCase;
use Exception;
use Illuminate\Foundation\Testing\RefreshDatabase;

class RetryServiceTest extends TestCase
{
    use RefreshDatabase;

    public function testItRetriesAFailingOperationAndThenSucceeds(): void
    {
        $retryService = app(RetryService::class);
        $attempts = 0;

        $result = $retryService->run(function () use (&$attempts) {
            $attempts++;
            if ($attempts < 3) {
                throw new Exception('Temporary failure');
            }

            return 'success';
        }, 3, 10); // 10ms delay for faster tests

        $this->assertEquals(3, $attempts);
        $this->assertEquals('success', $result);
    }

    public function testItThrowsAnExceptionAfterAllAttemptsFail(): void
    {
        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Persistent failure');

        $retryService = app(RetryService::class);
        $attempts = 0;

        $retryService->run(function () use (&$attempts) {
            $attempts++;
            throw new Exception('Persistent failure');
        }, 3, 10);
    }
}
