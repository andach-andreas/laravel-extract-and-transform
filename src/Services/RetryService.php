<?php

namespace Andach\ExtractAndTransform\Services;

use Closure;
use Illuminate\Support\Facades\Log;
use Throwable;

final class RetryService
{
    /**
     * Attempts to execute an operation, retrying with exponential backoff on failure.
     *
     * @param  Closure  $callable  The operation to execute.
     * @param  int  $times  The total number of times to attempt the operation.
     * @param  int  $initialDelay  The initial delay in milliseconds.
     * @return mixed The result of the successful operation.
     *
     * @throws Throwable If the operation does not succeed within the given number of attempts.
     */
    public function run(Closure $callable, int $times = 3, int $initialDelay = 100)
    {
        $attempts = 0;
        $delay = $initialDelay;

        while ($attempts < $times) {
            try {
                return $callable(); // Attempt to run the closure.
            } catch (Throwable $e) {
                $attempts++;

                // If this was the last attempt, re-throw the exception.
                if ($attempts >= $times) {
                    throw $e;
                }

                Log::warning("extract-and-transform: Attempt {$attempts} failed. Retrying in {$delay}ms.", [
                    'exception' => $e->getMessage(),
                ]);

                // Wait for the specified delay.
                usleep($delay * 1000);

                // Increase the delay for the next attempt (exponential backoff).
                $delay *= 2;
            }
        }
    }
}
