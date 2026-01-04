<?php

namespace Andach\ExtractAndTransform\Events;

use Andach\ExtractAndTransform\Models\SyncRun;
use Illuminate\Foundation\Events\Dispatchable;

final class SyncSucceeded
{
    use Dispatchable;

    public function __construct(
        public SyncRun $run
    ) {}
}
