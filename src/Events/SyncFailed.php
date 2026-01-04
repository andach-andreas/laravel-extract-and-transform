<?php

namespace Andach\ExtractAndTransform\Events;

use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Models\SyncRun;
use Illuminate\Foundation\Events\Dispatchable;

final class SyncFailed
{
    use Dispatchable;

    public function __construct(
        public SyncRun $run,
        public SyncProfile $profile
    ) {}
}
