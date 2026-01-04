<?php

namespace Andach\ExtractAndTransform\Events;

use Andach\ExtractAndTransform\Models\SyncProfile;
use Illuminate\Foundation\Events\Dispatchable;
use Illuminate\Queue\SerializesModels;

final class SyncStarting
{
    use Dispatchable, SerializesModels;

    public function __construct(
        public SyncProfile $profile
    ) {}
}
