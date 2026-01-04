<?php

namespace Andach\ExtractAndTransform\Strategies;

use Andach\ExtractAndTransform\Models\SyncProfile;
use Andach\ExtractAndTransform\Models\SyncRun;
use Andach\ExtractAndTransform\Source;

interface SyncStrategy
{
    public function run(SyncProfile $profile, Source $source, SyncRun $run): void;
}
