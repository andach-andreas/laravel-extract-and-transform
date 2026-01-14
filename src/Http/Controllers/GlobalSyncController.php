<?php

namespace Andach\ExtractAndTransform\Http\Controllers;

use Andach\ExtractAndTransform\Models\SyncProfile;
use Illuminate\Routing\Controller;

class GlobalSyncController extends Controller
{
    public function index()
    {
        $args = [];
        $args['profiles'] = SyncProfile::with(['source', 'runs' => function ($query) {
            $query->latest()->limit(1);
        }])->get();

        return view(config('extract-data.views.syncs.global_index', 'extract-data::syncs.global_index'), $args);
    }
}
