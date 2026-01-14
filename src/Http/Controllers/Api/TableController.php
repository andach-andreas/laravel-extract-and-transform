<?php

namespace Andach\ExtractAndTransform\Http\Controllers\Api;

use Illuminate\Routing\Controller;
use Illuminate\Support\Facades\Schema;

class TableController extends Controller
{
    public function columns($table)
    {
        if (! Schema::hasTable($table)) {
            return response()->json(['error' => 'Table not found'], 404);
        }

        $columns = Schema::getColumnListing($table);

        return response()->json($columns);
    }
}
