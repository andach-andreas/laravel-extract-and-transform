<?php

namespace Andach\ExtractAndTransform\Http\Controllers;

use Andach\ExtractAndTransform\Models\Transformation;
use Illuminate\Http\Request;
use Illuminate\Routing\Controller;
use Illuminate\Support\Facades\DB;

class TransformationController extends Controller
{
    private function getTables(): array
    {
        try {
            $tables = DB::getSchemaBuilder()->getTables();
            return collect($tables)->map(function ($table) {
                if (is_object($table)) {
                    return $table->name;
                }
                if (is_array($table)) {
                    return $table['name'] ?? reset($table);
                }
                return $table;
            })->values()->all();
        } catch (\Exception $e) {
            return [];
        }
    }

    public function index()
    {
        $args = [];
        $args['transformations'] = Transformation::with('runs')->get();
        return view(config('extract-data.views.transformations.index', 'extract-data::transformations.index'), $args);
    }

    public function create()
    {
        $args = [];
        $args['tables'] = $this->getTables();
        return view(config('extract-data.views.transformations.create', 'extract-data::transformations.create'), $args);
    }

    public function store(Request $request)
    {
        $data = $request->validate([
            'name' => 'required|string|max:255|unique:andach_leat_transformations,name',
            'configuration' => 'required|json',
        ]);

        $config = json_decode($data['configuration'], true);

        Transformation::create([
            'name' => $data['name'],
            'source_table' => $config['source'] ?? '',
            'destination_table_pattern' => $config['destination'] ?? '',
            'configuration' => $config,
        ]);

        $routePrefix = config('extract-data.route_name_prefix', 'andach-leat.');
        return redirect()->route($routePrefix . 'transformations.index')
            ->with('success', 'Transformation created successfully.');
    }

    public function edit(Transformation $transformation)
    {
        $args = [];
        $args['transformation'] = $transformation;
        $args['tables'] = $this->getTables();
        return view(config('extract-data.views.transformations.edit', 'extract-data::transformations.edit'), $args);
    }

    public function update(Request $request, Transformation $transformation)
    {
        $data = $request->validate([
            'name' => 'required|string|max:255|unique:andach_leat_transformations,name,' . $transformation->id,
            'configuration' => 'required|json',
        ]);

        $config = json_decode($data['configuration'], true);

        $transformation->update([
            'name' => $data['name'],
            'source_table' => $config['source'] ?? '',
            'destination_table_pattern' => $config['destination'] ?? '',
            'configuration' => $config,
        ]);

        $routePrefix = config('extract-data.route_name_prefix', 'andach-leat.');
        return redirect()->route($routePrefix . 'transformations.index')
            ->with('success', 'Transformation updated successfully.');
    }

    public function destroy(Transformation $transformation)
    {
        $transformation->delete();
        $routePrefix = config('extract-data.route_name_prefix', 'andach-leat.');
        return redirect()->route($routePrefix . 'transformations.index')
            ->with('success', 'Transformation deleted successfully.');
    }

    public function run(Transformation $transformation)
    {
        try {
            $transformation->run();
            return back()->with('success', 'Transformation run successfully.');
        } catch (\Exception $e) {
            return back()->with('error', 'Transformation failed: ' . $e->getMessage());
        }
    }
}
