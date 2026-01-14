<?php

namespace Andach\ExtractAndTransform\Http\Controllers;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Models\ExtractSource;
use Illuminate\Http\Request;
use Illuminate\Routing\Controller;

class SourceController extends Controller
{
    public function index()
    {
        $args = [];
        $args['sources'] = ExtractSource::all();

        return view(config('extract-data.views.sources.index', 'extract-data::sources.index'), $args);
    }

    public function create()
    {
        $args = [];
        $args['connectors'] = ExtractAndTransform::getConnectors();
        $args['schemas'] = [];
        foreach (array_keys($args['connectors']) as $key) {
            $args['schemas'][$key] = ExtractAndTransform::getConnectorConfigSchema($key);
        }

        return view(config('extract-data.views.sources.create', 'extract-data::sources.create'), $args);
    }

    public function store(Request $request)
    {
        $data = $request->validate([
            'name' => 'required|string|max:255|unique:andach_leat_extract_sources,name',
            'connector' => 'required|string',
            'config' => 'array',
        ]);

        ExtractAndTransform::createSource($data['name'], $data['connector'], $data['config'] ?? []);

        $routePrefix = config('extract-data.route_name_prefix', 'andach-leat.');

        return redirect()->route($routePrefix.'sources.index')
            ->with('success', 'Source created successfully.');
    }

    public function edit($id)
    {
        $args = [];
        $args['source'] = ExtractSource::findOrFail($id);
        $args['connectors'] = ExtractAndTransform::getConnectors();
        $args['schemas'] = [];
        foreach (array_keys($args['connectors']) as $key) {
            $args['schemas'][$key] = ExtractAndTransform::getConnectorConfigSchema($key);
        }

        return view(config('extract-data.views.sources.edit', 'extract-data::sources.edit'), $args);
    }

    public function update(Request $request, $id)
    {
        $source = ExtractSource::findOrFail($id);

        $data = $request->validate([
            'name' => 'required|string|max:255|unique:andach_leat_extract_sources,name,'.$source->id,
            'connector' => 'required|string',
            'config' => 'array',
        ]);

        $source->update([
            'name' => $data['name'],
            'connector' => $data['connector'],
            'config' => $data['config'] ?? [],
        ]);

        $routePrefix = config('extract-data.route_name_prefix', 'andach-leat.');

        return redirect()->route($routePrefix.'sources.index')
            ->with('success', 'Source updated successfully.');
    }

    public function destroy($id)
    {
        $source = ExtractSource::findOrFail($id);
        $source->delete();

        $routePrefix = config('extract-data.route_name_prefix', 'andach-leat.');

        return redirect()->route($routePrefix.'sources.index')
            ->with('success', 'Source deleted successfully.');
    }
}
