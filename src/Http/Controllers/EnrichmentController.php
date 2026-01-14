<?php

namespace Andach\ExtractAndTransform\Http\Controllers;

use Andach\ExtractAndTransform\Enrichment\EnrichmentRegistry;
use Andach\ExtractAndTransform\Models\EnrichmentProfile;
use Illuminate\Http\Request;
use Illuminate\Routing\Controller;
use Illuminate\Support\Facades\DB;

class EnrichmentController extends Controller
{
    public function __construct(
        private readonly EnrichmentRegistry $registry
    ) {}

    private function getTables(): array
    {
        try {
            $tables = DB::getSchemaBuilder()->getTables();
            return collect($tables)->map(function ($table) {
                return is_object($table) ? $table->name : ($table['name'] ?? reset($table));
            })->values()->all();
        } catch (\Exception $e) {
            return [];
        }
    }

    public function index()
    {
        $args = [];
        $args['enrichments'] = EnrichmentProfile::with('runs')->get();
        return view(config('extract-data.views.enrichments.index', 'extract-data::enrichments.index'), $args);
    }

    public function create()
    {
        $args = [];
        $args['tables'] = $this->getTables();
        $args['providers'] = $this->registry->all();
        $args['schemas'] = collect($args['providers'])->mapWithKeys(fn($p) => [$p->key() => $p->getConfigDefinition()])->all();

        return view(config('extract-data.views.enrichments.create', 'extract-data::enrichments.create'), $args);
    }

    public function store(Request $request)
    {
        $data = $request->validate([
            'name' => 'required|string|max:255|unique:andach_leat_enrichment_profiles,name',
            'provider' => 'required|string',
            'source_table' => 'required|string',
            'source_column' => 'required|string',
            'destination_table' => 'required|string',
            'config' => 'array',
        ]);

        EnrichmentProfile::create($data);

        $routePrefix = config('extract-data.route_name_prefix', 'andach-leat.');
        return redirect()->route($routePrefix . 'enrichments.index')
            ->with('success', 'Enrichment profile created successfully.');
    }

    public function edit(EnrichmentProfile $enrichment)
    {
        $args = [];
        $args['enrichment'] = $enrichment;
        $args['tables'] = $this->getTables();
        $args['providers'] = $this->registry->all();
        $args['schemas'] = collect($args['providers'])->mapWithKeys(fn($p) => [$p->key() => $p->getConfigDefinition()])->all();

        return view(config('extract-data.views.enrichments.edit', 'extract-data::enrichments.edit'), $args);
    }

    public function update(Request $request, EnrichmentProfile $enrichment)
    {
        $data = $request->validate([
            'name' => 'required|string|max:255|unique:andach_leat_enrichment_profiles,name,' . $enrichment->id,
            'provider' => 'required|string',
            'source_table' => 'required|string',
            'source_column' => 'required|string',
            'destination_table' => 'required|string',
            'config' => 'array',
        ]);

        $enrichment->update($data);

        $routePrefix = config('extract-data.route_name_prefix', 'andach-leat.');
        return redirect()->route($routePrefix . 'enrichments.index')
            ->with('success', 'Enrichment profile updated successfully.');
    }

    public function destroy(EnrichmentProfile $enrichment)
    {
        $enrichment->delete();
        $routePrefix = config('extract-data.route_name_prefix', 'andach-leat.');
        return redirect()->route($routePrefix . 'enrichments.index')
            ->with('success', 'Enrichment profile deleted successfully.');
    }

    public function run(EnrichmentProfile $enrichment)
    {
        try {
            $enrichment->run();
            return back()->with('success', 'Enrichment run successfully.');
        } catch (\Exception $e) {
            return back()->with('error', 'Enrichment failed: ' . $e->getMessage());
        }
    }
}
