<?php

namespace Andach\ExtractAndTransform\Http\Controllers;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Models\ExtractSource;
use Andach\ExtractAndTransform\Strategies\StrategyRegistry;
use Illuminate\Http\Request;
use Illuminate\Routing\Controller;

class SyncController extends Controller
{
    public function __construct(
        private readonly StrategyRegistry $strategyRegistry
    ) {}

    public function index($sourceId)
    {
        $args = [];
        $args['sourceModel'] = ExtractSource::findOrFail($sourceId);
        $sourceWrapper = ExtractAndTransform::getSourceFromModel($args['sourceModel']);

        try {
            $args['datasets'] = iterator_to_array($sourceWrapper->listDatasets());
        } catch (\Exception $e) {
            return back()->with('error', 'Failed to list datasets: '.$e->getMessage());
        }

        $args['profiles'] = $args['sourceModel']->syncProfiles->keyBy('dataset_identifier');

        return view(config('extract-data.views.syncs.index', 'extract-data::syncs.index'), $args);
    }

    public function configure(Request $request, $sourceId)
    {
        $args = [];
        $args['sourceModel'] = ExtractSource::findOrFail($sourceId);
        $sourceWrapper = ExtractAndTransform::getSourceFromModel($args['sourceModel']);
        $args['datasetIdentifier'] = $request->query('dataset');

        $routePrefix = config('extract-data.route_name_prefix', 'andach-leat.');

        if (! $args['datasetIdentifier']) {
            return redirect()->route($routePrefix.'syncs.index', $sourceId)->with('error', 'Dataset identifier required.');
        }

        try {
            $dataset = $sourceWrapper->getDataset($args['datasetIdentifier']);
            if (! $dataset) {
                throw new \Exception('Dataset not found.');
            }
            $args['schema'] = $dataset->getSchema();
        } catch (\Exception $e) {
            return redirect()->route($routePrefix.'syncs.index', $sourceId)->with('error', 'Failed to fetch schema: '.$e->getMessage());
        }

        $args['profile'] = $args['sourceModel']->syncProfiles()->firstOrNew(['dataset_identifier' => $args['datasetIdentifier']]);
        $args['activeVersion'] = $args['profile']->activeSchemaVersion;

        $strategies = array_keys($this->strategyRegistry->all());
        $args['strategies'] = [];
        foreach ($strategies as $strat) {
            $args['strategies'][$strat] = ucfirst(str_replace('_', ' ', $strat));
        }

        return view(config('extract-data.views.syncs.configure', 'extract-data::syncs.configure'), $args);
    }

    public function store(Request $request, $sourceId)
    {
        $sourceModel = ExtractSource::findOrFail($sourceId);
        $sourceWrapper = ExtractAndTransform::getSourceFromModel($sourceModel);

        $datasetIdentifier = $request->input('dataset');
        $strategy = $request->input('strategy', 'full_refresh');
        $tableName = $request->input('table_name');
        $mapping = $request->input('mapping', []);

        $cleanMapping = [];
        foreach ($mapping as $sourceKey => $destKey) {
            if (trim($destKey) !== '') {
                $cleanMapping[$sourceKey] = trim($destKey);
            } else {
                $cleanMapping[$sourceKey] = null;
            }
        }

        try {
            $sync = $sourceWrapper->sync($datasetIdentifier)
                ->withStrategy($strategy)
                ->mapColumns($cleanMapping);

            if ($tableName) {
                $sync->toTable($tableName);
            }

            $sync->run();

            $routePrefix = config('extract-data.route_name_prefix', 'andach-leat.');

            return redirect()->route($routePrefix.'syncs.index', $sourceId)
                ->with('success', "Sync for '$datasetIdentifier' configured and run successfully.");
        } catch (\Exception $e) {
            return back()->with('error', 'Sync failed: '.$e->getMessage())->withInput();
        }
    }
}
