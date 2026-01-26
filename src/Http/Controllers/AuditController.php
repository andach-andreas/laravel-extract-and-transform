<?php

namespace Andach\ExtractAndTransform\Http\Controllers;

use Andach\ExtractAndTransform\Facades\ExtractAndTransform;
use Andach\ExtractAndTransform\Models\AuditRun;
use Andach\ExtractAndTransform\Models\Correction;
use Andach\ExtractAndTransform\Models\SyncRun;
use Illuminate\Http\Request;
use Illuminate\Routing\Controller;
use Illuminate\Support\Str;

class AuditController extends Controller
{
    public function index($syncRunId)
    {
        $args = [];
        $args['syncRun'] = SyncRun::with('profile.source')->findOrFail($syncRunId);

        $args['tableName'] = $args['syncRun']->profile->activeSchemaVersion->local_table_name ?? null;

        if (! $args['tableName']) {
            return back()->with('error', 'No active table found for this sync.');
        }

        $args['auditRun'] = AuditRun::where('table_name', $args['tableName'])->latest()->first();

        $args['violations'] = $args['auditRun'] ? $args['auditRun']->getViolationsByRow() : collect();

        $args['failedRows'] = collect();
        if ($args['auditRun']) {
            // Use the identifier column from the audit run to fetch rows
            $identifier = $args['auditRun']->identifier_column;
            // If identifier is array (composite), we can't easily keyBy.
            // For now assuming single column identifier or __id.
            $key = is_array($identifier) ? '__id' : $identifier;

            $args['failedRows'] = $args['auditRun']->getFailedRows()->keyBy($key);
        }

        $args['corrections'] = Correction::where('table_name', $args['tableName'])
            ->get()
            ->groupBy('row_identifier')
            ->map(fn ($group) => $group->keyBy('column_name'));

        return view(config('extract-data.views.audit.index', 'extract-data::audit.index'), $args);
    }

    public function configure($syncRunId)
    {
        $args = [];
        $args['syncRun'] = SyncRun::with('profile')->findOrFail($syncRunId);
        $profile = $args['syncRun']->profile;

        $args['config'] = $profile->activeSchemaVersion->configuration['audit'] ?? [];
        $args['columns'] = \Illuminate\Support\Facades\Schema::getColumnListing($profile->activeSchemaVersion->local_table_name);

        $args['availableRules'] = [
            'required' => ['label' => 'Required', 'args' => []],
            'numeric' => ['label' => 'Numeric', 'args' => []],
            'integer' => ['label' => 'Integer', 'args' => []],
            'string' => ['label' => 'String', 'args' => []],
            'email' => ['label' => 'Email', 'args' => []],
            'url' => ['label' => 'URL', 'args' => []],
            'ip' => ['label' => 'IP Address', 'args' => []],
            'uuid' => ['label' => 'UUID', 'args' => []],
            'date_format' => ['label' => 'Date Format', 'args' => ['format']],
            'min_length' => ['label' => 'Min Length', 'args' => ['length']],
            'max_length' => ['label' => 'Max Length', 'args' => ['length']],
            'greater_than' => ['label' => 'Greater Than', 'args' => ['value']],
            'less_than' => ['label' => 'Less Than', 'args' => ['value']],
            'in' => ['label' => 'In List', 'args' => ['values (comma separated)']],
            'not_in' => ['label' => 'Not In List', 'args' => ['values (comma separated)']],
            'regex' => ['label' => 'Regex', 'args' => ['pattern']],
            'starts_with' => ['label' => 'Starts With', 'args' => ['prefix']],
            'ends_with' => ['label' => 'Ends With', 'args' => ['suffix']],
            'exists_in' => ['label' => 'Exists In Table', 'args' => ['table', 'column']],
        ];

        return view(config('extract-data.views.audit.configure', 'extract-data::audit.configure'), $args);
    }

    public function storeConfig(Request $request, $syncRunId)
    {
        $syncRun = SyncRun::with('profile')->findOrFail($syncRunId);
        $profile = $syncRun->profile;
        $activeVersion = $profile->activeSchemaVersion;

        $rules = $request->input('rules', []);

        $config = $activeVersion->configuration ?? [];
        $config['audit'] = $rules;

        $activeVersion->update(['configuration' => $config]);

        $routePrefix = config('extract-data.route_name_prefix', 'andach-leat.');

        return redirect()->route($routePrefix.'audit.index', $syncRunId)->with('success', 'Audit rules saved.');
    }

    public function run(Request $request, $syncRunId)
    {
        $syncRun = SyncRun::with('profile')->findOrFail($syncRunId);
        $profile = $syncRun->profile;
        $activeVersion = $profile->activeSchemaVersion;
        $tableName = $activeVersion->local_table_name;

        $config = $activeVersion->configuration['audit'] ?? [];

        if (empty($config)) {
            return back()->with('error', 'No audit rules configured.');
        }

        $audit = ExtractAndTransform::audit($tableName);

        // Use __id as the default identifier since it's guaranteed to exist
        // Ideally this should be configurable in the UI
        $audit->identifiedBy('__id');

        $checks = [];
        foreach ($config as $column => $rules) {
            if (empty($rules)) {
                continue;
            }

            $checks[$column] = function ($ruleBuilder) use ($rules) {
                foreach ($rules as $ruleConfig) {
                    $type = $ruleConfig['type'];
                    $args = $ruleConfig['args'] ?? [];

                    if (in_array($type, ['in', 'not_in']) && isset($args[0]) && is_string($args[0])) {
                        $args[0] = array_map('trim', explode(',', $args[0]));
                    }

                    $methodName = Str::camel($type);

                    if (method_exists($ruleBuilder, $methodName)) {
                        $ruleBuilder->{$methodName}(...$args);
                    }
                }
            };
        }

        $audit->check($checks);
        $audit->run();

        $routePrefix = config('extract-data.route_name_prefix', 'andach-leat.');

        return redirect()->route($routePrefix.'audit.index', $syncRunId)->with('success', 'Audit completed.');
    }

    public function storeCorrections(Request $request, $syncRunId)
    {
        $syncRun = SyncRun::with('profile')->findOrFail($syncRunId);
        $tableName = $syncRun->profile->activeSchemaVersion->local_table_name;

        $corrections = $request->input('corrections', []);

        foreach ($corrections as $rowId => $columns) {
            foreach ($columns as $column => $data) {
                if (isset($data['value'])) {
                    ExtractAndTransform::addCorrection(
                        $tableName,
                        $rowId,
                        $column,
                        $data['value'],
                        $data['reason'] ?? null
                    );
                }
            }
        }

        return back()->with('success', 'Corrections applied.');
    }

    public function reconcile(Request $request, $syncRunId)
    {
        $syncRun = SyncRun::with('profile')->findOrFail($syncRunId);
        $tableName = $syncRun->profile->activeSchemaVersion->local_table_name;

        $auditRun = AuditRun::where('table_name', $tableName)->latest()->first();

        if (! $auditRun) {
            return back()->with('error', 'No audit run found. Cannot determine identifier.');
        }

        $identifier = $auditRun->identifier_column;
        $destinationTable = $tableName.'_reconciled';

        try {
            $rowsAffected = ExtractAndTransform::reconcile($tableName, $destinationTable, $identifier);

            return back()->with('success', "Reconciliation complete. {$rowsAffected} rows updated in '{$destinationTable}'.");
        } catch (\Exception $e) {
            return back()->with('error', 'Reconciliation failed: '.$e->getMessage());
        }
    }
}
