@extends('extract-data::layout')

@section('content')
    <div class="flex justify-between items-center mb-6">
        <div>
            <h1 class="text-2xl font-semibold text-gray-900">Audit: {{ $syncRun->profile->dataset_identifier }}</h1>
            <p class="text-sm text-gray-500 mt-1">Table: {{ $tableName }}</p>
        </div>
        <div class="flex space-x-2">
            <a href="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'audit.configure', $syncRun->id) }}" class="bg-gray-200 hover:bg-gray-300 text-gray-700 font-bold py-2 px-4 rounded">
                Configure Rules
            </a>
            <form action="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'audit.run', $syncRun->id) }}" method="POST">
                @csrf
                <button type="submit" class="bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-2 px-4 rounded">
                    Run Audit
                </button>
            </form>
            @if($auditRun)
                <form action="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'audit.reconcile', $syncRun->id) }}" method="POST">
                    @csrf
                    <button type="submit" class="bg-green-600 hover:bg-green-700 text-white font-bold py-2 px-4 rounded">
                        Reconcile
                    </button>
                </form>
            @endif
        </div>
    </div>

    @if(!$auditRun)
        <div class="bg-yellow-50 border-l-4 border-yellow-400 p-4">
            <div class="flex">
                <div class="ml-3">
                    <p class="text-sm text-yellow-700">
                        No audit has been run for this table yet. Configure rules and click "Run Audit".
                    </p>
                </div>
            </div>
        </div>
    @else
        @if($auditRun->status === 'failed')
            <div class="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded relative mb-6" role="alert">
                <strong class="font-bold">Audit Failed!</strong>
                <span class="block sm:inline">{{ $auditRun->log_message }}</span>
            </div>
        @endif

        <div class="mb-6 grid grid-cols-1 gap-5 sm:grid-cols-3">
            <div class="bg-white overflow-hidden shadow rounded-lg">
                <div class="px-4 py-5 sm:p-6">
                    <dt class="text-sm font-medium text-gray-500 truncate">Total Violations</dt>
                    <dd class="mt-1 text-3xl font-semibold text-gray-900">{{ $auditRun->total_violations }}</dd>
                </div>
            </div>
            <div class="bg-white overflow-hidden shadow rounded-lg">
                <div class="px-4 py-5 sm:p-6">
                    <dt class="text-sm font-medium text-gray-500 truncate">Rows Scanned</dt>
                    <dd class="mt-1 text-3xl font-semibold text-gray-900">{{ $auditRun->total_rows_scanned }}</dd>
                </div>
            </div>
            <div class="bg-white overflow-hidden shadow rounded-lg">
                <div class="px-4 py-5 sm:p-6">
                    <dt class="text-sm font-medium text-gray-500 truncate">Last Run</dt>
                    <dd class="mt-1 text-3xl font-semibold text-gray-900">
                        {{ $auditRun->created_at->diffForHumans() }}
                        <span class="text-xs text-gray-500 block">({{ $auditRun->status }})</span>
                    </dd>
                </div>
            </div>
        </div>

        <form action="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'audit.correction', $syncRun->id) }}" method="POST">
            @csrf
            <div class="bg-white shadow overflow-hidden sm:rounded-lg mb-6">
                <div class="px-4 py-5 sm:px-6 flex justify-between items-center">
                    <h3 class="text-lg leading-6 font-medium text-gray-900">Violations</h3>
                    <button type="submit" class="bg-green-600 hover:bg-green-700 text-white font-bold py-2 px-4 rounded">
                        Save All Corrections
                    </button>
                </div>
                <table class="min-w-full divide-y divide-gray-200">
                    <thead class="bg-gray-50">
                        <tr>
                            <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Row ID</th>
                            <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Column</th>
                            <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Value</th>
                            <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Rule</th>
                            <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Correction</th>
                            <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Reason</th>
                        </tr>
                    </thead>
                    <tbody class="bg-white divide-y divide-gray-200">
                        @foreach($violations as $rowId => $logs)
                            @foreach($logs as $log)
                                @php
                                    $originalValue = $failedRows[$rowId]->{$log->column_name} ?? 'N/A';
                                    $correction = $corrections[$rowId][$log->column_name] ?? null;
                                    $currentValue = $correction ? $correction->new_value : '';
                                    $currentReason = $correction ? $correction->reason : '';
                                @endphp
                                <tr>
                                    <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                                        {{ $rowId }}
                                    </td>
                                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                        {{ $log->column_name }}
                                    </td>
                                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900 font-mono bg-red-50">
                                        {{ is_scalar($originalValue) ? $originalValue : json_encode($originalValue) }}
                                    </td>
                                    <td class="px-6 py-4 whitespace-nowrap text-sm text-red-600">
                                        {{ $log->rule_name }}
                                    </td>
                                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                        <input type="text"
                                               name="corrections[{{ $rowId }}][{{ $log->column_name }}][value]"
                                               value="{{ $currentValue }}"
                                               class="shadow-sm focus:ring-indigo-500 focus:border-indigo-500 block w-full sm:text-sm border-gray-300 rounded-md p-1 border">
                                    </td>
                                    <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                        <input type="text"
                                               name="corrections[{{ $rowId }}][{{ $log->column_name }}][reason]"
                                               value="{{ $currentReason }}"
                                               placeholder="Reason for change"
                                               class="shadow-sm focus:ring-indigo-500 focus:border-indigo-500 block w-full sm:text-sm border-gray-300 rounded-md p-1 border">
                                    </td>
                                </tr>
                            @endforeach
                        @endforeach
                    </tbody>
                </table>
            </div>
        </form>
    @endif
@endsection
