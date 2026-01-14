@extends('extract-data::layout')

@section('content')
    <div class="mb-6">
        <h1 class="text-2xl font-semibold text-gray-900">Configure Sync: {{ $datasetIdentifier }}</h1>
        <p class="text-sm text-gray-500 mt-1">Source: {{ $sourceModel->name }}</p>
    </div>

    <div class="bg-white shadow sm:rounded-lg p-6">
        <form action="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'syncs.store', $sourceModel->id) }}" method="POST">
            @csrf
            <input type="hidden" name="dataset" value="{{ $datasetIdentifier }}">

            <!-- Strategy & Table -->
            <div class="grid grid-cols-1 gap-y-6 gap-x-4 sm:grid-cols-6 mb-8">
                <div class="sm:col-span-3">
                    <label for="strategy" class="block text-sm font-medium text-gray-700">Sync Strategy</label>
                    <select id="strategy" name="strategy" class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm p-2 border">
                        @foreach($strategies as $key => $label)
                            <option value="{{ $key }}" {{ ($profile->strategy ?? 'full_refresh') === $key ? 'selected' : '' }}>
                                {{ $label }}
                            </option>
                        @endforeach
                    </select>
                </div>

                <div class="sm:col-span-3">
                    <label for="table_name" class="block text-sm font-medium text-gray-700">Destination Table Name</label>
                    <input type="text" name="table_name" id="table_name"
                           value="{{ old('table_name', $activeVersion->local_table_name ?? '') }}"
                           placeholder="Auto-generated if empty"
                           class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm p-2 border">
                    <p class="mt-1 text-xs text-gray-500">Leave empty to auto-generate based on source name.</p>
                </div>
            </div>

            <!-- Column Mapping -->
            <h3 class="text-lg font-medium text-gray-900 mb-4">Column Mapping</h3>
            <div class="border rounded-md overflow-hidden">
                <table class="min-w-full divide-y divide-gray-200">
                    <thead class="bg-gray-50">
                        <tr>
                            <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Source Column</th>
                            <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Type</th>
                            <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Destination Column</th>
                            <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Ignore</th>
                        </tr>
                    </thead>
                    <tbody class="bg-white divide-y divide-gray-200">
                        @foreach($schema->fields as $field)
                            @php
                                $mappedName = $activeVersion->column_mapping[$field->name] ?? $field->name;
                                $isIgnored = array_key_exists($field->name, $activeVersion->column_mapping ?? []) && $activeVersion->column_mapping[$field->name] === null;
                            @endphp
                            <tr x-data="{ ignored: {{ $isIgnored ? 'true' : 'false' }}, val: '{{ $mappedName }}' }">
                                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                                    {{ $field->name }}
                                </td>
                                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                    {{ $field->suggestedLocalType }}
                                </td>
                                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                    <input type="text"
                                           name="mapping[{{ $field->name }}]"
                                           x-model="val"
                                           x-bind:disabled="ignored"
                                           class="block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm p-1 border disabled:bg-gray-100">
                                </td>
                                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                    <input type="checkbox" x-model="ignored" class="h-4 w-4 text-indigo-600 focus:ring-indigo-500 border-gray-300 rounded">
                                </td>
                            </tr>
                        @endforeach
                    </tbody>
                </table>
            </div>

            <div class="flex justify-end mt-6">
                <a href="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'syncs.index', $sourceModel->id) }}" class="bg-gray-200 hover:bg-gray-300 text-gray-700 font-bold py-2 px-4 rounded mr-2">Cancel</a>
                <button type="submit" class="bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-2 px-4 rounded">Save & Run Sync</button>
            </div>
        </form>
    </div>
@endsection
