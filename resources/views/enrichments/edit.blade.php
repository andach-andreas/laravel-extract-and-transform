@extends('extract-data::layout')

@section('content')
    <div class="mb-6">
        <h1 class="text-2xl font-semibold text-gray-900">Edit Enrichment Profile</h1>
    </div>

    <div class="bg-white shadow sm:rounded-lg p-6"
         x-data="{
            provider: '{{ $enrichment->provider }}',
            sourceTable: '{{ $enrichment->source_table }}',
            sourceColumns: [],
            async fetchSourceColumns() {
                if (!this.sourceTable) return;
                let response = await fetch('{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'api.tables.columns', ['table' => ':table']) }}'.replace(':table', this.sourceTable));
                this.sourceColumns = await response.json();
            }
         }"
         x-init="fetchSourceColumns()">
        <form action="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'enrichments.update', $enrichment->id) }}" method="POST">
            @csrf
            @method('PUT')

            <div class="grid grid-cols-1 gap-y-6 gap-x-4 sm:grid-cols-2 mb-6">
                <div>
                    <label for="name" class="block text-sm font-medium text-gray-700">Name</label>
                    <input type="text" name="name" id="name" value="{{ $enrichment->name }}" required class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm p-2 border">
                </div>
                <div>
                    <label for="provider" class="block text-sm font-medium text-gray-700">Provider</label>
                    <select name="provider" id="provider" x-model="provider" required class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm p-2 border">
                        <option value="">Select a provider</option>
                        @foreach($providers as $p)
                            <option value="{{ $p->key() }}" {{ $enrichment->provider === $p->key() ? 'selected' : '' }}>{{ $p->label() }}</option>
                        @endforeach
                    </select>
                </div>
                <div>
                    <label for="source_table" class="block text-sm font-medium text-gray-700">Source Table</label>
                    <select name="source_table" id="source_table" x-model="sourceTable" required class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm p-2 border">
                        <option value="">Select Table</option>
                        @foreach($tables as $table)
                            <option value="{{ $table }}" {{ $enrichment->source_table === $table ? 'selected' : '' }}>{{ $table }}</option>
                        @endforeach
                    </select>
                </div>
                <div>
                    <label for="source_column" class="block text-sm font-medium text-gray-700">Source Column (Identifier)</label>
                    <select name="source_column" id="source_column" required class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm p-2 border">
                        <option value="">Select Column</option>
                        <template x-for="sc in sourceColumns" :key="sc">
                            <option :value="sc" x-text="sc" :selected="sc === '{{ $enrichment->source_column }}'"></option>
                        </template>
                    </select>
                </div>
                <div class="sm:col-span-2">
                    <label for="destination_table" class="block text-sm font-medium text-gray-700">Destination Table (Cache)</label>
                    <input type="text" name="destination_table" id="destination_table" value="{{ $enrichment->destination_table }}" required class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm p-2 border">
                </div>
            </div>

            <!-- Dynamic Config Fields -->
            <div class="border-t pt-4 mt-4">
                <h3 class="text-lg font-medium text-gray-900 mb-4">Provider Configuration</h3>
                @foreach($schemas as $key => $fields)
                    <div x-show="provider === '{{ $key }}'" style="display: none;">
                        @foreach($fields as $field)
                            <div class="mb-4">
                                <label for="config_{{ $key }}_{{ $field->key }}" class="block text-sm font-medium text-gray-700">
                                    {{ $field->label }}
                                    @if($field->required) <span class="text-red-500">*</span> @endif
                                </label>
                                <input type="{{ $field->type === 'password' ? 'password' : 'text' }}"
                                       name="config[{{ $field->key }}]"
                                       id="config_{{ $key }}_{{ $field->key }}"
                                       value="{{ $enrichment->config[$field->key] ?? '' }}"
                                       x-bind:disabled="provider !== '{{ $key }}'"
                                       class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm p-2 border">
                            </div>
                        @endforeach
                    </div>
                @endforeach
            </div>

            <div class="flex justify-end mt-6">
                <a href="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'enrichments.index') }}" class="bg-gray-200 hover:bg-gray-300 text-gray-700 font-bold py-2 px-4 rounded mr-2">Cancel</a>
                <button type="submit" class="bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-2 px-4 rounded">Update</button>
            </div>
        </form>
    </div>
@endsection
