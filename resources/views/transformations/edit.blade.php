@extends('extract-data::layout')

@section('content')
    <div class="mb-6">
        <h1 class="text-2xl font-semibold text-gray-900">Edit Transformation: {{ $transformation->name }}</h1>
    </div>

    <div class="bg-white shadow sm:rounded-lg p-6"
         x-data="{
            name: '{{ $transformation->name }}',
            sourceTable: '{{ $transformation->configuration['source'] ?? '' }}',
            destTable: '{{ $transformation->configuration['destination'] ?? '' }}',
            columns: [],
            sourceColumns: [],
            lookupColumns: {},

            async fetchSourceColumns() {
                if (!this.sourceTable) return;
                let response = await fetch('{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'api.tables.columns', ['table' => ':table']) }}'.replace(':table', this.sourceTable));
                this.sourceColumns = await response.json();
            },

            async fetchLookupColumns(table) {
                if (!table || this.lookupColumns[table]) return;
                let response = await fetch('{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'api.tables.columns', ['table' => ':table']) }}'.replace(':table', table));
                this.lookupColumns[table] = await response.json();
            },

            init() {
                this.fetchSourceColumns();
                let config = {{ json_encode($transformation->configuration['columns'] ?? []) }};
                for (let dest in config) {
                    let expr = config[dest];
                    let col = { dest: dest, type: expr.type, config: {} };

                    if (expr.type === 'column' || expr.type === 'col') {
                        col.type = 'column';
                        col.config.column = expr.column;
                    } else if (expr.type === 'string_function') {
                        if (expr.function === 'SPLIT_PART') {
                            col.type = 'split';
                            col.config.column = expr.column.column;
                            col.config.delimiter = expr.arguments[0];
                            col.config.index = expr.arguments[1];
                        } else {
                            col.config.function = expr.function;
                            col.config.column = expr.column.column;
                            if (expr.function === 'REPLACE') {
                                col.config.search = expr.arguments[0];
                                col.config.replace = expr.arguments[1];
                            }
                        }
                    } else if (expr.type === 'numeric_function') {
                        col.config.function = expr.function;
                        col.config.column = expr.column.column;
                        if (expr.function === 'ROUND') {
                            col.config.precision = expr.arguments[0];
                        }
                    } else if (expr.type === 'lookup') {
                        let step = expr.steps[0];
                        col.config.target_table = step.table;
                        col.config.local_key = step.local;
                        col.config.foreign_key = step.foreign;
                        col.config.target_column = step.target;
                        this.fetchLookupColumns(step.table);
                    }
                    this.columns.push(col);
                }
            },
            addColumn() {
                this.columns.push({ dest: '', type: 'column', config: { column: '' } });
            },
            removeColumn(index) {
                this.columns.splice(index, 1);
            },
            generateJson() {
                let config = {
                    source: this.sourceTable,
                    destination: this.destTable,
                    columns: {}
                };

                this.columns.forEach(col => {
                    if (col.dest) {
                        let expr = { type: col.type };

                        if (col.type === 'column') {
                            expr.column = col.config.column;
                        } else if (col.type === 'string_function') {
                            expr.function = col.config.function;
                            expr.column = { type: 'column', column: col.config.column };
                            if (col.config.function === 'REPLACE') {
                                expr.arguments = [col.config.search, col.config.replace];
                            } else {
                                expr.arguments = [];
                            }
                        } else if (col.type === 'numeric_function') {
                            expr.function = col.config.function;
                            expr.column = { type: 'column', column: col.config.column };
                            if (col.config.function === 'ROUND') {
                                expr.arguments = [parseInt(col.config.precision || 0)];
                            }
                        } else if (col.type === 'split') {
                            expr.type = 'string_function';
                            expr.function = 'SPLIT_PART';
                            expr.column = { type: 'column', column: col.config.column };
                            expr.arguments = [col.config.delimiter, parseInt(col.config.index || 0)];
                        } else if (col.type === 'lookup') {
                            expr.steps = [{
                                table: col.config.target_table,
                                local: col.config.local_key,
                                foreign: col.config.foreign_key,
                                target: col.config.target_column
                            }];
                        }

                        config.columns[col.dest] = expr;
                    }
                });

                return JSON.stringify(config, null, 4);
            }
         }"
         x-init="$watch('sourceTable', () => fetchSourceColumns()); init()">
        <form action="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'transformations.update', $transformation->id) }}" method="POST" @submit="document.getElementById('configuration').value = generateJson()">
            @csrf
            @method('PUT')
            <input type="hidden" name="configuration" id="configuration">

            <div class="grid grid-cols-1 gap-y-6 gap-x-4 sm:grid-cols-2 mb-6">
                <div>
                    <label for="name" class="block text-sm font-medium text-gray-700">Name</label>
                    <input type="text" name="name" id="name" value="{{ $transformation->name }}" required class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm p-2 border">
                </div>
                <div>
                    <label for="source_table" class="block text-sm font-medium text-gray-700">Source Table</label>
                    <select x-model="sourceTable" class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm p-2 border">
                        <option value="">Select Table</option>
                        @foreach($tables as $table)
                            <option value="{{ $table }}">{{ $table }}</option>
                        @endforeach
                    </select>
                </div>
                <div>
                    <label for="dest_table" class="block text-sm font-medium text-gray-700">Destination Table</label>
                    <input type="text" x-model="destTable" class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm p-2 border">
                </div>
            </div>

            <div class="border-t pt-4">
                <div class="flex justify-between items-center mb-4">
                    <h3 class="text-lg font-medium text-gray-900">Column Mapping</h3>
                    <button type="button" @click="addColumn()" class="text-sm text-indigo-600 hover:text-indigo-900 font-medium">
                        + Add Column
                    </button>
                </div>

                <div class="space-y-4">
                    <template x-for="(col, index) in columns" :key="index">
                        <div class="flex items-start space-x-3 bg-gray-50 p-3 rounded border">
                            <div class="w-1/4">
                                <label class="block text-xs font-medium text-gray-500">Destination Column</label>
                                <input type="text" x-model="col.dest" class="mt-1 block w-full rounded-md border-gray-300 shadow-sm sm:text-sm p-1 border">
                            </div>
                            <div class="w-1/4">
                                <label class="block text-xs font-medium text-gray-500">Transformation</label>
                                <select x-model="col.type" class="mt-1 block w-full rounded-md border-gray-300 shadow-sm sm:text-sm p-1 border">
                                    <option value="column">Direct Copy</option>
                                    <option value="string_function">String Function</option>
                                    <option value="numeric_function">Numeric Function</option>
                                    <option value="split">Split</option>
                                    <option value="lookup">Lookup</option>
                                </select>
                            </div>
                            <div class="flex-1">
                                <label class="block text-xs font-medium text-gray-500">Configuration</label>

                                <!-- Direct Copy -->
                                <div x-show="col.type === 'column'" class="mt-1">
                                    <select x-model="col.config.column" class="block w-full rounded-md border-gray-300 shadow-sm sm:text-sm p-1 border">
                                        <option value="">Select Source Column</option>
                                        <template x-for="sc in sourceColumns" :key="sc">
                                            <option :value="sc" x-text="sc"></option>
                                        </template>
                                    </select>
                                </div>

                                <!-- String Function -->
                                <div x-show="col.type === 'string_function'" class="mt-1 space-y-2">
                                    <div class="flex space-x-2">
                                        <select x-model="col.config.function" class="block w-1/3 rounded-md border-gray-300 shadow-sm sm:text-sm p-1 border">
                                            <option value="">Select Function</option>
                                            <option value="UPPER">UPPER</option>
                                            <option value="LOWER">LOWER</option>
                                            <option value="TRIM">TRIM</option>
                                            <option value="REPLACE">REPLACE</option>
                                        </select>
                                        <select x-model="col.config.column" class="block w-2/3 rounded-md border-gray-300 shadow-sm sm:text-sm p-1 border">
                                            <option value="">Select Source Column</option>
                                            <template x-for="sc in sourceColumns" :key="sc">
                                                <option :value="sc" x-text="sc"></option>
                                            </template>
                                        </select>
                                    </div>
                                    <div x-show="col.config.function === 'REPLACE'" class="flex space-x-2">
                                        <input type="text" x-model="col.config.search" placeholder="Search" class="block w-1/2 rounded-md border-gray-300 shadow-sm sm:text-sm p-1 border">
                                        <input type="text" x-model="col.config.replace" placeholder="Replace With" class="block w-1/2 rounded-md border-gray-300 shadow-sm sm:text-sm p-1 border">
                                    </div>
                                </div>

                                <!-- Numeric Function -->
                                <div x-show="col.type === 'numeric_function'" class="mt-1 flex space-x-2">
                                    <select x-model="col.config.function" class="block w-1/3 rounded-md border-gray-300 shadow-sm sm:text-sm p-1 border">
                                        <option value="ROUND">ROUND</option>
                                    </select>
                                    <select x-model="col.config.column" class="block w-1/3 rounded-md border-gray-300 shadow-sm sm:text-sm p-1 border">
                                        <option value="">Select Source Column</option>
                                        <template x-for="sc in sourceColumns" :key="sc">
                                            <option :value="sc" x-text="sc"></option>
                                        </template>
                                    </select>
                                    <input type="number" x-model="col.config.precision" placeholder="Precision" class="block w-1/3 rounded-md border-gray-300 shadow-sm sm:text-sm p-1 border">
                                </div>

                                <!-- Split -->
                                <div x-show="col.type === 'split'" class="mt-1 flex space-x-2">
                                    <select x-model="col.config.column" class="block w-1/3 rounded-md border-gray-300 shadow-sm sm:text-sm p-1 border">
                                        <option value="">Select Source Column</option>
                                        <template x-for="sc in sourceColumns" :key="sc">
                                            <option :value="sc" x-text="sc"></option>
                                        </template>
                                    </select>
                                    <input type="text" x-model="col.config.delimiter" placeholder="Delimiter" class="block w-1/3 rounded-md border-gray-300 shadow-sm sm:text-sm p-1 border">
                                    <input type="number" x-model="col.config.index" placeholder="Index (0-based)" class="block w-1/3 rounded-md border-gray-300 shadow-sm sm:text-sm p-1 border">
                                </div>

                                <!-- Lookup -->
                                <div x-show="col.type === 'lookup'" class="mt-1 grid grid-cols-2 gap-2">
                                    <select x-model="col.config.target_table" @change="fetchLookupColumns(col.config.target_table)" class="block w-full rounded-md border-gray-300 shadow-sm sm:text-sm p-1 border">
                                        <option value="">Target Table</option>
                                        @foreach($tables as $table)
                                            <option value="{{ $table }}">{{ $table }}</option>
                                        @endforeach
                                    </select>

                                    <select x-model="col.config.local_key" class="block w-full rounded-md border-gray-300 shadow-sm sm:text-sm p-1 border">
                                        <option value="">Local Key (Source Col)</option>
                                        <template x-for="sc in sourceColumns" :key="sc">
                                            <option :value="sc" x-text="sc"></option>
                                        </template>
                                    </select>

                                    <select x-model="col.config.foreign_key" class="block w-full rounded-md border-gray-300 shadow-sm sm:text-sm p-1 border">
                                        <option value="">Foreign Key (Target ID)</option>
                                        <template x-for="lc in (lookupColumns[col.config.target_table] || [])" :key="lc">
                                            <option :value="lc" x-text="lc"></option>
                                        </template>
                                    </select>

                                    <select x-model="col.config.target_column" class="block w-full rounded-md border-gray-300 shadow-sm sm:text-sm p-1 border">
                                        <option value="">Target Column to Fetch</option>
                                        <template x-for="lc in (lookupColumns[col.config.target_table] || [])" :key="lc">
                                            <option :value="lc" x-text="lc"></option>
                                        </template>
                                    </select>
                                </div>
                            </div>

                            <!-- Remove -->
                            <button type="button" @click="removeColumn(index)" class="text-red-600 hover:text-red-900 mt-6">
                                &times;
                            </button>
                        </div>
                    </template>
                </div>
            </div>

            <div class="mt-8 border-t pt-4">
                <details>
                    <summary class="text-sm text-gray-500 cursor-pointer">Advanced: View/Edit Raw JSON</summary>
                    <textarea x-bind:value="generateJson()" rows="10" class="mt-2 block w-full rounded-md border-gray-300 shadow-sm font-mono text-xs p-2 border bg-gray-50" readonly></textarea>
                </details>
            </div>

            <div class="flex justify-end mt-6">
                <a href="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'transformations.index') }}" class="bg-gray-200 hover:bg-gray-300 text-gray-700 font-bold py-2 px-4 rounded mr-2">Cancel</a>
                <button type="submit" class="bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-2 px-4 rounded">Update</button>
            </div>
        </form>
    </div>
@endsection
