@extends('extract-data::layout')

@section('content')
    <div class="mb-6">
        <h1 class="text-2xl font-semibold text-gray-900">Configure Audit Rules</h1>
        <p class="text-sm text-gray-500 mt-1">Table: {{ $syncRun->profile->activeSchemaVersion->local_table_name }}</p>
    </div>

    <div class="bg-white shadow sm:rounded-lg p-6"
         x-data="{
            rules: {{ json_encode((object)$config) }},
            availableRules: {{ json_encode($availableRules) }},
            addRule(column) {
                if (!this.rules[column]) {
                    this.rules[column] = [];
                }
                this.rules[column].push({ type: 'required', args: [] });
            },
            removeRule(column, index) {
                this.rules[column].splice(index, 1);
            },
            getArgs(type) {
                return this.availableRules[type]?.args || [];
            }
         }">
        <form action="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'audit.store_config', $syncRun->id) }}" method="POST">
            @csrf

            <div class="space-y-6">
                @foreach($columns as $column)
                    <div class="border rounded-md p-4">
                        <div class="flex justify-between items-center mb-4">
                            <h3 class="text-lg font-medium text-gray-900">{{ $column }}</h3>
                            <button type="button" @click="addRule('{{ $column }}')" class="text-sm text-indigo-600 hover:text-indigo-900 font-medium">
                                + Add Rule
                            </button>
                        </div>

                        <div class="space-y-3" x-show="rules['{{ $column }}'] && rules['{{ $column }}'].length > 0">
                            <template x-for="(rule, index) in rules['{{ $column }}']" :key="index">
                                <div class="flex items-start space-x-3 bg-gray-50 p-3 rounded">
                                    <!-- Rule Type -->
                                    <div class="w-1/3">
                                        <select :name="'rules[{{ $column }}][' + index + '][type]'"
                                                x-model="rule.type"
                                                class="block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm p-2 border">
                                            @foreach($availableRules as $key => $def)
                                                <option value="{{ $key }}">{{ $def['label'] }}</option>
                                            @endforeach
                                        </select>
                                    </div>

                                    <!-- Arguments -->
                                    <div class="flex-1 grid grid-cols-1 gap-2">
                                        <template x-for="(argName, argIndex) in getArgs(rule.type)" :key="argIndex">
                                            <input type="text"
                                                   :name="'rules[{{ $column }}][' + index + '][args][' + argIndex + ']'"
                                                   x-model="rule.args[argIndex]"
                                                   :placeholder="argName"
                                                   class="block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm p-2 border">
                                        </template>
                                        <div x-show="getArgs(rule.type).length === 0" class="text-sm text-gray-400 py-2">
                                            No arguments required.
                                        </div>
                                    </div>

                                    <!-- Remove -->
                                    <button type="button" @click="removeRule('{{ $column }}', index)" class="text-red-600 hover:text-red-900 p-2">
                                        &times;
                                    </button>
                                </div>
                            </template>
                        </div>
                        <div x-show="!rules['{{ $column }}'] || rules['{{ $column }}'].length === 0" class="text-sm text-gray-500 italic">
                            No rules configured.
                        </div>
                    </div>
                @endforeach
            </div>

            <div class="flex justify-end mt-6">
                <a href="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'audit.index', $syncRun->id) }}" class="bg-gray-200 hover:bg-gray-300 text-gray-700 font-bold py-2 px-4 rounded mr-2">Cancel</a>
                <button type="submit" class="bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-2 px-4 rounded">Save Configuration</button>
            </div>
        </form>
    </div>
@endsection
