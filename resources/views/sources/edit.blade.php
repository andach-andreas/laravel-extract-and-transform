@extends('extract-data::layout')

@section('content')
    <div class="mb-6">
        <h1 class="text-2xl font-semibold text-gray-900">Edit Source: {{ $source->name }}</h1>
    </div>

    <div class="bg-white shadow sm:rounded-lg p-6" x-data="{ connector: '{{ $source->connector }}' }">
        <form action="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'sources.update', $source->id) }}" method="POST">
            @csrf
            @method('PUT')

            <div class="mb-4">
                <label for="name" class="block text-sm font-medium text-gray-700">Name</label>
                <input type="text" name="name" id="name" value="{{ old('name', $source->name) }}" required class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm p-2 border">
            </div>

            <div class="mb-4">
                <label for="connector" class="block text-sm font-medium text-gray-700">Connector</label>
                <select name="connector" id="connector" x-model="connector" required class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm p-2 border">
                    @foreach($connectors as $key => $label)
                        <option value="{{ $key }}" {{ $source->connector === $key ? 'selected' : '' }}>{{ $label }}</option>
                    @endforeach
                </select>
            </div>

            <div class="border-t pt-4 mt-4">
                <h3 class="text-lg font-medium text-gray-900 mb-4">Configuration</h3>

                @foreach($schemas as $key => $fields)
                    <div x-show="connector === '{{ $key }}'" style="display: none;">
                        @foreach($fields as $field)
                            @php
                                $currentValue = '';
                                if ($source->connector === $key && isset($source->config[$field->key])) {
                                    $currentValue = $source->config[$field->key];
                                }
                            @endphp
                            <div class="mb-4">
                                <label for="config_{{ $key }}_{{ $field->key }}" class="block text-sm font-medium text-gray-700">
                                    {{ $field->label }}
                                    @if($field->required) <span class="text-red-500">*</span> @endif
                                </label>
                                <input type="{{ $field->type === 'password' ? 'password' : 'text' }}"
                                       name="config[{{ $field->key }}]"
                                       id="config_{{ $key }}_{{ $field->key }}"
                                       value="{{ old('config.'.$field->key, $currentValue) }}"
                                       {{ $field->required ? 'x-bind:required="connector === \'' . $key . '\'"' : '' }}
                                       x-bind:disabled="connector !== '{{ $key }}'"
                                       class="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm p-2 border">
                                @if($field->help)
                                    <p class="mt-1 text-sm text-gray-500">{{ $field->help }}</p>
                                @endif
                            </div>
                        @endforeach
                    </div>
                @endforeach
            </div>

            <div class="flex justify-end mt-6">
                <a href="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'sources.index') }}" class="bg-gray-200 hover:bg-gray-300 text-gray-700 font-bold py-2 px-4 rounded mr-2">Cancel</a>
                <button type="submit" class="bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-2 px-4 rounded">Update</button>
            </div>
        </form>
    </div>
@endsection
