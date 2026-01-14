@extends('extract-data::layout')

@section('content')
    <div class="flex justify-between items-center mb-6">
        <h1 class="text-2xl font-semibold text-gray-900">Sources</h1>
        <a href="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'sources.create') }}" class="bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-2 px-4 rounded">
            Add Source
        </a>
    </div>

    <div class="bg-white shadow overflow-hidden sm:rounded-md">
        <ul role="list" class="divide-y divide-gray-200">
            @forelse($sources as $source)
                <li>
                    <div class="px-4 py-4 sm:px-6 flex items-center justify-between">
                        <div class="flex items-center">
                            <div class="text-sm font-medium text-indigo-600 truncate">
                                {{ $source->name }}
                            </div>
                            <div class="ml-2 flex-shrink-0 flex">
                                <span class="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-green-100 text-green-800">
                                    {{ $source->connector }}
                                </span>
                            </div>
                        </div>
                        <div class="flex space-x-4">
                            <a href="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'syncs.index', $source->id) }}" class="text-indigo-600 hover:text-indigo-900 text-sm font-medium">Datasets</a>
                            <a href="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'sources.edit', $source->id) }}" class="text-gray-600 hover:text-gray-900 text-sm font-medium">Edit</a>
                            <form action="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'sources.destroy', $source->id) }}" method="POST" onsubmit="return confirm('Are you sure?');" class="inline">
                                @csrf
                                @method('DELETE')
                                <button type="submit" class="text-red-600 hover:text-red-900 text-sm font-medium">Delete</button>
                            </form>
                        </div>
                    </div>
                </li>
            @empty
                <li class="px-4 py-4 sm:px-6 text-gray-500 text-center">No sources found.</li>
            @endforelse
        </ul>
    </div>
@endsection
