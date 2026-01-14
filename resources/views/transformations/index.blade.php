@extends('extract-data::layout')

@section('content')
    <div class="flex justify-between items-center mb-6">
        <h1 class="text-2xl font-semibold text-gray-900">Transformations</h1>
        <a href="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'transformations.create') }}" class="bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-2 px-4 rounded">
            Create Transformation
        </a>
    </div>

    <div class="bg-white shadow overflow-hidden sm:rounded-md">
        <ul role="list" class="divide-y divide-gray-200">
            @forelse($transformations as $transformation)
                @php
                    $lastRun = $transformation->runs->last();
                @endphp
                <li>
                    <div class="px-4 py-4 sm:px-6 flex items-center justify-between">
                        <div class="flex items-center">
                            <div class="text-sm font-medium text-indigo-600 truncate">
                                {{ $transformation->name }}
                            </div>
                            <div class="ml-2 flex-shrink-0 flex">
                                @if($lastRun)
                                    <span class="px-2 inline-flex text-xs leading-5 font-semibold rounded-full
                                        {{ $lastRun->status === 'success' ? 'bg-green-100 text-green-800' : '' }}
                                        {{ $lastRun->status === 'failed' ? 'bg-red-100 text-red-800' : '' }}
                                        {{ $lastRun->status === 'running' ? 'bg-yellow-100 text-yellow-800' : '' }}">
                                        {{ ucfirst($lastRun->status) }}
                                    </span>
                                    <span class="text-xs text-gray-500 ml-2">{{ $lastRun->created_at->diffForHumans() }}</span>
                                @else
                                    <span class="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-gray-100 text-gray-800">
                                        Never Run
                                    </span>
                                @endif
                            </div>
                        </div>
                        <div class="flex space-x-4">
                            <form action="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'transformations.run', $transformation->id) }}" method="POST" class="inline">
                                @csrf
                                <button type="submit" class="text-green-600 hover:text-green-900 text-sm font-medium">Run</button>
                            </form>
                            <a href="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'transformations.edit', $transformation->id) }}" class="text-indigo-600 hover:text-indigo-900 text-sm font-medium">Edit</a>
                            <form action="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'transformations.destroy', $transformation->id) }}" method="POST" onsubmit="return confirm('Are you sure?');" class="inline">
                                @csrf
                                @method('DELETE')
                                <button type="submit" class="text-red-600 hover:text-red-900 text-sm font-medium">Delete</button>
                            </form>
                        </div>
                    </div>
                </li>
            @empty
                <li class="px-4 py-4 sm:px-6 text-gray-500 text-center">No transformations found.</li>
            @endforelse
        </ul>
    </div>
@endsection
