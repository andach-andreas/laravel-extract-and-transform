@extends('extract-data::layout')

@section('content')
    <div class="flex justify-between items-center mb-6">
        <h1 class="text-2xl font-semibold text-gray-900">Enrichment Profiles</h1>
        <a href="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'enrichments.create') }}" class="bg-indigo-600 hover:bg-indigo-700 text-white font-bold py-2 px-4 rounded">
            Create Enrichment
        </a>
    </div>

    <div class="bg-white shadow overflow-hidden sm:rounded-md">
        <ul role="list" class="divide-y divide-gray-200">
            @forelse($enrichments as $enrichment)
                @php
                    $lastRun = $enrichment->runs->last();
                @endphp
                <li>
                    <div class="px-4 py-4 sm:px-6 flex items-center justify-between">
                        <div class="flex items-center">
                            <div class="text-sm font-medium text-indigo-600 truncate">
                                {{ $enrichment->name }}
                            </div>
                            <div class="ml-2 flex-shrink-0 flex">
                                <span class="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-blue-100 text-blue-800">
                                    {{ $enrichment->provider }}
                                </span>
                                @if($lastRun)
                                    <span class="ml-2 px-2 inline-flex text-xs leading-5 font-semibold rounded-full
                                        {{ $lastRun->status === 'success' ? 'bg-green-100 text-green-800' : '' }}
                                        {{ $lastRun->status === 'failed' ? 'bg-red-100 text-red-800' : '' }}">
                                        {{ ucfirst($lastRun->status) }}
                                    </span>
                                @endif
                            </div>
                        </div>
                        <div class="flex space-x-4">
                            <form action="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'enrichments.run', $enrichment->id) }}" method="POST" class="inline">
                                @csrf
                                <button type="submit" class="text-green-600 hover:text-green-900 text-sm font-medium">Run</button>
                            </form>
                            <a href="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'enrichments.edit', $enrichment->id) }}" class="text-indigo-600 hover:text-indigo-900 text-sm font-medium">Edit</a>
                            <form action="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'enrichments.destroy', $enrichment->id) }}" method="POST" onsubmit="return confirm('Are you sure?');" class="inline">
                                @csrf
                                @method('DELETE')
                                <button type="submit" class="text-red-600 hover:text-red-900 text-sm font-medium">Delete</button>
                            </form>
                        </div>
                    </div>
                </li>
            @empty
                <li class="px-4 py-4 sm:px-6 text-gray-500 text-center">No enrichment profiles found.</li>
            @endforelse
        </ul>
    </div>
@endsection
