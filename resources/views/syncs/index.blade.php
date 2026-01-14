@extends('extract-data::layout')

@section('content')
    <div class="flex justify-between items-center mb-6">
        <div>
            <h1 class="text-2xl font-semibold text-gray-900">Datasets: {{ $sourceModel->name }}</h1>
            <p class="text-sm text-gray-500 mt-1">Connector: {{ $sourceModel->connector }}</p>
        </div>
        <a href="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'sources.index') }}" class="text-indigo-600 hover:text-indigo-900 font-medium">
            &larr; Back to Sources
        </a>
    </div>

    @if(session('error'))
        <div class="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded relative mb-4" role="alert">
            <span class="block sm:inline">{{ session('error') }}</span>
        </div>
    @endif

    @if(session('success'))
        <div class="bg-green-100 border border-green-400 text-green-700 px-4 py-3 rounded relative mb-4" role="alert">
            <span class="block sm:inline">{{ session('success') }}</span>
        </div>
    @endif

    <div class="bg-white shadow overflow-hidden sm:rounded-lg">
        <table class="min-w-full divide-y divide-gray-200">
            <thead class="bg-gray-50">
                <tr>
                    <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Dataset</th>
                    <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Identifier</th>
                    <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Last Sync</th>
                    <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                    <th scope="col" class="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
                </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-200">
                @forelse($datasets as $dataset)
                    @php
                        $profile = $profiles[$dataset->getIdentifier()] ?? null;
                        $lastRun = $profile ? $profile->runs()->latest()->first() : null;
                    @endphp
                    <tr>
                        <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                            {{ $dataset->getLabel() }}
                        </td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            {{ $dataset->getIdentifier() }}
                        </td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            @if($lastRun)
                                {{ $lastRun->created_at->diffForHumans() }}
                            @else
                                <span class="text-gray-400">Never</span>
                            @endif
                        </td>
                        <td class="px-6 py-4 whitespace-nowrap">
                            @if($lastRun)
                                <span class="px-2 inline-flex text-xs leading-5 font-semibold rounded-full
                                    {{ $lastRun->status === 'success' ? 'bg-green-100 text-green-800' : '' }}
                                    {{ $lastRun->status === 'failed' ? 'bg-red-100 text-red-800' : '' }}
                                    {{ $lastRun->status === 'running' ? 'bg-yellow-100 text-yellow-800' : '' }}">
                                    {{ ucfirst($lastRun->status) }}
                                </span>
                            @else
                                <span class="px-2 inline-flex text-xs leading-5 font-semibold rounded-full bg-gray-100 text-gray-800">
                                    Pending
                                </span>
                            @endif
                        </td>
                        <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                            <a href="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'syncs.configure', ['source' => $sourceModel->id, 'dataset' => $dataset->getIdentifier()]) }}" class="text-indigo-600 hover:text-indigo-900 mr-4">Configure</a>

                            <form action="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'syncs.store', $sourceModel->id) }}" method="POST" class="inline">
                                @csrf
                                <input type="hidden" name="dataset" value="{{ $dataset->getIdentifier() }}">
                                <button type="submit" class="text-gray-600 hover:text-gray-900">Quick Sync</button>
                            </form>
                        </td>
                    </tr>
                @empty
                    <tr>
                        <td colspan="5" class="px-6 py-4 text-center text-gray-500">No datasets found.</td>
                    </tr>
                @endforelse
            </tbody>
        </table>
    </div>
@endsection
