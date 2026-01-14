@extends('extract-data::layout')

@section('content')
    <div class="flex justify-between items-center mb-6">
        <h1 class="text-2xl font-semibold text-gray-900">All Syncs</h1>
    </div>

    <div class="bg-white shadow overflow-hidden sm:rounded-lg">
        <table class="min-w-full divide-y divide-gray-200">
            <thead class="bg-gray-50">
                <tr>
                    <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Source</th>
                    <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Dataset</th>
                    <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Last Sync</th>
                    <th scope="col" class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                    <th scope="col" class="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
                </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-200">
                @forelse($profiles as $profile)
                    @php
                        $lastRun = $profile->runs->first();
                    @endphp
                    <tr>
                        <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                            <a href="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'sources.edit', $profile->source->id) }}" class="text-indigo-600 hover:text-indigo-900">
                                {{ $profile->source->name }}
                            </a>
                            <span class="text-gray-500 text-xs ml-1">({{ $profile->source->connector }})</span>
                        </td>
                        <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                            {{ $profile->dataset_identifier }}
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
                            <a href="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'syncs.configure', ['source' => $profile->source->id, 'dataset' => $profile->dataset_identifier]) }}" class="text-indigo-600 hover:text-indigo-900 mr-4">Configure</a>

                            <form action="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'syncs.store', $profile->source->id) }}" method="POST" class="inline mr-4">
                                @csrf
                                <input type="hidden" name="dataset" value="{{ $profile->dataset_identifier }}">
                                <button type="submit" class="text-gray-600 hover:text-gray-900">Sync Now</button>
                            </form>

                            @if($lastRun && $lastRun->status === 'success')
                                <a href="{{ route(config('extract-data.route_name_prefix', 'andach-leat.') . 'audit.index', $lastRun->id) }}" class="text-green-600 hover:text-green-900 font-bold">Audit</a>
                            @endif
                        </td>
                    </tr>
                @empty
                    <tr>
                        <td colspan="5" class="px-6 py-4 text-center text-gray-500">No syncs configured yet.</td>
                    </tr>
                @endforelse
            </tbody>
        </table>
    </div>
@endsection
