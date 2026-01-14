<?php

use Andach\ExtractAndTransform\Http\Controllers\Api\TableController;
use Andach\ExtractAndTransform\Http\Controllers\AuditController;
use Andach\ExtractAndTransform\Http\Controllers\EnrichmentController;
use Andach\ExtractAndTransform\Http\Controllers\GlobalSyncController;
use Andach\ExtractAndTransform\Http\Controllers\SourceController;
use Andach\ExtractAndTransform\Http\Controllers\SyncController;
use Andach\ExtractAndTransform\Http\Controllers\TransformationController;
use Illuminate\Support\Facades\Route;

$prefix = config('extract-data.route_prefix', 'andach-leat');
$namePrefix = config('extract-data.route_name_prefix', 'andach-leat.');

Route::group(['prefix' => $prefix, 'as' => $namePrefix, 'middleware' => ['web']], function () {
    Route::resource('sources', SourceController::class);

    // Global Syncs
    Route::get('syncs', [GlobalSyncController::class, 'index'])->name('syncs.global_index');

    // Source-specific Sync Routes
    Route::get('sources/{source}/syncs', [SyncController::class, 'index'])->name('syncs.index');
    Route::get('sources/{source}/syncs/configure', [SyncController::class, 'configure'])->name('syncs.configure');
    Route::post('sources/{source}/syncs', [SyncController::class, 'store'])->name('syncs.store');

    // Audit Routes
    Route::get('syncs/{run}/audit', [AuditController::class, 'index'])->name('audit.index');
    Route::get('syncs/{run}/audit/configure', [AuditController::class, 'configure'])->name('audit.configure');
    Route::post('syncs/{run}/audit/config', [AuditController::class, 'storeConfig'])->name('audit.store_config');
    Route::post('syncs/{run}/audit/run', [AuditController::class, 'run'])->name('audit.run');
    Route::post('syncs/{run}/audit/correction', [AuditController::class, 'storeCorrections'])->name('audit.correction');
    Route::post('syncs/{run}/audit/reconcile', [AuditController::class, 'reconcile'])->name('audit.reconcile');

    // Transformation Routes
    Route::resource('transformations', TransformationController::class);
    Route::post('transformations/{transformation}/run', [TransformationController::class, 'run'])->name('transformations.run');

    // Enrichment Routes
    Route::resource('enrichments', EnrichmentController::class);
    Route::post('enrichments/{enrichment}/run', [EnrichmentController::class, 'run'])->name('enrichments.run');

    // Internal API
    Route::get('api/tables/{table}/columns', [TableController::class, 'columns'])->name('api.tables.columns');
});
