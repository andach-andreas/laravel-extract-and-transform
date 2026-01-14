<?php

return [
    'chunk_size' => 1000,

    /*
     * The prefix to use for the tables that are automatically generated to store
     * your synchronized data.
     */
    'table_prefix' => 'andach_',

    /*
    |--------------------------------------------------------------------------
    | Internal Table Prefix
    |--------------------------------------------------------------------------
    |
    | This prefix will be applied to all internal tables created by the package
    | (e.g. extract_sources, sync_profiles, sync_runs).
    |
    */
    'internal_table_prefix' => 'andach_leat_',

    /*
    |--------------------------------------------------------------------------
    | Route Prefix
    |--------------------------------------------------------------------------
    |
    | This prefix will be applied to the dashboard routes.
    | Default: 'andach-leat' -> /andach-leat/sources
    |
    */
    'route_prefix' => 'andach-leat',

    /*
    |--------------------------------------------------------------------------
    | Route Name Prefix
    |--------------------------------------------------------------------------
    |
    | This prefix will be applied to the dashboard route names.
    | Default: 'andach-leat.' -> route('andach-leat.sources.index')
    |
    */
    'route_name_prefix' => 'andach-leat.',

    /*
    |--------------------------------------------------------------------------
    | Views Configuration
    |--------------------------------------------------------------------------
    |
    | You can override the views used by the dashboard here.
    | By default, they point to the package's internal views.
    |
    */
    'views' => [
        'layout' => 'extract-data::layout',
        'sources' => [
            'index' => 'extract-data::sources.index',
            'create' => 'extract-data::sources.create',
            'edit' => 'extract-data::sources.edit',
        ],
        'syncs' => [
            'index' => 'extract-data::syncs.index',
            'configure' => 'extract-data::syncs.configure',
            'global_index' => 'extract-data::syncs.global_index',
        ],
        'audit' => [
            'index' => 'extract-data::audit.index',
            'configure' => 'extract-data::audit.configure',
        ],
        'transformations' => [
            'index' => 'extract-data::transformations.index',
            'create' => 'extract-data::transformations.create',
            'edit' => 'extract-data::transformations.edit',
        ],
        'enrichments' => [
            'index' => 'extract-data::enrichments.index',
            'create' => 'extract-data::enrichments.create',
            'edit' => 'extract-data::enrichments.edit',
        ],
    ],
];
