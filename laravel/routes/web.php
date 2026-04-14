<?php

use Illuminate\Support\Facades\Route;
use App\Http\Controllers\DataCleanController;
use App\Http\Controllers\DataMedianController;
use App\Http\Controllers\PgDataMedianSyncController;
use App\Http\Controllers\UploadStatsController;

Route::get('/', function () {
    return view('welcome');
});

Route::get('/data-clean', [DataCleanController::class, 'index']);
Route::post('/data-clean/convert', [DataCleanController::class, 'convert'])->name('data-clean.convert');
Route::post('/data-clean/convert-batch', [DataCleanController::class, 'convertBatch'])->name('data-clean.convert-batch');
Route::get('/demo', [DataCleanController::class, 'demo'])->name('demo');
Route::get('/histogram', [DataCleanController::class, 'histogram'])->name('histogram');

Route::post('/data-median/convert-batch', [DataMedianController::class, 'convertBatch'])->name('data-median.convert-batch');
Route::get('/data-median/demo', [DataMedianController::class, 'demo'])->name('data-median.demo');
Route::get('/data-median/api/stats', [DataMedianController::class, 'getStatsApi'])->name('data-median.api-stats');

Route::get('/upload-stats', [UploadStatsController::class, 'index'])->name('upload-stats');

// API: Sync data_clean (MySQL) -> data_median (PostgreSQL)
Route::get('/api/pg/data-median/sync', [PgDataMedianSyncController::class, 'sync'])->name('pg-data-median.sync');
Route::post('/api/pg/data-median/sync', [PgDataMedianSyncController::class, 'sync']);
