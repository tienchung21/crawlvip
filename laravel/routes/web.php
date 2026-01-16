<?php

use Illuminate\Support\Facades\Route;
use App\Http\Controllers\DataCleanController;

Route::get('/', function () {
    return view('welcome');
});

Route::get('/data-clean', [DataCleanController::class, 'index']);
Route::post('/data-clean/convert', [DataCleanController::class, 'convert'])->name('data-clean.convert');
Route::post('/data-clean/convert-batch', [DataCleanController::class, 'convertBatch'])->name('data-clean.convert-batch');
Route::get('/demo', [DataCleanController::class, 'demo'])->name('demo');
