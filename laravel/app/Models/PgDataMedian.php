<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class PgDataMedian extends Model
{
    protected $connection = 'pgsql';

    protected $table = 'data_median';

    public $timestamps = false;

    protected $guarded = [];
}

