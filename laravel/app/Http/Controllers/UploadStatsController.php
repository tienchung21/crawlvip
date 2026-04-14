<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;

class UploadStatsController extends Controller
{
    private function ensureUploadedAtColumn(): void
    {
        $exists = DB::selectOne(
            "SELECT 1 FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'data_full' AND column_name = 'uploaded_at' LIMIT 1"
        );
        if (!$exists) {
            DB::statement("ALTER TABLE data_full ADD COLUMN uploaded_at DATETIME NULL DEFAULT NULL");
            DB::statement("CREATE INDEX idx_data_full_uploaded_at ON data_full (uploaded_at)");
        }
    }

    public function index(Request $request)
    {
        $this->ensureUploadedAtColumn();

        $from = $request->query('from'); // YYYY-MM-DD
        $to = $request->query('to');     // YYYY-MM-DD
        // Ward section has its own filter (0 = tat ca)
        $wardProvinceId = (int) $request->query('ward_province_id', 63); // default HCM
        $wardSort = $request->query('ward_sort', 'total_desc');

        if (empty($from) || empty($to)) {
            // Default last 7 days (including today)
            $to = date('Y-m-d');
            $from = date('Y-m-d', strtotime('-6 days'));
        }

        $fromDt = $from . " 00:00:00";
        $toDt = $to . " 23:59:59";

        $base = DB::table('data_full as df')
            ->whereNotNull('df.uploaded_at')
            ->whereBetween('df.uploaded_at', [$fromDt, $toDt]);

        $totalUploaded = (clone $base)
            ->where('df.images_status', 'LISTING_UPLOADED')
            ->count();

        $totalDuplicate = (clone $base)
            ->where('df.images_status', 'DUPLICATE_SKIPPED')
            ->count();

        $withPhone = (clone $base)
            ->where('df.images_status', 'LISTING_UPLOADED')
            ->whereNotNull('df.phone')
            ->where('df.phone', '<>', '')
            ->count();

        $daily = (clone $base)
            ->select(DB::raw('DATE(df.uploaded_at) as day'), DB::raw('COUNT(*) as total'))
            ->where('df.images_status', 'LISTING_UPLOADED')
            ->groupBy(DB::raw('DATE(df.uploaded_at)'))
            ->orderBy('day', 'desc')
            ->get();

        $byType = (clone $base)
            ->select('df.type', 'df.property_type', DB::raw('COUNT(*) as total'))
            ->where('df.images_status', 'LISTING_UPLOADED')
            ->groupBy('df.type', 'df.property_type')
            ->orderBy('total', 'desc')
            ->limit(50)
            ->get();

        $byProvince = (clone $base)
            ->leftJoin('transaction_city_merge as p', function ($join) {
                $join->on('p.new_city_id', '=', 'df.province_id')
                    ->where('p.action_type', '=', 0);
            })
            ->select(
                'df.province_id',
                DB::raw("COALESCE(p.new_city_name, CONCAT('ID ', df.province_id)) as province_name"),
                DB::raw('COUNT(*) as total')
            )
            ->where('df.images_status', 'LISTING_UPLOADED')
            ->groupBy('df.province_id', 'province_name')
            // Prefer HCM on top even when totals tie (or are all 0 in a date range).
            ->orderByRaw('CASE WHEN df.province_id = 63 THEN 0 ELSE 1 END ASC')
            ->orderBy('total', 'desc')
            ->limit(30)
            ->get();

        // Ward (xa moi): show ALL wards from transaction_city_merge even if count = 0.
        // Heuristic: wards are rows with action_type=0, new_city_parent_id<>0, old_district_id<>0.
        $wardListBase = DB::table('transaction_city_merge as w')
            ->where('w.action_type', 0)
            ->where('w.new_city_parent_id', '<>', 0)
            ->where('w.old_district_id', '<>', 0);

        if ($wardProvinceId !== 0) {
            $counts = (clone $base)
                ->where('df.images_status', 'LISTING_UPLOADED')
                ->where('df.province_id', $wardProvinceId)
                ->whereNotNull('df.ward_id')
                ->where('df.ward_id', '<>', 0)
                ->select('df.ward_id', DB::raw('COUNT(*) as total'))
                ->groupBy('df.ward_id');

            $byWardQ = (clone $wardListBase)
                ->where('w.new_city_parent_id', $wardProvinceId)
                ->leftJoinSub($counts, 'c', function ($join) {
                    $join->on('c.ward_id', '=', 'w.new_city_id');
                })
                ->select(
                    DB::raw('w.new_city_id as ward_id'),
                    DB::raw("w.new_city_name as ward_name"),
                    DB::raw('COALESCE(c.total, 0) as total')
                );

            if ($wardSort === 'ward_name_asc') {
                $byWardQ->orderBy('ward_name', 'asc');
            } else {
                $byWardQ->orderByRaw('COALESCE(c.total,0) DESC');
            }

            $byWard = $byWardQ->get();
        } else {
            $counts = (clone $base)
                ->where('df.images_status', 'LISTING_UPLOADED')
                ->whereNotNull('df.ward_id')
                ->where('df.ward_id', '<>', 0)
                ->select('df.province_id', 'df.ward_id', DB::raw('COUNT(*) as total'))
                ->groupBy('df.province_id', 'df.ward_id');

            $byWardQ = (clone $wardListBase)
                ->leftJoinSub($counts, 'c', function ($join) {
                    $join->on('c.ward_id', '=', 'w.new_city_id')
                        ->on('c.province_id', '=', 'w.new_city_parent_id');
                })
                ->leftJoin('transaction_city_merge as p2', function ($join) {
                    $join->on('p2.new_city_id', '=', 'w.new_city_parent_id')
                        ->where('p2.action_type', '=', 0);
                })
                ->select(
                    DB::raw('w.new_city_parent_id as province_id'),
                    DB::raw("COALESCE(p2.new_city_name, CONCAT('ID ', w.new_city_parent_id)) as province_name"),
                    DB::raw('w.new_city_id as ward_id'),
                    DB::raw("w.new_city_name as ward_name"),
                    DB::raw('COALESCE(c.total, 0) as total')
                );

            if ($wardSort === 'province_asc_name_asc') {
                $byWardQ->orderBy('province_name', 'asc')->orderBy('ward_name', 'asc');
            } elseif ($wardSort === 'province_desc_total_desc') {
                $byWardQ->orderBy('province_name', 'desc')
                    ->orderByRaw('COALESCE(c.total,0) DESC')
                    ->orderBy('ward_name', 'asc');
            } elseif ($wardSort === 'province_asc_total_desc') {
                // HCM first, then province A-Z; within each province sort by total desc.
                $byWardQ->orderByRaw('CASE WHEN w.new_city_parent_id = 63 THEN 0 ELSE 1 END ASC')
                    ->orderBy('province_name', 'asc')
                    ->orderByRaw('COALESCE(c.total,0) DESC')
                    ->orderBy('ward_name', 'asc');
            } else {
                // Default for "Tat ca": group by province (HCM first) so the list is readable
                // even when most provinces have 0.
                $byWardQ->orderByRaw('CASE WHEN w.new_city_parent_id = 63 THEN 0 ELSE 1 END ASC')
                    ->orderBy('province_name', 'asc')
                    ->orderByRaw('COALESCE(c.total,0) DESC')
                    ->orderBy('ward_name', 'asc');
            }

            $byWard = $byWardQ->get();
        }

        $provinceOptions = DB::table('transaction_city_merge')
            ->select('new_city_id as id', 'new_city_name as name')
            ->where('action_type', 0)
            ->where('new_city_parent_id', 0)
            // Put HCM at the top of the selector.
            ->orderByRaw('CASE WHEN new_city_id = 63 THEN 0 ELSE 1 END ASC')
            ->orderBy('new_city_name', 'asc')
            ->get();

        return view('upload_stats', [
            'from' => $from,
            'to' => $to,
            'wardProvinceId' => $wardProvinceId,
            'wardSort' => $wardSort,
            'provinceOptions' => $provinceOptions,
            'totalUploaded' => $totalUploaded,
            'totalDuplicate' => $totalDuplicate,
            'withPhone' => $withPhone,
            'daily' => $daily,
            'byType' => $byType,
            'byProvince' => $byProvince,
            'byWard' => $byWard,
        ]);
    }
}
