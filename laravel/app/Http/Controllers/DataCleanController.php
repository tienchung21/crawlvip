<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;

class DataCleanController extends Controller
{
    private function ensureStatsTable(): void
    {
        DB::statement("
            CREATE TABLE IF NOT EXISTS data_clean_stats (
                id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                scope VARCHAR(16) NOT NULL,
                new_region_id INT NULL,
                new_region_name VARCHAR(255) NULL,
                new_ward_id INT NULL,
                new_ward_name VARCHAR(255) NULL,
                type VARCHAR(16) NULL,
                category INT NULL,
                median_group TINYINT NULL,
                month VARCHAR(7) NULL,
                avg_price_m2 DECIMAL(20,2) NULL,
                median_price_m2 DECIMAL(20,2) NULL,
                min_price_m2 DECIMAL(20,2) NULL,
                max_price_m2 DECIMAL(20,2) NULL,
                total_rows INT NULL,
                trimmed_rows INT NULL,
                converted_at DATETIME NULL,
                INDEX idx_scope_region (scope, new_region_id),
                INDEX idx_scope_ward (scope, new_ward_id),
                INDEX idx_month (month)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        " );

        $medianGroupColumn = DB::selectOne("
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'data_clean_stats'
              AND column_name = 'median_group'
            LIMIT 1
        ");
        if (!$medianGroupColumn) {
            DB::statement("
                ALTER TABLE data_clean_stats
                ADD COLUMN median_group TINYINT NULL
            ");
        }

        $minColumn = DB::selectOne("
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'data_clean_stats'
              AND column_name = 'min_price_m2'
            LIMIT 1
        ");
        if (!$minColumn) {
            DB::statement("
                ALTER TABLE data_clean_stats
                ADD COLUMN min_price_m2 DECIMAL(20,2) NULL
            ");
        }

        $maxColumn = DB::selectOne("
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'data_clean_stats'
              AND column_name = 'max_price_m2'
            LIMIT 1
        ");
        if (!$maxColumn) {
            DB::statement("
                ALTER TABLE data_clean_stats
                ADD COLUMN max_price_m2 DECIMAL(20,2) NULL
            ");
        }

        // Drop old problematic unique keys if they exist
        $oldKeys = ['uq_ward', 'uq_region', 'uq_ward_mg', 'uq_region_mg'];
        foreach ($oldKeys as $keyName) {
            $exists = DB::selectOne("
                SELECT 1
                FROM information_schema.statistics
                WHERE table_schema = DATABASE()
                  AND table_name = 'data_clean_stats'
                  AND index_name = ?
                LIMIT 1
            ", [$keyName]);
            if ($exists) {
                DB::statement("ALTER TABLE data_clean_stats DROP INDEX {$keyName}");
            }
        }

        // Create single unified unique key for both ward and region scopes
        // For ward scope: new_ward_id is NOT NULL
        // For region scope: new_ward_id is NULL (which is always unique in MySQL unique key)
        $wardScopeIndex = DB::selectOne("
            SELECT 1
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = 'data_clean_stats'
              AND index_name = 'uq_ward_scope'
            LIMIT 1
        ");
        if (!$wardScopeIndex) {
            DB::statement("
                ALTER TABLE data_clean_stats
                ADD UNIQUE KEY uq_ward_scope (scope, new_region_id, new_ward_id, type, median_group, month)
            ");
        }
    }


    private function ensureDataCleanIndexes(): void
    {
        $indexes = [
            'idx_dc_ward_stats' => 'cf_region_id_new, cf_ward_id_new, type, median_group, list_ym, price_m2_vnd',
            'idx_dc_region_stats' => 'cf_region_id_new, type, median_group, list_ym, price_m2_vnd',
        ];

        foreach ($indexes as $name => $columns) {
            $exists = DB::selectOne("
                SELECT 1
                FROM information_schema.statistics
                WHERE table_schema = DATABASE()
                  AND table_name = 'data_clean'
                  AND index_name = ?
                LIMIT 1
            ", [$name]);
            if (!$exists) {
                DB::statement("ALTER TABLE data_clean ADD INDEX {$name} ({$columns})");
            }
        }
    }


    private function computeTrimmedStats($baseQuery): array
    {
        $totalPrice = (clone $baseQuery)
            ->whereNotNull('d.price_m2_vnd')
            ->where('d.price_m2_vnd', '>', 0)
            ->count();

        $medianTrim = null;
        $trimmed = 0;
        $minTrim = null;
        $maxTrim = null;
        if ($totalPrice > 0) {
            $cut = (int) floor($totalPrice * 0.1);
            $trimmed = $totalPrice - ($cut * 2);

            if ($trimmed > 0) {
                $order = (clone $baseQuery)
                    ->whereNotNull('d.price_m2_vnd')
                    ->where('d.price_m2_vnd', '>', 0)
                    ->orderBy('d.price_m2_vnd');

                $minTrim = (clone $order)
                    ->offset($cut)
                    ->limit(1)
                    ->value('d.price_m2_vnd');
                $maxOffset = $cut + $trimmed - 1;
                $maxTrim = (clone $order)
                    ->offset($maxOffset)
                    ->limit(1)
                    ->value('d.price_m2_vnd');

                if ($trimmed % 2 === 1) {
                    $offset = $cut + intdiv($trimmed, 2);
                    $medianTrim = $order->offset($offset)->value('d.price_m2_vnd');
                } else {
                    $offset = $cut + (int) ($trimmed / 2 - 1);
                    $vals = $order->offset($offset)->limit(2)->pluck('d.price_m2_vnd')->values();

                    if ($vals->count() === 2) {
                        $medianTrim = ((float) $vals[0] + (float) $vals[1]) / 2;
                    } elseif ($vals->count() === 1) {
                        $medianTrim = (float) $vals[0];
                    }
                }
            }
        }

        return [$medianTrim, $totalPrice, $trimmed, $minTrim, $maxTrim];
    }

    public function convert(Request $request)
    {
        $this->ensureStatsTable();
        $this->ensureDataCleanIndexes();
        set_time_limit(0);

        $now = now();

        $base = DB::table('data_clean as d');

        $groups = (clone $base)
            ->selectRaw("d.cf_region_id_new AS region_id, d.cf_region_name_new AS region_name, d.cf_ward_id_new AS ward_id, d.cf_ward_name_new AS ward_name, d.type AS type, d.median_group AS median_group, d.list_ym AS month")
            ->whereNotNull('d.list_ym')
            ->whereNotNull('d.cf_region_id_new')
            ->whereNotNull('d.cf_ward_id_new')
            ->whereNotNull('d.type')
            ->whereNotNull('d.median_group')
            ->groupByRaw("d.cf_region_id_new, d.cf_region_name_new, d.cf_ward_id_new, d.cf_ward_name_new, d.type, d.median_group, d.list_ym")
            ->get();

        $rows = [];
        foreach ($groups as $g) {
            $groupBase = (clone $base)
                ->where('d.cf_region_id_new', $g->region_id)
                ->where('d.cf_ward_id_new', $g->ward_id)
                ->where('d.type', $g->type)
                ->where('d.median_group', $g->median_group)
                ->whereRaw("d.list_ym = ?", [$g->month]);

            $avg = (clone $groupBase)
                ->whereNotNull('d.price_m2_vnd')
                ->where('d.price_m2_vnd', '>', 0)
                ->avg('d.price_m2_vnd');

            [$medianTrim, $totalPrice, $trimmed, $minTrim, $maxTrim] = $this->computeTrimmedStats($groupBase);

            $rows[] = [
                'scope' => 'ward',
                'new_region_id' => $g->region_id,
                'new_region_name' => $g->region_name,
                'new_ward_id' => $g->ward_id,
                'new_ward_name' => $g->ward_name,
                'type' => $g->type,
                'median_group' => $g->median_group,
                'category' => null,
                'month' => $g->month,
                'avg_price_m2' => $avg,
                'median_price_m2' => $medianTrim,
                'min_price_m2' => $minTrim,
                'max_price_m2' => $maxTrim,
                'total_rows' => $totalPrice,
                'trimmed_rows' => $trimmed,
                'converted_at' => $now,
            ];
        }

        $regionGroups = (clone $base)
            ->selectRaw("d.cf_region_id_new AS region_id, d.cf_region_name_new AS region_name, d.type AS type, d.median_group AS median_group, d.list_ym AS month")
            ->whereNotNull('d.list_ym')
            ->whereNotNull('d.cf_region_id_new')
            ->whereNotNull('d.type')
            ->whereNotNull('d.median_group')
            ->groupByRaw("d.cf_region_id_new, d.cf_region_name_new, d.type, d.median_group, d.list_ym")
            ->get();

        foreach ($regionGroups as $g) {
            $groupBase = (clone $base)
                ->where('d.cf_region_id_new', $g->region_id)
                ->where('d.type', $g->type)
                ->where('d.median_group', $g->median_group)
                ->whereRaw("d.list_ym = ?", [$g->month]);

            $avg = (clone $groupBase)
                ->whereNotNull('d.price_m2_vnd')
                ->where('d.price_m2_vnd', '>', 0)
                ->avg('d.price_m2_vnd');

            [$medianTrim, $totalPrice, $trimmed, $minTrim, $maxTrim] = $this->computeTrimmedStats($groupBase);

            $rows[] = [
                'scope' => 'region',
                'new_region_id' => $g->region_id,
                'new_region_name' => $g->region_name,
                'new_ward_id' => null,
                'new_ward_name' => null,
                'type' => $g->type,
                'median_group' => $g->median_group,
                'category' => null,
                'month' => $g->month,
                'avg_price_m2' => $avg,
                'median_price_m2' => $medianTrim,
                'min_price_m2' => $minTrim,
                'max_price_m2' => $maxTrim,
                'total_rows' => $totalPrice,
                'trimmed_rows' => $trimmed,
                'converted_at' => $now,
            ];
        }

        if (!empty($rows)) {
            $wardRows = array_values(array_filter($rows, fn ($row) => $row['scope'] === 'ward'));
            $regionRows = array_values(array_filter($rows, fn ($row) => $row['scope'] === 'region'));

            if (!empty($wardRows)) {
                DB::table('data_clean_stats')->upsert(
                    $wardRows,
                    ['scope', 'new_region_id', 'new_ward_id', 'type', 'median_group', 'month'],
                    ['new_region_name', 'new_ward_name', 'avg_price_m2', 'median_price_m2', 'min_price_m2', 'max_price_m2', 'total_rows', 'trimmed_rows', 'converted_at']
                );
            }

            if (!empty($regionRows)) {
                DB::table('data_clean_stats')->upsert(
                    $regionRows,
                    ['scope', 'new_region_id', 'type', 'median_group', 'month'],
                    ['new_region_name', 'avg_price_m2', 'median_price_m2', 'min_price_m2', 'max_price_m2', 'total_rows', 'trimmed_rows', 'converted_at']
                );
            }
        }

        return redirect()->back()->with('status', 'Converted: ' . count($rows) . ' rows');
    }

    
    public function convertBatch(Request $request)
    {
        $this->ensureStatsTable();
        $this->ensureDataCleanIndexes();
        set_time_limit(0);

        $scope = $request->input('scope', 'ward');
        $offset = (int) $request->input('offset', 0);
        $limit = (int) $request->input('limit', 500);
        if ($limit < 50) {
            $limit = 50;
        } elseif ($limit > 2000) {
            $limit = 2000;
        }
        if ($offset < 0) {
            $offset = 0;
        }

        $processed = 0;
        if ($scope === 'ward') {
            // Ward scope: group by region + ward + median_group + month (bỏ type)
            $groupSql = <<<SQL
SELECT
    d.cf_region_id_new AS region_id,
    d.cf_region_name_new AS region_name,
    d.cf_ward_id_new AS ward_id,
    d.cf_ward_name_new AS ward_name,
    d.median_group AS median_group,
    d.list_ym AS month
FROM data_clean d
WHERE d.list_ym IS NOT NULL
  AND d.cf_region_id_new IS NOT NULL
  AND d.cf_ward_id_new IS NOT NULL
  AND d.median_group IS NOT NULL
GROUP BY d.cf_region_id_new, d.cf_region_name_new, d.cf_ward_id_new, d.cf_ward_name_new, d.median_group, d.list_ym
ORDER BY d.cf_region_id_new, d.cf_ward_id_new, d.median_group, d.list_ym
LIMIT {$limit} OFFSET {$offset}
SQL;
            $countSql = "SELECT COUNT(*) AS cnt FROM ({$groupSql}) AS grp";
            $countRow = DB::selectOne($countSql);
            $processed = (int) ($countRow->cnt ?? 0);

            if ($processed > 0) {
                $insertSql = <<<SQL
INSERT INTO data_clean_stats (
    scope, new_region_id, new_region_name, new_ward_id, new_ward_name, type,
    median_group, category, month, avg_price_m2, median_price_m2,
    min_price_m2, max_price_m2, total_rows, trimmed_rows, converted_at
)
SELECT
    'ward' AS scope,
    g.region_id,
    g.region_name,
    g.ward_id,
    g.ward_name,
    NULL AS type,
    g.median_group,
    NULL AS category,
    g.month,
    a.avg_price_m2,
    a.median_price_m2,
    a.min_price_m2,
    a.max_price_m2,
    COALESCE(a.total_rows, 0) AS total_rows,
    COALESCE(a.trimmed_rows, 0) AS trimmed_rows,
    NOW()
FROM ({$groupSql}) AS g
LEFT JOIN (
    SELECT
        region_id,
        ward_id,
        median_group,
        month,
        MAX(avg_price) AS avg_price_m2,
        MAX(cnt) AS total_rows,
        MAX(trimmed_cnt) AS trimmed_rows,
        MIN(price_m2_vnd) AS min_price_m2,
        MAX(price_m2_vnd) AS max_price_m2,
        AVG(CASE
            WHEN trimmed_cnt % 2 = 1 AND rn = cut + (trimmed_cnt + 1) / 2 THEN price_m2_vnd
            WHEN trimmed_cnt % 2 = 0 AND (rn = cut + trimmed_cnt / 2 OR rn = cut + trimmed_cnt / 2 + 1) THEN price_m2_vnd
        END) AS median_price_m2
    FROM (
        SELECT
            ranked.*,
            FLOOR(cnt * 0.1) AS cut,
            cnt - (2 * FLOOR(cnt * 0.1)) AS trimmed_cnt
        FROM (
            SELECT
                g2.region_id,
                g2.ward_id,
                g2.median_group,
                g2.month,
                d.price_m2_vnd,
                ROW_NUMBER() OVER (PARTITION BY g2.region_id, g2.ward_id, g2.median_group, g2.month ORDER BY d.price_m2_vnd) AS rn,
                COUNT(*) OVER (PARTITION BY g2.region_id, g2.ward_id, g2.median_group, g2.month) AS cnt,
                AVG(d.price_m2_vnd) OVER (PARTITION BY g2.region_id, g2.ward_id, g2.median_group, g2.month) AS avg_price
            FROM data_clean d
            JOIN ({$groupSql}) AS g2
              ON g2.region_id = d.cf_region_id_new
             AND g2.ward_id = d.cf_ward_id_new
             AND g2.median_group = d.median_group
             AND g2.month = d.list_ym
            WHERE d.price_m2_vnd IS NOT NULL
              AND d.price_m2_vnd > 0
        ) ranked
    ) trim
    WHERE cnt > 0 AND rn > cut AND rn <= cnt - cut
    GROUP BY region_id, ward_id, median_group, month
) a
  ON a.region_id = g.region_id
 AND a.ward_id = g.ward_id
 AND a.median_group = g.median_group
 AND a.month = g.month
ON DUPLICATE KEY UPDATE
    new_region_name = VALUES(new_region_name),
    new_ward_name = VALUES(new_ward_name),
    avg_price_m2 = VALUES(avg_price_m2),
    median_price_m2 = VALUES(median_price_m2),
    min_price_m2 = VALUES(min_price_m2),
    max_price_m2 = VALUES(max_price_m2),
    total_rows = VALUES(total_rows),
    trimmed_rows = VALUES(trimmed_rows),
    converted_at = VALUES(converted_at)
SQL;
                DB::statement($insertSql);
            }
        } elseif ($scope === 'region') {
            // Region scope: group by region + median_group + month (bỏ type)
            $groupSql = <<<SQL
SELECT
    d.cf_region_id_new AS region_id,
    d.cf_region_name_new AS region_name,
    d.median_group AS median_group,
    d.list_ym AS month
FROM data_clean d
WHERE d.list_ym IS NOT NULL
  AND d.cf_region_id_new IS NOT NULL
  AND d.median_group IS NOT NULL
GROUP BY d.cf_region_id_new, d.cf_region_name_new, d.median_group, d.list_ym
ORDER BY d.cf_region_id_new, d.median_group, d.list_ym
LIMIT {$limit} OFFSET {$offset}
SQL;
            $countSql = "SELECT COUNT(*) AS cnt FROM ({$groupSql}) AS grp";
            $countRow = DB::selectOne($countSql);
            $processed = (int) ($countRow->cnt ?? 0);

            if ($processed > 0) {
                $insertSql = <<<SQL
INSERT INTO data_clean_stats (
    scope, new_region_id, new_region_name, new_ward_id, new_ward_name, type,
    median_group, category, month, avg_price_m2, median_price_m2,
    min_price_m2, max_price_m2, total_rows, trimmed_rows, converted_at
)
SELECT
    'region' AS scope,
    g.region_id,
    g.region_name,
    NULL AS new_ward_id,
    NULL AS new_ward_name,
    NULL AS type,
    g.median_group,
    NULL AS category,
    g.month,
    a.avg_price_m2,
    a.median_price_m2,
    a.min_price_m2,
    a.max_price_m2,
    COALESCE(a.total_rows, 0) AS total_rows,
    COALESCE(a.trimmed_rows, 0) AS trimmed_rows,
    NOW()
FROM ({$groupSql}) AS g
LEFT JOIN (
    SELECT
        region_id,
        median_group,
        month,
        MAX(avg_price) AS avg_price_m2,
        MAX(cnt) AS total_rows,
        MAX(trimmed_cnt) AS trimmed_rows,
        MIN(price_m2_vnd) AS min_price_m2,
        MAX(price_m2_vnd) AS max_price_m2,
        AVG(CASE
            WHEN trimmed_cnt % 2 = 1 AND rn = cut + (trimmed_cnt + 1) / 2 THEN price_m2_vnd
            WHEN trimmed_cnt % 2 = 0 AND (rn = cut + trimmed_cnt / 2 OR rn = cut + trimmed_cnt / 2 + 1) THEN price_m2_vnd
        END) AS median_price_m2
    FROM (
        SELECT
            ranked.*,
            FLOOR(cnt * 0.1) AS cut,
            cnt - (2 * FLOOR(cnt * 0.1)) AS trimmed_cnt
        FROM (
            SELECT
                g2.region_id,
                g2.median_group,
                g2.month,
                d.price_m2_vnd,
                ROW_NUMBER() OVER (PARTITION BY g2.region_id, g2.median_group, g2.month ORDER BY d.price_m2_vnd) AS rn,
                COUNT(*) OVER (PARTITION BY g2.region_id, g2.median_group, g2.month) AS cnt,
                AVG(d.price_m2_vnd) OVER (PARTITION BY g2.region_id, g2.median_group, g2.month) AS avg_price
            FROM data_clean d
            JOIN ({$groupSql}) AS g2
              ON g2.region_id = d.cf_region_id_new
             AND g2.median_group = d.median_group
             AND g2.month = d.list_ym
            WHERE d.price_m2_vnd IS NOT NULL
              AND d.price_m2_vnd > 0
        ) ranked
    ) trim
    WHERE cnt > 0 AND rn > cut AND rn <= cnt - cut
    GROUP BY region_id, median_group, month
) a
  ON a.region_id = g.region_id
 AND a.median_group = g.median_group
 AND a.month = g.month
ON DUPLICATE KEY UPDATE
    new_region_name = VALUES(new_region_name),
    avg_price_m2 = VALUES(avg_price_m2),
    median_price_m2 = VALUES(median_price_m2),
    min_price_m2 = VALUES(min_price_m2),
    max_price_m2 = VALUES(max_price_m2),
    total_rows = VALUES(total_rows),
    trimmed_rows = VALUES(trimmed_rows),
    converted_at = VALUES(converted_at)
SQL;
                DB::statement($insertSql);
            }
        } else {
            // region_total scope: group by region + month only (toàn tỉnh, không phân theo median_group)
            $groupSql = <<<SQL
SELECT
    d.cf_region_id_new AS region_id,
    d.cf_region_name_new AS region_name,
    d.list_ym AS month
FROM data_clean d
WHERE d.list_ym IS NOT NULL
  AND d.cf_region_id_new IS NOT NULL
GROUP BY d.cf_region_id_new, d.cf_region_name_new, d.list_ym
ORDER BY d.cf_region_id_new, d.list_ym
LIMIT {$limit} OFFSET {$offset}
SQL;
            $countSql = "SELECT COUNT(*) AS cnt FROM ({$groupSql}) AS grp";
            $countRow = DB::selectOne($countSql);
            $processed = (int) ($countRow->cnt ?? 0);

            if ($processed > 0) {
                $insertSql = <<<SQL
INSERT INTO data_clean_stats (
    scope, new_region_id, new_region_name, new_ward_id, new_ward_name, type,
    median_group, category, month, avg_price_m2, median_price_m2,
    min_price_m2, max_price_m2, total_rows, trimmed_rows, converted_at
)
SELECT
    'region_total' AS scope,
    g.region_id,
    g.region_name,
    NULL AS new_ward_id,
    NULL AS new_ward_name,
    NULL AS type,
    NULL AS median_group,
    NULL AS category,
    g.month,
    a.avg_price_m2,
    a.median_price_m2,
    a.min_price_m2,
    a.max_price_m2,
    COALESCE(a.total_rows, 0) AS total_rows,
    COALESCE(a.trimmed_rows, 0) AS trimmed_rows,
    NOW()
FROM ({$groupSql}) AS g
LEFT JOIN (
    SELECT
        region_id,
        month,
        MAX(avg_price) AS avg_price_m2,
        MAX(cnt) AS total_rows,
        MAX(trimmed_cnt) AS trimmed_rows,
        MIN(price_m2_vnd) AS min_price_m2,
        MAX(price_m2_vnd) AS max_price_m2,
        AVG(CASE
            WHEN trimmed_cnt % 2 = 1 AND rn = cut + (trimmed_cnt + 1) / 2 THEN price_m2_vnd
            WHEN trimmed_cnt % 2 = 0 AND (rn = cut + trimmed_cnt / 2 OR rn = cut + trimmed_cnt / 2 + 1) THEN price_m2_vnd
        END) AS median_price_m2
    FROM (
        SELECT
            ranked.*,
            FLOOR(cnt * 0.1) AS cut,
            cnt - (2 * FLOOR(cnt * 0.1)) AS trimmed_cnt
        FROM (
            SELECT
                g2.region_id,
                g2.month,
                d.price_m2_vnd,
                ROW_NUMBER() OVER (PARTITION BY g2.region_id, g2.month ORDER BY d.price_m2_vnd) AS rn,
                COUNT(*) OVER (PARTITION BY g2.region_id, g2.month) AS cnt,
                AVG(d.price_m2_vnd) OVER (PARTITION BY g2.region_id, g2.month) AS avg_price
            FROM data_clean d
            JOIN ({$groupSql}) AS g2
              ON g2.region_id = d.cf_region_id_new
             AND g2.month = d.list_ym
            WHERE d.price_m2_vnd IS NOT NULL
              AND d.price_m2_vnd > 0
        ) ranked
    ) trim
    WHERE cnt > 0 AND rn > cut AND rn <= cnt - cut
    GROUP BY region_id, month
) a
  ON a.region_id = g.region_id
 AND a.month = g.month
ON DUPLICATE KEY UPDATE
    new_region_name = VALUES(new_region_name),
    avg_price_m2 = VALUES(avg_price_m2),
    median_price_m2 = VALUES(median_price_m2),
    min_price_m2 = VALUES(min_price_m2),
    max_price_m2 = VALUES(max_price_m2),
    total_rows = VALUES(total_rows),
    trimmed_rows = VALUES(trimmed_rows),
    converted_at = VALUES(converted_at)
SQL;
                DB::statement($insertSql);
            }
        }

        $doneScope = $processed < $limit;
        $nextScope = $scope;
        $nextOffset = $offset + $processed;
        $done = false;

        if ($doneScope && $scope === 'ward') {
            $nextScope = 'region';
            $nextOffset = 0;
        } elseif ($doneScope && $scope === 'region') {
            $nextScope = 'region_total';
            $nextOffset = 0;
        } elseif ($doneScope && $scope === 'region_total') {
            $done = true;
        }

        return response()->json([
                        'offset' => $offset,
            'processed' => $processed,
            'inserted' => $processed,
            'done_scope' => $doneScope,
            'next_scope' => $nextScope,
            'next_offset' => $nextOffset,
            'done' => $done,
        ]);
    }



    public function demo(Request $request)
    {
        $regionId = $request->query('region_id');
        $wardId = $request->query('ward_id');
        $month = $request->query('month');
        $medianGroup = $request->query('median_group');
        $scope = 'ward';

        $medianGroupLabels = [
            1 => 'Nha gan lien dat',
            2 => 'Can ho',
            3 => 'Dat',
            4 => 'Cho thue',
        ];

        $regions = DB::table('data_clean_stats')
            ->selectRaw('new_region_id AS region_id, COALESCE(new_region_name, CAST(new_region_id AS CHAR)) AS region_name')
            ->where('scope', $scope)
            ->whereNotNull('new_region_id')
            ->distinct()
            ->orderBy('region_name')
            ->get();

        $wards = collect();
        if (!empty($regionId)) {
            $wards = DB::table('data_clean_stats')
                ->selectRaw('new_ward_id AS ward_id, COALESCE(new_ward_name, CAST(new_ward_id AS CHAR)) AS ward_name')
                ->where('scope', 'ward')
                ->where('new_region_id', (int) $regionId)
                ->whereNotNull('new_ward_id')
                ->distinct()
                ->orderBy('ward_name')
                ->get();
        }

        $months = DB::table('data_clean_stats')
            ->where('scope', $scope)
            ->whereNotNull('month')
            ->distinct()
            ->orderBy('month')
            ->pluck('month')
            ->values();

        $base = DB::table('data_clean_stats')
            ->select('month', 'median_group', 'median_price_m2', 'avg_price_m2', 'min_price_m2', 'max_price_m2', 'trimmed_rows')
            ->where('scope', $scope)
            ->whereNotNull('month')
            ->whereNotNull('median_group');

        if (!empty($regionId)) {
            $base->where('new_region_id', (int) $regionId);
        }
        if (!empty($wardId) && $scope === 'ward') {
            $base->where('new_ward_id', (int) $wardId);
        }
        if (!empty($month)) {
            $base->where('month', $month);
        }
        if (!empty($medianGroup)) {
            $base->where('median_group', (int) $medianGroup);
        }

        $rows = (clone $base)
            ->orderBy('month')
            ->get();

        $monthList = $rows->pluck('month')->unique()->values()->all();
        $series = [];
        foreach ($medianGroupLabels as $group => $label) {
            $series[$group] = [
                'label' => $label,
                'values' => array_fill(0, count($monthList), null),
                'avg' => array_fill(0, count($monthList), null),
                'min' => array_fill(0, count($monthList), null),
                'max' => array_fill(0, count($monthList), null),
                'trimmed' => array_fill(0, count($monthList), null),
            ];
        }

        $monthIndex = array_flip($monthList);
        foreach ($rows as $row) {
            $group = (int) $row->median_group;
            if (!isset($series[$group])) {
                continue;
            }
            $idx = $monthIndex[$row->month] ?? null;
            if ($idx === null) {
                continue;
            }
            $series[$group]['values'][$idx] = $row->median_price_m2 !== null ? (float) $row->median_price_m2 : null;
            $series[$group]['avg'][$idx] = $row->avg_price_m2 !== null ? (float) $row->avg_price_m2 : null;
            $series[$group]['min'][$idx] = $row->min_price_m2 !== null ? (float) $row->min_price_m2 : null;
            $series[$group]['max'][$idx] = $row->max_price_m2 !== null ? (float) $row->max_price_m2 : null;
            $series[$group]['trimmed'][$idx] = $row->trimmed_rows !== null ? (int) $row->trimmed_rows : null;
        }

        return view('demo', [
            'regions' => $regions,
            'wards' => $wards,
            'filters' => [
                'region_id' => $regionId,
                'ward_id' => $wardId,
                'month' => $month,
                'median_group' => $medianGroup,
                            ],
            'months' => $months,
            'month_list' => $monthList,
            'medianGroupLabels' => $medianGroupLabels,
            'series' => $series,
        ]);
    }


    public function index(Request $request)
    {
        $regionId = $request->query('region_id');
        $wardId = $request->query('ward_id');
        $type = $request->query('type');
        $category = $request->query('category');
        $month = $request->query('month');
        $typeLabels = [
            's' => 'ban',
            'u' => 'thue',
        ];
        $categoryLabels = [
            1000 => 'Bat dong san (Chung)',
            1010 => 'Can ho/Chung cu',
            1020 => 'Nha o/Nha pho, biet thu, nha hem',
            1030 => 'Dat/Dat nen, dat tho cu',
            1040 => 'Van phong/Mat bang/Cho thue kinh doanh',
            1050 => 'Phong tro/Cho thue phong tro, o ghep',
        ];
        $limit = (int) $request->query('limit', 200);
        if ($limit < 10) {
            $limit = 10;
        } elseif ($limit > 10000) {
            $limit = 10000;
        }

                $regions = DB::table('data_clean')
            ->selectRaw('cf_region_id_new AS city_id, COALESCE(cf_region_name_new, CAST(cf_region_id_new AS CHAR)) AS city_title_news')
            ->whereNotNull('cf_region_id_new')
            ->distinct()
            ->orderBy('city_title_news')
            ->get();

                $wards = collect();
        if ($regionId) {
            $wards = DB::table('data_clean')
                ->selectRaw('cf_ward_id_new AS city_id, COALESCE(cf_ward_name_new, CAST(cf_ward_id_new AS CHAR)) AS new_name')
                ->where('cf_region_id_new', $regionId)
                ->whereNotNull('cf_ward_id_new')
                ->distinct()
                ->orderBy('new_name')
                ->get();
        }

        $months = DB::table('data_clean')
            ->select(DB::raw("list_ym as ym"))
            ->whereNotNull('list_ym')
            ->distinct()
            ->orderBy('ym', 'desc')
            ->pluck('ym');        $base = DB::table('data_clean as d');
        if ($wardId) {
            $base->where('d.cf_ward_id_new', (int) $wardId);
        } elseif ($regionId) {
            $base->where('d.cf_region_id_new', (int) $regionId);
        }

        $optionBase = clone $base;
        if (!empty($month)) {
            $optionBase->whereRaw("d.list_ym = ?", [$month]);
        }
        $types = (clone $optionBase)
            ->whereNotNull('d.type')
            ->distinct()
            ->orderBy('d.type')
            ->pluck('d.type');
        $categories = (clone $optionBase)
            ->whereNotNull('d.category')
            ->distinct()
            ->orderBy('d.category')
            ->pluck('d.category');

        if (!empty($type)) {
            $base->where('d.type', $type);
        }
        if (!empty($category)) {
            $base->where('d.category', $category);
        }
        if (!empty($month)) {
            $base->whereRaw("d.list_ym = ?", [$month]);
        }

        $rows = (clone $base)
            ->select(
                'd.ad_id',
                'd.list_id',
                'd.list_time',
                'd.orig_list_time',
                DB::raw("d.list_ym AS list_month"),
                'd.region_v2',
                'd.area_v2',
                'd.ward',
                'd.street_name',
                'd.street_number',
                'd.unique_street_id',
                'd.category',
                'd.size',
                'd.price',
                'd.type',
                'd.time_crawl',
                'd.price_m2_vnd'
            )
            ->orderBy('d.list_time', 'desc')
            ->limit($limit)
            ->get();

        $total = (clone $base)->count();
        $avg = (clone $base)->whereNotNull('d.price_m2_vnd')->avg('d.price_m2_vnd');

        $medianTrim = null;

        $totalPrice = (clone $base)
            ->whereNotNull('d.price_m2_vnd')
            ->where('d.price_m2_vnd', '>', 0)
            ->count();

        if ($totalPrice > 0) {
            $cut = (int) floor($totalPrice * 0.1);
            $trimmed = $totalPrice - ($cut * 2);

            if ($trimmed > 0) {
                $order = (clone $base)
                    ->whereNotNull('d.price_m2_vnd')
                    ->where('d.price_m2_vnd', '>', 0)
                    ->orderBy('d.price_m2_vnd');

                if ($trimmed % 2 === 1) {
                    $offset = $cut + intdiv($trimmed, 2);
                    $medianTrim = $order->offset($offset)->value('d.price_m2_vnd');
                } else {
                    $offset = $cut + (int) ($trimmed / 2 - 1);
                    $vals = $order->offset($offset)->limit(2)->pluck('d.price_m2_vnd')->values();

                    if ($vals->count() === 2) {
                        $medianTrim = ((float) $vals[0] + (float) $vals[1]) / 2;
                    } elseif ($vals->count() === 1) {
                        $medianTrim = (float) $vals[0];
                    }
                }
            }
        }

        return view('data_clean', [
            'regions' => $regions,
            'wards' => $wards,
            'months' => $months,
            'rows' => $rows,
            'total' => $total,
            'avg' => $avg,
            'medianTrim' => $medianTrim,
            'typeLabels' => $typeLabels,
            'categoryLabels' => $categoryLabels,
            'types' => $types,
            'categories' => $categories,
            'filters' => [
                'region_id' => $regionId,
                'ward_id' => $wardId,
                'type' => $type,
                'category' => $category,
                'month' => $month,
                'limit' => $limit,
            ],
        ]);
    }
}
 
