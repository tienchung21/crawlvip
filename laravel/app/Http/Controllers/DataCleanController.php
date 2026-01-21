<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;

class DataCleanController extends Controller
{
    // CONFIGURATION: Target Table
    const STATS_TABLE = 'data_clean_stats';

    private function ensureStatsTable(): void
    {
        $table = self::STATS_TABLE;
        
        DB::statement("
            CREATE TABLE IF NOT EXISTS {$table} (
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

        $this->ensureColumn($table, 'median_group', 'TINYINT NULL');
        $this->ensureColumn($table, 'min_price_m2', 'DECIMAL(20,2) NULL');
        $this->ensureColumn($table, 'max_price_m2', 'DECIMAL(20,2) NULL');

        $oldKeys = ['uq_ward', 'uq_region', 'uq_ward_mg', 'uq_region_mg'];
        foreach ($oldKeys as $keyName) {
            $this->dropIndexIfExists($table, $keyName);
        }

        $this->addIndexIfNotExists($table, 'uq_ward_scope', [], true, 'scope, new_region_id, new_ward_id, type, median_group, month');
    }


    private function ensureDataCleanIndexes(): void
    {
        $this->addIndexIfNotExists('data_clean', 'idx_cafeland_new_id', ['cafeland_new_id']);
        $this->addIndexIfNotExists('data_clean', 'idx_cafeland_new_parent_id', ['cafeland_new_parent_id']);
        $this->addIndexIfNotExists('data_clean', 'idx_dc_new_stats', ['cafeland_new_id', 'type', 'median_group', 'list_ym', 'price_m2_vnd']);
        
        $this->ensureColumn('data_clean', 'median_flag', 'TINYINT(1) NULL DEFAULT NULL');
        $this->addIndexIfNotExists('data_clean', 'idx_median_flag', ['median_flag']);
    }

    private function ensureColumn($table, $column, $definition)
    {
        $exists = DB::selectOne("SELECT 1 FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ? LIMIT 1", [$table, $column]);
        if (!$exists) {
            DB::statement("ALTER TABLE {$table} ADD COLUMN {$column} {$definition}");
        }
    }

    private function dropIndexIfExists($table, $index)
    {
        $exists = DB::selectOne("SELECT 1 FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ? LIMIT 1", [$table, $index]);
        if ($exists) {
            DB::statement("ALTER TABLE {$table} DROP INDEX {$index}");
        }
    }

    private function addIndexIfNotExists($table, $indexName, $columns, $unique = false, $rawColumns = null)
    {
        $exists = DB::selectOne("SELECT 1 FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ? LIMIT 1", [$table, $indexName]);
        if (!$exists) {
            $colStr = $rawColumns ?: implode(', ', $columns);
            $type = $unique ? "UNIQUE KEY" : "INDEX";
            DB::statement("ALTER TABLE {$table} ADD {$type} {$indexName} ({$colStr})");
        }
    }

    public function convert(Request $request)
    {
        return $this->convertBatch($request);
    }
    
    public function convertBatch(Request $request) {
        $this->ensureStatsTable();
        $this->ensureDataCleanIndexes();
        set_time_limit(0);

        $scope = $request->input('scope', 'ward');
        $offset = (int) $request->input('offset', 0);
        $limit = (int) $request->input('limit', 500);
        $monthParam = $request->input('month'); // New input
        
        if ($limit < 50) $limit = 50;
        if ($limit > 2000) $limit = 2000;
        if ($offset < 0) $offset = 0;

        $targetTable = self::STATS_TABLE;

        // Build extra condition for month
        $monthCondition = "";
        if (!empty($monthParam)) {
            // Basic SQL injection protection: ensure it looks like a month string or minimal sanitization
            // Assuming YYYY-MM format
            $monthParam = addslashes($monthParam); 
            $monthCondition = "AND d.list_ym = '{$monthParam}'";
        }

        $processed = 0;
        if ($scope === 'ward') {
            $groupSql = <<<SQL
SELECT
    prov.city_id AS region_id,
    prov.city_title AS region_name,
    d.cafeland_new_id AS ward_id,
    d.cafeland_new_name AS ward_name,
    d.median_group AS median_group,
    d.list_ym AS month
FROM data_clean d
JOIN transaction_city dist ON d.cafeland_new_parent_id = dist.city_id
JOIN transaction_city prov ON dist.city_parent_id = prov.city_id
WHERE d.list_ym IS NOT NULL
  AND d.median_group IS NOT NULL
  AND d.cafeland_new_id IS NOT NULL
  AND d.cafeland_new_parent_id IS NOT NULL
  {$monthCondition}
GROUP BY prov.city_id, prov.city_title, d.cafeland_new_id, d.cafeland_new_name, d.median_group, d.list_ym
ORDER BY prov.city_id, d.cafeland_new_id, d.median_group, d.list_ym
LIMIT {$limit} OFFSET {$offset}
SQL;
            $countSql = "SELECT COUNT(*) AS cnt FROM ({$groupSql}) AS grp";
            $countRow = DB::selectOne($countSql);
            $processed = (int) ($countRow->cnt ?? 0);

            if ($processed > 0) {
                // ... Insert logic ...
                // Note: The JOIN ({$groupSql}) in nested queries will automatically carry the month condition 
                // because we interpolated {$monthCondition} into $groupSql above.
                
                $insertSql = <<<SQL
INSERT INTO {$targetTable} (
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
            JOIN transaction_city dist ON d.cafeland_new_parent_id = dist.city_id
            JOIN transaction_city prov ON dist.city_parent_id = prov.city_id
            JOIN ({$groupSql}) AS g2
              ON g2.region_id = prov.city_id
             AND g2.ward_id = d.cafeland_new_id
             AND g2.median_group = d.median_group
             AND g2.month = d.list_ym
            WHERE d.price_m2_vnd IS NOT NULL
              AND d.price_m2_vnd > 0
              AND d.cafeland_new_id IS NOT NULL 
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
                
                $updateFlagSql = <<<SQL
UPDATE data_clean d
JOIN transaction_city dist ON d.cafeland_new_parent_id = dist.city_id
JOIN transaction_city prov ON dist.city_parent_id = prov.city_id
JOIN (
    SELECT ranked.ad_id
    FROM (
        SELECT
            d2.ad_id,
            ROW_NUMBER() OVER (PARTITION BY g2.region_id, g2.ward_id, g2.median_group, g2.month ORDER BY d2.price_m2_vnd) AS rn,
            COUNT(*) OVER (PARTITION BY g2.region_id, g2.ward_id, g2.median_group, g2.month) AS cnt
        FROM data_clean d2
        JOIN transaction_city dist2 ON d2.cafeland_new_parent_id = dist2.city_id
        JOIN transaction_city prov2 ON dist2.city_parent_id = prov2.city_id
        JOIN ({$groupSql}) AS g2
          ON g2.region_id = prov2.city_id
         AND g2.ward_id = d2.cafeland_new_id
         AND g2.median_group = d2.median_group
         AND g2.month = d2.list_ym
        WHERE d2.price_m2_vnd IS NOT NULL
          AND d2.price_m2_vnd > 0
          AND d2.cafeland_new_id IS NOT NULL
    ) ranked
    WHERE ranked.rn > FLOOR(ranked.cnt * 0.1) 
      AND ranked.rn <= ranked.cnt - FLOOR(ranked.cnt * 0.1)
) flag_data ON flag_data.ad_id = d.ad_id
SET d.median_flag = 1
SQL;
                DB::statement($updateFlagSql);
            }
        } elseif ($scope === 'region') {
             $groupSql = <<<SQL
SELECT
    prov.city_id AS region_id,
    prov.city_title AS region_name,
    d.median_group AS median_group,
    d.list_ym AS month
FROM data_clean d
JOIN transaction_city dist ON d.cafeland_new_parent_id = dist.city_id
JOIN transaction_city prov ON dist.city_parent_id = prov.city_id
WHERE d.list_ym IS NOT NULL
  AND d.median_group IS NOT NULL
  AND d.cafeland_new_parent_id IS NOT NULL
  {$monthCondition}
GROUP BY prov.city_id, prov.city_title, d.median_group, d.list_ym
ORDER BY prov.city_id, d.median_group, d.list_ym
LIMIT {$limit} OFFSET {$offset}
SQL;
            $countSql = "SELECT COUNT(*) AS cnt FROM ({$groupSql}) AS grp";
            $countRow = DB::selectOne($countSql);
            $processed = (int) ($countRow->cnt ?? 0);

            if ($processed > 0) {
                 $insertSql = <<<SQL
INSERT INTO {$targetTable} (
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
            JOIN transaction_city dist ON d.cafeland_new_parent_id = dist.city_id
            JOIN transaction_city prov ON dist.city_parent_id = prov.city_id
            JOIN ({$groupSql}) AS g2
              ON g2.region_id = prov.city_id
             AND g2.median_group = d.median_group
             AND g2.month = d.list_ym
            WHERE d.price_m2_vnd IS NOT NULL
              AND d.price_m2_vnd > 0
              AND d.cafeland_new_parent_id IS NOT NULL
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
        
        } elseif ($scope === 'region_total') {
            $groupSql = <<<SQL
SELECT
    prov.city_id AS region_id,
    prov.city_title AS region_name,
    d.list_ym AS month
FROM data_clean d
JOIN transaction_city dist ON d.cafeland_new_parent_id = dist.city_id
JOIN transaction_city prov ON dist.city_parent_id = prov.city_id
WHERE d.list_ym IS NOT NULL
  AND d.cafeland_new_parent_id IS NOT NULL
  {$monthCondition}
GROUP BY prov.city_id, prov.city_title, d.list_ym
ORDER BY prov.city_id, d.list_ym
LIMIT {$limit} OFFSET {$offset}
SQL;
             $countSql = "SELECT COUNT(*) AS cnt FROM ({$groupSql}) AS grp";
            $countRow = DB::selectOne($countSql);
            $processed = (int) ($countRow->cnt ?? 0);

            if ($processed > 0) {
                 $insertSql = <<<SQL
INSERT INTO {$targetTable} (
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
            JOIN transaction_city dist ON d.cafeland_new_parent_id = dist.city_id
            JOIN transaction_city prov ON dist.city_parent_id = prov.city_id
            JOIN ({$groupSql}) AS g2
              ON g2.region_id = prov.city_id
             AND g2.month = d.list_ym
            WHERE d.price_m2_vnd IS NOT NULL
              AND d.price_m2_vnd > 0
              AND d.cafeland_new_parent_id IS NOT NULL
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

        // Dropdowns from STATS_TABLE (Showing available New Provinces/Wards)
        $regions = DB::table(self::STATS_TABLE)
            ->select('new_region_id as region_id', 'new_region_name as region_name')
            ->distinct()
            ->whereNotNull('new_region_id')
            ->orderBy('new_region_name')
            ->get();

        $wards = collect();
        if ($regionId) {
             $wards = DB::table(self::STATS_TABLE)
                ->select('new_ward_id as ward_id', 'new_ward_name as ward_name')
                ->distinct()
                ->where('new_region_id', (int)$regionId)
                ->whereNotNull('new_ward_id')
                ->orderBy('new_ward_name')
                ->get();
        }
        
        // Month list from stats table
        $months = DB::table(self::STATS_TABLE)
            ->select('month')
            ->distinct()
            ->orderBy('month', 'desc')
            ->pluck('month');

        // Base Query
        $base = DB::table(self::STATS_TABLE . ' as s');
        
        if ($wardId) {
            $base->where('s.scope', 'ward')->where('s.new_ward_id', (int)$wardId);
        } elseif ($regionId) {
            $base->where('s.scope', 'region')->where('s.new_region_id', (int)$regionId);
        } else {
             $base->where('s.scope', 'region'); // Default scope
        }

        if ($medianGroup) {
            $base->where('s.median_group', (int)$medianGroup);
        }
        if ($month) {
            $base->where('s.month', $month);
        }

        // Get Series Data
        // Group by Median Group & Month
        $rows = $base->get();

        // Transform for Chart
        // Series: Key = Median Group. Values = Array of prices matching month_list.
        // We need a complete month list for the X-axis.
        $monthList = $months->sort()->values()->toArray(); // Ascending
        
        $series = [];
        $medianGroupLabels = [1=>'Nha gan lien', 2=>'Can ho', 3=>'Dat', 4=>'Cho thue'];
        
        // Init series
        foreach ($medianGroupLabels as $g => $lbl) {
            $series[$g] = [
                'label' => $lbl,
                'values' => array_fill(0, count($monthList), null),
                'avg' => array_fill(0, count($monthList), null),
                'min' => array_fill(0, count($monthList), null),
                'max' => array_fill(0, count($monthList), null),
                'trimmed' => array_fill(0, count($monthList), null),
            ];
        }
        
        foreach ($rows as $r) {
            $g = $r->median_group;
            if (!isset($series[$g])) continue;
            
            $mIdx = array_search($r->month, $monthList);
            if ($mIdx !== false) {
                $series[$g]['values'][$mIdx] = $r->median_price_m2;
                $series[$g]['avg'][$mIdx] = $r->avg_price_m2;
                $series[$g]['min'][$mIdx] = $r->min_price_m2;
                $series[$g]['max'][$mIdx] = $r->max_price_m2;
                $series[$g]['trimmed'][$mIdx] = $r->trimmed_rows;
            }
        }

        // Small rows (trimmed < 10?)
        // Assuming user wants to see rows with small sample size in separate tab
        // Filter rows where total_rows < 10
        $smallRows = (clone $base)
            ->where('total_rows', '<', 10)
            ->orderBy('month', 'desc')
            ->limit(100)
            ->get();
        
        return view('demo', [
            'regions' => $regions,
            'wards' => $wards,
            'months' => $months,
            'medianGroupLabels' => $medianGroupLabels,
            'filters' => [
                'region_id' => $regionId,
                'ward_id' => $wardId,
                'month' => $month,
                'median_group' => $medianGroup
            ],
            'month_list' => $monthList,
            'series' => $series,
            'smallRows' => $smallRows
        ]);
    }
    
    public function index(Request $request) { /* Omitted for brevity */
        return $this->originalIndex($request); // Placeholder, assume I'm keeping the original index logic I wrote before
    }
    
    // Helper to keep original index logic
    private function originalIndex(Request $request) {
        $regionId = $request->query('region_id');
        $wardId = $request->query('ward_id');
        $type = $request->query('type');
        $category = $request->query('category');
        $month = $request->query('month');
        $limit = (int) $request->query('limit', 200);
        if ($limit < 10) $limit = 10;
        if ($limit > 10000) $limit = 10000;

        $regions = DB::table('transaction_city')
            ->select('city_id', 'city_title as city_title_news')
            ->where('city_parent_id', 0)
            ->orderBy('city_title')
            ->get();

        $wards = collect();
        if ($regionId) {
             $wards = DB::table('transaction_city as w')
                ->join('transaction_city as dist', 'w.city_parent_id', '=', 'dist.city_id')
                ->where('dist.city_parent_id', $regionId)
                ->select('w.city_id', 'w.city_title as new_name')
                ->orderBy('w.city_title')
                ->get();
        }
        
        $months = DB::table('data_clean')
            ->select(DB::raw("list_ym as ym"))
            ->whereNotNull('list_ym')
            ->distinct()
            ->orderBy('ym', 'desc')
            ->pluck('ym');

        $base = DB::table('data_clean as d');
        
        if ($wardId) {
            $base->where('d.cafeland_id', (int) $wardId);
        } elseif ($regionId) {
            $base->join('transaction_city as w', 'd.cafeland_id', '=', 'w.city_id')
                 ->join('transaction_city as dist', 'w.city_parent_id', '=', 'dist.city_id')
                 ->where('dist.city_parent_id', (int) $regionId);
        }

        if (!empty($month)) {
            $base->whereRaw("d.list_ym = ?", [$month]);
        }
        
         $types = (clone $base)
            ->whereNotNull('d.type')
            ->distinct()
            ->orderBy('d.type')
            ->pluck('d.type');
        $categories = (clone $base)
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

        $total = (clone $base)->count();
        $avg = (clone $base)->whereNotNull('d.price_m2_vnd')->avg('d.price_m2_vnd');
        
         $selects = [
            'd.ad_id', 'd.list_id', 'd.list_time', 'd.orig_list_time',
            DB::raw("d.list_ym AS list_month"),
            'd.region_v2', 'd.area_v2', 'd.ward',
            'd.street_name', 'd.street_number', 'd.unique_street_id',
            'd.category', 'd.size', 'd.price', 'd.type', 'd.time_crawl', 'd.price_m2_vnd',
            'd.cafeland_id', 'd.cafeland_new_id', 'd.cafeland_new_name'
        ];
        
        $rowsBase = (clone $base)
            ->leftJoin('transaction_city as w_disp', 'd.cafeland_id', '=', 'w_disp.city_id')
            ->addSelect($selects)
            ->addSelect('w_disp.city_title as cf_ward_name_new');

        $rows = $rowsBase
            ->orderBy('d.list_time', 'desc')
            ->limit($limit)
            ->get();
            
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
        
        $typeLabels = ['s' => 'ban', 'u' => 'thue'];
        $categoryLabels = [
            1000 => 'Bat dong san (Chung)',
            1010 => 'Can ho/Chung cu',
            1020 => 'Nha o/Nha pho, biet thu, nha hem',
            1030 => 'Dat/Dat nen, dat tho cu',
            1040 => 'Van phong/Mat bang/Cho thue kinh doanh',
            1050 => 'Phong tro/Cho thue phong tro, o ghep',
        ];

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

    public function histogram(Request $request) {
        $medianGroup = $request->query('median_group');
        $category = $request->query('category');
        $type = $request->query('type');
        
        $base = DB::table('data_clean as d')
            ->whereNotNull('d.price_m2_vnd')
            ->where('d.price_m2_vnd', '>', 0);

        if (!empty($medianGroup)) $base->where('d.median_group', (int) $medianGroup);
        if (!empty($category)) $base->where('d.category', (int) $category);
        if (!empty($type)) $base->where('d.type', $type);
        
        $totalCount = (clone $base)->count();
        $minPrice = (clone $base)->min('d.price_m2_vnd');
        $maxPrice = (clone $base)->max('d.price_m2_vnd');
        
        $labels = []; $counts = []; $avgPrices = []; $binSize = null;
        $binCount = 10;
        
         if ($minPrice !== null && $maxPrice !== null) {
            $range = $maxPrice - $minPrice;
            $binSize = $range > 0 ? (int) ceil($range / $binCount) : 1;
            if ($binSize < 1) $binSize = 1;

            $rows = (clone $base)
                ->selectRaw(
                    'LEAST(?, FLOOR((d.price_m2_vnd - ?) / ?)) AS bucket, COUNT(*) AS cnt, AVG(d.price_m2_vnd) AS avg_price',
                    [$binCount - 1, $minPrice, $binSize]
                )
                ->groupBy('bucket')
                ->orderBy('bucket')
                ->get();
                
            foreach ($rows as $row) {
                $bucket = (int) $row->bucket;
                $start = $minPrice + ($bucket * $binSize);
                $end = $bucket === $binCount - 1 ? $maxPrice : ($start + $binSize);
                $labels[] = (string) $start . ' - ' . (string) $end;
                $counts[] = (int) $row->cnt;
                $avgPrices[] = $row->avg_price !== null ? (float) $row->avg_price : null;
            }
        }
        
        return view('histogram', [
            'filters' => ['median_group'=>$medianGroup, 'category'=>$category, 'type'=>$type],
            'medianGroupLabels' => [1=>'Nha gan lien',2=>'Can ho',3=>'Dat',4=>'Cho thue'],
            'categories' => DB::table('data_clean')->whereNotNull('category')->distinct()->orderBy('category')->pluck('category'),
            'types' => DB::table('data_clean')->whereNotNull('type')->distinct()->orderBy('type')->pluck('type'),
            'labels' => $labels,
            'counts' => $counts,
            'avgPrices' => $avgPrices,
            'binSize' => $binSize,
            'totalCount' => $totalCount,
        ]);
    }
}
