<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;

class DataMedianController extends Controller
{
    const STATS_TABLE = 'data_median';
    private static $schemaChecked = false;

    // ─── Schema helpers ────────────────────────────────────────────────

    private function ensureMedianTable(): void
    {
        $t = self::STATS_TABLE;

        DB::statement("
            CREATE TABLE IF NOT EXISTS {$t} (
                id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                scope VARCHAR(16) NOT NULL,
                new_region_id INT NULL,
                new_ward_id INT NULL,
                street_id INT NULL,
                project_id INT NULL,
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
                INDEX idx_scope_street (scope, street_id),
                INDEX idx_scope_project (scope, project_id),
                INDEX idx_month (month)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ");

        // ensure columns added later
        $this->ensureColumn($t, 'street_id', 'INT NULL AFTER new_ward_id');
        $this->ensureColumn($t, 'project_id', 'INT NULL AFTER new_ward_id');
        $this->ensureColumn($t, 'min_price_m2', 'DECIMAL(20,2) NULL');
        $this->ensureColumn($t, 'max_price_m2', 'DECIMAL(20,2) NULL');

        $this->dropIndexIfExists($t, 'uq_scope_key');
        $this->addIndexIfNotExists($t, 'uq_scope_key', [], true,
            'scope, new_region_id, new_ward_id, street_id, project_id, type, median_group, month');
    }

    private function ensureDataCleanV1Indexes(): void
    {
        // Single-column indexes kept for other code paths
        // idx_dm_cf_ward, idx_dm_cf_province, idx_dm_std_date, idx_dm_price_m2 — removed (covered by composites)

        $this->ensureColumn('data_clean_v1', 'median_flag', 'TINYINT(1) NULL DEFAULT NULL');
        // Note: idx_dm_median_flag removed — covered by idx_dm_flag_ward/region/project
        $this->dropIndexIfExists('data_clean_v1', 'idx_dm_median_flag');

        // ── std_month column (materialized month for fast GROUP BY) ──
        // Do not backfill here. Large UPDATEs inside a web request cause lock pressure
        // and make the convert endpoint return HTML 500 instead of JSON.
        $this->ensureColumn('data_clean_v1', 'std_month', 'VARCHAR(7) NULL');
        $this->addIndexIfNotExists('data_clean_v1', 'idx_dm_std_month', ['std_month']);

        // ── Composite indexes for each scope (median_flag leading for fast filter) ──
        $this->addIndexIfNotExists('data_clean_v1', 'idx_dm_flag_ward',
            ['median_flag', 'cf_province_id', 'cf_ward_id', 'median_group', 'std_month']);
        $this->addIndexIfNotExists('data_clean_v1', 'idx_dm_flag_region',
            ['median_flag', 'cf_province_id', 'median_group', 'std_month']);
        $this->addIndexIfNotExists('data_clean_v1', 'idx_dm_flag_project',
            ['median_flag', 'project_id', 'median_group', 'std_month']);

        // ── Land-median indexes (use land_price_status instead of median_flag) ──
        $this->addIndexIfNotExists('data_clean_v1', 'idx_dm_land_ward',
            ['land_price_status', 'cf_province_id', 'cf_ward_id', 'std_month']);
        $this->addIndexIfNotExists('data_clean_v1', 'idx_dm_land_region',
            ['land_price_status', 'cf_province_id', 'std_month']);
        $this->addIndexIfNotExists('data_clean_v1', 'idx_dm_land_project',
            ['land_price_status', 'project_id', 'std_month']);
        $this->addIndexIfNotExists('data_clean_v1', 'idx_dm_land_street',
            ['land_price_status', 'domain', 'cf_province_id', 'cf_street_id', 'std_month']);
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

    // ─── Convert Batch (optimized) ──────────────────────────────────────

    public function convertBatch(Request $request)
    {
        set_time_limit(0);
        $lockName = 'data_median_convert_batch';
        $lockHeld = false;

        try {
            $lockRow = DB::selectOne("SELECT GET_LOCK(?, 1) AS lck", [$lockName]);
            $lockHeld = ((int) ($lockRow->lck ?? 0)) === 1;

            if (!$lockHeld) {
                return response()->json([
                    'error' => 'median_convert_busy',
                    'message' => 'Median convert dang chay o request khac. Cho request hien tai xong roi thu lai.',
                ], 409);
            }

            // Skip expensive schema checks after first batch in this request cycle
            if (!self::$schemaChecked) {
                $this->ensureMedianTable();
                $this->ensureDataCleanV1Indexes();
                self::$schemaChecked = true;
            }

            $scope      = $request->input('scope', 'ward');
            $offset     = (int) $request->input('offset', 0);
            $limit      = (int) $request->input('limit', 500);
            $monthParam = $request->input('month');

            if ($limit < 50)   $limit = 50;
            if ($limit > 2000)  $limit = 2000;
            if ($offset < 0)    $offset = 0;

            $targetTable = self::STATS_TABLE;

            $monthCond = "";
            if (!empty($monthParam)) {
                $monthCond = "AND d.std_month = '" . addslashes($monthParam) . "'";
            }

            $isLandScope = str_starts_with($scope, 'land_');
            $baseScope = $isLandScope ? substr($scope, 5) : $scope;
            $metricValueExpr = $isLandScope
                ? "(d.price_land / d.std_area)"
                : "d.price_m2";
            $metricOrderExpr = $isLandScope
                ? "(d.price_land / d.std_area)"
                : "d.price_m2";
            $metricWhere = $isLandScope
                ? "AND d.land_price_status = 'DONE' AND d.price_land IS NOT NULL AND d.price_land > 0 AND d.std_area IS NOT NULL AND d.std_area > 0"
                : "AND d.price_m2 IS NOT NULL AND d.price_m2 > 0";

        // ── Scope configuration ─────────────────────────────────────
            switch ($baseScope) {
            case 'ward':
                if ($isLandScope) {
                    $selectCols = "d.cf_province_id AS region_id, d.cf_ward_id AS ward_id, d.std_month AS month";
                    $groupBy    = "d.cf_province_id, d.cf_ward_id, d.std_month";
                    $extraWhere = "AND d.cf_ward_id IS NOT NULL AND d.cf_province_id IS NOT NULL";
                    $joinBack   = "g.region_id = d.cf_province_id AND g.ward_id = d.cf_ward_id AND g.month = d.std_month";
                } else {
                    $selectCols = "d.cf_province_id AS region_id, d.cf_ward_id AS ward_id, d.median_group AS mg, d.std_month AS month";
                    $groupBy    = "d.cf_province_id, d.cf_ward_id, d.median_group, d.std_month";
                    $extraWhere = "AND d.median_group IS NOT NULL AND d.cf_ward_id IS NOT NULL AND d.cf_province_id IS NOT NULL";
                    $joinBack   = "g.region_id = d.cf_province_id AND g.ward_id = d.cf_ward_id AND g.mg = d.median_group AND g.month = d.std_month";
                }
                $priceOrder = $isLandScope
                    ? "d.cf_province_id, d.cf_ward_id, d.std_month, {$metricOrderExpr}"
                    : "d.cf_province_id, d.cf_ward_id, d.median_group, d.std_month, {$metricOrderExpr}";
                break;
            case 'region':
                if ($isLandScope) {
                    $selectCols = "d.cf_province_id AS region_id, d.std_month AS month";
                    $groupBy    = "d.cf_province_id, d.std_month";
                    $extraWhere = "AND d.cf_province_id IS NOT NULL";
                    $joinBack   = "g.region_id = d.cf_province_id AND g.month = d.std_month";
                } else {
                    $selectCols = "d.cf_province_id AS region_id, d.median_group AS mg, d.std_month AS month";
                    $groupBy    = "d.cf_province_id, d.median_group, d.std_month";
                    $extraWhere = "AND d.median_group IS NOT NULL AND d.cf_province_id IS NOT NULL";
                    $joinBack   = "g.region_id = d.cf_province_id AND g.mg = d.median_group AND g.month = d.std_month";
                }
                $priceOrder = $isLandScope
                    ? "d.cf_province_id, d.std_month, {$metricOrderExpr}"
                    : "d.cf_province_id, d.median_group, d.std_month, {$metricOrderExpr}";
                break;
            case 'region_total':
                $selectCols = "d.cf_province_id AS region_id, d.std_month AS month";
                $groupBy    = "d.cf_province_id, d.std_month";
                $extraWhere = "AND d.cf_province_id IS NOT NULL";
                $joinBack   = "g.region_id = d.cf_province_id AND g.month = d.std_month";
                $priceOrder = "d.cf_province_id, d.std_month, {$metricOrderExpr}";
                break;
            case 'project':
                if ($isLandScope) {
                    $selectCols = "d.project_id AS project_id, d.std_month AS month";
                    $groupBy    = "d.project_id, d.std_month";
                    $extraWhere = "AND d.project_id IS NOT NULL AND d.project_id > 0";
                    $joinBack   = "g.project_id = d.project_id AND g.month = d.std_month";
                } else {
                    $selectCols = "d.project_id AS project_id, d.median_group AS mg, d.std_month AS month";
                    $groupBy    = "d.project_id, d.median_group, d.std_month";
                    $extraWhere = "AND d.median_group IS NOT NULL AND d.project_id IS NOT NULL AND d.project_id > 0";
                    $joinBack   = "g.project_id = d.project_id AND g.mg = d.median_group AND g.month = d.std_month";
                }
                $priceOrder = $isLandScope
                    ? "d.project_id, d.std_month, {$metricOrderExpr}"
                    : "d.project_id, d.median_group, d.std_month, {$metricOrderExpr}";
                break;
            case 'street':
                // Street scope: chỉ tính cho domain nhadat, group theo province + street + median_group + month
                if ($isLandScope) {
                    $selectCols = "d.cf_province_id AS region_id, d.cf_street_id AS street_id, d.std_month AS month";
                    $groupBy    = "d.cf_province_id, d.cf_street_id, d.std_month";
                    $extraWhere = "AND d.cf_street_id IS NOT NULL AND d.cf_street_id > 0 AND d.cf_province_id IS NOT NULL AND d.domain = 'nhadat'";
                    $joinBack   = "g.region_id = d.cf_province_id AND g.street_id = d.cf_street_id AND g.month = d.std_month";
                } else {
                    $selectCols = "d.cf_province_id AS region_id, d.cf_street_id AS street_id, d.median_group AS mg, d.std_month AS month";
                    $groupBy    = "d.cf_province_id, d.cf_street_id, d.median_group, d.std_month";
                    $extraWhere = "AND d.median_group IS NOT NULL AND d.cf_street_id IS NOT NULL AND d.cf_street_id > 0 AND d.cf_province_id IS NOT NULL AND d.domain = 'nhadat'";
                    $joinBack   = "g.region_id = d.cf_province_id AND g.street_id = d.cf_street_id AND g.mg = d.median_group AND g.month = d.std_month";
                }
                $priceOrder = $isLandScope
                    ? "d.cf_province_id, d.cf_street_id, d.std_month, {$metricOrderExpr}"
                    : "d.cf_province_id, d.cf_street_id, d.median_group, d.std_month, {$metricOrderExpr}";
                break;
            default:
                return response()->json(['error' => 'invalid scope'], 400);
        }

        // ── Step 1: Fetch one batch of groups directly (avoid temp-table CTAS lock pressure) ──
        // For 'ward' scope, always use offset 0 because we update median_flag=1 after
        // each batch, which shrinks the flag=0 set. Using a growing offset would skip groups.
        // Non-ward scopes compute from ALL data (no median_flag filter) because they
        // need the complete dataset for accurate region/project medians.
            $effectiveOffset = ($scope === 'ward') ? 0 : $offset;
            $flagCond = ($scope === 'ward') ? 'AND d.median_flag = 0' : '';
            // Street scope must be nhadat-only in both grouping and detail fetch steps.
            $domainCond = ($baseScope === 'street') ? "AND d.domain = 'nhadat'" : '';

            $groupRows = DB::select("
                SELECT {$selectCols}
                FROM data_clean_v1 d
                WHERE d.std_month IS NOT NULL {$metricWhere} {$flagCond} {$extraWhere} {$domainCond} {$monthCond}
                GROUP BY {$groupBy}
                ORDER BY {$groupBy}
                LIMIT {$limit} OFFSET {$effectiveOffset}
            ");

            $processed = count($groupRows);

            if ($processed > 0) {
            // ── Step 2: Fetch all price data for these groups (single query) ──
            $groupPredicates = [];
            foreach ($groupRows as $g) {
                $monthVal = "'" . addslashes($g->month) . "'";
                switch ($baseScope) {
                    case 'ward':
                        $regionId = (int) ($g->region_id ?? 0);
                        $wardId = (int) ($g->ward_id ?? 0);
                        if ($isLandScope) {
                            $groupPredicates[] = "(d.cf_province_id = {$regionId} AND d.cf_ward_id = {$wardId} AND d.std_month = {$monthVal})";
                        } else {
                            $mg = (int) ($g->mg ?? 0);
                            $groupPredicates[] = "(d.cf_province_id = {$regionId} AND d.cf_ward_id = {$wardId} AND d.median_group = {$mg} AND d.std_month = {$monthVal})";
                        }
                        break;
                    case 'region':
                        $regionId = (int) ($g->region_id ?? 0);
                        if ($isLandScope) {
                            $groupPredicates[] = "(d.cf_province_id = {$regionId} AND d.std_month = {$monthVal})";
                        } else {
                            $mg = (int) ($g->mg ?? 0);
                            $groupPredicates[] = "(d.cf_province_id = {$regionId} AND d.median_group = {$mg} AND d.std_month = {$monthVal})";
                        }
                        break;
                    case 'region_total':
                        $regionId = (int) ($g->region_id ?? 0);
                        $groupPredicates[] = "(d.cf_province_id = {$regionId} AND d.std_month = {$monthVal})";
                        break;
                    case 'project':
                        $projectId = (int) ($g->project_id ?? 0);
                        if ($isLandScope) {
                            $groupPredicates[] = "(d.project_id = {$projectId} AND d.std_month = {$monthVal})";
                        } else {
                            $mg = (int) ($g->mg ?? 0);
                            $groupPredicates[] = "(d.project_id = {$projectId} AND d.median_group = {$mg} AND d.std_month = {$monthVal})";
                        }
                        break;
                    case 'street':
                        $regionId = (int) ($g->region_id ?? 0);
                        $streetId = (int) ($g->street_id ?? 0);
                        if ($isLandScope) {
                            $groupPredicates[] = "(d.cf_province_id = {$regionId} AND d.cf_street_id = {$streetId} AND d.std_month = {$monthVal})";
                        } else {
                            $mg = (int) ($g->mg ?? 0);
                            $groupPredicates[] = "(d.cf_province_id = {$regionId} AND d.cf_street_id = {$streetId} AND d.median_group = {$mg} AND d.std_month = {$monthVal})";
                        }
                        break;
                }
            }

            $groupWhere = implode(" OR ", $groupPredicates);
            $rows = DB::select("
                SELECT d.ad_id,
                       d.cf_province_id AS region_id,
                       d.cf_ward_id AS ward_id,
                       d.cf_street_id AS street_id,
                       d.project_id,
                       d.median_group AS mg,
                       d.std_month AS month,
                       {$metricValueExpr} AS metric_value
                FROM data_clean_v1 d
                WHERE ({$groupWhere}) {$metricWhere} {$flagCond} {$domainCond}
                ORDER BY {$priceOrder}
            ");

            // ── Step 3: Group rows in PHP + collect ad_ids for flag UPDATE ──
            $groups = [];
            $allAdIds = [];
            foreach ($rows as $r) {
                switch ($baseScope) {
                    case 'ward':
                        $key = $isLandScope
                            ? "{$r->region_id}|{$r->ward_id}|{$r->month}"
                            : "{$r->region_id}|{$r->ward_id}|{$r->mg}|{$r->month}";
                        break;
                    case 'region':
                        $key = $isLandScope
                            ? "{$r->region_id}|{$r->month}"
                            : "{$r->region_id}|{$r->mg}|{$r->month}";
                        break;
                    case 'region_total':
                        $key = "{$r->region_id}|{$r->month}";
                        break;
                    case 'project':
                        $key = $isLandScope
                            ? "{$r->project_id}|{$r->month}"
                            : "{$r->project_id}|{$r->mg}|{$r->month}";
                        break;
                    case 'street':
                        $key = $isLandScope
                            ? "{$r->region_id}|{$r->street_id}|{$r->month}"
                            : "{$r->region_id}|{$r->street_id}|{$r->mg}|{$r->month}";
                        break;
                }
                if (!isset($groups[$key])) {
                    $groups[$key] = [
                        'region_id'  => $r->region_id ?? null,
                        'ward_id'    => $r->ward_id ?? null,
                        'street_id'  => $r->street_id ?? null,
                        'project_id' => $r->project_id ?? null,
                        'mg'         => $isLandScope ? 5 : ($r->mg ?? null),
                        'month'      => $r->month,
                        'prices'     => [],
                    ];
                }
                $groups[$key]['prices'][] = (float) $r->metric_value;
                $allAdIds[] = $r->ad_id;
            }

            // ── Step 4: Compute trimmed median per group in PHP ──
            $insertValues = [];
            foreach ($groups as $g) {
                $prices = $g['prices']; // already sorted from SQL ORDER BY
                $cnt = count($prices);
                if ($cnt === 0) continue;

                $cut = (int) floor($cnt * 0.1);
                $trimmed = ($cut > 0 && $cnt > 2) ? array_slice($prices, $cut, $cnt - 2 * $cut) : $prices;
                $tCnt = count($trimmed);
                if ($tCnt === 0) { $trimmed = $prices; $tCnt = $cnt; }

                // Trimmed median
                $mid = intdiv($tCnt, 2);
                $median = ($tCnt % 2 === 1)
                    ? $trimmed[$mid]
                    : ($trimmed[$mid - 1] + $trimmed[$mid]) / 2.0;

                $avg = array_sum($trimmed) / $tCnt;
                $min = $trimmed[0];
                $max = $trimmed[$tCnt - 1];

                // Build scope-specific INSERT columns
                switch ($baseScope) {
                    case 'ward':
                        $rid = (int)($g['region_id'] ?? 0);
                        $wid = (int)($g['ward_id'] ?? 0);
                        $mgv = $isLandScope ? 5 : (int)($g['mg'] ?? 0);
                        $scopeCols = "'ward', {$rid}, {$wid}, 0, 0, NULL, {$mgv}";
                        break;
                    case 'region':
                        $rid = (int)($g['region_id'] ?? 0);
                        $mgv = $isLandScope ? 5 : (int)($g['mg'] ?? 0);
                        $scopeCols = "'region', {$rid}, 0, 0, 0, NULL, {$mgv}";
                        break;
                    case 'region_total':
                        $rid = (int)($g['region_id'] ?? 0);
                        $scopeCols = "'region_total', {$rid}, 0, 0, 0, NULL, " . ($isLandScope ? 5 : 0);
                        break;
                    case 'project':
                        $pid = (int)($g['project_id'] ?? 0);
                        $mgv = $isLandScope ? 5 : (int)($g['mg'] ?? 0);
                        $scopeCols = "'project', 0, 0, 0, {$pid}, NULL, {$mgv}";
                        break;
                    case 'street':
                        $rid = (int)($g['region_id'] ?? 0);
                        $sid = (int)($g['street_id'] ?? 0);
                        $mgv = $isLandScope ? 5 : (int)($g['mg'] ?? 0);
                        $scopeCols = "'street', {$rid}, 0, {$sid}, 0, NULL, {$mgv}";
                        break;
                }

                $month = "'" . addslashes($g['month']) . "'";
                $insertValues[] = "({$scopeCols}, NULL, {$month}, "
                    . round($avg, 2) . ", " . round($median, 2) . ", "
                    . round($min, 2) . ", " . round($max, 2) . ", "
                    . $cnt . ", " . $tCnt . ", NOW())";
            }

            // ── Step 5: Batch INSERT into data_median ──
            if (!empty($insertValues)) {
                foreach (array_chunk($insertValues, 500) as $chunk) {
                    DB::statement("INSERT INTO {$targetTable}
                        (scope, new_region_id, new_ward_id, street_id, project_id, type,
                         median_group, category, month, avg_price_m2, median_price_m2,
                         min_price_m2, max_price_m2, total_rows, trimmed_rows, converted_at)
                        VALUES " . implode(",\n", $chunk) . "
                        ON DUPLICATE KEY UPDATE
                            avg_price_m2    = VALUES(avg_price_m2),
                            median_price_m2 = VALUES(median_price_m2),
                            min_price_m2    = VALUES(min_price_m2),
                            max_price_m2    = VALUES(max_price_m2),
                            total_rows      = VALUES(total_rows),
                            trimmed_rows    = VALUES(trimmed_rows),
                            converted_at    = VALUES(converted_at)
                    ");
                }
            }

            // ── Step 6: Mark ALL rows in processed groups as done ──
            // Uses ad_ids collected in Step 3 for fast UPDATE via unique index
            if ($scope === 'ward' && !empty($allAdIds)) {
                foreach (array_chunk($allAdIds, 5000) as $chunk) {
                    $placeholders = implode(',', array_fill(0, count($chunk), '?'));
                    DB::update("UPDATE data_clean_v1 SET median_flag = 1 WHERE ad_id IN ({$placeholders})", $chunk);
                }
            }
            }

            // ── Auto-chain to next scope ─────────────────────────────────
            $doneScope  = $processed < $limit;
            $nextScope  = $scope;
            // Ward scope: flag update shrinks the set, so always restart from 0.
            // Other scopes: no flag update, so advance offset normally.
            $nextOffset = ($scope === 'ward') ? 0 : ($offset + $processed);
            $done       = false;

            if ($doneScope) {
                switch ($scope) {
                    case 'ward':         $nextScope = 'region';       $nextOffset = 0; break;
                    case 'region':       $nextScope = 'region_total'; $nextOffset = 0; break;
                    case 'region_total': $nextScope = 'project';      $nextOffset = 0; break;
                    case 'project':      $nextScope = 'street';       $nextOffset = 0; break;
                    case 'street':       $nextScope = 'land_ward';     $nextOffset = 0; break;
                    case 'land_ward':    $nextScope = 'land_region';   $nextOffset = 0; break;
                    case 'land_region':  $nextScope = 'land_region_total'; $nextOffset = 0; break;
                    case 'land_region_total': $nextScope = 'land_project'; $nextOffset = 0; break;
                    case 'land_project': $nextScope = 'land_street';   $nextOffset = 0; break;
                    case 'land_street':  $done = true;                                 break;
                }
            }

            return response()->json([
                'scope'      => $scope,
                'offset'     => $offset,
                'processed'  => $processed,
                'inserted'   => $processed,
                'done_scope' => $doneScope,
                'next_scope' => $nextScope,
                'next_offset'=> $nextOffset,
                'done'       => $done,
            ]);
        } catch (\Throwable $e) {
            return response()->json([
                'error' => 'convert_batch_failed',
                'message' => $e->getMessage(),
            ], 500);
        } finally {
            if ($lockHeld) {
                try {
                    DB::selectOne("SELECT RELEASE_LOCK(?)", [$lockName]);
                } catch (\Throwable $ignore) {
                }
            }
        }
    }

    // ─── Demo page ─────────────────────────────────────────────────────

    public function demo(Request $request)
    {
        $this->ensureMedianTable();

        $regionId    = $request->query('region_id');
        $wardId      = $request->query('ward_id');
        $streetId    = $request->query('street_id');
        $projectId   = $request->query('project_id');
        $month       = $request->query('month');
        $medianGroup = $request->query('median_group');

        // Dropdown: regions
        $regions = DB::table(self::STATS_TABLE . ' as s')
            ->join('transaction_city_merge as m', 's.new_region_id', '=', 'm.new_city_id')
            ->select('s.new_region_id as region_id', 'm.new_city_name as region_name')
            ->distinct()
            ->whereNotNull('s.new_region_id')
            ->orderBy('m.new_city_name')
            ->get();

        // Dropdown: wards (filtered by region)
        $wards = collect();
        if ($regionId) {
            $wards = DB::table(self::STATS_TABLE . ' as s')
                ->join('transaction_city_merge as m', 's.new_ward_id', '=', 'm.new_city_id')
                ->select('s.new_ward_id as ward_id', 'm.new_city_name as ward_name')
                ->distinct()
                ->where('s.new_region_id', (int) $regionId)
                ->whereNotNull('s.new_ward_id')
                ->orderBy('m.new_city_name')
                ->get();
        }

        // Dropdown: streets (filtered by region)
        $streets = collect();
        if ($regionId) {
            $streets = DB::table(self::STATS_TABLE . ' as s')
                ->join('location_street as ls', 's.street_id', '=', 'ls.record_id')
                ->select('s.street_id', 'ls.title as street_name')
                ->distinct()
                ->where('s.scope', 'street')
                ->where('s.new_region_id', (int) $regionId)
                ->whereNotNull('s.street_id')
                ->where('s.street_id', '>', 0)
                ->orderBy('ls.title')
                ->get();
        }

        // Dropdown: projects
        $projects = DB::table(self::STATS_TABLE . ' as s')
            ->join('duan as d', 's.project_id', '=', 'd.duan_id')
            ->select('s.project_id', 'd.duan_ten as project_name')
            ->distinct()
            ->whereNotNull('s.project_id')
            ->where('s.project_id', '>', 0)
            ->orderBy('d.duan_ten')
            ->get();

        // Dropdown: months
        $months = DB::table(self::STATS_TABLE)
            ->select('month')
            ->distinct()
            ->orderBy('month', 'desc')
            ->pluck('month');

        // Base query
        $base = DB::table(self::STATS_TABLE . ' as s');

        if ($projectId) {
            $base->where('s.scope', 'project')->where('s.project_id', (int) $projectId);
        } elseif ($streetId) {
            $base->where('s.scope', 'street')->where('s.street_id', (int) $streetId);
        } elseif ($wardId) {
            $base->where('s.scope', 'ward')->where('s.new_ward_id', (int) $wardId);
        } elseif ($regionId) {
            $base->where('s.scope', 'region')->where('s.new_region_id', (int) $regionId);
        } else {
            $base->where('s.scope', 'region');
        }

        if ($medianGroup) {
            $base->where('s.median_group', (int) $medianGroup);
        }
        if ($month) {
            $base->where('s.month', $month);
        }

        $rows = $base->get();

        // Build chart series
        $monthList = $months->sort()->values()->toArray();

        $medianGroupLabels = [1 => 'Nha gan lien', 2 => 'Can ho', 3 => 'Dat', 4 => 'Cho thue', 5 => 'Gia dat'];
        $series = [];
        foreach ($medianGroupLabels as $g => $lbl) {
            $series[$g] = [
                'label'   => $lbl,
                'values'  => array_fill(0, count($monthList), null),
                'avg'     => array_fill(0, count($monthList), null),
                'min'     => array_fill(0, count($monthList), null),
                'max'     => array_fill(0, count($monthList), null),
                'trimmed' => array_fill(0, count($monthList), null),
            ];
        }

        foreach ($rows as $r) {
            $g = $r->median_group;
            if (!isset($series[$g])) continue;
            $mIdx = array_search($r->month, $monthList);
            if ($mIdx !== false) {
                $series[$g]['values'][$mIdx]  = $r->median_price_m2;
                $series[$g]['avg'][$mIdx]     = $r->avg_price_m2;
                $series[$g]['min'][$mIdx]     = $r->min_price_m2;
                $series[$g]['max'][$mIdx]     = $r->max_price_m2;
                $series[$g]['trimmed'][$mIdx] = $r->trimmed_rows;
            }
        }

        // Small rows query
        $smallQuery = DB::table(self::STATS_TABLE . ' as s')->select('s.*');
        if ($projectId) {
            $smallQuery->where('s.project_id', (int) $projectId);
        } elseif ($streetId) {
            $smallQuery->where('s.street_id', (int) $streetId);
        } elseif ($wardId) {
            $smallQuery->where('s.new_ward_id', (int) $wardId);
        } elseif ($regionId) {
            $smallQuery->where('s.new_region_id', (int) $regionId);
        }
        if ($medianGroup) {
            $smallQuery->where('s.median_group', (int) $medianGroup);
        }
        if ($month) {
            $smallQuery->where('s.month', $month);
        }

        $smallRows = $smallQuery
            ->addSelect([
                'region_name' => DB::table('transaction_city_merge')
                    ->select('new_city_name')
                    ->whereColumn('new_city_id', 's.new_region_id')
                    ->limit(1),
                'ward_name' => DB::table('transaction_city_merge')
                    ->select('new_city_name')
                    ->whereColumn('new_city_id', 's.new_ward_id')
                    ->limit(1),
                'project_name' => DB::table('duan')
                    ->select('duan_ten')
                    ->whereColumn('duan_id', 's.project_id')
                    ->limit(1),
                'street_name' => DB::table('location_street')
                    ->select('title')
                    ->whereColumn('record_id', 's.street_id')
                    ->limit(1)
            ])
            ->where('s.total_rows', '<', 10)
            ->orderBy('s.month', 'desc')
            ->limit(200)
            ->get();

        // Statistics for Small Data (<10 rows, valid wards only)
        $smallStats = [
            'total_valid' => 0,
            'with_group' => null,
            'with_month' => null
        ];

        // Base stats query: Region selected + Valid Ward Name (exists in merge) + Rows < 10
        $statsBase = DB::table(self::STATS_TABLE . ' as s')
            ->join('transaction_city_merge as m', 's.new_ward_id', '=', 'm.new_city_id')
            ->where('s.scope', 'ward')
            ->where('s.total_rows', '<', 10);

        if ($regionId) {
            $statsBase->where('s.new_region_id', (int) $regionId);
        }

        // 1. Total distinct wards with ANY small data record (in this region)
        // This count might be heavy if not indexed properly, but for demo OK.
        // "bao nhiêu xã bé hơn 10" implies globally or current context?
        // If filtered by Month/Group, user usually sees list.
        // Let's providing counts based on POTENTIAL filters.

        // Count 1: Total valid wards having <10 rows (Global or Region context)
        $smallStats['total_valid'] = (clone $statsBase)->distinct('s.new_ward_id')->count('s.new_ward_id');

        // Count 2: "bao nhiêu xã thuộc median này bé hơn"
        if ($medianGroup) {
            $smallStats['with_group'] = (clone $statsBase)
                ->where('s.median_group', (int) $medianGroup)
                ->distinct('s.new_ward_id')
                ->count('s.new_ward_id');
        }

        // Count 3: "có bao nhiêu xã trong tháng này <10"
        if ($month) {
            $smallStats['with_month'] = (clone $statsBase)
                ->where('s.month', $month)
                ->distinct('s.new_ward_id')
                ->count('s.new_ward_id');
        }

        return view('data_median_demo', [
            'regions'           => $regions,
            'wards'             => $wards,
            'streets'           => $streets,
            'projects'          => $projects,
            'months'            => $months,
            'medianGroupLabels' => $medianGroupLabels,
            'filters'           => [
                'region_id'    => $regionId,
                'ward_id'      => $wardId,
                'street_id'    => $streetId,
                'project_id'   => $projectId,
                'month'        => $month,
                'median_group' => $medianGroup,
            ],
            'month_list' => $monthList,
            'series'     => $series,
            'smallRows'  => $smallRows,
            'smallStats' => $smallStats,
        ]);
    }
    public function getStatsApi(Request $request)
    {
        // Support flexible param names (ward_id/ward, city_id/city)
        $wardId      = $request->query('ward_id') ?? $request->query('ward');
        $streetId    = $request->query('street_id') ?? $request->query('street');
        $cityId      = $request->query('city_id') ?? $request->query('city'); 
        $projectId   = $request->query('project_id');
        $monthInput  = $request->query('month');
        $yearInput   = $request->query('year');
        $medianGroup = $request->query('median_group');

        // Construct month string YYYY-MM
        if (!$monthInput || !$yearInput) {
             return response()->json(['error' => 'Missing month or year'], 400);
        }
        $monthStr = $yearInput . '-' . str_pad($monthInput, 2, '0', STR_PAD_LEFT);

        // Detect scope
        $scope = 'region';
        $filterCol = 'new_region_id';
        $filterVal = $cityId;

        if ($projectId) {
            $scope = 'project';
            $filterCol = 'project_id';
            $filterVal = $projectId;
        } elseif ($streetId) {
            $scope = 'street';
            $filterCol = 'street_id';
            $filterVal = $streetId;
        } elseif ($wardId) {
            $scope = 'ward';
            $filterCol = 'new_ward_id';
            $filterVal = $wardId;
        } elseif (!$cityId) {
             return response()->json(['error' => 'Missing ID (city_id, ward_id, street_id, or project_id)'], 400);
        }

        if (!$medianGroup) {
             return response()->json(['error' => 'Missing median_group'], 400);
        }

        // Query Helper
        $getData = function($m) use ($scope, $filterCol, $filterVal, $medianGroup) {
            return DB::table(self::STATS_TABLE)
                ->where('scope', $scope)
                ->where($filterCol, (int)$filterVal)
                ->where('median_group', (int)$medianGroup)
                ->where('month', $m)
                ->first();
        };

        // Current Data
        $current = $getData($monthStr);

        if (!$current || $current->total_rows < 10) {
             return response()->json(['message' => 'du lieu ko du'], 200); // 200 OK requested
        }

        // Previous Month
        $prevMonthDate = date('Y-m', strtotime($monthStr . '-01 -1 month'));
        $prevMonthData = $getData($prevMonthDate);

        // Previous Year
        $prevYearDate = date('Y-m', strtotime($monthStr . '-01 -1 year'));
        $prevYearData = $getData($prevYearDate);

        // Calculations (Growth based on Median Price) ONLY if previous period has sufficient data
        $mom = null;
        if ($prevMonthData && $prevMonthData->total_rows >= 10 && $prevMonthData->median_price_m2 > 0) {
            $mom = ($current->median_price_m2 - $prevMonthData->median_price_m2) / $prevMonthData->median_price_m2 * 100;
        }

        $yoy = null;
        if ($prevYearData && $prevYearData->total_rows >= 10 && $prevYearData->median_price_m2 > 0) {
            $yoy = ($current->median_price_m2 - $prevYearData->median_price_m2) / $prevYearData->median_price_m2 * 100;
        }

        // Format function for pct (string output)
        $formatPct = function ($val) {
            if ($val === null) return null;
            return ($val > 0 ? '+' : '') . number_format($val, 2, '.', '');
        };

        // Format price (decimal(20,2) from DB is string, keep it strictly formatted)
        $formatPrice = function ($val) {
            if ($val === null) return null;
            return number_format((float)$val, 2, '.', '');
        };

        $response = [
            'scope'           => $scope,
            'avg'             => $formatPrice($current->avg_price_m2),
            'median'          => $formatPrice($current->median_price_m2),
            'min'             => $formatPrice($current->min_price_m2),
            'max'             => $formatPrice($current->max_price_m2),
            'total_rows'      => (int)$current->trimmed_rows,
            'month_growth'    => $formatPct($mom),
            'year_growth'     => $formatPct($yoy),
        ];

        // Add ID keys based on scope/request
        if ($cityId) $response['city_id'] = $cityId;
        if ($wardId) $response['ward_id'] = $wardId;
        if ($streetId) $response['street_id'] = $streetId;
        if ($projectId) $response['project_id'] = $projectId;

        return response()->json($response);
    }
}
