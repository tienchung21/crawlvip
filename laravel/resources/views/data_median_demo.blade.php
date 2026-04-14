<!doctype html>
<html lang="vi">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Data Median (data_clean_v1)</title>
    <style>
        :root {
            --bg: #0f1116;
            --panel: #171b23;
            --muted: #9aa3b2;
            --text: #e7eaf0;
            --line1: #ff8f3d;
            --line2: #4dd6b7;
            --line3: #7db5ff;
            --line4: #f3c969;
            --line5: #ff5f87;
        }
        body { background: var(--bg); color: var(--text); font-family: Arial, sans-serif; margin: 0; }
        .container { max-width: 1200px; margin: 24px auto; padding: 0 16px; }
        h1 { margin: 0 0 8px; font-size: 28px; }
        .subtitle { color: var(--muted); margin-bottom: 16px; }
        .filters { display: grid; grid-template-columns: 1fr 1fr 1fr 1fr 1fr 1fr 1fr auto; gap: 12px; margin-bottom: 16px; }
        .filters label { display: block; font-size: 12px; color: var(--muted); margin-bottom: 6px; }
        .filters select { width: 100%; padding: 8px; background: #1b1f27; color: var(--text); border: 1px solid #2c3240; border-radius: 6px; }
        .card { background: var(--panel); border-radius: 10px; padding: 12px; }
        .chart-wrap { margin-top: 12px; }
        svg { width: 100%; height: 260px; display: block; }
        table { width: 100%; border-collapse: collapse; margin-top: 16px; }
        th, td { padding: 8px; border-bottom: 1px solid #2c3240; font-size: 12px; }
        th { text-align: left; color: var(--muted); }
        .note { color: var(--muted); font-size: 12px; margin-top: 8px; }
        .tabs { display:flex; gap:8px; margin: 4px 0 12px; }
        .tab-btn { padding:6px 10px; background:#1b1f27; color:var(--text); border:1px solid #2c3240; border-radius:6px; cursor:pointer; font-size:12px; }
        .tab-btn.active { background:#2563eb; border-color:#2563eb; }
        .tab-panel { display:none; }
        .tab-panel.active { display:block; }
        /* Convert Panel */
        .convert-panel { background: var(--panel); border-radius: 10px; padding: 16px; margin-bottom: 20px; border: 1px solid #2c3240; }
        .convert-panel h2 { margin: 0 0 12px; font-size: 18px; }
        .convert-controls { display: flex; gap: 12px; align-items: end; flex-wrap: wrap; margin-bottom: 12px; }
        .convert-controls label { display: block; font-size: 12px; color: var(--muted); margin-bottom: 4px; }
        .convert-controls select,
        .convert-controls input { padding: 8px; background: #1b1f27; color: var(--text); border: 1px solid #2c3240; border-radius: 6px; width: 120px; }
        .btn-convert { padding: 8px 20px; border: 0; border-radius: 6px; cursor: pointer; font-weight: bold; font-size: 14px; }
        .btn-start { background: #22c55e; color: #fff; }
        .btn-stop { background: #ef4444; color: #fff; }
        .btn-start:disabled { background: #555; cursor: not-allowed; }
        .convert-log { background: #0d0f13; border: 1px solid #2c3240; border-radius: 6px; padding: 10px; max-height: 240px; overflow-y: auto; font-family: monospace; font-size: 12px; line-height: 1.6; }
        .convert-log .scope { color: #7db5ff; }
        .convert-log .ok { color: #22c55e; }
        .convert-log .err { color: #ef4444; }
        .convert-log .info { color: #f3c969; }
        .convert-status { display: flex; gap: 16px; margin: 8px 0; font-size: 13px; }
        .convert-status span { color: var(--muted); }
        .convert-status strong { color: var(--text); }
    </style>
    <meta name="csrf-token" content="{{ csrf_token() }}">
</head>
<body>
<div class="container">
    <h1>Data Median <small style="font-size:14px;color:var(--muted)">(data_clean_v1)</small></h1>
    <div class="subtitle">Thong ke median theo group, khu vuc, du an, va thang.</div>

    {{-- ═══ CONVERT PANEL ═══ --}}
    <div class="convert-panel">
        <h2>⚡ Convert Batch — Tinh Median</h2>
        <div class="convert-controls">
            <div>
                <label>Bat dau tu scope</label>
                <select id="cv-scope">
                    <option value="ward">ward</option>
                    <option value="region">region</option>
                    <option value="region_total">region_total</option>
                    <option value="project">project</option>
                    <option value="street">street</option>
                </select>
            </div>
            <div>
                <label>Batch size</label>
                <input type="number" id="cv-limit" value="500" min="50" max="2000">
            </div>
            <div>
                <label>Thang (tuy chon)</label>
                <input type="text" id="cv-month" placeholder="2025-01" style="width:100px;">
            </div>
            <div>
                <button class="btn-convert btn-start" id="cv-start" onclick="cvStart()">▶ Bat dau Convert</button>
                <button class="btn-convert btn-stop" id="cv-stop" onclick="cvStop()" style="display:none;">⏹ Dung lai</button>
            </div>
        </div>
        <div class="convert-status">
            <span>Scope: <strong id="cv-cur-scope">-</strong></span>
            <span>Offset: <strong id="cv-cur-offset">0</strong></span>
            <span>Tong da xu ly: <strong id="cv-total">0</strong></span>
            <span>Trang thai: <strong id="cv-state">Cho</strong></span>
        </div>
        <div class="convert-log" id="cv-log"></div>
    </div>

    {{-- ═══ FILTERS ═══ --}}
    <form method="get">
        <div class="filters">
            <div>
                <label>Khu vuc (cf_province_id)</label>
                <select name="region_id" onchange="this.form.submit()">
                    <option value="">Tat ca</option>
                    @foreach ($regions as $r)
                        <option value="{{ $r->region_id }}" {{ ($filters['region_id'] == $r->region_id) ? 'selected' : '' }}>
                            {{ $r->region_name ?? $r->region_id }}
                        </option>
                    @endforeach
                </select>
            </div>
            <div>
                <label>Phuong/xa (cf_ward_id)</label>
                <select name="ward_id">
                    <option value="">Tat ca</option>
                    @foreach ($wards as $w)
                        <option value="{{ $w->ward_id }}" {{ ($filters['ward_id'] == $w->ward_id) ? 'selected' : '' }}>
                            {{ $w->ward_name ?? $w->ward_id }}
                        </option>
                    @endforeach
                </select>
            </div>
            <div>
                <label>Street (street_id)</label>
                <select name="street_id">
                    <option value="">Tat ca</option>
                    @foreach ($streets as $s)
                        <option value="{{ $s->street_id }}" {{ ($filters['street_id'] == $s->street_id) ? 'selected' : '' }}>
                            {{ $s->street_name ?? $s->street_id }}
                        </option>
                    @endforeach
                </select>
            </div>
            <div>
                <label>Du an (project_id)</label>
                <select name="project_id">
                    <option value="">Tat ca</option>
                    @foreach ($projects as $p)
                        <option value="{{ $p->project_id }}" {{ ($filters['project_id'] == $p->project_id) ? 'selected' : '' }}>
                            {{ $p->project_name ?? $p->project_id }}
                        </option>
                    @endforeach
                </select>
            </div>
            <div>
                <label>Thang</label>
                <select name="month">
                    <option value="">Tat ca</option>
                    @foreach ($months as $m)
                        <option value="{{ $m }}" {{ ($filters['month'] == $m) ? 'selected' : '' }}>
                            {{ $m }}
                        </option>
                    @endforeach
                </select>
            </div>
            <div>
                <label>Median group</label>
                <select name="median_group">
                    <option value="">Tat ca</option>
                    @foreach ($medianGroupLabels as $g => $label)
                        <option value="{{ $g }}" {{ ($filters['median_group'] == $g) ? 'selected' : '' }}>
                            {{ $g }} - {{ $label }}
                        </option>
                    @endforeach
                </select>
            </div>
            <div style="display:flex; align-items:end;">
                <button type="submit" style="padding:8px 12px; background:#2563eb; color:#fff; border:0; border-radius:6px; cursor:pointer;">Loc</button>
            </div>
        </div>
    </form>

    {{-- ═══ TABS ═══ --}}
    <div class="tabs">
        <button class="tab-btn active" type="button" data-tab="tab-main">Du lieu >= 10</button>
        <button class="tab-btn" type="button" data-tab="tab-small">Du lieu < 10</button>
    </div>

    <div class="tab-panel active" id="tab-main">
        <div class="card chart-wrap">
        <div class="note" id="chart-note"></div>
        <div id="trend-chart" style="height:320px; width:100%;"></div>
    </div>

    <table>
        <thead>
            <tr>
                <th>month</th>
                @foreach ($series as $item)
                    <th>{{ $item['label'] }} (median)</th>
                    <th>{{ $item['label'] }} (avg)</th>
                    <th>{{ $item['label'] }} (min)</th>
                    <th>{{ $item['label'] }} (max)</th>
                    <th>{{ $item['label'] }} (trimmed)</th>
                @endforeach
            </tr>
        </thead>
        <tbody>
        @forelse ($month_list as $idx => $m)
            <tr>
                <td>{{ $m }}</td>
                @foreach ($series as $item)
                    <td>
                        {{ $item['values'][$idx] !== null ? number_format($item['values'][$idx], 2) : '-' }}
                    </td>
                    <td>
                        {{ $item['avg'][$idx] !== null ? number_format($item['avg'][$idx], 2) : '-' }}
                    </td>
                    <td>
                        {{ $item['min'][$idx] !== null ? number_format($item['min'][$idx], 2) : '-' }}
                    </td>
                    <td>
                        {{ $item['max'][$idx] !== null ? number_format($item['max'][$idx], 2) : '-' }}
                    </td>
                    <td>
                        {{ $item['trimmed'][$idx] !== null ? number_format($item['trimmed'][$idx]) : '-' }}
                    </td>
                @endforeach
            </tr>
        @empty
            <tr><td colspan="21">empty</td></tr>
        @endforelse
        </tbody>
    </table>
    </div>

    <div class="tab-panel" id="tab-small">
        <div style="background:#1b1f27; border:1px solid #2c3240; padding:12px; margin-bottom:12px; border-radius:6px; font-size:13px; color:#ddd;">
            <div style="margin-bottom:4px">Tong so xa hop le (co ten trong merge) co du lieu < 10: <strong style="color:#fff">{{ number_format($smallStats['total_valid']) }}</strong></div>
            @if($smallStats['with_group'] !== null)
            <div style="margin-bottom:4px">So xa thuoc median nay ({{ $filters['median_group'] }}) < 10: <strong style="color:#fff">{{ number_format($smallStats['with_group']) }}</strong></div>
            @endif
            @if($smallStats['with_month'] !== null)
            <div style="margin-bottom:4px">So xa trong thang nay ({{ $filters['month'] }}) < 10: <strong style="color:#fff">{{ number_format($smallStats['with_month']) }}</strong></div>
            @endif
        </div>
        <div class="card">
            <table>
                <thead>
                    <tr>
                        <th>Scope</th>
                        <th>Region ID</th>
                        <th>Ward ID</th>
                        <th>Street ID</th>
                        <th>Project ID</th>
                        <th>Thang</th>
                        <th>Median group</th>
                        <th>Tong so tin</th>
                    </tr>
                </thead>
                <tbody>
                @forelse ($smallRows as $row)
                    <tr>
                        <td>{{ $row->scope }}</td>
                        <td>{{ $row->region_name ?? $row->new_region_id ?? '-' }}</td>
                        <td>{{ $row->ward_name ?? $row->new_ward_id ?? '-' }}</td>
                        <td>{{ $row->street_name ?? $row->street_id ?? '-' }}</td>
                        <td>{{ $row->project_name ?? $row->project_id ?? '-' }}</td>
                        <td>{{ $row->month }}</td>
                        <td>{{ $row->median_group }} - {{ $medianGroupLabels[$row->median_group] ?? '' }}</td>
                        <td>{{ number_format($row->total_rows) }}</td>
                    </tr>
                @empty
                    <tr><td colspan="8">empty</td></tr>
                @endforelse
                </tbody>
            </table>
        </div>
    </div>
</div>

{{-- ═══ CONVERT BATCH JS ═══ --}}
<script>
var cvRunning = false;
var cvTotal = 0;

function cvLog(msg, cls) {
    var log = document.getElementById('cv-log');
    var line = document.createElement('div');
    line.className = cls || '';
    line.textContent = '[' + new Date().toLocaleTimeString() + '] ' + msg;
    log.appendChild(line);
    log.scrollTop = log.scrollHeight;
}

function cvStop() {
    cvRunning = false;
    document.getElementById('cv-start').style.display = '';
    document.getElementById('cv-stop').style.display = 'none';
    document.getElementById('cv-state').textContent = 'Da dung';
    cvLog('⏹ Da dung boi nguoi dung.', 'info');
}

function cvStart() {
    if (cvRunning) return;
    cvRunning = true;
    cvTotal = 0;
    document.getElementById('cv-start').style.display = 'none';
    document.getElementById('cv-stop').style.display = '';
    document.getElementById('cv-log').innerHTML = '';
    document.getElementById('cv-total').textContent = '0';

    var scope = document.getElementById('cv-scope').value;
    var limit = parseInt(document.getElementById('cv-limit').value) || 500;
    var month = document.getElementById('cv-month').value.trim();

    cvLog('▶ Bat dau convert: scope=' + scope + ', limit=' + limit + (month ? ', month=' + month : ''), 'info');
    cvRun(scope, 0, limit, month);
}

function cvRun(scope, offset, limit, month) {
    if (!cvRunning) return;

    document.getElementById('cv-cur-scope').textContent = scope;
    document.getElementById('cv-cur-offset').textContent = offset;
    document.getElementById('cv-state').textContent = 'Dang chay...';

    var body = new FormData();
    body.append('scope', scope);
    body.append('offset', offset);
    body.append('limit', limit);
    if (month) body.append('month', month);

    var csrfToken = document.querySelector('meta[name="csrf-token"]').getAttribute('content');

    fetch('{{ route("data-median.convert-batch") }}', {
        method: 'POST',
        headers: { 'X-CSRF-TOKEN': csrfToken },
        body: body
    })
    .then(function(r) {
        return r.json().then(function(data) {
            if (!r.ok) {
                var msg = (data && (data.message || data.error)) ? (data.message || data.error) : ('HTTP ' + r.status);
                throw new Error(msg);
            }
            return data;
        }).catch(function(err) {
            if (err instanceof Error) throw err;
            throw new Error('Phan hoi JSON khong hop le');
        });
    })
    .then(function(data) {
        if (typeof data.processed === 'undefined') {
            throw new Error(data.message || data.error || 'Phan hoi thieu truong processed');
        }
        cvTotal += (data.processed || 0);
        document.getElementById('cv-total').textContent = cvTotal.toLocaleString();

        cvLog('scope=' + scope + ' offset=' + offset + ' processed=' + data.processed
              + ' done_scope=' + data.done_scope, data.processed > 0 ? 'ok' : 'scope');

        if (data.done) {
            cvRunning = false;
            document.getElementById('cv-start').style.display = '';
            document.getElementById('cv-stop').style.display = 'none';
            document.getElementById('cv-state').textContent = '✅ Hoan thanh!';
            cvLog('✅ TAT CA SCOPES DA HOAN THANH! Tong: ' + cvTotal.toLocaleString(), 'ok');
            return;
        }

        if (!cvRunning) return;

        if (data.done_scope && data.next_scope !== scope) {
            cvLog('→ Chuyen sang scope: ' + data.next_scope, 'info');
        }

        // Next batch (small delay to avoid overwhelming)
        setTimeout(function() {
            cvRun(data.next_scope, data.next_offset, limit, month);
        }, 200);
    })
    .catch(function(err) {
        cvLog('❌ Loi: ' + err.message, 'err');
        document.getElementById('cv-state').textContent = 'Loi!';
        cvRunning = false;
        document.getElementById('cv-start').style.display = '';
        document.getElementById('cv-stop').style.display = 'none';
    });
}
</script>

{{-- ═══ CHART JS ═══ --}}
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<script>
(function () {
    var months = @json($month_list);
    var series = @json($series);

    if (!months.length) {
        return;
    }

    var colors = {
        1: '#ff8f3d',
        2: '#4dd6b7',
        3: '#7db5ff',
        4: '#f3c969',
        5: '#ff5f87'
    };

    var chart = echarts.init(document.getElementById('trend-chart'));
    function fmt(v) {
        if (v === null || v === undefined) return '-';
        try {
            return new Intl.NumberFormat('vi-VN', { maximumFractionDigits: 2 }).format(v);
        } catch (e) {
            return Number(v).toFixed(2);
        }
    }

    var seriesList = Object.keys(series).map(function (key) {
        var item = series[key];
        return {
            name: item.label,
            type: 'line',
            smooth: true,
            showSymbol: true,
            symbolSize: 6,
            data: item.values,
            lineStyle: { width: 2, color: colors[key] || '#fff' },
            itemStyle: { color: colors[key] || '#fff' }
        };
    });

    chart.setOption({
        backgroundColor: 'transparent',
        color: Object.keys(series).map(function (k) { return colors[k] || '#fff'; }),
        tooltip: {
            trigger: 'axis',
            axisPointer: { type: 'line' },
            backgroundColor: 'rgba(255,255,255,0.95)',
            borderColor: '#e6e6e6',
            borderWidth: 1,
            textStyle: { color: '#111' },
            formatter: function (params) {
                if (!params.length) return '';
                var idx = params[0].dataIndex;
                var html = '<div style="font-weight:600;margin-bottom:6px">' + months[idx] + '</div>';
                params.forEach(function (p) {
                    var key = Object.keys(series).find(function (k) { return series[k].label === p.seriesName; });
                    var item = series[key];
                    var median = item.values[idx];
                    var min = item.min[idx];
                    var max = item.max[idx];
                    var trimmed = item.trimmed[idx];
                    html += '<div style="margin-bottom:4px">'
                        + '<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:' + (colors[key] || '#111') + ';margin-right:6px"></span>'
                        + p.seriesName + ': ' + fmt(median) + ' tr/m²'
                        + '<br><span style="color:#666">Khoang gia: ' + fmt(min) + ' - ' + fmt(max) + ' tr/m²'
                        + (trimmed !== null ? ' | N sau cat: ' + fmt(trimmed) : '') + '</span>'
                        + '</div>';
                });
                return html;
            }
        },
        grid: { left: 80, right: 20, top: 20, bottom: 40, containLabel: true },
        xAxis: {
            type: 'category',
            data: months,
            axisLine: { lineStyle: { color: '#2c3240' } },
            axisLabel: { color: '#9aa3b2' },
            axisTick: { lineStyle: { color: '#2c3240' } }
        },
        yAxis: {
            type: 'value',
            axisLine: { lineStyle: { color: '#2c3240' } },
            axisLabel: { color: '#9aa3b2' },
            splitLine: { lineStyle: { color: '#2c3240', opacity: 0.4 } }
        },
        legend: {
            bottom: 0,
            textStyle: { color: '#9aa3b2' }
        },
        series: seriesList
    });

    window.addEventListener('resize', function () {
        chart.resize();
    });
})();
</script>
<script>
(function () {
    var buttons = document.querySelectorAll('.tab-btn');
    var panels = document.querySelectorAll('.tab-panel');
    buttons.forEach(function (btn) {
        btn.addEventListener('click', function () {
            buttons.forEach(function (b) { b.classList.remove('active'); });
            panels.forEach(function (p) { p.classList.remove('active'); });
            btn.classList.add('active');
            var target = document.getElementById(btn.dataset.tab);
            if (target) target.classList.add('active');
        });
    });
})();
</script>

</body>
</html>
