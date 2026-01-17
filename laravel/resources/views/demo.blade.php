<!doctype html>
<html lang="vi">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Demo Stats</title>
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
        }
        body { background: var(--bg); color: var(--text); font-family: Arial, sans-serif; margin: 0; }
        .container { max-width: 1200px; margin: 24px auto; padding: 0 16px; }
        h1 { margin: 0 0 8px; font-size: 28px; }
        .subtitle { color: var(--muted); margin-bottom: 16px; }
        .filters { display: grid; grid-template-columns: 2fr 2fr 1fr 1fr 1fr; gap: 12px; margin-bottom: 16px; }
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
    </style>
</head>
<body>
<div class="container">
    <h1>Demo Stats</h1>
    <div class="subtitle">Thong ke median theo group, khu vuc, va thang.</div>

    <form method="get">
        <div class="filters">
            <div>
                <label>Khu vuc</label>
                <select name="region_id">
                    <option value="">Tat ca</option>
                    @foreach ($regions as $r)
                        <option value="{{ $r->region_id }}" {{ ($filters['region_id'] == $r->region_id) ? 'selected' : '' }}>
                            {{ $r->region_name }}
                        </option>
                    @endforeach
                </select>
            </div>
            <div>
                <label>Phuong/xa</label>
                <select name="ward_id">
                    <option value="">Tat ca</option>
                    @foreach ($wards as $w)
                        <option value="{{ $w->ward_id }}" {{ ($filters['ward_id'] == $w->ward_id) ? 'selected' : '' }}>
                            {{ $w->ward_name }}
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
        <div class="card">
            <table>
                <thead>
                    <tr>
                        <th>Khu vuc</th>
                        <th>Phuong/xa</th>
                        <th>Thang</th>
                        <th>Median group</th>
                        <th>Tong so tin</th>
                    </tr>
                </thead>
                <tbody>
                @forelse ($smallRows as $row)
                    <tr>
                        <td>{{ $row->new_region_name ?? $row->new_region_id }}</td>
                        <td>{{ $row->new_ward_name ?? $row->new_ward_id }}</td>
                        <td>{{ $row->month }}</td>
                        <td>{{ $row->median_group }} - {{ $medianGroupLabels[$row->median_group] ?? '' }}</td>
                        <td>{{ number_format($row->total_rows) }}</td>
                    </tr>
                @empty
                    <tr><td colspan="5">empty</td></tr>
                @endforelse
                </tbody>
            </table>
        </div>
    </div>
</div>
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
        4: '#f3c969'
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
                        + p.seriesName + ': ' + fmt(median) + ' tr/m?'
                        + '<br><span style="color:#666">Khoang gia: ' + fmt(min) + ' - ' + fmt(max) + ' tr/m?'
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
