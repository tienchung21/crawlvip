<!doctype html>
<html lang="vi">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Histogram Data Clean</title>
    <style>
        body { background:#0f1116; color:#e6e6e6; font-family: Arial, sans-serif; margin:0; }
        .container { max-width:1200px; margin:24px auto; padding:0 16px; }
        h1 { margin:0 0 8px; font-size:28px; }
        .subtitle { color:#a0a0a0; margin-bottom:16px; }
        .filters { display:grid; grid-template-columns:2fr 1fr 1fr 1fr; gap:12px; margin-bottom:16px; }
        .filters label { display:block; font-size:12px; color:#b0b0b0; margin-bottom:6px; }
        .filters select, .filters input { width:100%; padding:8px; background:#1b1f27; color:#fff; border:1px solid #2c3240; border-radius:6px; }
        .btn { padding:8px 12px; background:#2563eb; color:#fff; border:0; border-radius:6px; cursor:pointer; }
        .card { background:#1b1f27; padding:12px; border-radius:8px; }
        #chart { height:460px; }
        .note { color:#a0a0a0; font-size:12px; margin-top:8px; }
    </style>
</head>
<body>
<div class="container">
    <h1>Histogram gia trung binh (price_m2_vnd)</h1>
    <div class="subtitle">Thong ke so luong tin theo khoang gia (tu dong chia 10 nhom), loc theo median group.</div>

    <form method="get">
        <div class="filters">
            <div>
                <label>Median group</label>
                <select name="median_group">
                    <option value="">Tat ca</option>
                    @foreach ($medianGroupLabels as $id => $label)
                        <option value="{{ $id }}" {{ ($filters['median_group'] == $id) ? 'selected' : '' }}>
                            {{ $id }} - {{ $label }}
                        </option>
                    @endforeach
                </select>
            </div>
            <div>
                <label>Type</label>
                <select name="type">
                    <option value="">Tat ca</option>
                    @foreach ($types as $t)
                        <option value="{{ $t }}" {{ ($filters['type'] == $t) ? 'selected' : '' }}>
                            {{ $t }}
                        </option>
                    @endforeach
                </select>
            </div>
            <div>
                <label>Category</label>
                <select name="category">
                    <option value="">Tat ca</option>
                    @foreach ($categories as $c)
                        <option value="{{ $c }}" {{ ($filters['category'] == $c) ? 'selected' : '' }}>
                            {{ $c }}
                        </option>
                    @endforeach
                </select>
            </div>
            <div style="display:flex; align-items:flex-end;">
                <button class="btn" type="submit">Loc</button>
            </div>
        </div>
    </form>

    <div class="card" style="margin-bottom:12px;">
        <div class="label">Tong so tin</div>
        <div class="value">{{ number_format($totalCount) }}</div>
    </div>

    <div class="card">
        <div id="chart"></div>
        <div class="note">Moi cot la so luong tin dang trong mot khoang gia.</div>
    </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<script>
    (function () {
        var labels = @json($labels);
        var counts = @json($counts);
        var avgPrices = @json($avgPrices);

        var chart = echarts.init(document.getElementById("chart"));

        function formatNumber(n) {
            if (n === null || n === undefined) return "-";
            return new Intl.NumberFormat("vi-VN").format(n);
        }
        function formatRange(label) {
            if (!label) return "-";
            var parts = String(label).split(" - ");
            if (parts.length !== 2) return label;
            var start = Number(parts[0]);
            var end = Number(parts[1]);
            if (Number.isNaN(start) || Number.isNaN(end)) return label;
            return formatNumber(start) + " - " + formatNumber(end);
        }

        chart.setOption({
            backgroundColor: "transparent",
            grid: { left: 60, right: 40, top: 30, bottom: 60, containLabel: true },
            tooltip: {
                trigger: "axis",
                axisPointer: { type: "shadow" },
                formatter: function (params) {
                    var idx = params[0].dataIndex;
                    var count = counts[idx];
                    var avg = avgPrices[idx];
                    return (
                        formatRange(labels[idx]) + "<br/>" +
                        "So tin: " + formatNumber(count) + "<br/>" +
                        "Gia TB: " + formatNumber(avg)
                    );
                }
            },
            xAxis: {
                type: "category",
                data: labels,
                axisLabel: {
                    color: "#b0b0b0",
                    rotate: 30,
                    formatter: function (value) {
                        return formatRange(value);
                    }
                }
            },
            yAxis: [
                {
                    type: "value",
                    name: "So tin",
                    axisLabel: { color: "#b0b0b0" },
                    nameTextStyle: { color: "#b0b0b0" }
                },
                {
                    type: "value",
                    name: "Gia TB",
                    axisLabel: { color: "#b0b0b0" },
                    nameTextStyle: { color: "#b0b0b0" }
                }
            ],
            series: [
                {
                    name: "So tin",
                    type: "bar",
                    data: counts,
                    itemStyle: { color: "#60a5fa" }
                },
                {
                    name: "Gia TB",
                    type: "line",
                    yAxisIndex: 1,
                    data: avgPrices,
                    smooth: true,
                    itemStyle: { color: "#f59e0b" }
                }
            ]
        });

        window.addEventListener("resize", function () {
            chart.resize();
        });
    })();
</script>
</body>
</html>
