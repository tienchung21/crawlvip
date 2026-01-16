<!doctype html>
<html lang="vi">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Data Clean Viewer</title>
    <style>
        body { background:#0f1116; color:#e6e6e6; font-family: Arial, sans-serif; margin:0; }
        .container { max-width:1200px; margin:24px auto; padding:0 16px; }
        h1 { margin:0 0 8px; font-size:28px; }
        .subtitle { color:#a0a0a0; margin-bottom:16px; }
        .filters { display:grid; grid-template-columns:2fr 2fr 1fr 1fr 1fr 1fr; gap:12px; margin-bottom:16px; }
        .filters label { display:block; font-size:12px; color:#b0b0b0; margin-bottom:6px; }
        .filters select, .filters input { width:100%; padding:8px; background:#1b1f27; color:#fff; border:1px solid #2c3240; border-radius:6px; }
        .stats { display:grid; grid-template-columns:1fr 1fr 1fr; gap:12px; margin:16px 0; }
        .card { background:#1b1f27; padding:12px; border-radius:8px; }
        .card .label { color:#b0b0b0; font-size:12px; }
        .card .value { font-size:20px; margin-top:6px; }
        table { width:100%; border-collapse:collapse; margin-top:12px; }
        th, td { padding:8px; border-bottom:1px solid #2c3240; font-size:12px; }
        th { text-align:left; color:#b0b0b0; }
        .note { color:#a0a0a0; font-size:12px; margin-top:8px; }
        .btn { padding:8px 12px; background:#2563eb; color:#fff; border:0; border-radius:6px; cursor:pointer; }
    </style>
</head>
<body>
<div class="container">
    <h1>Data Clean Viewer</h1>
    <div class="subtitle">Hien thi du lieu tu bang data_clean.</div>
    @if (session('status'))
        <div class="note">{{ session('status') }}</div>
    @endif


    <form method="get" id="filters-form">
        <div class="filters">
            <div>
                <label>Tinh/Thanh pho moi</label>
                <select name="region_id" id="region_id">
                    <option value="">Tat ca</option>
                    @foreach ($regions as $r)
                        <option value="{{ $r->city_id }}" {{ ($filters['region_id'] == $r->city_id) ? 'selected' : '' }}>
                            {{ $r->city_title_news }}
                        </option>
                    @endforeach
                </select>
            </div>
            <div>
                <label>Phuong/xa moi</label>
                <select name="ward_id">
                    <option value="">Tat ca</option>
                    @foreach ($wards as $w)
                        <option value="{{ $w->city_id }}" {{ ($filters['ward_id'] == $w->city_id) ? 'selected' : '' }}>
                            {{ $w->new_name }}
                        </option>
                    @endforeach
                </select>
            </div>
            <div>
                <label>So dong</label>
                <input type="number" name="limit" value="{{ $filters['limit'] }}">
            </div>
            <div>
                <label>Type</label>
                <select name="type">
                    <option value="">Tat ca</option>
                    @foreach ($types as $t)
                        <option value="{{ $t }}" {{ ($filters['type'] == $t) ? 'selected' : '' }}>
                            {{ $t }}{{ isset($typeLabels[$t]) ? ' - ' . $typeLabels[$t] : '' }}
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
                            {{ $c }}{{ isset($categoryLabels[$c]) ? ' - ' . $categoryLabels[$c] : '' }}
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
        </div>
        <button class="btn" type="submit">Loc</button>
    </form>
    
    <form method="post" action="{{ route('data-clean.convert') }}">
        @csrf
        <button class="btn" type="submit">Convert stats</button>
    </form>
    <button class="btn" type="button" id="convert-batch" style="margin-left:8px;">Convert stats (batch)</button>
    <div class="note" id="convert-progress"></div>


    <div class="stats">
        <div class="card">
            <div class="label">Tong so dong</div>
            <div class="value">{{ $total }}</div>
        </div>
        <div class="card">
            <div class="label">Gia trung binh</div>
            <div class="value">{{ $avg !== null ? number_format($avg, 2) : '-' }}</div>
        </div>
        <div class="card">
            <div class="label">Gia trung vi (cat 10%)</div>
            <div class="value">{{ $medianTrim !== null ? number_format($medianTrim, 2) : '-' }}</div>
        </div>
    </div>
    <div class="note">
        Cong thuc: sap xep price_m2_vnd tang dan, loai 10% thap nhat + 10% cao nhat,
        N = so mau con lai. Neu N le -> lay phan tu giua; neu N chan -> (A[N/2 - 1] + A[N/2]) / 2.
    </div>

    <table>
        <thead>
            <tr>
                <th>ad_id</th>
                <th>list_id</th>
                <th>list_time</th>
                <th>list_month</th>
                <th>region_v2</th>
                <th>area_v2</th>
                <th>cf_ward_name_new</th>
                <th>street_name</th>
                <th>street_number</th>
                <th>unique_street_id</th>
                <th>category</th>
                <th>size</th>
                <th>price</th>
                <th>type</th>
                <th>time_crawl</th>
                <th>price_m2_vnd</th>
            </tr>
        </thead>
        <tbody>
        @forelse ($rows as $row)
            <tr>
                <td>{{ $row->ad_id }}</td>
                <td>{{ $row->list_id }}</td>
                <td>{{ $row->list_time }}</td>
                <td>{{ $row->list_month }}</td>
                <td>{{ $row->region_v2 }}</td>
                <td>{{ $row->area_v2 }}</td>
                <td>{{ $row->ward }}</td>
                <td>{{ $row->street_name }}</td>
                <td>{{ $row->street_number }}</td>
                <td>{{ $row->unique_street_id }}</td>
                <td>{{ $row->category }}</td>
                <td>{{ $row->size }}</td>
                <td>{{ $row->price }}</td>
                <td>{{ $row->type }}</td>
                <td>{{ $row->time_crawl }}</td>
                <td>{{ $row->price_m2_vnd }}</td>
            </tr>
        @empty
            <tr><td colspan="16">empty</td></tr>
        @endforelse
        </tbody>
    </table>
</div>
<script>
    (function () {
        var form = document.getElementById("filters-form");
        var region = document.getElementById("region_id");
        if (form && region) {
            region.addEventListener("change", function () {
                form.submit();
            });
        }
    })();
</script>
<script>
    (function () {
        var form = document.getElementById("filters-form");
        var region = document.getElementById("region_id");
        if (form && region) {
            region.addEventListener("change", function () {
                form.submit();
            });
        }

        var btn = document.getElementById("convert-batch");
        var progress = document.getElementById("convert-progress");
        if (btn && progress) {
            var running = false;
            btn.addEventListener("click", async function () {
                if (running) return;
                running = true;
                btn.disabled = true;
                progress.textContent = "Dang chay...";
                var startedAt = Date.now();

                var scope = "ward";
                var offset = 0;
                var limit = 500;  // Increased from 50 for faster processing
                var totalInserted = 0;

                while (true) {
                    var body = new URLSearchParams();
                    body.append("scope", scope);
                    body.append("offset", String(offset));
                    body.append("limit", String(limit));

                    var resp = await fetch("{{ route('data-clean.convert-batch') }}", {
                        method: "POST",
                        headers: {
                            "X-CSRF-TOKEN": "{{ csrf_token() }}",
                            "Content-Type": "application/x-www-form-urlencoded",
                        },
                        body: body.toString(),
                    });

                    if (!resp.ok) {
                        progress.textContent = "Loi: " + resp.status + " " + resp.statusText;
                        break;
                    }

                    var data = await resp.json();
                    var elapsed = Math.round((Date.now() - startedAt) / 1000);
                    totalInserted += data.processed;
                    progress.textContent = "Scope=" + data.scope + " | offset=" + data.offset + " | batch=" + data.processed + " | total=" + totalInserted + " | elapsed=" + elapsed + "s";

                    if (data.done) {
                        progress.textContent += " | Done (all scopes)";
                        break;
                    }

                    // Continue with next batch
                    if (data.done_scope) {
                        scope = data.next_scope;
                        offset = data.next_offset;
                        progress.textContent += " | Switching to scope: " + scope;
                    } else {
                        offset = data.next_offset;
                    }
                }

                btn.disabled = false;
                running = false;
            });
        }
    })();
</script>
</body>
</html>