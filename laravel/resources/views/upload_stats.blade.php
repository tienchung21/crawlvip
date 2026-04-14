<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Dashboard Upload Tin</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 16px; color: #111; }
        .row { display: flex; gap: 12px; flex-wrap: wrap; }
        .card { border: 1px solid #ddd; border-radius: 8px; padding: 12px; min-width: 220px; background: #fff; }
        .kpi { font-size: 28px; font-weight: 700; margin: 6px 0 0; }
        .muted { color: #666; font-size: 12px; }
        h1 { margin: 0 0 12px; }
        h2 { margin: 18px 0 8px; font-size: 16px; }
        form { border: 1px solid #eee; border-radius: 8px; padding: 12px; background: #fafafa; }
        label { font-size: 12px; color: #333; display: block; margin-bottom: 4px; }
        input, select { padding: 6px 8px; border: 1px solid #ccc; border-radius: 6px; }
        button { padding: 7px 10px; border: 1px solid #111; background: #111; color: #fff; border-radius: 6px; cursor: pointer; }
        table { width: 100%; border-collapse: collapse; margin-top: 8px; }
        th, td { border: 1px solid #eee; padding: 8px; text-align: left; font-size: 13px; }
        th { background: #f5f5f5; }
        .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
        @media (max-width: 900px) { .grid2 { grid-template-columns: 1fr; } }
        .topbar { display:flex; justify-content: space-between; align-items:center; gap:12px; flex-wrap:wrap; }
        a { color: #0b57d0; text-decoration: none; }
        a:hover { text-decoration: underline; }
    </style>
</head>
<body>
    <div class="topbar">
        <h1>Dashboard Upload Tin (API)</h1>
        <div class="muted">
            Nguồn: <code>data_full.uploaded_at</code> + <code>images_status</code>
        </div>
    </div>

    <form method="get" action="{{ route('upload-stats') }}">
        <div class="row">
            <div>
                <label>Từ ngày</label>
                <input type="date" name="from" value="{{ $from }}">
            </div>
            <div>
                <label>Đến ngày</label>
                <input type="date" name="to" value="{{ $to }}">
            </div>
            <div style="align-self:end;">
                <button type="submit">Lọc</button>
            </div>
        </div>
    </form>

    <div class="row" style="margin-top:12px;">
        <div class="card">
            <div class="muted">Tổng tin đẩy thành công</div>
            <div class="kpi">{{ number_format($totalUploaded) }}</div>
            <div class="muted">Trong khoảng ngày đã chọn</div>
        </div>
        <div class="card">
            <div class="muted">Tin trùng (skip)</div>
            <div class="kpi">{{ number_format($totalDuplicate) }}</div>
            <div class="muted">DUPLICATE_SKIPPED</div>
        </div>
        <div class="card">
            <div class="muted">Tin có số điện thoại</div>
            <div class="kpi">{{ number_format($withPhone) }}</div>
            <div class="muted">Chỉ tính LISTING_UPLOADED</div>
        </div>
    </div>

    <div class="grid2" style="margin-top:12px;">
        <div class="card">
            <h2>Theo ngày</h2>
            <table>
                <thead>
                    <tr>
                        <th>Ngày</th>
                        <th>Số tin (OK)</th>
                    </tr>
                </thead>
                <tbody>
                    @forelse ($daily as $r)
                        <tr>
                            <td>{{ $r->day }}</td>
                            <td>{{ number_format($r->total) }}</td>
                        </tr>
                    @empty
                        <tr><td colspan="2" class="muted">Chưa có dữ liệu trong khoảng này.</td></tr>
                    @endforelse
                </tbody>
            </table>
        </div>

        <div class="card">
            <h2>Loại hình (bán/thuê + property_type)</h2>
            <table>
                <thead>
                    <tr>
                        <th>Type</th>
                        <th>Property Type</th>
                        <th>Số tin</th>
                    </tr>
                </thead>
                <tbody>
                    @forelse ($byType as $r)
                        <tr>
                            <td>{{ $r->type }}</td>
                            <td>{{ $r->property_type }}</td>
                            <td>{{ number_format($r->total) }}</td>
                        </tr>
                    @empty
                        <tr><td colspan="3" class="muted">Chưa có dữ liệu.</td></tr>
                    @endforelse
                </tbody>
            </table>
        </div>
    </div>

    <div class="grid2" style="margin-top:12px;">
        <div class="card">
            <h2>Khu vực: Tỉnh/TP (top 30)</h2>
            <table>
                <thead>
                    <tr>
                        <th>Province</th>
                        <th>ID</th>
                        <th>Số tin</th>
                    </tr>
                </thead>
                <tbody>
                    @forelse ($byProvince as $r)
                        <tr>
                            <td>{{ $r->province_name }}</td>
                            <td>{{ $r->province_id }}</td>
                            <td>{{ number_format($r->total) }}</td>
                        </tr>
                    @empty
                        <tr><td colspan="3" class="muted">Chưa có dữ liệu.</td></tr>
                    @endforelse
                </tbody>
            </table>
        </div>

        <div class="card">
            <h2>Xã/Phường trong tỉnh đã chọn</h2>
            <div class="muted">Tên lấy từ <code>transaction_city_merge</code> (action_type=0)</div>
            <form method="get" action="{{ route('upload-stats') }}" style="margin-top:10px;">
                <input type="hidden" name="from" value="{{ $from }}">
                <input type="hidden" name="to" value="{{ $to }}">
                <div class="row">
                    <div>
                        <label>Tỉnh/TP</label>
                        <select name="ward_province_id">
                            <option value="0" {{ (int)$wardProvinceId === 0 ? 'selected' : '' }}>Tất cả</option>
                            @foreach ($provinceOptions as $p)
                                <option value="{{ $p->id }}" {{ (int)$wardProvinceId === (int)$p->id ? 'selected' : '' }}>
                                    {{ $p->name }} ({{ $p->id }})
                                </option>
                            @endforeach
                        </select>
                    </div>
                    <div>
                        <label>Sắp xếp</label>
                        <select name="ward_sort">
                            <option value="total_desc" {{ $wardSort === 'total_desc' ? 'selected' : '' }}>Số tin giảm dần</option>
                            <option value="province_asc_total_desc" {{ $wardSort === 'province_asc_total_desc' ? 'selected' : '' }}>Tỉnh A-Z, số tin giảm dần</option>
                            <option value="province_desc_total_desc" {{ $wardSort === 'province_desc_total_desc' ? 'selected' : '' }}>Tỉnh Z-A, số tin giảm dần</option>
                            <option value="province_asc_name_asc" {{ $wardSort === 'province_asc_name_asc' ? 'selected' : '' }}>Tỉnh A-Z, phường/xã A-Z</option>
                            <option value="ward_name_asc" {{ $wardSort === 'ward_name_asc' ? 'selected' : '' }}>Phường/xã A-Z (khi chọn 1 tỉnh)</option>
                        </select>
                    </div>
                    <div style="align-self:end;">
                        <button type="submit">Lọc riêng</button>
                    </div>
                </div>
            </form>
            <table>
                <thead>
                    <tr>
                        @if ((int)$wardProvinceId === 0)
                            <th>Tỉnh/TP</th>
                            <th>Province ID</th>
                        @endif
                        <th>Xã/Phường</th>
                        <th>ID</th>
                        <th>Số tin</th>
                    </tr>
                </thead>
                <tbody>
                    @forelse ($byWard as $r)
                        <tr>
                            @if ((int)$wardProvinceId === 0)
                                <td>{{ $r->province_name }}</td>
                                <td>{{ $r->province_id }}</td>
                            @endif
                            <td>{{ $r->ward_name }}</td>
                            <td>{{ $r->ward_id }}</td>
                            <td>{{ number_format($r->total) }}</td>
                        </tr>
                    @empty
                        <tr><td colspan="{{ ((int)$wardProvinceId === 0) ? 5 : 3 }}" class="muted">Chưa có dữ liệu.</td></tr>
                    @endforelse
                </tbody>
            </table>
        </div>
    </div>

    <p class="muted" style="margin-top:14px;">
        Gợi ý: Muốn có thống kê theo ngày chính xác, chỉ tính các tin có <code>uploaded_at</code> và <code>images_status='LISTING_UPLOADED'</code>.
    </p>
</body>
</html>
