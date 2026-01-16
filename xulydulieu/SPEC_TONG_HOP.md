# SPEC TONG HOP TRIEN KHAI
# MODULE PHAN TICH GIA & THANH KHOAN BDS
# Nguon du lieu
# Crawler thi truong
# nhadat.cafeland.vn

I. MUC TIEU HE THONG
Xay dung he thong phan tich nham:
1. Gia & thi truong
Tao mat bang gia chuan theo:

- Khu vuc (toi cap duong, co dieu kien)
- Loai hinh BDS
- Hinh thuc giao dich (ban / thue)

Xac dinh:
egion_v2 = 13000    

2. Thanh khoan
Danh gia muc do thanh khoan theo khu vuc
Phan anh toc do hap thu thi truong

3. Phuc vu
UI nguoi dung
SEO landing page gia
Bao cao noi bo / phan tich chien luoc

II. PHAM VI & PHAN NHOM DU LIEU
1. Cap khu vuc (area_type)
- city
- district
- ward
- street (bat co dieu kien)

2. Loai hinh BDS (property_type)
- apartment
- house
- land
- villa
- office
- warehouse

3. Hinh thuc giao dich (sale_type)
- sell
- rent

NGUYEN TAC BAT BIEN
- Khong so sanh khac loai hinh
- Khong so sanh ban voi thue
- Khong so sanh hai duong khac nhau
- Khong tron nguon du lieu trong cung chi so

III. NGUON DU LIEU & QUY TAC UU TIEN
Source | Dac diem | Chi so su dung
crawler | Gia thi truong rong | Gia, DOM
nhadat.cafeland.vn | Co hanh vi nguoi dung | Gia, DOM, interest, removed

Uu tien nguon
Neu cung to hop: (area + property_type + sale_type)
- Uu tien nhadat.cafeland.vn
- Fallback sang crawler neu thieu
- Khong cong gop 2 nguon

IV. NHOM 1 - CHUAN HOA & LOC DU LIEU (BAT BUOC)
1. Du lieu dau vao
- price_total_vnd
- area_m2
- created_at
- area_id
- street_id (neu co)

2. Chuan hoa
- price_per_m2 = price_total_vnd / area_m2

3. Dieu kien loai bo
Loai tin neu:
- price_total_vnd IS NULL
- area_m2 <= 0
- price_type IN ('thoa_thuan', 'lien_he')
- price_per_m2 < min_price_threshold(area)
- price_per_m2 > max_price_threshold(area)
- area_m2 < min_area_by_property_type
- price_per_m2 lech > 200% so voi median khu vuc

Luu y: de y don vi tien te, don vi tinh (gia/Tong dien tich hay gia/m2)

V. NHOM 2 - PHAN NHOM PHAN TICH (SEGMENTATION)
Moi tap tinh toan theo to hop:
(area_type, area_id, property_type, sale_type)

Dieu kien bat cap duong
total_listing >= min_listing_street (config, goi y = 15)

Fallback bat buoc
street -> ward

VI. NHOM 3 - GIA MAT BANG (CORE PRICE METRICS)
1. Cat bien outlier
Ap dung khi:
total_listing >= 20
- Loai 10% thap nhat
- Loai 10% cao nhat

2. Chi so chinh
- median_price_per_m2 (chi so loi)
- avg_price_per_m2
- min_price_per_m2
- max_price_per_m2
- total_listing

VII. NHOM 4 - DO TIN CAY DU LIEU (CONFIDENCE)
So tin | Muc
< 10 | Low
10-30 | Medium
> 30 | High

Quy tac hien thi
- Low -> "Gia tham khao"
- Medium -> Hien thi + canh bao
- High -> Hien thi day du

VIII. NHOM 5 - THANH KHOAN THI TRUONG
1. DOM
DOM = today - created_at
DOM_score = 1 - (DOM / max_DOM)

2. Ty le tin go
removed_ratio = removed_30d / total_30d

3. Muc do quan tam
interest_score =
views * 0.3
+ calls * 0.4
+ saves * 0.3

IX. NHOM 6 - DIEM THANH KHOAN TONG HOP
Cong thuc
liquidity_score =
DOM_score * 0.4
+ removed_ratio * 0.3
+ interest_score * 0.3

Quy tac theo nguon
nhadat.cafeland.vn -> full
crawler -> liquidity_score = DOM_score * 1

Chuan hoa & phan loai
Diem | Muc
>= 80 | Rat tot
60-79 | Tot
40-59 | Trung binh
< 40 | Kem

X. NHOM 7 - DINH GIA TIN DANG (PRICE POSITIONING)
Cong thuc
price_index =
listing.price_per_m2 / median_price_area

Phan loai
Gia tri | Nhan dinh
< 0.9 | Thap hon thi truong
0.9-1.1 | Phu hop thi truong
> 1.1 | Cao hon thi truong

UI khong dung tu "re / dat"

XI. NHOM 8 - BIEN DONG GIA THEO THOI GIAN
1. Don vi
period_type: month
period_key: YYYY-MM

2. Cong thuc
price_change_value =
median_current - median_previous

price_change_percent =
(price_change_value / median_previous) * 100

3. Phan loai xu huong
% | Xu huong
> +3% | Tang
-3% -> +3% | Di ngang
< -3% | Giam

XII. NHOM 8A - DIEU KIEN TINH TREND
Cap | Tin/thang
street | >= 10
ward | >= 15
district | >= 30

Khong du -> khong hien thi trend

XIII. NHOM 8B - LAM MIN (ANTI-NOISE)
median_3m = (m-2 + m-1 + m) / 3
Uu tien dung median_3m cho UI

XIV. NHOM 9 - CONFIG DONG (KHONG HARDCODE)
- min_price_threshold_by_city
- max_price_threshold_by_city
- trim_percent
- min_listing_street
- min_listing_monthly
- liquidity_weights
- price_trend_threshold
- moving_average_months
- enable_street_level

XV. NHOM 10 - LUU TRU DU LIEU
area_price_stat
- area_type
- area_id
- property_type
- sale_type
- median_price_per_m2
- total_listing
- confidence
- liquidity_score
- data_source
- calculated_at

area_price_trend
- area_type
- area_id
- property_type
- sale_type
- period_key
- median_price
- change_percent
- trend
- smoothed

XVI. NHOM 11 - CO CHE TINH TOAN
Batch job (cron)
Khong realtime

Chu ky
- District: hang ngay
- Ward / Street: 2-3 ngay

XVII. NGUYEN TAC HIEN THI & SEO
- Luon co fallback
- Luon canh bao khi data yeu
- Khong so sanh cheo khu vuc

Text SEO mau
"Gia can ho duong Nguyen Kiem thang 01/2026 tang 4,2% so voi thang truoc"

XVIII. GIAI DOAN TRIEN KHAI
Phase 1
- Chuan hoa du lieu
- Median cap phuong

Phase 2
- Thanh khoan
- Price positioning

Phase 3
- Bien dong theo thang

Phase 4
- Cap duong (street)

XIX. KET LUAN CHOT
- Trung vi = chi so loi
- Bien dong = dinh huong
- Thanh khoan = tham khao
- Khong du data -> khong phan tich
- Street la level cao cap, khong bat mac dinh
