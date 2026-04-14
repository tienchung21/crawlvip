# MeeyMap detail -> scraped_details_flat update plan

Nguon:
- `GET https://apiv3.meeymap.com/api/article/detail/{code}`

Muc tieu:
- doc cac dong `meeymap.com` da co trong `scraped_details_flat`
- lay `matin`
- goi `detail/{matin}`
- update them cac cot dang can bo sung
- luu anh tu `media`

## 1) Nguon vao

Bang:
- `scraped_details_flat`

Filter uu tien:
- `domain = 'meeymap.com'`
- va mot trong cac dieu kien sau:
  - `mota IS NULL`
  - `sophongngu IS NULL`
  - `sophongvesinh IS NULL`
  - `huongnha IS NULL`
  - `huongbancong IS NULL`
  - `sotang IS NULL`
  - `thuocduan IS NULL`
  - `ward_ext IS NULL`
  - `street_ext IS NULL`
  - `lat IS NULL`
  - `lng IS NULL`

Khoa goi detail:
- `matin` -> dung lam `{code}` cho API detail

## 2) Field update tu detail




```text
scraped_details_flat.mota            <- detail.description
scraped_details_flat.sophongngu      <- detail.totalBedroom
scraped_details_flat.sophongvesinh   <- detail.totalBathroom
scraped_details_flat.huongnha        <- detail.directions.label
scraped_details_flat.huongbancong    <- detail.balconyDirection.label
scraped_details_flat.sotang          <- detail.totalFloor
scraped_details_flat.thuocduan       <- detail.project._id
scraped_details_flat.street_ext      <- detail.street.code
scraped_details_flat.ward_ext        <- detail.ward.code
scraped_details_flat.lat             <- detail.location.coordinates[1]
scraped_details_flat.lng             <- detail.location.coordinates[0]
```

## 3) Anh

Bang:
- `scraped_detail_images`

Luu tu:
- `detail.media`

Mapping:

```text
detail.media[i].url  -> scraped_detail_images.image_url
detail_id            -> id cua dong trong scraped_details_flat
idx                  -> thu tu anh
```

Ghi chu:
- `media` la mot mang, moi phan tu la 1 object anh rieng
- can loop tung `media[i]`, lay `media[i].url` de insert thanh tung dong
- khong luu ca object `media` vao 1 cot
- neu `mediaType = 1` thi xem la anh
- bo qua item nao khong co `url`
- `thumbnail` khong can luu rieng neu da co trong `media`
- neu can co the uu tien anh dau lam `idx = 0`

## 4) API response mau dang dung

Sample ban vua dua cho thay:

```text
code                 = 305806221
description          = co
media                = co
city.code            = 56
district.code        = 568
ward.code            = 22327
street.code          = co object street, dung street.code neu co
location.coordinates = [109.2102938, 12.298822]  -> [lng, lat]
totalBedroom         = null
totalBathroom        = null
totalFloor           = null
project              = null
directions           = null
balconyDirection     = []
```

## 5) De xuat tool

Ten file:
- `craw/meeymap_detail_updater.py`

Flow:
1. chon batch tu `scraped_details_flat`
2. lay `matin`
3. goi detail API
4. update cac cot o muc 2
5. luu anh o muc 3
6. ghi log/checkpoint

Log can co:
- `sdf_id`
- `matin`
- `status`
- `error`

Checkpoint nen co:
- `last_id`
- `last_matin`
- `done_count`
- `error_count`

## 6) Cac diem da chot theo note

1. `duongvao`: bo
2. `phaply`: bo
3. `noithat`: bo
4. `loaitin`: bo
5. `diachi`: bo
6. location-like fields: uu tien `code/id`, khong can `name`
7. `project`: lay id, khong lay name
8. `district_ext/city_ext`: khong doi, giu nhu phase search
9. `ward_id/street_id`: bo, khong dung
10. `loaibds`: tam thoi khong update
