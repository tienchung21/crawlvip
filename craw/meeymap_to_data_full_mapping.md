# MeeyMap -> scraped_details_flat mapping

Nguon:
- `search`: `POST https://apiv3.meeymap.com/api/article/search`
- `detail`: `GET https://apiv3.meeymap.com/api/article/detail/{code}`

Nguyen tac:
- phase 1 chi luu theo dung cac note ban da ghi o response `search`
- field nao ban khong chot hoac da note "bo"/`NULL` thi insert `NULL`
- phase 2 moi goi `detail` de update bo sung

## 1) Phase `search`

```text
scraped_details_flat.matin         <- search.code
scraped_details_flat.url           <- search.url
scraped_details_flat.domain        <- meeymap.com
scraped_details_flat.title         <- search.title
scraped_details_flat.khoanggia     <- search.priceLabel.totalPrice
scraped_details_flat.dientich      <- search.area
scraped_details_flat.chieungang    <- search.facade
scraped_details_flat.chieudai      <- search.depth
scraped_details_flat.tenmoigioi    <- search.createdBy.name
scraped_details_flat.sodienthoai   <- search.createdBy.phone
scraped_details_flat.ngaydang      <- search.createdAt
scraped_details_flat.city_ext      <- search.city.code
scraped_details_flat.district_ext  <- search.district.code
scraped_details_flat.loaihinh      <- search.typeOfHouse.value
scraped_details_flat.trade_type    <- IF(search.category.value = 'mua_ban', 's', 'u')
```

## 2) Phase `search` de `NULL`

```text
scraped_details_flat.img_count
scraped_details_flat.mota
scraped_details_flat.sophongngu
scraped_details_flat.sophongvesinh
scraped_details_flat.huongnha
scraped_details_flat.huongbancong
scraped_details_flat.mattien
scraped_details_flat.duongvao
scraped_details_flat.sotang
scraped_details_flat.loaihinhnhao
scraped_details_flat.dientichsudung
scraped_details_flat.gia_m2
scraped_details_flat.gia_mn
scraped_details_flat.dacdiemnhadat
scraped_details_flat.phaply
scraped_details_flat.noithat
scraped_details_flat.thuocduan
scraped_details_flat.trangthaiduan
scraped_details_flat.map
scraped_details_flat.loaitin
scraped_details_flat.ngayhethan
scraped_details_flat.diachi
scraped_details_flat.city_code
scraped_details_flat.district_id
scraped_details_flat.ward_id
scraped_details_flat.street_id
scraped_details_flat.ward_ext
scraped_details_flat.street_ext
scraped_details_flat.lat
scraped_details_flat.lng
scraped_details_flat.loaibds
```

## 3) Phase `detail` update sau

```text
scraped_details_flat.sophongngu      <- detail.totalBedroom
scraped_details_flat.sophongvesinh   <- detail.totalBathroom
scraped_details_flat.huongnha        <- detail.directions.label
scraped_details_flat.huongbancong    <- detail.balconyDirection.label
scraped_details_flat.duongvao        <- detail.wideRoadLabel
scraped_details_flat.sotang          <- detail.totalFloor
scraped_details_flat.phaply          <- detail.legalPaperLabel
scraped_details_flat.noithat         <- detail.furniture
scraped_details_flat.thuocduan       <- detail.project.name
scraped_details_flat.loaitin         <- detail.userTypesLabel
scraped_details_flat.diachi          <- detail.addressLabel
scraped_details_flat.ward_ext        <- detail.ward.translation[0].name
scraped_details_flat.street_ext      <- detail.street.translation[0].name
scraped_details_flat.ward_id         <- detail.ward.code
scraped_details_flat.street_id       <- detail.street.code
scraped_details_flat.lat             <- detail.location.coordinates[1]
scraped_details_flat.lng             <- detail.location.coordinates[0]
scraped_details_flat.loaibds         <- detail.typeOfHouse.name
```

## 4) Note da doc tu file ban sua

```text
- trade_type: mua_ban = s, nguoc lai = u
- city_code: bo
- district_id: bo
- city_ext: dung search.city.code
- district_ext: dung search.district.code
- mota: de NULL
- mattien: bo
- sodienthoai: dung phone thang
- diachi: bo
- ward_id/street_id: bo o phase search
```

## 5) Ve viec insert `NULL`

```text
- insert NULL khong lam cham query theo cach dang ke
- MySQL xu ly NULL binh thuong
- chi loi neu cot do bi NOT NULL
- trong case nay, de NULL o cac cot phu la on
```
