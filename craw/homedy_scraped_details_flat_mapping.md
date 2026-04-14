# Homedy -> scraped_details_flat

Nguon:
- file mau: `craw/responsehomdy.json`
- dich den: `scraped_details_flat`
- domain co dinh: `homedy.com`

## Ghi chu chung

- `created_at`: khong can map, DB tu set bang `CURRENT_TIMESTAMP`
- `url`: ghep thanh `https://homedy.com/` + `Products[].Url`
- `img_count`: lay tu `MediaCount`
- anh: parse `MediaJson` va insert vao `scraped_detail_images`

## Mapping da chot

```text
Homedy field                         -> scraped_details_flat column
Id                                   -> matin
Name                                 -> title
Description                          -> mota
Address                              -> diachi
HtmlPriceTotal + HtmlPriceTotalCurrency -> khoanggia
Acreage                              -> dientich
BedRoom                              -> sophongngu
BathRoom                             -> sophongvesinh
StartDate                            -> ngaydang
Agency.FullName                      -> tenmoigioi
Agency.Mobile                        -> sodienthoai 
Latitude                             -> lat
Longitude                            -> lng 
CityId                               -> city_ext
DistrictId                           -> district_ext
WardId                               -> ward_ext
StreetId                             -> street_ext
TypeId                               -> trade_type   (1 = s, 2 = u)
CategoryId                           -> loaihinh
MediaCount                           -> img_count
'https://homedy.com/'.Url            -> url
'homedy.com'                         -> domain
```

## Rule xu ly cu the

### 1. Gia

- `khoanggia = HtmlPriceTotal + ' ' + HtmlPriceTotalCurrency`
- Vi du: `2,68` + `Tỷ` -> `2,68 Tỷ`

### 2. Dien tich

- uu tien `Acreage`
- neu can chuoi hien thi thi fallback `HtmlAcreage`

### 3. Vi tri / ma khu vuc

- `city_id = CityId`
- `ward_id = WardId`
- `street_id = StreetId`
- Tam thoi dung ID nguon de dua vao cac cot ext/raw:
  - `city_ext = CityId`
  - `district_ext = DistrictId`
  - `ward_ext = WardId`
  - `street_ext = StreetId`

### 4. Giao dich

- `trade_type = TypeId`
- quy uoc:
  - `1 = s`
  - `2 = u`

