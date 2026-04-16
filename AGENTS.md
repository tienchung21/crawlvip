# Crawlvip: Daily Crawl + ETL + Upload (Runbook)

Tai lieu nay mo ta pipeline crawl/convert/upload hang ngay theo cac file/script hien co trong repo nay.

## 1) Tong quan luong du lieu

1. Listing crawl (thu thap link tin)
   - Script: `craw/daily_mogi_crawl.py` (phase 1)
   - Crawler: `craw/manual_mogi_rent.py` (`ManualMogiCrawler.run_crawling`)
   - Output: ghi link vao MySQL table `collected_links` (domain = `mogi.vn`)

2. Detail crawl (cao chi tiet tung tin)
   - Script: `craw/daily_mogi_crawl.py` (phase 2) chay `craw/mogi_detail_crawler.py` qua subprocess
   - Output: ghi data vao `scraped_details_flat`, cap nhat trang thai link trong `collected_links`

3. ETL Mogi (chuan hoa sang `data_full`)
   - Script: `craw/auto/run_mogi_etl.py`
   - SQL logic: `craw/auto/sql_convert_mogi.sql`
   - Output: insert/update vao `data_full` + backfill `slug_name`

4. Convert Alonhadat (chuan hoa sang `data_full`)
   - Script: `craw/auto/convert_alonhadat_to_data_full.py`
   - Input: `scraped_details_flat` (domain `alonhadat.com.vn`)
   - Output: insert/update vao `data_full`
   - Ghi chu:
     - Match vung bang `transaction_city_merge`
     - Match du an bang `duan_alonhadat_duan_merge`

5. Convert Guland (chuan hoa sang `data_full`)
   - Script: `craw/auto/convert_guland_to_data_full.py`
   - Input: `scraped_details_flat` (domain `guland.vn`)
   - Output: insert/update vao `data_full`
   - Ghi chu:
     - Match vung bang `transaction_city_merge`
     - Match du an bang `duan_guland_duan_merge`

6. (Optional) Xu ly anh + Upload listing len Cafeland API
   - Services: `craw/ftp_image_processor.py` + `craw/listing_uploader.py`
   - Wrapper start/stop: `start_upload_services.sh`

## 2) Yeu cau moi truong

- Python + venv: `venv/`
- Dependencies: `requirements.txt`
- MySQL (mac dinh): host `localhost`, user `root`, password rong, DB `craw_db`
  - Neu DB khac, sua default trong `craw/database.py` (hoac update cac script khoi tao `Database(...)`).

## 3) Chay thu cong (one-shot)

### 3.1 Crawl Mogi (listing + detail)

```bash
cd /home/chungnt/crawlvip
source venv/bin/activate
cd craw
python daily_mogi_crawl.py
```

Log cron/append: `craw/daily_mogi_cron.log` (neu chay qua wrapper `.sh`).

### 3.2 Chay ETL Mogi

```bash
cd /home/chungnt/crawlvip
source venv/bin/activate
python craw/auto/run_mogi_etl.py
```

### 3.3 Chay convert Alonhadat

```bash
cd /home/chungnt/crawlvip
source venv/bin/activate
python craw/auto/convert_alonhadat_to_data_full.py --preview-limit 500 --insert
```

### 3.4 Chay convert Guland

```bash
cd /home/chungnt/crawlvip
source venv/bin/activate
python craw/auto/convert_guland_to_data_full.py --preview-limit 500 --insert
```



### 3.5 (Optional) Start upload services

```bash
cd /home/chungnt/crawlvip
./start_upload_services.sh
```

Logs (o repo root):
- `mogi_etl.log`
- `alonhadat_etl.log`
- `guland_etl.log`
- `project_id_backfill.log`
- `ftp_processor.log`
- `listing_uploader.log`

## 4) Chay hang ngay bang cron

Repo dang co wrapper crawl:
- `craw/daily_mogi_crawl.sh` (append log vao `craw/daily_mogi_cron.log`)

### 4.1 Cron cho crawl

Vi du chay 08:00 moi ngay:

```cron
0 8 * * * /bin/bash /home/chungnt/crawlvip/craw/daily_mogi_crawl.sh
```

### 4.2 Cron cho ETL / convert (khuyen nghi)

ETL/convert nen chay sau crawl (vi du 09:30). Tuy thoi gian crawl thuc te, chinh lai cho phu hop:

```cron
30 9 * * * /home/chungnt/crawlvip/venv/bin/python /home/chungnt/crawlvip/craw/auto/run_mogi_etl.py >> /home/chungnt/crawlvip/craw/daily_mogi_cron.log 2>&1
35 9 * * * /home/chungnt/crawlvip/venv/bin/python /home/chungnt/crawlvip/craw/auto/convert_alonhadat_to_data_full.py --preview-limit 500 --insert >> /home/chungnt/crawlvip/craw/daily_mogi_cron.log 2>&1
40 9 * * * /home/chungnt/crawlvip/venv/bin/python /home/chungnt/crawlvip/craw/auto/convert_guland_to_data_full.py --preview-limit 500 --insert >> /home/chungnt/crawlvip/craw/daily_mogi_cron.log 2>&1
```

Hoac neu muon chay cung wrapper service:

```bash
cd /home/chungnt/crawlvip
./start_upload_services.sh
```

Wrapper nay hien tai da bao gom:
- `run_mogi_etl.py`
- `convert_alonhadat_to_data_full.py --preview-limit 500 --insert`
- `convert_guland_to_data_full.py --preview-limit 500 --insert`
- `update_project_id.py --new-only --batch 2000`
- `ftp_image_processor.py`
- `listing_uploader.py`

## 5) Proxy / Cloudflare notes

- `craw/daily_mogi_crawl.py` co logic doc proxy tu env `HTTP_PROXY`/`http_proxy` cho listing crawler.
- Tuy nhien `craw/daily_mogi_crawl.sh` dang `unset http_proxy` va `unset https_proxy`.
  - Neu can chay qua proxy, bo 2 dong `unset` trong `craw/daily_mogi_crawl.sh` hoac export proxy truoc khi chay `python daily_mogi_crawl.py`.

## 6) Trang thai thuong gap (de debug nhanh)

- `collected_links.status`: thuong co `PENDING` / `IN_PROGRESS` / `DONE` / `FAILED` (tuy logic trong `craw/mogi_detail_crawler.py` va `craw/database.py`)
- `data_full.images_status`: flow upload anh/listing dung `IMAGES_READY` -> `UPLOADING` -> `LISTING_UPLOADED` (hoac status loi tuong ung)
- `ad_listing_detail.data_no_full_converted`: flag convert sang `data_no_full`
