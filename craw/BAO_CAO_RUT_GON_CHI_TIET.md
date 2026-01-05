# Bao cao tong quan du an Web Scraper

## 1) Muc tieu
- Xay dung bo cong cu crawl du lieu bat dong san gom: lay danh sach link, crawl chi tiet, tai anh, tu dong lap lich.
- Cung cap extension de tao template lay selector nhanh chong.
- Cung cap dashboard quan ly, theo doi va kiem soat quy trinh crawl.

## 2) Pham vi chinh
- Extension (Chrome): Chon selector, tao template, che do listing/detail.
- Crawler listing: Lay link tu danh muc theo template + next page.
- Crawler detail: Lay cac truong du lieu tu trang chi tiet theo template.
- Image downloader: Tai anh theo link da luu trong DB.
- Scheduler: Tu dong chay theo lich, pipeline listing -> detail -> image.
- Dashboard: Quan ly task, cau hinh thong so, xem log.

## 3) Cong nghe
- Extension: JavaScript, Chrome Extension API.
- Listing: Python + Nodriver.
- Detail: Python + Crawl4AI/Playwright.
- DB: MySQL.
- Dashboard: Streamlit.
- Scheduler: Python (vong lap, cron theo task).
- Image: Python Requests + DB.

## 4) Timeline rut gon (de xuat)
Tong thoi gian: 12 ngay (chia 5 phase, moi phase co dau ra ro rang).

### Bang timeline tong
| Phase | Noi dung | Thoi gian | Dau ra |
| --- | --- | --- | --- |
| P1 | Extension + template | 3 ngay | Template on dinh, chon selector tot hon |
| P2 | Listing + Detail | 3 ngay | Listing on dinh, detail on dinh |
| P3 | Image + DB | 2 ngay | Tai anh on dinh, status DB ro rang |
| P4 | Scheduler + Dashboard | 3 ngay | Task chay tu dong, log ro rang |
| P5 | Test + Bao cao | 1 ngay | UAT + bao cao tong ket |

## 5) Chi tiet tung phase

### P1 (Ngay 1-3): Extension + Template
Muc tieu: Tao template dung, selector chinh xac, xu ly truong hop a khong co class.
Cong viec:
- Chinh logic listmode: neu a khong co class thi leo len parent de lay class.
- Them cac nut chon next (next v1, next li, next last pagination).
- Chuan hoa output template: itemSelector, nextPageSelector, url.
Dau ra:
- Tao template listing/detail dung cho batdongsan, nhatot, nhadat.
Rui ro:
- HTML trang thay doi lam selector sai.

### P2 (Ngay 4-6): Listing + Detail
Muc tieu: Listing va detail chay on dinh, giong logic tab3.
Cong viec:
- Listing: querySelectorAll itemSelector, lay href tu a con.
- Next page: click selector, log trang.
- Detail: dung template chi tiet, luu DB.
Dau ra:
- Lay link dung, khong bi trung/loi.
- Chi tiet luu duoc day du vao scraped_details_flat.
Rui ro:
- Cloudflare/anti-bot, timeout.

### P3 (Ngay 7-8): Image + DB
Muc tieu: Tai anh tu scraped_detail_images, cap nhat status.
Cong viec:
- Dong bo status PENDING/DOWNLOADED/FAILED.
- Retry FAILED toi 3 lan, log ro rang.
- Ghi log tong ket so luong.
Dau ra:
- Anh tai xong, status cap nhat dung.
Rui ro:
- 403, can header hoac referer.

### P4 (Ngay 9-11): Scheduler + Dashboard
Muc tieu: Task chay theo lich, co log va trang thai.
Cong viec:
- Task co the chay tung stage rieng (listing/detail/image).
- Hien trang thai Running/Next run/Last run.
- Log chi tiet tung stage.
Dau ra:
- Task chay tu dong, bao cao log day du.
Rui ro:
- Task long chay, can retry va gioi han.

### P5 (Ngay 12): Test + Bao cao
Muc tieu: Xac nhan on dinh va bao cao tong ket.
Cong viec:
- Test 3 site: batdongsan, nhatot, cafeland.
- Do ty le loi, danh gia hieu nang.
Bao cao:
- Tong hop so link, so detail, so anh.

## 6) Chuc nang chi tiet (bang tong hop)
| STT | Chuc nang | Mo ta | Trang thai | Cong nghe | Ghi chu |
| --- | --- | --- | --- | --- | --- |
| 1 | Extension chon selector | Click de lay class/xpath, tao template | Dang cai tien | JS + Chrome API | Can xu ly a khong co class |
| 2 | Listing crawler | Lay link theo template + next page | On dinh | Nodriver | Can log so link moi |
| 3 | Detail crawler | Lay truong du lieu chi tiet | On dinh | Crawl4AI/Playwright | Co the bi chan |
| 4 | Image downloader | Tai anh tu DB, retry failed | On dinh | Requests | 403 can header |
| 5 | Scheduler | Chay listing/detail/image theo lich | Dang hoan thien | Python | Can log ro |
| 6 | Dashboard | Cau hinh, xem log | On dinh | Streamlit | Chua toi uu DB lon |

## 7) Dau ra bat buoc
- Template listing/detail hoat dong cho it nhat 3 site.
- Listing: lay dung so link, co next page.
- Detail: luu duoc vao scraped_details_flat, khong loi SQL.
- Image: tai duoc anh + update status.
- Scheduler: chay dung stage da chon, log ro rang.

## 8) Rui ro va giai phap
- Anti-bot/Cloudflare: su dung profile, delay, fake scroll.
- 403 khi tai anh: them header, retry.
- HTML thay doi: can template update nhanh.

## 9) Ket luan
Timeline rut gon 12 ngay, tap trung vao on dinh listing/detail, sau do hoan thien scheduler + bao cao.
