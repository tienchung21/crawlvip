from seleniumbase import SB
import csv
import time
import os

# Tên file kết quả
FILE_KET_QUA = "DANH_SACH_DIA_LY_FULL.csv"

def get_geo_full_v3():
    # headless=False để bố nhìn thấy nó làm việc
    with SB(uc=True, headless=False) as sb:
        print("🚀 Đang mở trình duyệt vào Nhà Tốt...")
        sb.open("https://www.nhatot.com/mua-ban-bat-dong-san")
        
        # Đợi xíu cho web load xong các mã bảo mật
        print("⏳ Đang đợi web load xong...")
        sb.sleep(5) 

        # --- BƯỚC 1: LẤY DANH SÁCH TỈNH (63 Tỉnh) ---
        print("\n📡 Đang lấy danh sách 63 Tỉnh/Thành...")
        
        # Reset biến tạm
        sb.execute_script("window.data_tinh = null;")
        
        # Gọi lệnh y hệt thanh chọn khu vực
        sb.execute_script("""
            fetch('https://gateway.chotot.com/v1/public/geo/regions')
                .then(r => r.json())
                .then(d => window.data_tinh = d.result)
                .catch(e => window.data_tinh = []);
        """)
        
        # Đợi dữ liệu về
        ds_tinh = []
        for _ in range(20): # Chờ tối đa 10s
            ds_tinh = sb.execute_script("return window.data_tinh;")
            if ds_tinh is not None: break
            time.sleep(0.5)
            
        if not ds_tinh:
            print("❌ Không lấy được Tỉnh. Bố thử reset mạng nhé.")
            return

        print(f"✅ Đã có {len(ds_tinh)} Tỉnh. Bắt đầu quét Huyện/Xã...")
        
        # Mở file để ghi dần (tránh mất điện là mất hết)
        with open(FILE_KET_QUA, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            # Ghi tiêu đề cột
            writer.writerow(['ID_Tinh', 'Ten_Tinh', 'ID_Huyen', 'Ten_Huyen', 'ID_Xa', 'Ten_Xa'])
            
            # --- BƯỚC 2: QUÉT TỪNG TỈNH ĐỂ LẤY HUYỆN ---
            total_xa = 0
            for tinh in ds_tinh:
                tinh_id = tinh['region_id']
                tinh_ten = tinh['region_name']
                
                # Gọi API lấy Huyện
                sb.execute_script("window.data_huyen = null;")
                sb.execute_script(f"""
                    fetch('https://gateway.chotot.com/v1/public/geo/regions/{tinh_id}/areas')
                        .then(r => r.json())
                        .then(d => window.data_huyen = d.result)
                        .catch(e => window.data_huyen = []);
                """)
                
                ds_huyen = []
                for _ in range(10):
                    ds_huyen = sb.execute_script("return window.data_huyen;")
                    if ds_huyen is not None: break
                    time.sleep(0.2)
                
                if not ds_huyen: continue

                # --- BƯỚC 3: QUÉT TỪNG HUYỆN ĐỂ LẤY XÃ ---
                print(f"   📂 Đang quét: {tinh_ten} ({len(ds_huyen)} huyện)...")
                
                for huyen in ds_huyen:
                    huyen_id = huyen['area_id']
                    huyen_ten = huyen['area_name']
                    
                    # Gọi API lấy Xã
                    sb.execute_script("window.data_xa = null;")
                    sb.execute_script(f"""
                        fetch('https://gateway.chotot.com/v1/public/geo/areas/{huyen_id}/wards')
                            .then(r => r.json())
                            .then(d => window.data_xa = d.result)
                            .catch(e => window.data_xa = []);
                    """)
                    
                    ds_xa = []
                    for _ in range(10):
                        ds_xa = sb.execute_script("return window.data_xa;")
                        if ds_xa is not None: break
                        time.sleep(0.1)
                    
                    # Ghi ngay vào file
                    if ds_xa:
                        rows_to_write = []
                        for xa in ds_xa:
                            rows_to_write.append([
                                tinh_id, tinh_ten,
                                huyen_id, huyen_ten,
                                xa['ward_id'], xa['ward_name']
                            ])
                        writer.writerows(rows_to_write)
                        total_xa += len(ds_xa)
                    
                    # Nghỉ tí cho server thở (0.05 giây)
                    time.sleep(0.05)

        print("\n" + "="*40)
        print(f"🏆 HOÀN TẤT! Đã lưu {total_xa} dòng dữ liệu.")
        print(f"📂 File kết quả: {os.path.abspath(FILE_KET_QUA)}")

if __name__ == "__main__":
    get_geo_full_v3()