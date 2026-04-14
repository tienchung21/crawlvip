
import sys
import os
import time

sys.path.append(os.getcwd())
try:
    from craw.database import Database
except ImportError:
    sys.path.append('/home/chungnt/crawlvip')
    from craw.database import Database

def backfill_schema():
    print("=== STARTING FULL SCHEMA BACKFILL (BATCHED ID RANGE) ===")
    print("Target Columns: property_type, type, floors, house_direction, road_width, living_rooms")
    
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    try:
        # Get Min/Max ID
        print("Getting ID range...")
        cursor.execute("SELECT MIN(id), MAX(id) FROM data_full")
        row = cursor.fetchone()
        if isinstance(row, dict):
             min_id = row['MIN(id)']
             max_id = row['MAX(id)']
        else:
             min_id = row[0]
             max_id = row[1]
             
        if min_id is None:
            print("Table empty.")
            return

        print(f"ID Range: {min_id} -> {max_id}")
        
        batch_size = 5000
        current_id = min_id
        total_updated = 0
        
        start_global = time.time()
        
        while current_id <= max_id:
            end_id = current_id + batch_size
            
            # Using the same mapping logic as SQL Convert
            sql = f"""
                UPDATE data_full df
                JOIN scraped_details_flat sdf ON df.id_img = sdf.id
                SET 
                    -- Property Type
                    df.property_type = 
                    CASE
                        -- MUA (SELL)
                        WHEN sdf.trade_type = 'mua' OR sdf.trade_type = 'bán' THEN
                            CASE
                                WHEN sdf.loaihinh = 'Nhà hẻm ngõ' THEN 'Bán nhà riêng'
                                WHEN sdf.loaihinh = 'Nhà mặt tiền phố' THEN 'Bán nhà riêng'
                                WHEN sdf.loaihinh = 'Căn hộ chung cư' THEN 'Bán căn hộ chung cư'
                                WHEN sdf.loaihinh = 'Đất thổ cư' THEN 'Bán đất thổ cư'
                                WHEN sdf.loaihinh = 'Nhà biệt thự, liền kề' THEN 'Bán biệt thự'
                                WHEN sdf.loaihinh = 'Đường nội bộ' THEN 'Bán nhà phố dự án'
                                WHEN sdf.loaihinh = 'Đất nền dự án' THEN 'Bán đất nền dự án'
                                WHEN sdf.loaihinh = 'Đất nông nghiệp' THEN 'Bán đất nông, lâm nghiệp'
                                WHEN sdf.loaihinh = 'Căn hộ dịch vụ' THEN 'Bán căn hộ Mini, Dịch vụ'
                                WHEN sdf.loaihinh = 'Căn hộ tập thể, cư xá' THEN 'Bán căn hộ chung cư'
                                WHEN sdf.loaihinh = 'Căn hộ Penthouse' THEN 'Bán căn hộ chung cư'
                                WHEN sdf.loaihinh = 'Đất kho xưởng' THEN 'Bán đất thổ cư'
                                WHEN sdf.loaihinh = 'Căn hộ Officetel' THEN 'Bán căn hộ chung cư'
                                ELSE sdf.loaihinh
                            END
                
                        -- THUE (RENT)
                        WHEN sdf.trade_type = 'thuê' THEN
                            CASE
                                WHEN sdf.loaihinh = 'Căn hộ chung cư' THEN 'Căn hộ chung cư'
                                WHEN sdf.loaihinh = 'Nhà mặt tiền phố' THEN 'Nhà phố'
                                WHEN sdf.loaihinh = 'Căn hộ dịch vụ' THEN 'Nhà hàng - Khách sạn'
                                WHEN sdf.loaihinh = 'Phòng trọ, nhà trọ' THEN 'Phòng trọ'
                                WHEN sdf.loaihinh = 'Nhà hẻm ngõ' THEN 'Nhà riêng'
                                WHEN sdf.loaihinh = 'Nhà biệt thự, liền kề' THEN 'Biệt thự'
                                WHEN sdf.loaihinh = 'Văn phòng' THEN 'Văn phòng'
                                WHEN sdf.loaihinh = 'Nhà xưởng, kho bãi' THEN 'Nhà Kho - Xưởng'
                                WHEN sdf.loaihinh = 'Đường nội bộ' THEN 'Nhà phố'
                                WHEN sdf.loaihinh = 'Căn hộ Penthouse' THEN 'Căn hộ chung cư'
                                WHEN sdf.loaihinh = 'Căn hộ tập thể, cư xá' THEN 'Căn hộ chung cư'
                                WHEN sdf.loaihinh = 'Căn hộ Officetel' THEN 'Căn hộ chung cư'
                                ELSE sdf.loaihinh
                            END
                        ELSE sdf.loaihinh
                    END,
                    
                    -- Type
                    df.type = 
                    CASE 
                        WHEN sdf.trade_type = 'mua' OR sdf.trade_type = 'bán' THEN 's'
                        WHEN sdf.trade_type = 'thuê' THEN 'u'
                        ELSE NULL 
                    END,
                    
                    -- Other columns
                    df.floors = sdf.sotang,
                    df.house_direction = sdf.huongnha,
                    df.road_width = sdf.duongvao,
                    df.living_rooms = NULL
                    
                WHERE df.id >= {current_id} AND df.id < {end_id}
                AND (df.property_type IS NULL OR df.type IS NULL)
            """
            try:
                t0 = time.time()
                cursor.execute(sql)
                cnt = cursor.rowcount
                conn.commit()
                dt = time.time() - t0
                
                total_updated += cnt
                if cnt > 0:
                     print(f"Backfilled {cnt} rows in range [{current_id}, {end_id}) - {dt:.2f}s")
                     
            except Exception as e:
                print(f"Error in batch {current_id}: {e}")
                pass
            
            current_id = end_id

        print(f"\n=== BACKFILL COMPLETED ===")
        print(f"Total Rows Updated: {total_updated}")
        print(f"Total Time: {time.time() - start_global:.2f}s")

    except Exception as e:
        print(f"Fatal Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    backfill_schema()
