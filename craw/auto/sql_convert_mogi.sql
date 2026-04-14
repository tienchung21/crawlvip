
-- 1. Insert into final table (Direct filter full=1 and Check Duplicates)
INSERT INTO data_full (
    title,
    address,
    posted_at,
    img,
    price,
    area,
    width,
    length,
    description,
    bathrooms,
    bedrooms,
    legal_status,
    lat,
    `long`,
    broker_name,
    phone,
    source,
    time_converted_at,
    source_post_id,
    city,
    district,
    ward,
    street,
    province_id,
    district_id,
    ward_id,
    street_id,
    property_type, -- Renamed from loaihinh
    cat_id,          -- New
    type_id,         -- New
    type,
    unit,            -- New
    floors,          -- New
    house_direction, -- New (was huongnha)
    road_width,      -- New (was duongtruocnha)
    living_rooms,    -- New (was sophongkhach)
    id_img,           -- New
    project_name,
    stratum_id        -- Legal status ID for API
)
SELECT
    sdf.title,
    sdf.diachi,
    STR_TO_DATE(sdf.ngaydang, '%d/%m/%Y') AS posted_at,
    (
        SELECT di.image_url
        FROM scraped_detail_images di
        WHERE di.detail_id = sdf.id
        ORDER BY di.idx ASC
        LIMIT 1
    ) AS img,

    CASE
        WHEN sdf.khoanggia IS NULL OR sdf.khoanggia = '' THEN NULL
        -- Case: Tỷ (Start with 'tỷ')
        WHEN sdf.khoanggia REGEXP 'tỷ' THEN
            (CAST(REPLACE(REGEXP_SUBSTR(sdf.khoanggia, '[0-9]+([.,][0-9]+)?'), ',', '.') AS DECIMAL(15,2)) * 1000000000)
            + 
            CASE WHEN sdf.khoanggia REGEXP 'triệu' THEN
                CAST(REPLACE(REGEXP_SUBSTR(SUBSTRING_INDEX(sdf.khoanggia, 'tỷ', -1), '[0-9]+([.,][0-9]+)?'), ',', '.') AS DECIMAL(15,2)) * 1000000
            ELSE 0 END
            
        -- Case: Triệu (Start with 'triệu' but NO 'tỷ')
        WHEN sdf.khoanggia REGEXP 'triệu' THEN
            (CAST(REPLACE(REGEXP_SUBSTR(sdf.khoanggia, '[0-9]+([.,][0-9]+)?'), ',', '.') AS DECIMAL(15,2)) * 1000000)
            +
            CASE WHEN sdf.khoanggia REGEXP 'nghìn|ngàn' THEN
                CAST(REPLACE(REGEXP_SUBSTR(SUBSTRING_INDEX(sdf.khoanggia, 'triệu', -1), '[0-9]+([.,][0-9]+)?'), ',', '.') AS DECIMAL(15,2)) * 1000
            ELSE 0 END
            
        ELSE NULL
    END AS price,

    CAST(
        COALESCE(
            CASE 
                WHEN sdf.dientich IS NULL OR sdf.dientich = '' THEN NULL
                WHEN LENGTH(REPLACE(REGEXP_SUBSTR(sdf.dientich, '[0-9]+([.,][0-9]+)?'), ',', '.')) > 8 THEN NULL 
                ELSE NULLIF(REPLACE(REGEXP_SUBSTR(sdf.dientich, '[0-9]+([.,][0-9]+)?'), ',', '.'), '')
            END,
            CASE 
                WHEN sdf.dientichsudung IS NULL OR sdf.dientichsudung = '' THEN NULL
                WHEN LENGTH(REPLACE(REGEXP_SUBSTR(sdf.dientichsudung, '[0-9]+([.,][0-9]+)?'), ',', '.')) > 8 THEN NULL 
                ELSE NULLIF(REPLACE(REGEXP_SUBSTR(sdf.dientichsudung, '[0-9]+([.,][0-9]+)?'), ',', '.'), '')
            END
        )
    AS DECIMAL(10,2)) AS area,

    CAST(
        NULLIF(
            REPLACE(
                REPLACE(
                    REPLACE(
                        SUBSTRING_INDEX(
                            SUBSTRING_INDEX(REGEXP_SUBSTR(sdf.dientich, '\\([0-9.,xX ]+\\)'), 'x', 1),
                            '(', -1
                        ),
                        ')', ''
                    ),
                    '(', ''
                ),
                ',', '.'
            ),
        '')
    AS DECIMAL(10,2)) AS width,

    CAST(
        NULLIF(
            REPLACE(
                REPLACE(
                    REPLACE(
                        SUBSTRING_INDEX(REGEXP_SUBSTR(sdf.dientich, '\\([0-9.,xX ]+\\)'), 'x', -1),
                        ')', ''
                    ),
                    '(', ''
                ),
                ',', '.'
            ),
        '')
    AS DECIMAL(10,2)) AS length,

    sdf.mota,
    sdf.sophongvesinh,
    sdf.sophongngu,
    sdf.phaply,
    CAST(SUBSTRING_INDEX(sdf.map, ',', 1) AS DECIMAL(20,14)) AS lat,
    CAST(SUBSTRING_INDEX(sdf.map, ',', -1) AS DECIMAL(20,14)) AS `long`,
    sdf.tenmoigioi,
    sdf.sodienthoai,
    'mogi' AS source,
    NOW() AS time_converted_at,
    sdf.matin AS source_post_id,
    sdf.city_ext,
    sdf.district_ext,
    sdf.ward_ext,
    sdf.street_ext,
    
    -- Map IDs from location_mogi (use cafeland_new_id for API)
    loc_city.cafeland_new_id AS province_id,
    loc_dist.cafeland_new_id AS district_id,
    loc_ward.cafeland_new_id AS ward_id,
    sdf.mogi_street_id AS street_id,

    -- Property Type Converter (was Loai Hinh)
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
                ELSE sdf.loaihinh -- Fallback
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
                ELSE sdf.loaihinh -- Fallback
            END
        ELSE sdf.loaihinh
    END AS property_type,
    
    -- Cat ID:
    -- 1 = nhà/căn hộ bán
    -- 2 = đất bán
    -- 3 = thuê
    CASE
        WHEN sdf.trade_type = 'thuê' THEN 3
        WHEN sdf.loaihinh LIKE '%Đất thổ cư%' THEN 2
        WHEN sdf.loaihinh LIKE '%Đất nền%' THEN 2
        WHEN sdf.loaihinh LIKE '%Nông nghiệp%' OR sdf.loaihinh LIKE '%Lâm nghiệp%' THEN 2
        ELSE 1
    END AS cat_id,
    
    -- Type ID Mapping (Based on User Provided DOM Values)
    CASE
        -- RENT (cat_id=3)
        WHEN sdf.trade_type = 'thuê' THEN
            CASE
                -- 1. Nhà phố (Matches: Nhà phố, Mặt tiền) -> Value 1
                WHEN sdf.loaihinh LIKE '%Nhà phố%' OR sdf.loaihinh LIKE '%Mặt tiền%' THEN 1
                
                -- 2. Nhà riêng (Matches: Nhà riêng, Nhà hẻm) -> Value 2
                WHEN sdf.loaihinh LIKE '%Nhà riêng%' OR sdf.loaihinh LIKE '%Nhà hẻm%' THEN 2
                
                -- 3. Biệt thự (Matches: Biệt thự) -> Value 3
                WHEN sdf.loaihinh LIKE '%Biệt thự%' THEN 3
                
                -- 5. Căn hộ chung cư (Matches: Chung cư, Tập thể, Penthouse, Officetel) -> Value 5
                WHEN sdf.loaihinh LIKE '%Chung cư%' OR sdf.loaihinh LIKE '%Tập thể%' OR sdf.loaihinh LIKE '%Penthouse%' OR sdf.loaihinh LIKE '%Officetel%' THEN 5
                
                -- 6. Văn phòng (Matches: Văn phòng) -> Value 6
                WHEN sdf.loaihinh LIKE '%Văn phòng%' THEN 6
                
                -- 12. Mặt bằng (Matches: Mặt bằng) -> Value 12
                WHEN sdf.loaihinh LIKE '%Mặt bằng%' THEN 12
                
                -- 13. Nhà hàng - Khách sạn (Matches: Khách sạn, Nhà hàng, Dịch vụ) -> Value 13
                WHEN sdf.loaihinh LIKE '%Khách sạn%' OR sdf.loaihinh LIKE '%Nhà hàng%' OR sdf.loaihinh LIKE '%Căn hộ dịch vụ%' THEN 13
                
                -- 14. Nhà Kho - Xưởng (Matches: Kho, Xưởng) -> Value 14
                WHEN sdf.loaihinh LIKE '%Kho%' OR sdf.loaihinh LIKE '%Xưởng%' THEN 14
                
                -- 15. Phòng trọ (Matches: Phòng trọ, Nhà trọ) -> Value 15
                WHEN sdf.loaihinh LIKE '%Phòng trọ%' OR sdf.loaihinh LIKE '%Nhà trọ%' THEN 15
                
                -- 57. Đất khu công nghiệp (Matches: Công nghiệp) -> Value 57
                WHEN sdf.loaihinh LIKE '%Công nghiệp%' THEN 57
                
                ELSE 2 -- Default to Nhà riêng
            END
        
        -- SALE (cat_id=1)
        ELSE
            CASE
                -- 2. Bán nhà riêng (Matches: Nhà riêng, Nhà hẻm) -> Value 2
                WHEN sdf.loaihinh LIKE '%Nhà riêng%' OR sdf.loaihinh LIKE '%Nhà hẻm%' THEN 2
                
                -- 1. Bán nhà phố dự án (Matches: Nhà phố, Mặt tiền, Đường nội bộ) -> Value 1
                WHEN sdf.loaihinh LIKE '%Nhà phố%' OR sdf.loaihinh LIKE '%Mặt tiền%' OR sdf.loaihinh LIKE '%Đường nội bộ%' THEN 1
                
                -- 3. Bán biệt thự (Matches: Biệt thự) -> Value 3
                WHEN sdf.loaihinh LIKE '%Biệt thự%' THEN 3
                
                -- 5. Bán căn hộ chung cư (Matches: Chung cư, Tập thể, Penthouse, Officetel) -> Value 5
                WHEN sdf.loaihinh LIKE '%Chung cư%' OR sdf.loaihinh LIKE '%Tập thể%' OR sdf.loaihinh LIKE '%Penthouse%' OR sdf.loaihinh LIKE '%Officetel%' THEN 5
                
                -- 56. Bán căn hộ Mini, Dịch vụ (Matches: Mini, Căn hộ dịch vụ) -> Value 56
                WHEN sdf.loaihinh LIKE '%Mini%' OR sdf.loaihinh LIKE '%Căn hộ dịch vụ%' THEN 56
                
                -- 13. Bán nhà hàng - Khách sạn (Matches: Khách sạn, Nhà hàng) -> Value 13
                WHEN sdf.loaihinh LIKE '%Khách sạn%' OR sdf.loaihinh LIKE '%Nhà hàng%' THEN 13
                
                -- 14. Bán kho, nhà xưởng (Matches: Kho, Xưởng) -> Value 14
                WHEN sdf.loaihinh LIKE '%Kho%' OR sdf.loaihinh LIKE '%Xưởng%' OR sdf.loaihinh LIKE '%Đất kho xưởng%' THEN 14
                
                -- 8. Bán đất nền dự án (Matches: Đất nền) -> Value 8
                WHEN sdf.loaihinh LIKE '%Đất nền%' THEN 8
                
                -- 11. Bán đất thổ cư (Matches: Đất thổ cư) -> Value 11
                WHEN sdf.loaihinh LIKE '%Đất thổ cư%' THEN 11
                
                -- 10. Bán đất nông, lâm nghiệp (Matches: Nông nghiệp, Lâm nghiệp) -> Value 10
                WHEN sdf.loaihinh LIKE '%Nông nghiệp%' OR sdf.loaihinh LIKE '%Lâm nghiệp%' THEN 10
                
                ELSE 2 -- Default to Bán nhà riêng (2)
            END
    END AS type_id,
    
    -- Type char (kept for backward compatibility if needed, or remove)
    CASE 
        WHEN sdf.trade_type = 'mua' OR sdf.trade_type = 'bán' THEN 's'
        WHEN sdf.trade_type = 'thuê' THEN 'u'
        ELSE NULL 
    END AS type,
    
    -- Unit logic
    CASE 
        WHEN sdf.trade_type = 'thuê' THEN 'tháng'
        ELSE 'md' 
    END AS unit,
    
    -- New columns
    sdf.sotang AS floors,
    sdf.huongnha AS house_direction,
    sdf.duongvao AS road_width,
    NULL AS living_rooms, -- sophongkhach not in source
    sdf.id AS id_img,
    sdf.thuocduan AS project_name,
    
    -- Legal status to stratum_id
    CASE 
        WHEN LOWER(sdf.phaply) LIKE '%sổ hồng%' THEN 1
        WHEN LOWER(sdf.phaply) LIKE '%sổ đỏ%' THEN 1
        WHEN LOWER(sdf.phaply) LIKE '%đã có sổ%' THEN 1
        WHEN LOWER(sdf.phaply) LIKE '%hợp đồng mua bán%' THEN 2
        WHEN LOWER(sdf.phaply) LIKE '%hđmb%' THEN 2
        WHEN LOWER(sdf.phaply) LIKE '%đang chờ sổ%' THEN 3
        WHEN LOWER(sdf.phaply) LIKE '%giấy tờ hợp lệ%' THEN 4
        ELSE NULL
    END AS stratum_id
FROM scraped_details_flat sdf
INNER JOIN (
    -- Deduplicate at source: keep only latest row (max id) per matin.
    -- This prevents inserting multiple rows with the same source_post_id in one ETL run.
    SELECT matin, MAX(id) AS max_id
    FROM scraped_details_flat
    WHERE domain = 'mogi'
      AND full = 1
      AND COALESCE(datafull_converted, 0) = 0
      AND matin IS NOT NULL
      AND matin <> ''
    GROUP BY matin
) dedup ON dedup.max_id = sdf.id
-- Join Location Mogi to get Cafeland IDs
LEFT JOIN location_mogi loc_city ON sdf.mogi_city_id = loc_city.mogi_id AND loc_city.type = 'CITY'
LEFT JOIN location_mogi loc_dist ON sdf.mogi_district_id = loc_dist.mogi_id AND loc_dist.type = 'DISTRICT'
LEFT JOIN location_mogi loc_ward ON sdf.mogi_ward_id = loc_ward.mogi_id AND loc_ward.type = 'WARD'
-- LEFT JOIN location_mogi loc_street ON sdf.mogi_street_id = loc_street.mogi_id AND loc_street.type = 'STREET'

WHERE sdf.domain = 'mogi' 
  AND sdf.full = 1
  AND COALESCE(sdf.datafull_converted, 0) = 0
  AND sdf.matin IS NOT NULL
  AND sdf.matin <> ''
  AND STR_TO_DATE(sdf.ngaydang, '%d/%m/%Y') >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
  -- Only include listings with valid ward mapping in location_mogi
  AND loc_ward.cafeland_new_id IS NOT NULL
  -- Anti-duplicate check: Do not insert if source_post_id already exists in data_full
  AND NOT EXISTS (
      SELECT 1 FROM data_full df 
      WHERE df.source_post_id = sdf.matin
  );
