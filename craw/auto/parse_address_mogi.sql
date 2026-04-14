
-- =========================================================================
-- SCRIPT: Tách địa chỉ (Address Parsing) cho bảng scraped_details_flat
-- Logic: Cắt ngược từ cuối lên (Tỉnh -> Quận -> Phường -> Đường)
-- =========================================================================

-- 1. Thêm các cột chứa dữ liệu tách (nếu chưa có)
-- Lưu ý: MySQL cũ có thể không support 'IF NOT EXISTS' trong ALTER TABLE. 
-- Nếu lỗi, hãy chạy ALTER TABLE thủ công hoặc bỏ qua dòng này nếu cột đã có.
DROP PROCEDURE IF EXISTS upgrade_scraped_details_schema;
DELIMITER //
CREATE PROCEDURE upgrade_scraped_details_schema()
BEGIN
    IF NOT EXISTS (
        SELECT * FROM information_schema.COLUMNS 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = 'scraped_details_flat' 
        AND COLUMN_NAME = 'city_ext'
    ) THEN
        ALTER TABLE scraped_details_flat 
        ADD COLUMN city_ext VARCHAR(100) NULL AFTER diachi,
        ADD COLUMN district_ext VARCHAR(100) NULL AFTER city_ext,
        ADD COLUMN ward_ext VARCHAR(100) NULL AFTER district_ext,
        ADD COLUMN street_ext VARCHAR(255) NULL AFTER ward_ext;
        
        CREATE INDEX idx_city_ext ON scraped_details_flat(city_ext);
        CREATE INDEX idx_district_ext ON scraped_details_flat(district_ext);
        CREATE INDEX idx_ward_ext ON scraped_details_flat(ward_ext);
        CREATE INDEX idx_street_ext ON scraped_details_flat(street_ext);
    END IF;
END//
DELIMITER ;
CALL upgrade_scraped_details_schema();
DROP PROCEDURE upgrade_scraped_details_schema;

-- 2. Cập nhật dữ liệu (Parsing Logic)
UPDATE scraped_details_flat
SET 
    -- A. TÁCH TỈNH/THÀNH (Phần cuối cùng)
    city_ext = TRIM(SUBSTRING_INDEX(diachi, ',', -1)),

    -- B. TÁCH QUẬN/HUYỆN (Phần thứ 2 từ dưới lên)
    district_ext = CASE 
        -- Phải có ít nhất 1 dấu phẩy (>= 2 thành phần)
        WHEN LENGTH(diachi) - LENGTH(REPLACE(diachi, ',', '')) >= 1 
        THEN TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(diachi, ',', -2), ',', 1))
        ELSE NULL 
    END,

    -- C. TÁCH PHƯỜNG/XÃ (Phần thứ 3 từ dưới lên)
    ward_ext = CASE 
        -- Phải có ít nhất 2 dấu phẩy (>= 3 thành phần)
        WHEN LENGTH(diachi) - LENGTH(REPLACE(diachi, ',', '')) >= 2 
        THEN TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(diachi, ',', -3), ',', 1))
        ELSE NULL 
    END,

    -- D. TÁCH ĐƯỜNG/PHỐ (Các phần còn lại ở đầu)
    street_ext = CASE 
        -- Chỉ tách tên đường khi có đủ 4 thành phần (>= 3 dấu phẩy)
        WHEN LENGTH(diachi) - LENGTH(REPLACE(diachi, ',', '')) >= 3 
        THEN TRIM(
            SUBSTRING(
                diachi, 
                1, 
                LENGTH(diachi) - LENGTH(SUBSTRING_INDEX(diachi, ',', -3)) - 1
            )
        )
        ELSE NULL 
    END
WHERE diachi IS NOT NULL AND (city_ext IS NULL OR city_ext = '');
