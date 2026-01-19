
CREATE TABLE IF NOT EXISTS cenhomes_provinces (
  id BIGINT NOT NULL PRIMARY KEY,
  title VARCHAR(255) NULL,
  name VARCHAR(255) NULL,
  english_name VARCHAR(255) NULL,
  slug VARCHAR(255) NULL,
  cenhomes_url VARCHAR(255) NULL,
  lat DOUBLE NULL,
  lng DOUBLE NULL,
  raw_json JSON NULL,
  updated_at DATETIME NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS cenhomes_districts (
  id BIGINT NOT NULL PRIMARY KEY,
  province_id BIGINT NULL,
  title VARCHAR(255) NULL,
  name VARCHAR(255) NULL,
  english_name VARCHAR(255) NULL,
  slug VARCHAR(255) NULL,
  cenhomes_url VARCHAR(255) NULL,
  lat DOUBLE NULL,
  lng DOUBLE NULL,
  raw_json JSON NULL,
  updated_at DATETIME NULL,
  INDEX idx_province_id (province_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS cenhomes_wards (
  id BIGINT NOT NULL PRIMARY KEY,
  province_id BIGINT NULL,
  district_id BIGINT NULL,
  title VARCHAR(255) NULL,
  name VARCHAR(255) NULL,
  english_name VARCHAR(255) NULL,
  slug VARCHAR(255) NULL,
  cenhomes_url VARCHAR(255) NULL,
  lat DOUBLE NULL,
  lng DOUBLE NULL,
  raw_json JSON NULL,
  updated_at DATETIME NULL,
  INDEX idx_district_id (district_id),
  INDEX idx_ward_province_id (province_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
