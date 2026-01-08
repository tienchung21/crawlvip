-- Full schema for craw_db (structure only, no data)

CREATE DATABASE IF NOT EXISTS `craw_db`
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE `craw_db`;

-- Collected listing links
CREATE TABLE IF NOT EXISTS `collected_links` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `url` VARCHAR(2000) NOT NULL UNIQUE,
  `status` VARCHAR(50) NOT NULL DEFAULT 'PENDING',
  `domain` VARCHAR(255) DEFAULT NULL,
  `loaihinh` VARCHAR(255) DEFAULT NULL,
  `city_id` INT DEFAULT NULL,
  `city_name` VARCHAR(255) DEFAULT NULL,
  `ward_id` INT DEFAULT NULL,
  `ward_name` VARCHAR(255) DEFAULT NULL,
  `new_city_id` INT DEFAULT NULL,
  `new_city_name` VARCHAR(255) DEFAULT NULL,
  `new_ward_id` INT DEFAULT NULL,
  `new_ward_name` VARCHAR(255) DEFAULT NULL,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_collected_links_url` (`url`(100)),
  INDEX `idx_collected_links_domain` (`domain`),
  INDEX `idx_collected_links_loaihinh` (`loaihinh`),
  INDEX `idx_collected_links_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Raw scraped detail JSON
CREATE TABLE IF NOT EXISTS `scraped_details` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `link_id` INT NULL,
  `url` VARCHAR(2000) NOT NULL,
  `domain` VARCHAR(255) DEFAULT NULL,
  `data_json` LONGTEXT,
  `success` TINYINT(1) DEFAULT 1,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_scraped_details_url` (`url`(100)),
  INDEX `idx_scraped_details_domain` (`domain`),
  INDEX `idx_scraped_details_success` (`success`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Flattened detail table
CREATE TABLE IF NOT EXISTS `scraped_details_flat` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `link_id` INT NULL,
  `url` VARCHAR(2000) NOT NULL,
  `domain` VARCHAR(255) DEFAULT NULL,
  `title` TEXT,
  `img_count` INT DEFAULT NULL,
  `mota` TEXT,
  `khoanggia` VARCHAR(255),
  `dientich` VARCHAR(255),
  `sophongngu` VARCHAR(255),
  `sophongvesinh` VARCHAR(255),
  `huongnha` VARCHAR(255),
  `huongbancong` VARCHAR(255),
  `mattien` VARCHAR(255),
  `duongvao` VARCHAR(255),
  `sotang` VARCHAR(255),
  `loaihinhnhao` VARCHAR(255),
  `dientichsudung` VARCHAR(255),
  `gia_m2` VARCHAR(255),
  `gia_mn` VARCHAR(255),
  `dacdiemnhadat` VARCHAR(255),
  `chieungang` VARCHAR(255),
  `chieudai` VARCHAR(255),
  `phaply` VARCHAR(255),
  `noithat` VARCHAR(255),
  `thuocduan` VARCHAR(255),
  `trangthaiduan` VARCHAR(255),
  `tenmoigioi` VARCHAR(255),
  `sodienthoai` VARCHAR(255),
  `map` VARCHAR(255),
  `matin` VARCHAR(255),
  `loaitin` VARCHAR(255),
  `ngayhethan` VARCHAR(255),
  `ngaydang` VARCHAR(255),
  `thoigianvaoo` VARCHAR(255),
  `giadien` VARCHAR(255),
  `gianuoc` VARCHAR(255),
  `giainternet` VARCHAR(255),
  `sotiencoc` VARCHAR(255),
  `tangso` VARCHAR(255),
  `loaihinhvanphong` VARCHAR(255),
  `loaihinhdat` VARCHAR(255),
  `loaihinhcanho` VARCHAR(255),
  `diachi` TEXT,
  `diachicu` TEXT,
  `loaibds` VARCHAR(255),
  `phongan` VARCHAR(255),
  `nhabep` VARCHAR(255),
  `santhuong` VARCHAR(255),
  `chodexehoi` VARCHAR(255),
  `chinhchu` VARCHAR(255),
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_sdf_url` (`url`(100)),
  INDEX `idx_sdf_domain` (`domain`),
  INDEX `idx_sdf_link` (`link_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Detail images per detail_id
CREATE TABLE IF NOT EXISTS `scraped_detail_images` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `detail_id` INT NOT NULL,
  `image_url` VARCHAR(2000) NOT NULL,
  `idx` INT DEFAULT NULL,
  `status` VARCHAR(50) NOT NULL DEFAULT 'PENDING',
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_sdi_detail` (`detail_id`),
  INDEX `idx_sdi_status` (`status`),
  INDEX `idx_sdi_url` (`image_url`(150)),
  CONSTRAINT `fk_sdi_detail`
    FOREIGN KEY (`detail_id`) REFERENCES `scraped_details_flat`(`id`)
    ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Downloaded images log
CREATE TABLE IF NOT EXISTS `downloaded_images` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `image_url` VARCHAR(2000) NOT NULL,
  `file_path` VARCHAR(2000) DEFAULT NULL,
  `status` VARCHAR(50) NOT NULL DEFAULT 'PENDING',
  `domain` VARCHAR(255) DEFAULT NULL,
  `error` TEXT DEFAULT NULL,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `downloaded_at` TIMESTAMP NULL,
  INDEX `idx_downloaded_images_url` (`image_url`(100)),
  INDEX `idx_downloaded_images_status` (`status`),
  INDEX `idx_downloaded_images_domain` (`domain`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Scheduler tasks
CREATE TABLE IF NOT EXISTS `scheduler_tasks` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `name` VARCHAR(255) NOT NULL,
  `active` TINYINT(1) NOT NULL DEFAULT 1,
  `is_running` TINYINT(1) NOT NULL DEFAULT 0,
  `run_now` TINYINT(1) NOT NULL DEFAULT 0,
  `enable_listing` TINYINT(1) NOT NULL DEFAULT 1,
  `enable_detail` TINYINT(1) NOT NULL DEFAULT 1,
  `enable_image` TINYINT(1) NOT NULL DEFAULT 0,
  `schedule_type` VARCHAR(20) NOT NULL DEFAULT 'interval',
  `interval_minutes` INT DEFAULT NULL,
  `run_times` VARCHAR(255) DEFAULT NULL,
  `listing_template_path` VARCHAR(2000) DEFAULT NULL,
  `detail_template_path` VARCHAR(2000) DEFAULT NULL,
  `start_url` VARCHAR(2000) DEFAULT NULL,
  `max_pages` INT DEFAULT 1,
  `domain` VARCHAR(255) DEFAULT NULL,
  `loaihinh` VARCHAR(255) DEFAULT NULL,
  `city_id` INT DEFAULT NULL,
  `city_name` VARCHAR(255) DEFAULT NULL,
  `ward_id` INT DEFAULT NULL,
  `ward_name` VARCHAR(255) DEFAULT NULL,
  `new_city_id` INT DEFAULT NULL,
  `new_city_name` VARCHAR(255) DEFAULT NULL,
  `new_ward_id` INT DEFAULT NULL,
  `new_ward_name` VARCHAR(255) DEFAULT NULL,
  `cancel_requested` TINYINT(1) NOT NULL DEFAULT 0,
  `listing_show_browser` TINYINT(1) DEFAULT 1,
  `listing_fake_scroll` TINYINT(1) DEFAULT 1,
  `listing_fake_hover` TINYINT(1) DEFAULT 0,
  `listing_wait_load_min` FLOAT DEFAULT 20,
  `listing_wait_load_max` FLOAT DEFAULT 30,
  `listing_wait_next_min` FLOAT DEFAULT 10,
  `listing_wait_next_max` FLOAT DEFAULT 20,
  `detail_show_browser` TINYINT(1) DEFAULT 0,
  `detail_fake_scroll` TINYINT(1) DEFAULT 1,
  `detail_fake_hover` TINYINT(1) DEFAULT 1,
  `detail_wait_load_min` FLOAT DEFAULT 2,
  `detail_wait_load_max` FLOAT DEFAULT 5,
  `detail_delay_min` FLOAT DEFAULT 2,
  `detail_delay_max` FLOAT DEFAULT 3,
  `image_dir` VARCHAR(2000) DEFAULT NULL,
  `images_per_minute` INT DEFAULT 30,
  `image_domain` VARCHAR(255) DEFAULT NULL,
  `image_status` VARCHAR(50) DEFAULT NULL,
  `last_run_at` TIMESTAMP NULL,
  `next_run_at` TIMESTAMP NULL,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX `idx_scheduler_tasks_active` (`active`),
  INDEX `idx_scheduler_tasks_next` (`next_run_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Scheduler logs
CREATE TABLE IF NOT EXISTS `scheduler_logs` (
  `id` INT AUTO_INCREMENT PRIMARY KEY,
  `task_id` INT NOT NULL,
  `stage` VARCHAR(50) DEFAULT NULL,
  `status` VARCHAR(50) DEFAULT NULL,
  `message` TEXT DEFAULT NULL,
  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX `idx_scheduler_logs_task` (`task_id`),
  INDEX `idx_scheduler_logs_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
