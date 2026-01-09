-- Tạo database cho crawler
CREATE DATABASE IF NOT EXISTS `craw_db` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Sử dụng database
USE `craw_db`;

-- Tạo bảng collected_links
CREATE TABLE IF NOT EXISTS `collected_links` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `url` VARCHAR(2000) NOT NULL UNIQUE,
    `status` VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    `domain` VARCHAR(255) DEFAULT NULL,
    `loaihinh` VARCHAR(255) DEFAULT NULL,
    `trade_type` VARCHAR(50) DEFAULT NULL,
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_collected_links_url` (`url`(100)),
    INDEX `idx_collected_links_status` (`status`),
    INDEX `idx_collected_links_domain` (`domain`),
    INDEX `idx_collected_links_loaihinh` (`loaihinh`),
    INDEX `idx_collected_links_trade_type` (`trade_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
