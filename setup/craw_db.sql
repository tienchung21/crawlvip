-- phpMyAdmin SQL Dump
-- version 5.2.3
-- https://www.phpmyadmin.net/
--
-- Máy chủ: localhost:3306
-- Thời gian đã tạo: Th1 19, 2026 lúc 02:13 AM
-- Phiên bản máy phục vụ: 8.0.30
-- Phiên bản PHP: 8.4.13

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Cơ sở dữ liệu: `craw_db`
--

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `ad_listing_detail`
--

CREATE TABLE `ad_listing_detail` (
  `account_id` longtext COLLATE utf8mb4_unicode_ci,
  `account_name` longtext COLLATE utf8mb4_unicode_ci,
  `account_oid` longtext COLLATE utf8mb4_unicode_ci,
  `ad_features` longtext COLLATE utf8mb4_unicode_ci,
  `ad_id` bigint NOT NULL,
  `ad_labels` longtext COLLATE utf8mb4_unicode_ci,
  `area` longtext COLLATE utf8mb4_unicode_ci,
  `area_name` longtext COLLATE utf8mb4_unicode_ci,
  `area_v2` longtext COLLATE utf8mb4_unicode_ci,
  `avatar` longtext COLLATE utf8mb4_unicode_ci,
  `average_rating` longtext COLLATE utf8mb4_unicode_ci,
  `average_rating_for_seller` longtext COLLATE utf8mb4_unicode_ci,
  `body` longtext COLLATE utf8mb4_unicode_ci,
  `business_days` longtext COLLATE utf8mb4_unicode_ci,
  `category` longtext COLLATE utf8mb4_unicode_ci,
  `category_name` longtext COLLATE utf8mb4_unicode_ci,
  `contain_videos` longtext COLLATE utf8mb4_unicode_ci,
  `date` longtext COLLATE utf8mb4_unicode_ci,
  `fee_type` longtext COLLATE utf8mb4_unicode_ci,
  `full_name` longtext COLLATE utf8mb4_unicode_ci,
  `furnishing_sell` longtext COLLATE utf8mb4_unicode_ci,
  `house_type` longtext COLLATE utf8mb4_unicode_ci,
  `image` longtext COLLATE utf8mb4_unicode_ci,
  `image_thumbnails` longtext COLLATE utf8mb4_unicode_ci,
  `images` longtext COLLATE utf8mb4_unicode_ci,
  `inspection_images` longtext COLLATE utf8mb4_unicode_ci,
  `is_sticky` longtext COLLATE utf8mb4_unicode_ci,
  `is_zalo_show` longtext COLLATE utf8mb4_unicode_ci,
  `label_campaigns` longtext COLLATE utf8mb4_unicode_ci,
  `latitude` longtext COLLATE utf8mb4_unicode_ci,
  `list_id` longtext COLLATE utf8mb4_unicode_ci,
  `list_time` longtext COLLATE utf8mb4_unicode_ci,
  `location` longtext COLLATE utf8mb4_unicode_ci,
  `longitude` longtext COLLATE utf8mb4_unicode_ci,
  `number_of_images` longtext COLLATE utf8mb4_unicode_ci,
  `orig_list_time` longtext COLLATE utf8mb4_unicode_ci,
  `params` longtext COLLATE utf8mb4_unicode_ci,
  `price` longtext COLLATE utf8mb4_unicode_ci,
  `price_million_per_m2` longtext COLLATE utf8mb4_unicode_ci,
  `price_string` longtext COLLATE utf8mb4_unicode_ci,
  `property_legal_document` longtext COLLATE utf8mb4_unicode_ci,
  `protection_entitlement` longtext COLLATE utf8mb4_unicode_ci,
  `pty_characteristics` longtext COLLATE utf8mb4_unicode_ci,
  `pty_jupiter` longtext COLLATE utf8mb4_unicode_ci,
  `pty_map` longtext COLLATE utf8mb4_unicode_ci,
  `pty_map_modifier` longtext COLLATE utf8mb4_unicode_ci,
  `pty_project_name` longtext COLLATE utf8mb4_unicode_ci,
  `region` longtext COLLATE utf8mb4_unicode_ci,
  `region_name` longtext COLLATE utf8mb4_unicode_ci,
  `region_name_v3` longtext COLLATE utf8mb4_unicode_ci,
  `region_v2` longtext COLLATE utf8mb4_unicode_ci,
  `rooms` longtext COLLATE utf8mb4_unicode_ci,
  `seller_info` longtext COLLATE utf8mb4_unicode_ci,
  `shop` longtext COLLATE utf8mb4_unicode_ci,
  `size` longtext COLLATE utf8mb4_unicode_ci,
  `size_unit_string` longtext COLLATE utf8mb4_unicode_ci,
  `sold_ads` longtext COLLATE utf8mb4_unicode_ci,
  `special_display_images` longtext COLLATE utf8mb4_unicode_ci,
  `specific_service_offered` longtext COLLATE utf8mb4_unicode_ci,
  `state` longtext COLLATE utf8mb4_unicode_ci,
  `status` longtext COLLATE utf8mb4_unicode_ci,
  `street_name` longtext COLLATE utf8mb4_unicode_ci,
  `street_number` longtext COLLATE utf8mb4_unicode_ci,
  `streetnumber_display` longtext COLLATE utf8mb4_unicode_ci,
  `subject` longtext COLLATE utf8mb4_unicode_ci,
  `thumbnail_image` longtext COLLATE utf8mb4_unicode_ci,
  `total_rating` longtext COLLATE utf8mb4_unicode_ci,
  `total_rating_for_seller` longtext COLLATE utf8mb4_unicode_ci,
  `type` longtext COLLATE utf8mb4_unicode_ci,
  `videos` longtext COLLATE utf8mb4_unicode_ci,
  `ward` longtext COLLATE utf8mb4_unicode_ci,
  `ward_name` longtext COLLATE utf8mb4_unicode_ci,
  `ward_name_v3` longtext COLLATE utf8mb4_unicode_ci,
  `webp_image` longtext COLLATE utf8mb4_unicode_ci,
  `address` longtext COLLATE utf8mb4_unicode_ci,
  `apartment_feature` longtext COLLATE utf8mb4_unicode_ci,
  `apartment_type` longtext COLLATE utf8mb4_unicode_ci,
  `balconydirection` longtext COLLATE utf8mb4_unicode_ci,
  `block` longtext COLLATE utf8mb4_unicode_ci,
  `commercial_type` longtext COLLATE utf8mb4_unicode_ci,
  `company_ad` longtext COLLATE utf8mb4_unicode_ci,
  `deposit` longtext COLLATE utf8mb4_unicode_ci,
  `detail_address` longtext COLLATE utf8mb4_unicode_ci,
  `direction` longtext COLLATE utf8mb4_unicode_ci,
  `floornumber` longtext COLLATE utf8mb4_unicode_ci,
  `floors` longtext COLLATE utf8mb4_unicode_ci,
  `furnishing_rent` longtext COLLATE utf8mb4_unicode_ci,
  `has_video` longtext COLLATE utf8mb4_unicode_ci,
  `is_block_similar_ads_other_agent` longtext COLLATE utf8mb4_unicode_ci,
  `is_good_room` longtext COLLATE utf8mb4_unicode_ci,
  `is_main_street` longtext COLLATE utf8mb4_unicode_ci,
  `land_type` longtext COLLATE utf8mb4_unicode_ci,
  `length` longtext COLLATE utf8mb4_unicode_ci,
  `living_size` longtext COLLATE utf8mb4_unicode_ci,
  `location_id` longtext COLLATE utf8mb4_unicode_ci,
  `project_oid` longtext COLLATE utf8mb4_unicode_ci,
  `projectid` longtext COLLATE utf8mb4_unicode_ci,
  `projectimages` longtext COLLATE utf8mb4_unicode_ci,
  `property_status` longtext COLLATE utf8mb4_unicode_ci,
  `shop_alias` longtext COLLATE utf8mb4_unicode_ci,
  `size_unit` longtext COLLATE utf8mb4_unicode_ci,
  `special_display` longtext COLLATE utf8mb4_unicode_ci,
  `sticky_ad_type` longtext COLLATE utf8mb4_unicode_ci,
  `stickyad_feature` longtext COLLATE utf8mb4_unicode_ci,
  `toilets` longtext COLLATE utf8mb4_unicode_ci,
  `unique_street_id` longtext COLLATE utf8mb4_unicode_ci,
  `unitnumber` longtext COLLATE utf8mb4_unicode_ci,
  `unitnumber_display` longtext COLLATE utf8mb4_unicode_ci,
  `width` longtext COLLATE utf8mb4_unicode_ci,
  `raw_json` json DEFAULT NULL,
  `__source_file` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `__source_o` int DEFAULT NULL,
  `time_crawl` bigint DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `cenhomedetail`
--

CREATE TABLE `cenhomedetail` (
  `id` bigint NOT NULL,
  `trade_type` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `address` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `agency_avatar` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `agency_avg_rate` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `agency_fullname` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `agency_phone_number` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `agency_total_review` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `area` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `area_range` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `balcony_direction` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `bathroom` int DEFAULT NULL,
  `bathroom_range` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `bedroom` int DEFAULT NULL,
  `bedroom_range` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `block_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `build_time` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `canonical_url` varchar(500) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `category_real_estate` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `contact_address` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `contact_email` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `contact_full_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `contact_phone` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `content` longtext COLLATE utf8mb4_unicode_ci,
  `create_time` datetime DEFAULT NULL,
  `creator_type` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `depth` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `direction` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `district_id` bigint DEFAULT NULL,
  `drying_yard` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `facade` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `facade_count` int DEFAULT NULL,
  `feature` json DEFAULT NULL,
  `fee_bearer` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `floor` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `floor_count` int DEFAULT NULL,
  `floor_detail` json DEFAULT NULL,
  `floor_under_ground` int DEFAULT NULL,
  `furniture_status` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `geoloc` json DEFAULT NULL,
  `is_deal` tinyint(1) DEFAULT NULL,
  `is_exclusive` tinyint(1) DEFAULT NULL,
  `is_urgent` tinyint(1) DEFAULT NULL,
  `kitchen` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `legal_paper` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `location` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `media3ds` json DEFAULT NULL,
  `media_images` json DEFAULT NULL,
  `media_videos` json DEFAULT NULL,
  `meta_description` text COLLATE utf8mb4_unicode_ci,
  `meta_keywords` text COLLATE utf8mb4_unicode_ci,
  `meta_title` text COLLATE utf8mb4_unicode_ci,
  `negotiate` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `payment_status` int DEFAULT NULL,
  `period` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `price` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `price_range` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `properties` json DEFAULT NULL,
  `property_type` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `province_id` bigint DEFAULT NULL,
  `publish_time` datetime DEFAULT NULL,
  `quality` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `real_estate_code` varchar(100) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `real_estate_status` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `reception_room` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `request_id` bigint DEFAULT NULL,
  `road` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `search_district` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `search_province` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `shape` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `slug` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `status` int DEFAULT NULL,
  `suggest_district` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `suggest_location` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `suggest_province` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `suggest_ward` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `tags` json DEFAULT NULL,
  `title` text COLLATE utf8mb4_unicode_ci,
  `total_price` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `type_of_house` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `unit_price_id` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  `urgent_scope` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `use_area` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `user_avatar` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `user_avg_rate` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `user_fullname` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `user_total_review` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `ward_id` bigint DEFAULT NULL,
  `width` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `cenhomes_ads`
--

CREATE TABLE `cenhomes_ads` (
  `id` bigint NOT NULL,
  `trade_type` varchar(10) COLLATE utf8mb4_unicode_ci NOT NULL,
  `title` text COLLATE utf8mb4_unicode_ci,
  `slug` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `category_real_estate` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `address` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `location` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `search_district` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `search_province` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `area` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `use_area` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `balcony_direction` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `bathroom_range` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `bedroom_range` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `price` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `create_time` datetime DEFAULT NULL,
  `publish_time` datetime DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  `media_images` json DEFAULT NULL,
  `raw_json` json DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `crawl` tinyint(1) DEFAULT NULL,
  `crawl_status` tinyint(1) DEFAULT NULL,
  `crawl_http` int DEFAULT NULL,
  `crawl_time` datetime DEFAULT NULL,
  `crawl_error` text COLLATE utf8mb4_unicode_ci
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `collected_links`
--

CREATE TABLE `collected_links` (
  `id` int NOT NULL,
  `url` varchar(2000) COLLATE utf8mb4_unicode_ci NOT NULL,
  `status` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'PENDING',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `domain` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `loaihinh` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `city_id` int DEFAULT NULL,
  `city_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `ward_id` int DEFAULT NULL,
  `ward_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `new_city_id` int DEFAULT NULL,
  `new_city_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `new_ward_id` int DEFAULT NULL,
  `new_ward_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `trade_type` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `data_clean`
--

CREATE TABLE `data_clean` (
  `ad_id` bigint DEFAULT NULL,
  `list_id` bigint DEFAULT NULL,
  `list_time` bigint DEFAULT NULL,
  `orig_list_time` bigint DEFAULT NULL,
  `region_v2` bigint DEFAULT NULL,
  `area_v2` bigint DEFAULT NULL,
  `ward` bigint DEFAULT NULL,
  `street_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `street_number` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `unique_street_id` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `category` int DEFAULT NULL,
  `size` double DEFAULT NULL,
  `price` bigint DEFAULT NULL,
  `type` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `time_crawl` bigint DEFAULT NULL,
  `price_m2_vnd` decimal(18,2) DEFAULT NULL,
  `cf_ward_id_new` int DEFAULT NULL,
  `cf_ward_name_new` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `cf_region_id_new` int DEFAULT NULL,
  `cf_region_name_new` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `list_ym` char(7) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `median_group` tinyint DEFAULT NULL COMMENT '1: nha gan lien dat, 2: can ho, 3: dat, 4: cho thue',
  `median_flag` tinyint(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `data_clean_stats`
--

CREATE TABLE `data_clean_stats` (
  `id` bigint UNSIGNED NOT NULL,
  `scope` varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL,
  `new_region_id` int DEFAULT NULL,
  `new_region_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `new_ward_id` int DEFAULT NULL,
  `new_ward_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `type` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `category` int DEFAULT NULL,
  `month` varchar(7) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `avg_price_m2` decimal(20,2) DEFAULT NULL,
  `median_price_m2` decimal(20,2) DEFAULT NULL,
  `total_rows` int DEFAULT NULL,
  `trimmed_rows` int DEFAULT NULL,
  `converted_at` datetime DEFAULT NULL,
  `median_group` tinyint DEFAULT NULL,
  `min_price_m2` decimal(20,2) DEFAULT NULL,
  `max_price_m2` decimal(20,2) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `downloaded_images`
--

CREATE TABLE `downloaded_images` (
  `id` int NOT NULL,
  `image_url` varchar(2000) COLLATE utf8mb4_unicode_ci NOT NULL,
  `file_path` varchar(2000) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `status` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'PENDING',
  `domain` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `error` text COLLATE utf8mb4_unicode_ci,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `downloaded_at` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `location_detail`
--

CREATE TABLE `location_detail` (
  `region_id` bigint NOT NULL,
  `area_id` bigint DEFAULT NULL,
  `ward_id` bigint DEFAULT NULL,
  `level` tinyint NOT NULL,
  `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name_url` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `unit_type` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `source` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'loadRegionsV2',
  `raw_payload` json DEFAULT NULL,
  `is_active` tinyint(1) NOT NULL DEFAULT '1',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `area_id_n` bigint GENERATED ALWAYS AS (ifnull(`area_id`,0)) STORED NOT NULL,
  `ward_id_n` bigint GENERATED ALWAYS AS (ifnull(`ward_id`,0)) STORED NOT NULL,
  `nhatot_slug` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `nhadat_nhatot`
--

CREATE TABLE `nhadat_nhatot` (
  `id` int NOT NULL,
  `old_city_id` int DEFAULT NULL COMMENT 'ID tinh/thanh cu (nhadat/cafeland)',
  `old_city_name` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Ten tinh/thanh cu (nhadat/cafeland)',
  `old_city_url` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Slug tinh/thanh cu (nhadat/cafeland)',
  `region_id` bigint DEFAULT NULL COMMENT 'ID tinh/thanh (nhatot - location_detail)',
  `region_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Ten tinh/thanh (nhatot - location_detail)',
  `match_type` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'slug' COMMENT 'Kieu match: slug|name|manual',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `cf_district_id` int DEFAULT NULL,
  `cf_district_name` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `cf_district_url` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `nt_district_id` bigint DEFAULT NULL,
  `nt_district_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `nt_district_url` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `cf_ward_id` int DEFAULT NULL,
  `cf_ward_name` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `cf_ward_slug` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `nt_ward_id` bigint DEFAULT NULL,
  `nt_ward_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `nt_ward_slug` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `nhadat_nhatot_ward`
--

CREATE TABLE `nhadat_nhatot_ward` (
  `nt_ward_id` bigint NOT NULL,
  `nt_ward_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `nt_ward_slug` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `region_id` bigint DEFAULT NULL,
  `area_id` bigint DEFAULT NULL,
  `cf_ward_id` int DEFAULT NULL,
  `cf_ward_name` varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `cf_ward_slug` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `match_type` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `scheduler_logs`
--

CREATE TABLE `scheduler_logs` (
  `id` int NOT NULL,
  `task_id` int NOT NULL,
  `stage` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `status` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `message` text COLLATE utf8mb4_unicode_ci,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `scheduler_tasks`
--

CREATE TABLE `scheduler_tasks` (
  `id` int NOT NULL,
  `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `active` tinyint(1) NOT NULL DEFAULT '1',
  `schedule_type` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'interval',
  `interval_minutes` int DEFAULT NULL,
  `run_times` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `listing_template_path` varchar(2000) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `detail_template_path` varchar(2000) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `start_url` varchar(2000) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `max_pages` int DEFAULT '1',
  `domain` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `loaihinh` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `image_dir` varchar(2000) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `images_per_minute` int DEFAULT '30',
  `last_run_at` timestamp NULL DEFAULT NULL,
  `next_run_at` timestamp NULL DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `listing_show_browser` tinyint(1) DEFAULT '1',
  `listing_fake_scroll` tinyint(1) DEFAULT '1',
  `listing_fake_hover` tinyint(1) DEFAULT '0',
  `listing_wait_load_min` float DEFAULT '20',
  `listing_wait_load_max` float DEFAULT '30',
  `listing_wait_next_min` float DEFAULT '10',
  `listing_wait_next_max` float DEFAULT '20',
  `detail_show_browser` tinyint(1) DEFAULT '0',
  `detail_fake_scroll` tinyint(1) DEFAULT '1',
  `detail_fake_hover` tinyint(1) DEFAULT '1',
  `detail_wait_load_min` float DEFAULT '2',
  `detail_wait_load_max` float DEFAULT '5',
  `detail_delay_min` float DEFAULT '2',
  `detail_delay_max` float DEFAULT '3',
  `is_running` tinyint(1) NOT NULL DEFAULT '0',
  `enable_listing` tinyint(1) NOT NULL DEFAULT '1',
  `enable_detail` tinyint(1) NOT NULL DEFAULT '1',
  `enable_image` tinyint(1) NOT NULL DEFAULT '0',
  `run_now` tinyint(1) NOT NULL DEFAULT '0',
  `cancel_requested` tinyint(1) NOT NULL DEFAULT '0',
  `city_id` int DEFAULT NULL,
  `city_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `ward_id` int DEFAULT NULL,
  `ward_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `new_city_id` int DEFAULT NULL,
  `new_city_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `new_ward_id` int DEFAULT NULL,
  `new_ward_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `image_domain` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `image_status` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `trade_type` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `scraped_details`
--

CREATE TABLE `scraped_details` (
  `id` int NOT NULL,
  `link_id` int DEFAULT NULL,
  `url` varchar(2000) COLLATE utf8mb4_unicode_ci NOT NULL,
  `domain` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `data_json` longtext COLLATE utf8mb4_unicode_ci,
  `success` tinyint(1) DEFAULT '1',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `scraped_details_flat`
--

CREATE TABLE `scraped_details_flat` (
  `id` int NOT NULL,
  `link_id` int DEFAULT NULL,
  `url` varchar(2000) COLLATE utf8mb4_unicode_ci NOT NULL,
  `domain` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `title` text COLLATE utf8mb4_unicode_ci,
  `img_count` int DEFAULT NULL,
  `mota` text COLLATE utf8mb4_unicode_ci,
  `khoanggia` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `dientich` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sophongngu` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `phaply` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `noithat` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `thuocduan` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `trangthaiduan` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `tenmoigioi` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sodienthoai` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `map` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `matin` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `loaitin` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `ngayhethan` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `ngaydang` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `diachi` text COLLATE utf8mb4_unicode_ci,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `sophongvesinh` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `huongnha` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `huongbancong` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `mattien` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `duongvao` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sotang` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `loaihinhnhao` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `dientichsudung` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `gia_m2` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `dacdiemnhadat` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `chieungang` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `chieudai` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `diachicu` text COLLATE utf8mb4_unicode_ci,
  `loaibds` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `phongan` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `nhabep` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `santhuong` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `chodexehoi` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `chinhchu` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `thoigianvaoo` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `giadien` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `gianuoc` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `giainternet` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sotiencoc` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `tangso` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `loaihinhvanphong` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `loaihinhdat` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `loaihinhcanho` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `gia_mn` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `scraped_detail_images`
--

CREATE TABLE `scraped_detail_images` (
  `id` int NOT NULL,
  `detail_id` int NOT NULL,
  `image_url` varchar(2000) COLLATE utf8mb4_unicode_ci NOT NULL,
  `idx` int DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `status` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'PENDING'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `transaction_city`
--

CREATE TABLE `transaction_city` (
  `city_id` smallint UNSIGNED NOT NULL,
  `city_parent_id` smallint UNSIGNED NOT NULL DEFAULT '0',
  `cf_location_id` int DEFAULT '0',
  `pic_thumb` varchar(64) DEFAULT NULL,
  `city_title` varchar(128) DEFAULT NULL,
  `city_title_no` varchar(250) DEFAULT NULL,
  `city_titleen` varchar(128) DEFAULT NULL,
  `city_realias` varchar(255) DEFAULT NULL,
  `city_order` smallint UNSIGNED NOT NULL DEFAULT '1',
  `city_counter` int UNSIGNED NOT NULL DEFAULT '0',
  `city_enabled` tinyint UNSIGNED NOT NULL DEFAULT '0',
  `city_lat` varchar(80) DEFAULT NULL,
  `city_lng` varchar(80) DEFAULT NULL,
  `city_loai` int NOT NULL DEFAULT '2',
  `city_title_news` varchar(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci DEFAULT NULL,
  `city_parent_of_id` int DEFAULT '0',
  `city_title_full` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `new_city_parent_id` int DEFAULT '0',
  `new_city_title` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `new_city_title_no` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `ward_of_district_id` int DEFAULT '0',
  `ward_of_district_url` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `check_data_id` int NOT NULL DEFAULT '0',
  `is_processed` tinyint UNSIGNED NOT NULL DEFAULT '0' COMMENT '0: chưa xử lý, 1: đã gộp',
  `is_merged` tinyint UNSIGNED NOT NULL DEFAULT '0' COMMENT '0: giữ nguyên, 1: bị gộp vào tỉnh khác'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `transaction_city_merge`
--

CREATE TABLE `transaction_city_merge` (
  `id` int UNSIGNED NOT NULL,
  `new_city_id` smallint UNSIGNED NOT NULL COMMENT 'ID tỉnh/thành/phường/xã sau sáp nhập',
  `old_city_id` smallint UNSIGNED NOT NULL COMMENT 'ID tỉnh/thành/phường/xã đã bị sáp nhập',
  `new_city_parent_id` smallint UNSIGNED DEFAULT '0' COMMENT 'ID cha (tỉnh/huyện) mới sau sáp nhập',
  `old_city_parent_id` smallint UNSIGNED DEFAULT '0' COMMENT 'ID cha (tỉnh/huyện) cũ trước sáp nhập',
  `old_district_id` int DEFAULT '0' COMMENT 'ID của Quận/Huyện cũ',
  `new_city_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Tên sau sáp nhập',
  `old_city_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Tên đã bị sáp nhập',
  `new_city_url` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `old_city_url` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `action_type` int DEFAULT '0',
  `redirect` int DEFAULT '0'
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `transaction_city_mergev2`
--

CREATE TABLE `transaction_city_mergev2` (
  `id` int UNSIGNED NOT NULL,
  `new_city_id` smallint UNSIGNED NOT NULL COMMENT 'ID tỉnh/thành/phường/xã sau sáp nhập',
  `old_city_id` smallint UNSIGNED NOT NULL COMMENT 'ID tỉnh/thành/phường/xã đã bị sáp nhập',
  `new_city_parent_id` smallint UNSIGNED DEFAULT '0' COMMENT 'ID cha (tỉnh/huyện) mới sau sáp nhập',
  `old_city_parent_id` smallint UNSIGNED DEFAULT '0' COMMENT 'ID cha (tỉnh/huyện) cũ trước sáp nhập',
  `old_district_id` int DEFAULT '0' COMMENT 'ID của Quận/Huyện cũ',
  `new_city_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Tên sau sáp nhập',
  `old_city_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'Tên đã bị sáp nhập',
  `new_city_url` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `old_city_url` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `action_type` int DEFAULT '0',
  `redirect` int DEFAULT '0',
  `old_parent_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `old_parent_url` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `old_district_name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `old_district_url` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `transaction_city_new`
--

CREATE TABLE `transaction_city_new` (
  `city_id` smallint UNSIGNED NOT NULL,
  `city_parent_id` smallint UNSIGNED NOT NULL DEFAULT '0',
  `cf_location_id` int DEFAULT '0',
  `pic_thumb` varchar(64) DEFAULT NULL,
  `city_title` varchar(128) DEFAULT NULL,
  `city_title_no` varchar(250) DEFAULT NULL,
  `city_titleen` varchar(128) DEFAULT NULL,
  `city_realias` varchar(255) DEFAULT NULL,
  `city_order` smallint UNSIGNED NOT NULL DEFAULT '1',
  `city_counter` int UNSIGNED NOT NULL DEFAULT '0',
  `city_enabled` tinyint UNSIGNED NOT NULL DEFAULT '0',
  `city_lat` varchar(50) DEFAULT NULL,
  `city_lng` varchar(50) DEFAULT NULL,
  `city_loai` int NOT NULL DEFAULT '2',
  `city_title_news` varchar(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci DEFAULT NULL,
  `city_parent_of_id` int DEFAULT '0',
  `city_title_full` varchar(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `new_city_parent_id` int DEFAULT '0',
  `new_city_title` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `new_city_title_no` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `new_city_title_seo` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `check_data_id` int NOT NULL DEFAULT '0',
  `is_processed` tinyint UNSIGNED NOT NULL DEFAULT '0' COMMENT '0: chưa xử lý, 1: đã gộp',
  `is_merged` tinyint UNSIGNED NOT NULL DEFAULT '0' COMMENT '0: giữ nguyên, 1: bị gộp vào tỉnh khác'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

--
-- Chỉ mục cho các bảng đã đổ
--

--
-- Chỉ mục cho bảng `ad_listing_detail`
--
ALTER TABLE `ad_listing_detail`
  ADD PRIMARY KEY (`ad_id`);

--
-- Chỉ mục cho bảng `cenhomedetail`
--
ALTER TABLE `cenhomedetail`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `cenhomes_ads`
--
ALTER TABLE `cenhomes_ads`
  ADD PRIMARY KEY (`id`);

--
-- Chỉ mục cho bảng `collected_links`
--
ALTER TABLE `collected_links`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `uk_collected_links_url` (`url`(255)),
  ADD KEY `idx_collected_links_url` (`url`(255)),
  ADD KEY `idx_collected_links_status` (`status`),
  ADD KEY `idx_collected_links_domain` (`domain`),
  ADD KEY `idx_collected_links_loaihinh` (`loaihinh`),
  ADD KEY `idx_collected_links_updated_at` (`updated_at`),
  ADD KEY `idx_collected_links_trade_type` (`trade_type`);

--
-- Chỉ mục cho bảng `data_clean`
--
ALTER TABLE `data_clean`
  ADD KEY `idx_data_clean_region_ward` (`region_v2`,`ward`),
  ADD KEY `idx_list_ym` (`list_ym`),
  ADD KEY `idx_dc_ward_stats` (`cf_region_id_new`,`cf_ward_id_new`,`type`,`median_group`,`list_ym`,`price_m2_vnd`),
  ADD KEY `idx_dc_region_stats` (`cf_region_id_new`,`type`,`median_group`,`list_ym`,`price_m2_vnd`),
  ADD KEY `idx_median_flag` (`median_flag`);

--
-- Chỉ mục cho bảng `data_clean_stats`
--
ALTER TABLE `data_clean_stats`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `uq_ward_scope` (`scope`,`new_region_id`,`new_ward_id`,`type`,`median_group`,`month`),
  ADD KEY `idx_scope_region` (`scope`,`new_region_id`),
  ADD KEY `idx_scope_ward` (`scope`,`new_ward_id`),
  ADD KEY `idx_month` (`month`);

--
-- Chỉ mục cho bảng `downloaded_images`
--
ALTER TABLE `downloaded_images`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx_downloaded_images_url` (`image_url`(100)),
  ADD KEY `idx_downloaded_images_status` (`status`),
  ADD KEY `idx_downloaded_images_domain` (`domain`);

--
-- Chỉ mục cho bảng `location_detail`
--
ALTER TABLE `location_detail`
  ADD PRIMARY KEY (`region_id`,`area_id_n`,`ward_id_n`),
  ADD KEY `idx_level` (`level`),
  ADD KEY `idx_region_area` (`region_id`,`area_id_n`),
  ADD KEY `idx_name_url` (`name_url`);

--
-- Chỉ mục cho bảng `nhadat_nhatot`
--
ALTER TABLE `nhadat_nhatot`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx_nhadat_nhatot_ward` (`match_type`,`region_id`,`nt_ward_id`);

--
-- Chỉ mục cho bảng `nhadat_nhatot_ward`
--
ALTER TABLE `nhadat_nhatot_ward`
  ADD PRIMARY KEY (`nt_ward_id`);

--
-- Chỉ mục cho bảng `scheduler_logs`
--
ALTER TABLE `scheduler_logs`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx_scheduler_logs_task` (`task_id`),
  ADD KEY `idx_scheduler_logs_created` (`created_at`);

--
-- Chỉ mục cho bảng `scheduler_tasks`
--
ALTER TABLE `scheduler_tasks`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx_scheduler_tasks_active` (`active`),
  ADD KEY `idx_scheduler_tasks_next` (`next_run_at`);

--
-- Chỉ mục cho bảng `scraped_details`
--
ALTER TABLE `scraped_details`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx_scraped_details_url` (`url`(100)),
  ADD KEY `idx_scraped_details_domain` (`domain`),
  ADD KEY `idx_scraped_details_success` (`success`);

--
-- Chỉ mục cho bảng `scraped_details_flat`
--
ALTER TABLE `scraped_details_flat`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx_sdf_url` (`url`(100)),
  ADD KEY `idx_sdf_domain` (`domain`),
  ADD KEY `idx_sdf_link` (`link_id`);

--
-- Chỉ mục cho bảng `scraped_detail_images`
--
ALTER TABLE `scraped_detail_images`
  ADD PRIMARY KEY (`id`),
  ADD KEY `idx_sdi_detail` (`detail_id`),
  ADD KEY `idx_sdi_url` (`image_url`(150)),
  ADD KEY `idx_sdi_status` (`status`);

--
-- Chỉ mục cho bảng `transaction_city`
--
ALTER TABLE `transaction_city`
  ADD PRIMARY KEY (`city_id`),
  ADD KEY `cat_title` (`city_title`),
  ADD KEY `city_realias` (`city_realias`),
  ADD KEY `city_parent_id` (`city_parent_id`),
  ADD KEY `city_title_no` (`city_title_no`),
  ADD KEY `city_titleen` (`city_titleen`),
  ADD KEY `city_enabled` (`city_enabled`),
  ADD KEY `cf_location_id` (`cf_location_id`),
  ADD KEY `city_order` (`city_order`),
  ADD KEY `city_id` (`city_id`,`city_parent_id`,`cf_location_id`,`city_title_no`,`city_order`,`city_enabled`);

--
-- Chỉ mục cho bảng `transaction_city_merge`
--
ALTER TABLE `transaction_city_merge`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `old_city_id` (`old_city_id`),
  ADD KEY `new_city_id` (`new_city_id`);

--
-- Chỉ mục cho bảng `transaction_city_mergev2`
--
ALTER TABLE `transaction_city_mergev2`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `old_city_id` (`old_city_id`),
  ADD KEY `new_city_id` (`new_city_id`);

--
-- Chỉ mục cho bảng `transaction_city_new`
--
ALTER TABLE `transaction_city_new`
  ADD PRIMARY KEY (`city_id`),
  ADD KEY `cat_title` (`city_title`),
  ADD KEY `city_realias` (`city_realias`),
  ADD KEY `city_parent_id` (`city_parent_id`),
  ADD KEY `city_title_no` (`city_title_no`),
  ADD KEY `city_titleen` (`city_titleen`),
  ADD KEY `city_enabled` (`city_enabled`),
  ADD KEY `cf_location_id` (`cf_location_id`),
  ADD KEY `city_order` (`city_order`),
  ADD KEY `city_id` (`city_id`,`city_parent_id`,`cf_location_id`,`city_title_no`,`city_order`,`city_enabled`),
  ADD KEY `new_city_title_no` (`new_city_title_no`);

--
-- AUTO_INCREMENT cho các bảng đã đổ
--

--
-- AUTO_INCREMENT cho bảng `collected_links`
--
ALTER TABLE `collected_links`
  MODIFY `id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `data_clean_stats`
--
ALTER TABLE `data_clean_stats`
  MODIFY `id` bigint UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `downloaded_images`
--
ALTER TABLE `downloaded_images`
  MODIFY `id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `nhadat_nhatot`
--
ALTER TABLE `nhadat_nhatot`
  MODIFY `id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `scheduler_logs`
--
ALTER TABLE `scheduler_logs`
  MODIFY `id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `scheduler_tasks`
--
ALTER TABLE `scheduler_tasks`
  MODIFY `id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `scraped_details`
--
ALTER TABLE `scraped_details`
  MODIFY `id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `scraped_details_flat`
--
ALTER TABLE `scraped_details_flat`
  MODIFY `id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `scraped_detail_images`
--
ALTER TABLE `scraped_detail_images`
  MODIFY `id` int NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `transaction_city`
--
ALTER TABLE `transaction_city`
  MODIFY `city_id` smallint UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `transaction_city_merge`
--
ALTER TABLE `transaction_city_merge`
  MODIFY `id` int UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `transaction_city_mergev2`
--
ALTER TABLE `transaction_city_mergev2`
  MODIFY `id` int UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT cho bảng `transaction_city_new`
--
ALTER TABLE `transaction_city_new`
  MODIFY `city_id` smallint UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- Ràng buộc đối với các bảng kết xuất
--

--
-- Ràng buộc cho bảng `scraped_detail_images`
--
ALTER TABLE `scraped_detail_images`
  ADD CONSTRAINT `fk_sdi_detail` FOREIGN KEY (`detail_id`) REFERENCES `scraped_details_flat` (`id`) ON DELETE CASCADE;

--
-- Ràng buộc cho bảng `transaction_city_merge`
--
ALTER TABLE `transaction_city_merge`
  ADD CONSTRAINT `transaction_city_merge_ibfk_1` FOREIGN KEY (`new_city_id`) REFERENCES `transaction_city` (`city_id`),
  ADD CONSTRAINT `transaction_city_merge_ibfk_2` FOREIGN KEY (`old_city_id`) REFERENCES `transaction_city` (`city_id`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
