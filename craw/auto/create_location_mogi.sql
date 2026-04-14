
CREATE TABLE IF NOT EXISTS `location_mogi` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `mogi_id` int(11) unsigned NOT NULL COMMENT 'ID from Mogi (CityId, DistrictId, WardId, StreetId)',
  `parent_id` int(11) unsigned DEFAULT '0' COMMENT 'Mogi ID of the parent location',
  `name` varchar(255) DEFAULT NULL,
  `slug` varchar(255) DEFAULT NULL,
  `type` enum('CITY','DISTRICT','WARD','STREET') NOT NULL,
  `code` varchar(50) DEFAULT NULL,
  `created_at` timestamp DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_mogi_id_type` (`mogi_id`,`type`),
  KEY `idx_parent_id` (`parent_id`),
  KEY `idx_slug` (`slug`),
  KEY `idx_type` (`type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
