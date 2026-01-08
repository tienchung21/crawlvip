"""
Database module for storing collected links and scraping results
Supports MySQL
"""

import os
from datetime import datetime
from typing import List, Optional
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from pathlib import Path
import json

# Khởi tạo biến mặc định
MYSQL_AVAILABLE = False
USE_MYSQL_CONNECTOR = False

try:
    import pymysql
    pymysql.install_as_MySQLdb()
    import MySQLdb
    MYSQL_AVAILABLE = True
    USE_MYSQL_CONNECTOR = False  # Dùng pymysql
except ImportError:
    try:
        import mysql.connector
        MYSQL_AVAILABLE = True
        USE_MYSQL_CONNECTOR = True  # Dùng mysql.connector
    except ImportError:
        MYSQL_AVAILABLE = False
        USE_MYSQL_CONNECTOR = False


class Database:
    """Database handler for collected links - MySQL only"""
    
    def __init__(self, 
                 host: str = "localhost",
                 user: str = "root",
                 password: str = "",
                 database: str = "craw_db",
                 port: int = 3306):
        """
        Initialize MySQL database connection
        
        Args:
            host: MySQL host
            user: MySQL user
            password: MySQL password
            database: Database name
            port: MySQL port
        """
        if not MYSQL_AVAILABLE:
            raise ImportError("MySQL library not found. Install with: pip install pymysql or pip install mysql-connector-python")
        
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.use_mysql_connector = USE_MYSQL_CONNECTOR if MYSQL_AVAILABLE else False
        
        # Don't cache connection - create new one each time for thread safety
        self.init_db()
    
    def get_connection(self, use_database: bool = True):
        """
        Get MySQL database connection (creates new connection each time for thread safety)
        
        Args:
            use_database: If True, connect to specific database. If False, connect without database (for creating database)
        """
        if self.use_mysql_connector:
            import mysql.connector
            conn_params = {
                'host': self.host,
                'user': self.user,
                'password': self.password,
                'port': self.port,
                'autocommit': False
            }
            if use_database:
                conn_params['database'] = self.database
            conn = mysql.connector.connect(**conn_params)
            return conn
        else:
            import MySQLdb
            conn_params = {
                'host': self.host,
                'user': self.user,
                'password': self.password,
                'port': self.port
            }
            if use_database:
                conn_params['db'] = self.database
            conn = MySQLdb.connect(**conn_params)
            return conn
    
    def init_db(self):
        """Initialize database tables"""
        # First, connect without database to create it if needed
        conn = self.get_connection(use_database=False)
        cursor = conn.cursor()
        
        # Create database if not exists
        try:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{self.database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            conn.commit()
        except Exception as e:
            # Database might already exist, ignore error
            print(f"Note: Database creation: {e}")
        
        cursor.close()
        conn.close()
        
        # Now connect to the database
        conn = self.get_connection(use_database=True)
        cursor = conn.cursor()
        
        # Create collected_links table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS collected_links (
                id INT AUTO_INCREMENT PRIMARY KEY,
                url VARCHAR(2000) NOT NULL UNIQUE,
                status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
                domain VARCHAR(255) DEFAULT NULL,
                loaihinh VARCHAR(255) DEFAULT NULL,
                city_id INT DEFAULT NULL,
                city_name VARCHAR(255) DEFAULT NULL,
                ward_id INT DEFAULT NULL,
                ward_name VARCHAR(255) DEFAULT NULL,
                new_city_id INT DEFAULT NULL,
                new_city_name VARCHAR(255) DEFAULT NULL,
                new_ward_id INT DEFAULT NULL,
                new_ward_name VARCHAR(255) DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_collected_links_url (url(100)),
                INDEX idx_collected_links_domain (domain),
                INDEX idx_collected_links_loaihinh (loaihinh),
                INDEX idx_collected_links_status (status)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        # Ensure domain column exists (for older deployments)
        try:
            cursor.execute("ALTER TABLE collected_links ADD COLUMN domain VARCHAR(255) DEFAULT NULL")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE collected_links ADD INDEX idx_collected_links_domain (domain)")
        except Exception:
            pass
        # Ensure loaihinh column exists/index
        try:
            cursor.execute("ALTER TABLE collected_links ADD COLUMN loaihinh VARCHAR(255) DEFAULT NULL")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE collected_links ADD INDEX idx_collected_links_loaihinh (loaihinh)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE collected_links ADD COLUMN city_id INT DEFAULT NULL")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE collected_links ADD COLUMN city_name VARCHAR(255) DEFAULT NULL")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE collected_links ADD COLUMN ward_id INT DEFAULT NULL")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE collected_links ADD COLUMN ward_name VARCHAR(255) DEFAULT NULL")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE collected_links ADD COLUMN new_city_id INT DEFAULT NULL")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE collected_links ADD COLUMN new_city_name VARCHAR(255) DEFAULT NULL")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE collected_links ADD COLUMN new_ward_id INT DEFAULT NULL")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE collected_links ADD COLUMN new_ward_name VARCHAR(255) DEFAULT NULL")
        except Exception:
            pass
        
        # Thêm cột updated_at để track thời điểm cập nhật status (dùng cho reset IN_PROGRESS)
        try:
            cursor.execute("""
                ALTER TABLE collected_links 
                ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            """)
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE collected_links ADD INDEX idx_collected_links_updated_at (updated_at)")
        except Exception:
            pass

        # Create scraped_details table (lưu kết quả cào chi tiết)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraped_details (
                id INT AUTO_INCREMENT PRIMARY KEY,
                link_id INT NULL,
                url VARCHAR(2000) NOT NULL,
                domain VARCHAR(255) DEFAULT NULL,
                data_json LONGTEXT,
                success TINYINT(1) DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_scraped_details_url (url(100)),
                INDEX idx_scraped_details_domain (domain),
                INDEX idx_scraped_details_success (success)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')

        # Create scraped_details_flat table (các cột cụ thể cho detail)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraped_details_flat (
                id INT AUTO_INCREMENT PRIMARY KEY,
                link_id INT NULL,
                url VARCHAR(2000) NOT NULL,
                domain VARCHAR(255) DEFAULT NULL,
                title TEXT,
                img_count INT DEFAULT NULL,
                mota TEXT,
                khoanggia VARCHAR(255),
                dientich VARCHAR(255),
                sophongngu VARCHAR(255),
                sophongvesinh VARCHAR(255),
                huongnha VARCHAR(255),
                huongbancong VARCHAR(255),
                mattien VARCHAR(255),
                duongvao VARCHAR(255),
                sotang VARCHAR(255),
                loaihinhnhao VARCHAR(255),
                dientichsudung VARCHAR(255),
                gia_m2 VARCHAR(255),
                gia_mn VARCHAR(255),
                dacdiemnhadat VARCHAR(255),
                chieungang VARCHAR(255),
                chieudai VARCHAR(255),
                phaply VARCHAR(255),
                noithat VARCHAR(255),
                thuocduan VARCHAR(255),
                trangthaiduan VARCHAR(255),
                tenmoigioi VARCHAR(255),
                sodienthoai VARCHAR(255),
                map VARCHAR(255),
                matin VARCHAR(255),
                loaitin VARCHAR(255),
                ngayhethan VARCHAR(255),
                ngaydang VARCHAR(255),
                thoigianvaoo VARCHAR(255),
                giadien VARCHAR(255),
                gianuoc VARCHAR(255),
                giainternet VARCHAR(255),
                sotiencoc VARCHAR(255),
                tangso VARCHAR(255),
                loaihinhvanphong VARCHAR(255),
                loaihinhdat VARCHAR(255),
                loaihinhcanho VARCHAR(255),
                diachi TEXT,
                diachicu TEXT,
                loaibds VARCHAR(255),
                phongan VARCHAR(255),
                nhabep VARCHAR(255),
                santhuong VARCHAR(255),
                chodexehoi VARCHAR(255),
                chinhchu VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_sdf_url (url(100)),
                INDEX idx_sdf_domain (domain),
                INDEX idx_sdf_link (link_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        # Ensure sophongvesinh column exists
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN sophongvesinh VARCHAR(255)")
        except Exception:
            pass
        # Ensure additional spec columns exist
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN huongnha VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN huongbancong VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN mattien VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN duongvao VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN sotang VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN loaihinhnhao VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN dientichsudung VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN gia_m2 VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN gia_mn VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN dacdiemnhadat VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN chieungang VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN chieudai VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN diachicu TEXT")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN loaibds VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN phongan VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN nhabep VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN santhuong VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN chodexehoi VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN chinhchu VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN thoigianvaoo VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN giadien VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN gianuoc VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN giainternet VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN sotiencoc VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN tangso VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN loaihinhvanphong VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN loaihinhdat VARCHAR(255)")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_details_flat ADD COLUMN loaihinhcanho VARCHAR(255)")
        except Exception:
            pass

        # Create scraped_detail_images table (lưu danh sách ảnh theo detail_id)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraped_detail_images (
                id INT AUTO_INCREMENT PRIMARY KEY,
                detail_id INT NOT NULL,
                image_url VARCHAR(2000) NOT NULL,
                idx INT DEFAULT NULL,
                status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_sdi_detail (detail_id),
                INDEX idx_sdi_status (status),
                INDEX idx_sdi_url (image_url(150)),
                CONSTRAINT fk_sdi_detail FOREIGN KEY (detail_id) REFERENCES scraped_details_flat(id) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        # Ensure status column exists for scraped_detail_images
        try:
            cursor.execute("ALTER TABLE scraped_detail_images ADD COLUMN status VARCHAR(50) NOT NULL DEFAULT 'PENDING'")
        except Exception:
            pass
        try:
            cursor.execute("ALTER TABLE scraped_detail_images ADD INDEX idx_sdi_status (status)")
        except Exception:
            pass
        try:
            cursor.execute("UPDATE scraped_detail_images SET status='PENDING' WHERE status IS NULL OR status=''")
        except Exception:
            pass

        # Create downloaded_images table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS downloaded_images (
                id INT AUTO_INCREMENT PRIMARY KEY,
                image_url VARCHAR(2000) NOT NULL,
                file_path VARCHAR(2000) DEFAULT NULL,
                status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
                domain VARCHAR(255) DEFAULT NULL,
                error TEXT DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                downloaded_at TIMESTAMP NULL,
                INDEX idx_downloaded_images_url (image_url(100)),
                INDEX idx_downloaded_images_status (status),
                INDEX idx_downloaded_images_domain (domain)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')

        # Create scheduler_tasks table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scheduler_tasks (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                active TINYINT(1) NOT NULL DEFAULT 1,
                is_running TINYINT(1) NOT NULL DEFAULT 0,
                run_now TINYINT(1) NOT NULL DEFAULT 0,
                enable_listing TINYINT(1) NOT NULL DEFAULT 1,
                enable_detail TINYINT(1) NOT NULL DEFAULT 1,
                enable_image TINYINT(1) NOT NULL DEFAULT 0,
                schedule_type VARCHAR(20) NOT NULL DEFAULT 'interval',
                interval_minutes INT DEFAULT NULL,
                run_times VARCHAR(255) DEFAULT NULL,
                listing_template_path VARCHAR(2000) DEFAULT NULL,
                detail_template_path VARCHAR(2000) DEFAULT NULL,
                start_url VARCHAR(2000) DEFAULT NULL,
                max_pages INT DEFAULT 1,
                domain VARCHAR(255) DEFAULT NULL,
                loaihinh VARCHAR(255) DEFAULT NULL,
                city_id INT DEFAULT NULL,
                city_name VARCHAR(255) DEFAULT NULL,
                ward_id INT DEFAULT NULL,
                ward_name VARCHAR(255) DEFAULT NULL,
                new_city_id INT DEFAULT NULL,
                new_city_name VARCHAR(255) DEFAULT NULL,
                new_ward_id INT DEFAULT NULL,
                new_ward_name VARCHAR(255) DEFAULT NULL,
                cancel_requested TINYINT(1) NOT NULL DEFAULT 0,
                listing_show_browser TINYINT(1) DEFAULT 1,
                listing_fake_scroll TINYINT(1) DEFAULT 1,
                listing_fake_hover TINYINT(1) DEFAULT 0,
                listing_wait_load_min FLOAT DEFAULT 20,
                listing_wait_load_max FLOAT DEFAULT 30,
                listing_wait_next_min FLOAT DEFAULT 10,
                listing_wait_next_max FLOAT DEFAULT 20,
                detail_show_browser TINYINT(1) DEFAULT 0,
                detail_fake_scroll TINYINT(1) DEFAULT 1,
                detail_fake_hover TINYINT(1) DEFAULT 1,
                detail_wait_load_min FLOAT DEFAULT 2,
                detail_wait_load_max FLOAT DEFAULT 5,
                detail_delay_min FLOAT DEFAULT 2,
                detail_delay_max FLOAT DEFAULT 3,
                image_dir VARCHAR(2000) DEFAULT NULL,
                images_per_minute INT DEFAULT 30,
                image_domain VARCHAR(255) DEFAULT NULL,
                image_status VARCHAR(50) DEFAULT NULL,
                last_run_at TIMESTAMP NULL,
                next_run_at TIMESTAMP NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_scheduler_tasks_active (active),
                INDEX idx_scheduler_tasks_next (next_run_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')

        # Ensure scheduler_tasks columns exist (for older deployments)
        for col_sql in [
            "ALTER TABLE scheduler_tasks ADD COLUMN is_running TINYINT(1) NOT NULL DEFAULT 0",
            "ALTER TABLE scheduler_tasks ADD COLUMN run_now TINYINT(1) NOT NULL DEFAULT 0",
            "ALTER TABLE scheduler_tasks ADD COLUMN enable_listing TINYINT(1) NOT NULL DEFAULT 1",
            "ALTER TABLE scheduler_tasks ADD COLUMN enable_detail TINYINT(1) NOT NULL DEFAULT 1",
            "ALTER TABLE scheduler_tasks ADD COLUMN enable_image TINYINT(1) NOT NULL DEFAULT 0",
            "ALTER TABLE scheduler_tasks ADD COLUMN cancel_requested TINYINT(1) NOT NULL DEFAULT 0",
            "ALTER TABLE scheduler_tasks ADD COLUMN listing_show_browser TINYINT(1) DEFAULT 1",
            "ALTER TABLE scheduler_tasks ADD COLUMN listing_fake_scroll TINYINT(1) DEFAULT 1",
            "ALTER TABLE scheduler_tasks ADD COLUMN listing_fake_hover TINYINT(1) DEFAULT 0",
            "ALTER TABLE scheduler_tasks ADD COLUMN listing_wait_load_min FLOAT DEFAULT 20",
            "ALTER TABLE scheduler_tasks ADD COLUMN listing_wait_load_max FLOAT DEFAULT 30",
            "ALTER TABLE scheduler_tasks ADD COLUMN listing_wait_next_min FLOAT DEFAULT 10",
            "ALTER TABLE scheduler_tasks ADD COLUMN listing_wait_next_max FLOAT DEFAULT 20",
            "ALTER TABLE scheduler_tasks ADD COLUMN detail_show_browser TINYINT(1) DEFAULT 0",
            "ALTER TABLE scheduler_tasks ADD COLUMN detail_fake_scroll TINYINT(1) DEFAULT 1",
            "ALTER TABLE scheduler_tasks ADD COLUMN detail_fake_hover TINYINT(1) DEFAULT 1",
            "ALTER TABLE scheduler_tasks ADD COLUMN detail_wait_load_min FLOAT DEFAULT 2",
            "ALTER TABLE scheduler_tasks ADD COLUMN detail_wait_load_max FLOAT DEFAULT 5",
            "ALTER TABLE scheduler_tasks ADD COLUMN detail_delay_min FLOAT DEFAULT 2",
            "ALTER TABLE scheduler_tasks ADD COLUMN detail_delay_max FLOAT DEFAULT 3",
            "ALTER TABLE scheduler_tasks ADD COLUMN image_domain VARCHAR(255) DEFAULT NULL",
            "ALTER TABLE scheduler_tasks ADD COLUMN image_status VARCHAR(50) DEFAULT NULL",
            "ALTER TABLE scheduler_tasks ADD COLUMN city_id INT DEFAULT NULL",
            "ALTER TABLE scheduler_tasks ADD COLUMN city_name VARCHAR(255) DEFAULT NULL",
            "ALTER TABLE scheduler_tasks ADD COLUMN ward_id INT DEFAULT NULL",
            "ALTER TABLE scheduler_tasks ADD COLUMN ward_name VARCHAR(255) DEFAULT NULL",
            "ALTER TABLE scheduler_tasks ADD COLUMN new_city_id INT DEFAULT NULL",
            "ALTER TABLE scheduler_tasks ADD COLUMN new_city_name VARCHAR(255) DEFAULT NULL",
            "ALTER TABLE scheduler_tasks ADD COLUMN new_ward_id INT DEFAULT NULL",
            "ALTER TABLE scheduler_tasks ADD COLUMN new_ward_name VARCHAR(255) DEFAULT NULL",
        ]:
            try:
                cursor.execute(col_sql)
            except Exception:
                pass

        # Create scheduler_logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scheduler_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                task_id INT NOT NULL,
                stage VARCHAR(50) DEFAULT NULL,
                status VARCHAR(50) DEFAULT NULL,
                message TEXT DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_scheduler_logs_task (task_id),
                INDEX idx_scheduler_logs_created (created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        ''')
        
        conn.commit()
        cursor.close()
        conn.close()
    
    def normalize_url(self, url: str) -> str:
        """
        Normalize URL to ensure consistent comparison
        - Remove fragment (#)
        - Sort query parameters
        - Remove trailing slash (except root)
        - Lowercase scheme and host
        """
        if not url or not isinstance(url, str):
            return url
        
        try:
            parsed = urlparse(url.strip())
            
            # Normalize scheme and netloc (lowercase)
            scheme = parsed.scheme.lower()
            netloc = parsed.netloc.lower()
            
            # Remove fragment
            fragment = ''
            
            # Sort query parameters for consistency
            query_params = parse_qs(parsed.query, keep_blank_values=True)
            # Sort by key
            sorted_params = sorted(query_params.items())
            query = urlencode(sorted_params, doseq=True)
            
            # Remove trailing slash from path (except root)
            path = parsed.path.rstrip('/') or '/'
            
            # Reconstruct URL
            normalized = urlunparse((scheme, netloc, path, parsed.params, query, fragment))
            
            return normalized
        except Exception as e:
            # If normalization fails, return original URL stripped
            print(f"Warning: URL normalization failed for {url[:50]}: {e}")
            return url.strip()
    
    def add_collected_links(
        self,
        links_list: List[str],
        domain: Optional[str] = None,
        loaihinh: Optional[str] = None,
        city_id: Optional[int] = None,
        city_name: Optional[str] = None,
        ward_id: Optional[int] = None,
        ward_name: Optional[str] = None,
        new_city_id: Optional[int] = None,
        new_city_name: Optional[str] = None,
        new_ward_id: Optional[int] = None,
        new_ward_name: Optional[str] = None,
    ) -> int:
        """
        Bulk insert links, skipping duplicates
        
        Args:
            links_list: List of URL strings
            domain: Optional domain label to store (e.g., 'batdongsan', 'nhatot')
            
        Returns:
            Number of new links added
        """
        if not links_list:
            return 0
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        added_count = 0
        for url in links_list:
            if not url or not isinstance(url, str):
                continue
            
            # Normalize URL before inserting
            normalized_url = self.normalize_url(url)
            
            try:
                # Use INSERT IGNORE for MySQL (skips duplicates)
                cursor.execute('''
                    INSERT IGNORE INTO collected_links (
                        url, status, domain, loaihinh,
                        city_id, city_name, ward_id, ward_name,
                        new_city_id, new_city_name, new_ward_id, new_ward_name,
                        created_at
                    )
                    VALUES (%s, 'PENDING', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    normalized_url,
                    domain,
                    loaihinh,
                    city_id,
                    city_name,
                    ward_id,
                    ward_name,
                    new_city_id,
                    new_city_name,
                    new_ward_id,
                    new_ward_name,
                    datetime.now()
                ))
                
                if cursor.rowcount > 0:
                    added_count += 1
                else:
                    # URL đã tồn tại trong DB, bỏ qua (KHÔNG thêm vào)
                    pass
            except Exception as e:
                # Skip if error (likely duplicate or invalid)
                print(f"Error adding link {normalized_url[:50]}: {e}")
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        return added_count

    def add_scraped_detail(self, url: str, data: dict, domain: Optional[str] = None, link_id: Optional[int] = None, success: bool = True):
        """
        Lưu kết quả cào chi tiết vào bảng scraped_details
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO scraped_details (link_id, url, domain, data_json, success, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (
                link_id,
                url,
                domain,
                json.dumps(data, ensure_ascii=False) if data is not None else None,
                1 if success else 0,
                datetime.now()
            ))
            conn.commit()
        except Exception as e:
            print(f"Error adding scraped detail {url[:80]}: {e}")
        finally:
            cursor.close()
            conn.close()
    
    def get_recent_details(self, limit: int = 200, domain: Optional[str] = None):
        """
        Lấy các bản ghi cào chi tiết gần đây
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            if domain:
                cursor.execute('''
                    SELECT id, link_id, url, domain, success, created_at
                    FROM scraped_details
                    WHERE domain = %s
                    ORDER BY id DESC
                    LIMIT %s
                ''', (domain, limit))
            else:
                cursor.execute('''
                    SELECT id, link_id, url, domain, success, created_at
                    FROM scraped_details
                    ORDER BY id DESC
                    LIMIT %s
                ''', (limit,))
            rows = cursor.fetchall()
            result = []
            for row in rows:
                if isinstance(row, tuple):
                    result.append({
                        'id': row[0],
                        'link_id': row[1],
                        'url': row[2],
                        'domain': row[3],
                        'success': row[4],
                        'created_at': row[5]
                    })
                else:
                    # dict cursor
                    result.append({
                        'id': row.get('id'),
                        'link_id': row.get('link_id'),
                        'url': row.get('url'),
                        'domain': row.get('domain'),
                        'success': row.get('success'),
                        'created_at': row.get('created_at')
                    })
            return result
        finally:
            cursor.close()
            conn.close()

    def add_scraped_detail_flat(self, url: str, data: dict, domain: Optional[str] = None, link_id: Optional[int] = None) -> Optional[int]:
        """
        Lưu bản ghi detail vào bảng scraped_details_flat với các cột cụ thể.
        """
        if not data:
            return None
        data_lower = {}
        if isinstance(data, dict):
            for k, v in data.items():
                if isinstance(k, str):
                    data_lower[k.strip().lower()] = v

        def _get_data_value(key: str, *aliases: str):
            val = data.get(key) if isinstance(data, dict) else None
            if val is not None:
                return val
            for alias in aliases:
                val = data.get(alias) if isinstance(data, dict) else None
                if val is not None:
                    return val
            key_lower = key.strip().lower()
            if key_lower in data_lower:
                return data_lower.get(key_lower)
            for alias in aliases:
                alias_lower = alias.strip().lower()
                if alias_lower in data_lower:
                    return data_lower.get(alias_lower)
            return None
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            imgs = data.get('img')
            if isinstance(imgs, list):
                img_count = len(imgs)
            else:
                img_count = 1 if imgs else None
            map_value = data.get('map')
            if isinstance(map_value, list):
                map_value = next((v for v in map_value if isinstance(v, str) and v.strip()), None)
            elif isinstance(map_value, dict):
                map_value = map_value.get('src') or map_value.get('url') or map_value.get('value')
            if isinstance(map_value, str):
                map_value = map_value.strip()
            cursor.execute('''
                INSERT INTO scraped_details_flat (
                    link_id, url, domain, title, img_count, mota, khoanggia, dientich,
                    sophongngu, sophongvesinh, huongnha, huongbancong, mattien, duongvao, phaply, noithat,
                    sotang, loaihinhnhao, dientichsudung, gia_m2, gia_mn, dacdiemnhadat, chieungang, chieudai, thuocduan,
                    trangthaiduan, tenmoigioi, sodienthoai, map, matin, loaitin, ngayhethan, ngaydang, diachi,
                    thoigianvaoo, giadien, gianuoc, giainternet, sotiencoc, tangso, loaihinhvanphong, loaihinhdat, loaihinhcanho,
                    diachicu, loaibds, phongan, nhabep, santhuong, chodexehoi, chinhchu
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            ''', (
                link_id,
                url,
                domain,
                data.get('title'),
                img_count,
                data.get('mota'),
                data.get('khoanggia'),
                data.get('dientich'),
                data.get('sophongngu'),
                data.get('sophongvesinh'),
                data.get('huongnha'),
                data.get('huongbancong'),
                data.get('mattien'),
                data.get('duongvao'),
                data.get('phaply'),
                data.get('noithat'),
                data.get('sotang'),
                data.get('loaihinhnhao') or data.get('loaibds'),
                data.get('dientichsudung'),
                _get_data_value('gia_m2', 'gia/m2', 'gia m2', 'gia_m²', 'gia/m²', 'gia m²'),
                _get_data_value('gia_mn', 'gia/mn', 'gia mn'),
                data.get('dacdiemnhadat'),
                data.get('chieungang'),
                data.get('chieudai'),
                data.get('thuocduan'),
                data.get('trangthaiduan'),
                _get_data_value('tenmoigioi', 'moigioi', 'ten moi gioi', 'ten_moi_gioi'),
                data.get('sodienthoai'),
                map_value,
                data.get('matin'),
                data.get('loaitin'),
                data.get('ngayhethan'),
                data.get('ngaydang'),
                data.get('diachi'),
                data.get('thoigianvaoo'),
                data.get('giadien'),
                data.get('gianuoc'),
                data.get('giainternet'),
                data.get('sotiencoc'),
                data.get('tangso'),
                data.get('loaihinhvanphong'),
                data.get('loaihinhdat'),
                data.get('loaihinhcanho'),
                data.get('diachicu'),
                data.get('loaibds'),
                data.get('phongan'),
                data.get('nhabep'),
                data.get('santhuong'),
                data.get('chodexehoi'),
                data.get('chinhchu')
            ))
            conn.commit()
            return cursor.lastrowid
        except Exception as e:
            print(f"Error adding scraped_detail_flat {url[:80]}: {e}")
            return None
        finally:
            cursor.close()
            conn.close()

    def add_detail_images(self, detail_id: int, images: list):
        """
        Lưu danh sách ảnh vào bảng scraped_detail_images gắn với detail_id.
        """
        if not images or detail_id is None:
            return
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            idx_counter = 0
            for img in images:
                if not img:
                    continue
                cursor.execute('''
                    INSERT INTO scraped_detail_images (detail_id, image_url, idx, status)
                    VALUES (%s, %s, %s, 'PENDING')
                ''', (detail_id, img, idx_counter))
                idx_counter += 1
            conn.commit()
        except Exception as e:
            print(f"Error adding detail images for detail_id {detail_id}: {e}")
        finally:
            cursor.close()
            conn.close()

    # -------- Image history helpers --------
    def count_detail_images(self) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM scraped_detail_images")
            row = cursor.fetchone()
            return int(row[0]) if row else 0
        finally:
            cursor.close()
            conn.close()

    def count_detail_images_filtered(self, domain: Optional[str] = None, status: Optional[str] = None) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            query = '''
                SELECT COUNT(*)
                FROM scraped_detail_images di
                JOIN scraped_details_flat df ON df.id = di.detail_id
                WHERE 1=1
            '''
            params = []
            if domain:
                query += ' AND df.domain = %s'
                params.append(domain)
            if status:
                query += ' AND di.status = %s'
                params.append(status)
            cursor.execute(query, params)
            row = cursor.fetchone()
            return int(row[0]) if row else 0
        finally:
            cursor.close()
            conn.close()

    def get_detail_images_paginated(self, limit: int = 20, offset: int = 0):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''
                SELECT id, detail_id, image_url, idx, status, created_at
                FROM scraped_detail_images
                ORDER BY id DESC
                LIMIT %s OFFSET %s
                ''',
                (limit, offset)
            )
            rows = cursor.fetchall()
            result = []
            for row in rows:
                if isinstance(row, tuple):
                    result.append({
                        'id': row[0],
                        'detail_id': row[1],
                        'image_url': row[2],
                        'idx': row[3],
                        'status': row[4],
                        'created_at': row[5],
                    })
                else:
                    result.append({
                        'id': row.get('id'),
                        'detail_id': row.get('detail_id'),
                        'image_url': row.get('image_url'),
                        'idx': row.get('idx'),
                        'status': row.get('status'),
                        'created_at': row.get('created_at'),
                    })
            return result
        finally:
            cursor.close()
            conn.close()

    def get_detail_image_domains(self) -> List[str]:
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''
                SELECT DISTINCT df.domain
                FROM scraped_detail_images di
                JOIN scraped_details_flat df ON df.id = di.detail_id
                WHERE df.domain IS NOT NULL AND df.domain <> ''
                ORDER BY df.domain ASC
                '''
            )
            rows = cursor.fetchall()
            domains = []
            for row in rows:
                if isinstance(row, tuple):
                    domains.append(row[0])
                else:
                    domains.append(row.get('domain'))
            return [d for d in domains if d]
        finally:
            cursor.close()
            conn.close()

    def get_detail_images_by_id_range(self, start_id: int, end_id: int, domain: Optional[str] = None, status: Optional[str] = None):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            query = '''
                SELECT di.id, di.detail_id, di.image_url, di.idx, di.status, di.created_at, df.domain
                FROM scraped_detail_images di
                JOIN scraped_details_flat df ON df.id = di.detail_id
                WHERE di.id BETWEEN %s AND %s
            '''
            params = [start_id, end_id]
            if domain:
                query += ' AND df.domain = %s'
                params.append(domain)
            if status:
                query += ' AND di.status = %s'
                params.append(status)
            query += ' ORDER BY di.id ASC'
            cursor.execute(query, params)
            rows = cursor.fetchall()
            result = []
            for row in rows:
                if isinstance(row, tuple):
                    result.append({
                        'id': row[0],
                        'detail_id': row[1],
                        'image_url': row[2],
                        'idx': row[3],
                        'status': row[4],
                        'created_at': row[5],
                        'domain': row[6],
                    })
                else:
                    result.append({
                        'id': row.get('id'),
                        'detail_id': row.get('detail_id'),
                        'image_url': row.get('image_url'),
                        'idx': row.get('idx'),
                        'status': row.get('status'),
                        'created_at': row.get('created_at'),
                        'domain': row.get('domain'),
                    })
            return result
        finally:
            cursor.close()
            conn.close()

    def get_detail_images_paginated_filtered(self, limit: int = 20, offset: int = 0, domain: Optional[str] = None, status: Optional[str] = None):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            query = '''
                SELECT di.id, di.detail_id, di.image_url, di.idx, di.status, di.created_at, df.domain
                FROM scraped_detail_images di
                JOIN scraped_details_flat df ON df.id = di.detail_id
                WHERE 1=1
            '''
            params = []
            if domain:
                query += ' AND df.domain = %s'
                params.append(domain)
            if status:
                query += ' AND di.status = %s'
                params.append(status)
            query += ' ORDER BY di.id DESC LIMIT %s OFFSET %s'
            params.extend([limit, offset])
            cursor.execute(query, params)
            rows = cursor.fetchall()
            result = []
            for row in rows:
                if isinstance(row, tuple):
                    result.append({
                        'id': row[0],
                        'detail_id': row[1],
                        'image_url': row[2],
                        'idx': row[3],
                        'status': row[4],
                        'created_at': row[5],
                        'domain': row[6],
                    })
                else:
                    result.append({
                        'id': row.get('id'),
                        'detail_id': row.get('detail_id'),
                        'image_url': row.get('image_url'),
                        'idx': row.get('idx'),
                        'status': row.get('status'),
                        'created_at': row.get('created_at'),
                        'domain': row.get('domain'),
                    })
            return result
        finally:
            cursor.close()
            conn.close()
    
    def update_detail_image_status(self, image_id: int, status: str):
        if not image_id:
            return
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE scraped_detail_images SET status=%s WHERE id=%s",
                (status, image_id)
            )
            conn.commit()
        except Exception as e:
            print(f"Error updating detail image status {image_id}: {e}")
        finally:
            cursor.close()
            conn.close()

    def sync_detail_image_statuses(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                UPDATE scraped_detail_images di
                JOIN downloaded_images dl
                  ON dl.image_url = di.image_url AND dl.status = 'SUCCESS'
                SET di.status = 'DOWNLOADED'
                WHERE di.status = 'PENDING'
            ''')
            conn.commit()
        except Exception as e:
            print(f"Error syncing image statuses: {e}")
        finally:
            cursor.close()
            conn.close()

    def add_downloaded_image(self, image_url: str, file_path: Optional[str], status: str, domain: Optional[str] = None, error: Optional[str] = None):
        """
        Insert a downloaded image record
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO downloaded_images (image_url, file_path, status, domain, error, downloaded_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (image_url, file_path, status, domain, error, datetime.now() if status == 'SUCCESS' else None))
            conn.commit()
        except Exception as e:
            print(f"Error adding downloaded image {image_url[:80]}: {e}")
        finally:
            cursor.close()
            conn.close()
    
    def get_recent_images(self, limit: int = 500, domain: Optional[str] = None):
        """
        Get recent downloaded images
        """
        conn = self.get_connection()
        cursor = conn.cursor(dictionary=True) if self.use_mysql_connector else conn.cursor(MySQLdb.cursors.DictCursor)
        try:
            if domain:
                cursor.execute('''
                    SELECT * FROM downloaded_images
                    WHERE domain = %s
                    ORDER BY id DESC
                    LIMIT %s
                ''', (domain, limit))
            else:
                cursor.execute('''
                    SELECT * FROM downloaded_images
                    ORDER BY id DESC
                    LIMIT %s
                ''', (limit,))
            rows = cursor.fetchall()
            return rows
        finally:
            cursor.close()
            conn.close()
    
    def get_recent_links(self, limit: int = 100, status: Optional[str] = None, domain: Optional[str] = None, loaihinh: Optional[str] = None) -> List[dict]:
        """
        Get recent collected links
        
        Args:
            limit: Maximum number of links to return
            status: Filter by status ('PENDING', 'CRAWLED', 'ERROR') or None for all
            
        Returns:
            List of dictionaries with link data
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if self.use_mysql_connector:
            # mysql.connector returns tuples, need to convert to dict
            if status:
                if domain:
                    cursor.execute('''
                        SELECT id, url, status, domain, loaihinh, created_at
                        FROM collected_links
                        WHERE status = %s AND (domain = %s OR %s IS NULL) AND (loaihinh = %s OR %s IS NULL)
                        ORDER BY created_at DESC
                        LIMIT %s
                    ''', (status, domain, domain, loaihinh, loaihinh, limit))
                else:
                    cursor.execute('''
                        SELECT id, url, status, domain, loaihinh, created_at
                        FROM collected_links
                        WHERE status = %s AND (loaihinh = %s OR %s IS NULL)
                        ORDER BY created_at DESC
                        LIMIT %s
                    ''', (status, loaihinh, loaihinh, limit))
            else:
                if domain:
                    cursor.execute('''
                        SELECT id, url, status, domain, loaihinh, created_at
                        FROM collected_links
                        WHERE domain = %s AND (loaihinh = %s OR %s IS NULL)
                        ORDER BY created_at DESC
                        LIMIT %s
                    ''', (domain, loaihinh, loaihinh, limit))
                else:
                    cursor.execute('''
                        SELECT id, url, status, domain, loaihinh, created_at
                        FROM collected_links
                        WHERE (loaihinh = %s OR %s IS NULL)
                        ORDER BY created_at DESC
                        LIMIT %s
                    ''', (loaihinh, loaihinh, limit))
            
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            result = [
                {
                    'id': row[0],
                    'url': row[1],
                    'status': row[2],
                    'domain': row[3],
                    'loaihinh': row[4],
                    'created_at': row[5]
                }
                for row in rows
            ]
        else:
            # MySQLdb returns dict-like rows
            if status:
                if domain:
                    cursor.execute('''
                        SELECT id, url, status, domain, loaihinh, created_at
                        FROM collected_links
                        WHERE status = %s AND domain = %s AND (loaihinh = %s OR %s IS NULL)
                        ORDER BY created_at DESC
                        LIMIT %s
                    ''', (status, domain, loaihinh, loaihinh, limit))
                else:
                    cursor.execute('''
                        SELECT id, url, status, domain, loaihinh, created_at
                        FROM collected_links
                        WHERE status = %s AND (loaihinh = %s OR %s IS NULL)
                        ORDER BY created_at DESC
                        LIMIT %s
                    ''', (status, loaihinh, loaihinh, limit))
            else:
                if domain:
                    cursor.execute('''
                        SELECT id, url, status, domain, loaihinh, created_at
                        FROM collected_links
                        WHERE domain = %s AND (loaihinh = %s OR %s IS NULL)
                        ORDER BY created_at DESC
                        LIMIT %s
                    ''', (domain, loaihinh, loaihinh, limit))
                else:
                    cursor.execute('''
                        SELECT id, url, status, domain, loaihinh, created_at
                        FROM collected_links
                        WHERE (loaihinh = %s OR %s IS NULL)
                        ORDER BY created_at DESC
                        LIMIT %s
                    ''', (loaihinh, loaihinh, limit))
            
            rows = cursor.fetchall()
            result = [
                {
                    'id': row[0] if isinstance(row, tuple) else row['id'],
                    'url': row[1] if isinstance(row, tuple) else row['url'],
                    'status': row[2] if isinstance(row, tuple) else row['status'],
                    'domain': row[3] if isinstance(row, tuple) else row['domain'],
                    'loaihinh': row[4] if isinstance(row, tuple) else row.get('loaihinh'),
                    'created_at': row[5] if isinstance(row, tuple) else row['created_at']
                }
                for row in rows
            ]
        
        cursor.close()
        conn.close()
        return result
    
    def reset_stale_in_progress_links(self, timeout_minutes: int = 30) -> int:
        """
        Reset các link bị kẹt ở status IN_PROGRESS quá lâu về PENDING.
        Điều này xảy ra khi task crash/bị tắt giữa chừng.
        
        Args:
            timeout_minutes: Số phút tối đa cho phép link ở trạng thái IN_PROGRESS
            
        Returns:
            Số link đã được reset
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # Tìm và reset các link IN_PROGRESS quá timeout_minutes phút
            cursor.execute('''
                UPDATE collected_links
                SET status = 'PENDING'
                WHERE status = 'IN_PROGRESS'
                  AND updated_at < DATE_SUB(NOW(), INTERVAL %s MINUTE)
            ''', (timeout_minutes,))
            affected = cursor.rowcount
            conn.commit()
            if affected > 0:
                print(f"[Database] Reset {affected} stale IN_PROGRESS link(s) back to PENDING")
            return affected
        except Exception as e:
            print(f"[Database] Error resetting stale links: {e}")
            conn.rollback()
            return 0
        finally:
            cursor.close()
            conn.close()
    
    def update_link_status(self, url: str, status: str):
        """
        Update status of a link
        
        Args:
            url: URL to update
            status: New status ('PENDING', 'CRAWLED', 'ERROR')
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE collected_links
            SET status = %s
            WHERE url = %s
        ''', (status, url))
        
        conn.commit()
        cursor.close()
        conn.close()
    
    def get_pending_links_count(self) -> int:
        """Get count of pending links"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM collected_links WHERE status = %s', ('PENDING',))
        result = cursor.fetchone()
        count = result[0] if isinstance(result, tuple) else result
        cursor.close()
        conn.close()
        return count
    
    def get_links_by_id_range(self, min_id: int, max_id: int) -> List[dict]:
        """
        Get links within ID range, ordered by id descending
        
        Args:
            min_id: Minimum ID
            max_id: Maximum ID
            
        Returns:
            List of dictionaries with link data
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, url, status, domain, created_at
            FROM collected_links
            WHERE id >= %s AND id <= %s
            ORDER BY id DESC
        ''', (min_id, max_id))
        
        if self.use_mysql_connector:
            rows = cursor.fetchall()
            result = [
                {
                    'id': row[0],
                    'url': row[1],
                    'status': row[2],
                    'domain': row[3],
                    'created_at': row[4]
                }
                for row in rows
            ]
        else:
            rows = cursor.fetchall()
            result = [
                {
                    'id': row[0] if isinstance(row, tuple) else row['id'],
                    'url': row[1] if isinstance(row, tuple) else row['url'],
                    'status': row[2] if isinstance(row, tuple) else row['status'],
                    'domain': row[3] if isinstance(row, tuple) else row['domain'],
                    'created_at': row[4] if isinstance(row, tuple) else row['created_at']
                }
                for row in rows
            ]
        
        cursor.close()
        conn.close()
        return result
    
    def reset_id_sequence(self):
        """
        Reset ID sequence để làm ID liên tục từ 1
        Cảnh báo: Chỉ dùng khi cần thiết, sẽ thay đổi tất cả ID hiện có
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Tạo bảng tạm với dữ liệu mới
            cursor.execute('''
                CREATE TABLE collected_links_new (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    url VARCHAR(2000) NOT NULL UNIQUE,
                    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
                    domain VARCHAR(255) DEFAULT NULL,
                    loaihinh VARCHAR(255) DEFAULT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_collected_links_url (url(100)),
                    INDEX idx_collected_links_status (status),
                    INDEX idx_collected_links_domain (domain),
                    INDEX idx_collected_links_loaihinh (loaihinh)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            ''')
            
            # Copy dữ liệu và reset ID
            cursor.execute('''
                INSERT INTO collected_links_new (url, status, domain, loaihinh, created_at)
                SELECT url, status, domain, loaihinh, created_at
                FROM collected_links
                ORDER BY id
            ''')
            
            # Xóa bảng cũ và đổi tên bảng mới
            cursor.execute('DROP TABLE collected_links')
            cursor.execute('ALTER TABLE collected_links_new RENAME TO collected_links')
            
            conn.commit()
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            conn.rollback()
            cursor.close()
            conn.close()
            raise e

    # =========================
    # Scheduler helpers
    # =========================
    def list_scheduler_tasks(self, active_only: bool = False):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            if active_only:
                cursor.execute('''
                    SELECT id, name, active, is_running, run_now, enable_listing, enable_detail, enable_image,
                           schedule_type, interval_minutes, run_times,
                           listing_template_path, detail_template_path, start_url, max_pages,
                           domain, loaihinh,
                           city_id, city_name, ward_id, ward_name,
                           new_city_id, new_city_name, new_ward_id, new_ward_name,
                           cancel_requested, listing_show_browser, listing_fake_scroll, listing_fake_hover,
                           listing_wait_load_min, listing_wait_load_max,
                           listing_wait_next_min, listing_wait_next_max,
                           detail_show_browser, detail_fake_scroll, detail_fake_hover,
                           detail_wait_load_min, detail_wait_load_max,
                           detail_delay_min, detail_delay_max,
                           image_dir, images_per_minute, image_domain, image_status,
                           last_run_at, next_run_at, created_at, updated_at
                    FROM scheduler_tasks
                    WHERE active = 1
                    ORDER BY id DESC
                ''')
            else:
                cursor.execute('''
                    SELECT id, name, active, is_running, run_now, enable_listing, enable_detail, enable_image,
                           schedule_type, interval_minutes, run_times,
                           listing_template_path, detail_template_path, start_url, max_pages,
                           domain, loaihinh,
                           city_id, city_name, ward_id, ward_name,
                           new_city_id, new_city_name, new_ward_id, new_ward_name,
                           cancel_requested, listing_show_browser, listing_fake_scroll, listing_fake_hover,
                           listing_wait_load_min, listing_wait_load_max,
                           listing_wait_next_min, listing_wait_next_max,
                           detail_show_browser, detail_fake_scroll, detail_fake_hover,
                           detail_wait_load_min, detail_wait_load_max,
                           detail_delay_min, detail_delay_max,
                           image_dir, images_per_minute, image_domain, image_status,
                           last_run_at, next_run_at, created_at, updated_at
                    FROM scheduler_tasks
                    ORDER BY id DESC
                ''')
            rows = cursor.fetchall()
            result = []
            for row in rows:
                if isinstance(row, tuple):
                    result.append({
                        'id': row[0],
                        'name': row[1],
                        'active': row[2],
                        'is_running': row[3],
                        'run_now': row[4],
                        'enable_listing': row[5],
                        'enable_detail': row[6],
                        'enable_image': row[7],
                        'schedule_type': row[8],
                        'interval_minutes': row[9],
                        'run_times': row[10],
                        'listing_template_path': row[11],
                        'detail_template_path': row[12],
                        'start_url': row[13],
                        'max_pages': row[14],
                        'domain': row[15],
                        'loaihinh': row[16],
                        'city_id': row[17],
                        'city_name': row[18],
                        'ward_id': row[19],
                        'ward_name': row[20],
                        'new_city_id': row[21],
                        'new_city_name': row[22],
                        'new_ward_id': row[23],
                        'new_ward_name': row[24],
                        'cancel_requested': row[25],
                        'listing_show_browser': row[26],
                        'listing_fake_scroll': row[27],
                        'listing_fake_hover': row[28],
                        'listing_wait_load_min': row[29],
                        'listing_wait_load_max': row[30],
                        'listing_wait_next_min': row[31],
                        'listing_wait_next_max': row[32],
                        'detail_show_browser': row[33],
                        'detail_fake_scroll': row[34],
                        'detail_fake_hover': row[35],
                        'detail_wait_load_min': row[36],
                        'detail_wait_load_max': row[37],
                        'detail_delay_min': row[38],
                        'detail_delay_max': row[39],
                        'image_dir': row[40],
                        'images_per_minute': row[41],
                        'image_domain': row[42],
                        'image_status': row[43],
                        'last_run_at': row[44],
                        'next_run_at': row[45],
                        'created_at': row[46],
                        'updated_at': row[47],
                    })
                else:
                    result.append(row)
            return result
        finally:
            cursor.close()
            conn.close()

    def add_scheduler_task(self, task: dict) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO scheduler_tasks (
                    name, active, is_running, enable_listing, enable_detail, enable_image,
                    schedule_type, interval_minutes, run_times,
                    listing_template_path, detail_template_path, start_url, max_pages,
                    domain, loaihinh,
                    city_id, city_name, ward_id, ward_name,
                    new_city_id, new_city_name, new_ward_id, new_ward_name,
                    cancel_requested,
                    listing_show_browser, listing_fake_scroll, listing_fake_hover,
                    listing_wait_load_min, listing_wait_load_max,
                    listing_wait_next_min, listing_wait_next_max,
                    detail_show_browser, detail_fake_scroll, detail_fake_hover,
                    detail_wait_load_min, detail_wait_load_max,
                    detail_delay_min, detail_delay_max,
                    image_dir, images_per_minute, image_domain, image_status, last_run_at, next_run_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                task.get('name'),
                1 if task.get('active', True) else 0,
                1 if task.get('is_running', False) else 0,
                1 if task.get('enable_listing', True) else 0,
                1 if task.get('enable_detail', True) else 0,
                1 if task.get('enable_image', False) else 0,
                task.get('schedule_type', 'interval'),
                task.get('interval_minutes'),
                task.get('run_times'),
                task.get('listing_template_path'),
                task.get('detail_template_path'),
                task.get('start_url'),
                task.get('max_pages', 1),
                task.get('domain'),
                task.get('loaihinh'),
                task.get('city_id'),
                task.get('city_name'),
                task.get('ward_id'),
                task.get('ward_name'),
                task.get('new_city_id'),
                task.get('new_city_name'),
                task.get('new_ward_id'),
                task.get('new_ward_name'),
                1 if task.get('cancel_requested', False) else 0,
                task.get('listing_show_browser', 1),
                task.get('listing_fake_scroll', 1),
                task.get('listing_fake_hover', 0),
                task.get('listing_wait_load_min', 20),
                task.get('listing_wait_load_max', 30),
                task.get('listing_wait_next_min', 10),
                task.get('listing_wait_next_max', 20),
                task.get('detail_show_browser', 0),
                task.get('detail_fake_scroll', 1),
                task.get('detail_fake_hover', 1),
                task.get('detail_wait_load_min', 2),
                task.get('detail_wait_load_max', 5),
                task.get('detail_delay_min', 2),
                task.get('detail_delay_max', 3),
                task.get('image_dir'),
                task.get('images_per_minute', 30),
                task.get('image_domain'),
                task.get('image_status'),
                task.get('last_run_at'),
                task.get('next_run_at')
            ))
            conn.commit()
            return cursor.lastrowid
        finally:
            cursor.close()
            conn.close()

    def update_scheduler_task(self, task_id: int, updates: dict):
        if not updates:
            return
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            fields = []
            values = []
            for k, v in updates.items():
                fields.append(f"{k} = %s")
                values.append(v)
            values.append(task_id)
            sql = f"UPDATE scheduler_tasks SET {', '.join(fields)} WHERE id = %s"
            cursor.execute(sql, tuple(values))
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def set_task_active(self, task_id: int, active: bool):
        self.update_scheduler_task(task_id, {'active': 1 if active else 0})

    def request_task_cancel(self, task_id: int):
        """Yêu cầu hủy task - set cancel_requested=1, is_running=0, VÀ active=0 để ngăn task tự chạy lại"""
        # Quan trọng: phải set active=0 để tránh scheduler pick lại ngay lập tức
        # vì get_due_tasks() sẽ pick task khi active=1 và is_running=0
        self.update_scheduler_task(task_id, {'cancel_requested': 1, 'is_running': 0, 'active': 0})

    def clear_task_cancel(self, task_id: int):
        self.update_scheduler_task(task_id, {'cancel_requested': 0})

    def delete_scheduler_task(self, task_id: int):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM scheduler_tasks WHERE id = %s", (task_id,))
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def add_scheduler_log(self, task_id: int, stage: str, status: str, message: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO scheduler_logs (task_id, stage, status, message)
                VALUES (%s, %s, %s, %s)
            ''', (task_id, stage, status, message))
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    def get_scheduler_logs(self, task_id: int, limit: int = 200):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT id, task_id, stage, status, message, created_at
                FROM scheduler_logs
                WHERE task_id = %s
                ORDER BY id DESC
                LIMIT %s
            ''', (task_id, limit))
            rows = cursor.fetchall()
            result = []
            for row in rows:
                if isinstance(row, tuple):
                    result.append({
                        'id': row[0],
                        'task_id': row[1],
                        'stage': row[2],
                        'status': row[3],
                        'message': row[4],
                        'created_at': row[5],
                    })
                else:
                    result.append(row)
            return result
        finally:
            cursor.close()
            conn.close()

    def get_due_tasks(self, now_ts) -> list:
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                SELECT id, name, active, is_running, run_now,
                       enable_listing, enable_detail, enable_image,
                       schedule_type, interval_minutes, run_times,
                       listing_template_path, detail_template_path, start_url, max_pages,
                       domain, loaihinh,
                       city_id, city_name, ward_id, ward_name,
                       new_city_id, new_city_name, new_ward_id, new_ward_name,
                       cancel_requested,
                       listing_show_browser, listing_fake_scroll, listing_fake_hover,
                       listing_wait_load_min, listing_wait_load_max,
                       listing_wait_next_min, listing_wait_next_max,
                       detail_show_browser, detail_fake_scroll, detail_fake_hover,
                       detail_wait_load_min, detail_wait_load_max,
                       detail_delay_min, detail_delay_max,
                       image_dir, images_per_minute,
                       last_run_at, next_run_at
                FROM scheduler_tasks
                WHERE active = 1 AND is_running = 0 AND cancel_requested = 0 
                  AND (run_now = 1 OR next_run_at IS NULL OR next_run_at <= %s)
                ORDER BY run_now DESC, id ASC
            ''', (now_ts,))
            rows = cursor.fetchall()
            result = []
            for row in rows:
                if isinstance(row, tuple):
                    result.append({
                        'id': row[0],
                        'name': row[1],
                        'active': row[2],
                        'is_running': row[3],
                        'run_now': row[4],
                        'enable_listing': row[5],
                        'enable_detail': row[6],
                        'enable_image': row[7],
                        'schedule_type': row[8],
                        'interval_minutes': row[9],
                        'run_times': row[10],
                        'listing_template_path': row[11],
                        'detail_template_path': row[12],
                        'start_url': row[13],
                        'max_pages': row[14],
                        'domain': row[15],
                        'loaihinh': row[16],
                        'city_id': row[17],
                        'city_name': row[18],
                        'ward_id': row[19],
                        'ward_name': row[20],
                        'new_city_id': row[21],
                        'new_city_name': row[22],
                        'new_ward_id': row[23],
                        'new_ward_name': row[24],
                        'cancel_requested': row[25],
                        'listing_show_browser': row[26],
                        'listing_fake_scroll': row[27],
                        'listing_fake_hover': row[28],
                        'listing_wait_load_min': row[29],
                        'listing_wait_load_max': row[30],
                        'listing_wait_next_min': row[31],
                        'listing_wait_next_max': row[32],
                        'detail_show_browser': row[33],
                        'detail_fake_scroll': row[34],
                        'detail_fake_hover': row[35],
                        'detail_wait_load_min': row[36],
                        'detail_wait_load_max': row[37],
                        'detail_delay_min': row[38],
                        'detail_delay_max': row[39],
                        'image_dir': row[40],
                        'images_per_minute': row[41],
                        'last_run_at': row[42],
                        'next_run_at': row[43],
                    })
                else:
                    result.append(row)
            return result
        finally:
            cursor.close()
            conn.close()

    def is_task_cancel_requested(self, task_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT cancel_requested FROM scheduler_tasks WHERE id = %s", (task_id,))
            row = cursor.fetchone()
            if row is None:
                return False
            if isinstance(row, tuple):
                return bool(row[0])
            return bool(row)
        finally:
            cursor.close()
            conn.close()

    def update_task_run(self, task_id: int, last_run_at, next_run_at):
        self.update_scheduler_task(task_id, {'last_run_at': last_run_at, 'next_run_at': next_run_at})

    def get_pending_links(self, limit: int = 100, domain: Optional[str] = None, loaihinh: Optional[str] = None):
        # Reset các link IN_PROGRESS quá 30 phút về PENDING (do task crash/bị tắt)
        try:
            self.reset_stale_in_progress_links(timeout_minutes=30)
        except Exception:
            pass
        
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            rows = []
            try:
                cursor.execute("START TRANSACTION")
                if domain or loaihinh:
                    cursor.execute('''
                        SELECT id, url, status, domain, loaihinh, created_at
                        FROM collected_links
                        WHERE status = 'PENDING'
                          AND (domain = %s OR %s IS NULL)
                          AND (loaihinh = %s OR %s IS NULL)
                        ORDER BY id ASC
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    ''', (domain, domain, loaihinh, loaihinh, limit))
                else:
                    cursor.execute('''
                        SELECT id, url, status, domain, loaihinh, created_at
                        FROM collected_links
                        WHERE status = 'PENDING'
                        ORDER BY id ASC
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                    ''', (limit,))
                rows = cursor.fetchall()
                if rows:
                    ids = []
                    for row in rows:
                        if isinstance(row, tuple):
                            ids.append(row[0])
                        else:
                            ids.append(row.get('id'))
                    if ids:
                        placeholders = ','.join(['%s'] * len(ids))
                        cursor.execute(
                            f"UPDATE collected_links SET status='IN_PROGRESS' WHERE id IN ({placeholders})",
                            ids
                        )
                conn.commit()
            except Exception:
                conn.rollback()
                # Fallback for older MySQL versions (no SKIP LOCKED)
                if domain or loaihinh:
                    cursor.execute('''
                        SELECT id, url, status, domain, loaihinh, created_at
                        FROM collected_links
                        WHERE status = 'PENDING' AND (domain = %s OR %s IS NULL) AND (loaihinh = %s OR %s IS NULL)
                        ORDER BY id ASC
                        LIMIT %s
                    ''', (domain, domain, loaihinh, loaihinh, limit))
                else:
                    cursor.execute('''
                        SELECT id, url, status, domain, loaihinh, created_at
                        FROM collected_links
                        WHERE status = 'PENDING'
                        ORDER BY id ASC
                        LIMIT %s
                    ''', (limit,))
                rows = cursor.fetchall()
            result = []
            for row in rows:
                if isinstance(row, tuple):
                    result.append({
                        'id': row[0],
                        'url': row[1],
                        'status': row[2],
                        'domain': row[3],
                        'loaihinh': row[4],
                        'created_at': row[5],
                    })
                else:
                    result.append(row)
            return result
        finally:
            cursor.close()
            conn.close()

    def get_undownloaded_detail_images(self, limit: int = 200, domain: Optional[str] = None):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            if domain:
                cursor.execute('''
                    SELECT di.id, di.detail_id, di.image_url, di.idx, di.status, di.created_at
                    FROM scraped_detail_images di
                    JOIN scraped_details_flat df ON df.id = di.detail_id
                    WHERE di.status = 'PENDING'
                      AND df.domain = %s
                      AND NOT EXISTS (
                        SELECT 1 FROM downloaded_images dl
                        WHERE dl.image_url = di.image_url AND dl.status = 'SUCCESS'
                      )
                    ORDER BY di.id ASC
                    LIMIT %s
                ''', (domain, limit))
            else:
                cursor.execute('''
                    SELECT di.id, di.detail_id, di.image_url, di.idx, di.status, di.created_at
                    FROM scraped_detail_images di
                    WHERE di.status = 'PENDING'
                    AND NOT EXISTS (
                        SELECT 1 FROM downloaded_images dl
                        WHERE dl.image_url = di.image_url AND dl.status = 'SUCCESS'
                    )
                    ORDER BY di.id ASC
                    LIMIT %s
                ''', (limit,))
            rows = cursor.fetchall()
            result = []
            for row in rows:
                if isinstance(row, tuple):
                    result.append({
                        'id': row[0],
                        'detail_id': row[1],
                        'image_url': row[2],
                        'idx': row[3],
                        'status': row[4],
                        'created_at': row[5],
                    })
                else:
                    result.append(row)
            return result
        finally:
            cursor.close()
            conn.close()
