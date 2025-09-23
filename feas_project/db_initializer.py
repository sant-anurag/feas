"""
feas_project/db_initializer.py

Class-based DB initializer for FEAS using mysql.connector (same style as your reference).
Place inside the `feas_project/` package (next to settings.py).

Behavior:
 - Reads DB connection info from Django settings.DATABASES['default'].
 - Creates schema using raw SQL (idempotent).
 - Seeds 'roles' lookup table.
 - Stores a one-time init flag in `system_settings` (configurable via settings.DB_INIT_DONE_TABLE).
 - Safe to call multiple times; real work runs only on the first call.
"""

import os
import sys
import traceback
from typing import Dict, Tuple

import mysql.connector
from mysql.connector import errorcode, Error

try:
    import django
    from django.conf import settings

    if not settings.configured:
        dj_settings_module = os.getenv("DJANGO_SETTINGS_MODULE")
        if not dj_settings_module:
            raise RuntimeError(
                "DJANGO_SETTINGS_MODULE not set. Set it or call this from inside Django."
            )
        django.setup()
except Exception as exc:
    settings = None  # allow import even if running standalone


class DatabaseInitializer:
    INIT_KEY = "db_initialized"
    DEFAULT_INIT_TABLE = "system_settings"

    def __init__(self, db_config: Dict = None):
        if db_config:
            self.db_config = db_config
        else:
            self.db_config = self._get_db_config_from_settings()

        self.init_table = (
            getattr(settings, "DB_INIT_DONE_TABLE", self.DEFAULT_INIT_TABLE)
            if settings is not None
            else self.DEFAULT_INIT_TABLE
        )

        self.ddl_statements = self._build_ddls(self.init_table)

        self.role_inserts = [
            ("ADMIN", "Administrator"),
            ("PDL", "Program Development Lead"),
            ("COE_LEADER", "COE Leader"),
            ("TEAM_LEAD", "Team Lead"),
            ("EMPLOYEE", "Employee"),
        ]

    def _get_db_config_from_settings(self) -> Dict:
        if settings is None:
            raise RuntimeError(
                "Django settings are not available. Set DJANGO_SETTINGS_MODULE or call from inside Django."
            )

        dbs = settings.DATABASES.get("default", {})
        cfg = {
            "host": dbs.get("HOST", "127.0.0.1") or "127.0.0.1",
            "port": int(dbs.get("PORT", 3306) or 3306),
            "user": dbs.get("USER", "root") or "",
            "password": dbs.get("PASSWORD", "root") or "",
            "database": dbs.get("NAME", "feasdb") or "",
            "charset": "utf8mb4",
            "use_unicode": True,
        }
        return cfg

    def _build_ddls(self, init_table_name: str) -> Tuple[str, ...]:
        return tuple(
            [
                f"""
                CREATE TABLE IF NOT EXISTS `{init_table_name}` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `key_name` VARCHAR(128) NOT NULL UNIQUE,
                    `value_text` TEXT,
                    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS `users` (
                    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                    `username` VARCHAR(150) NOT NULL UNIQUE,
                    `email` VARCHAR(254),
                    `ldap_id` VARCHAR(255) UNIQUE,
                    `role` VARCHAR(32) NOT NULL DEFAULT 'EMPLOYEE',
                    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS `coes` (
                    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                    `name` VARCHAR(255) NOT NULL UNIQUE,
                    `leader_user_id` BIGINT,
                    `description` TEXT,
                    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (`leader_user_id`) REFERENCES `users`(`id`) ON DELETE SET NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS `domains` (
                    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                    `coe_id` BIGINT NOT NULL,
                    `name` VARCHAR(255) NOT NULL,
                    `lead_user_id` BIGINT,
                    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY `uq_domain_coe_name` (`coe_id`, `name`),
                    FOREIGN KEY (`coe_id`) REFERENCES `coes`(`id`) ON DELETE CASCADE,
                    FOREIGN KEY (`lead_user_id`) REFERENCES `users`(`id`) ON DELETE SET NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS `projects` (
                    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                    `name` VARCHAR(255) NOT NULL,
                    `pdl_user_id` BIGINT,
                    `start_date` DATE,
                    `end_date` DATE,
                    `description` TEXT,
                    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY `uq_project_name` (`name`),
                    FOREIGN KEY (`pdl_user_id`) REFERENCES `users`(`id`) ON DELETE SET NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                # NEW: project_coes mapping table (many-to-many: project <-> coe)
                """
                CREATE TABLE IF NOT EXISTS `project_coes` (
                    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                    `project_id` BIGINT NOT NULL,
                    `coe_id` BIGINT NOT NULL,
                    `assigned_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY `uq_project_coe` (`project_id`, `coe_id`),
                    FOREIGN KEY (`project_id`) REFERENCES `projects`(`id`) ON DELETE CASCADE,
                    FOREIGN KEY (`coe_id`) REFERENCES `coes`(`id`) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS `allocations` (
                    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                    `user_id` BIGINT NOT NULL,
                    `project_id` BIGINT NOT NULL,
                    `month_start` DATE NOT NULL,
                    `total_hours` INT UNSIGNED NOT NULL DEFAULT 0,
                    `pending_hours` INT UNSIGNED NOT NULL DEFAULT 0,
                    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY `uq_alloc_user_project_month` (`user_id`, `project_id`, `month_start`),
                    FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE,
                    FOREIGN KEY (`project_id`) REFERENCES `projects`(`id`) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS `weekly_allocations` (
                    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                    `allocation_id` BIGINT NOT NULL,
                    `week_number` TINYINT NOT NULL,
                    `hours` INT UNSIGNED NOT NULL DEFAULT 0,
                    `status` VARCHAR(16) NOT NULL DEFAULT 'PENDING',
                    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY `uq_week_alloc` (`allocation_id`, `week_number`),
                    FOREIGN KEY (`allocation_id`) REFERENCES `allocations`(`id`) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS `notifications` (
                    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                    `user_id` BIGINT NOT NULL,
                    `message` TEXT NOT NULL,
                    `is_read` TINYINT(1) NOT NULL DEFAULT 0,
                    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS `audit_log` (
                    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                    `user_id` BIGINT,
                    `action` VARCHAR(255) NOT NULL,
                    `meta` JSON,
                    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE SET NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS `ldap_sync_history` (
                    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                    `synced_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    `synced_by` VARCHAR(255),
                    `details` TEXT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS `roles` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `role_key` VARCHAR(64) NOT NULL UNIQUE,
                    `display_name` VARCHAR(128) NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                """
                CREATE TABLE IF NOT EXISTS `allocation_items` (
                      `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                      `allocation_id` BIGINT NOT NULL,    -- FK to allocations.id
                      `project_id` BIGINT NOT NULL,
                      `coe_id` BIGINT NOT NULL,
                      `domain_id` BIGINT,                 -- nullable
                      `user_id` BIGINT,                   -- FK to users.id (created on demand via ldap username)
                      `user_ldap` VARCHAR(255) NOT NULL,  -- original ldap identifier / username
                      `total_hours` INT UNSIGNED NOT NULL DEFAULT 0,
                      `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                      `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                      UNIQUE KEY uq_alloc_item (allocation_id, coe_id, user_id),
                      FOREIGN KEY (`allocation_id`) REFERENCES `allocations`(`id`) ON DELETE CASCADE,
                      FOREIGN KEY (`project_id`) REFERENCES `projects`(`id`) ON DELETE CASCADE,
                      FOREIGN KEY (`coe_id`) REFERENCES `coes`(`id`) ON DELETE CASCADE,
                      FOREIGN KEY (`domain_id`) REFERENCES `domains`(`id`) ON DELETE SET NULL,
                      FOREIGN KEY (`user_id`) REFERENCES `users`(`id`) ON DELETE SET NULL
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

                """,
                """
               CREATE TABLE IF NOT EXISTS `ldap_directory` (
                  `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                  `username` VARCHAR(150) NOT NULL,
                  `email` VARCHAR(254),
                  `cn` VARCHAR(255),
                  `givenName` VARCHAR(150),
                  `sn` VARCHAR(150),
                  `title` VARCHAR(255),
                  `department` VARCHAR(255),
                  `telephoneNumber` VARCHAR(64),
                  `mobile` VARCHAR(64),
                  `manager_dn` VARCHAR(512),
                  `ldap_dn` VARCHAR(1024) NOT NULL,
                  `ldap_dn_hash` CHAR(64) NOT NULL,
                  `attributes_json` JSON NULL,
                  `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  UNIQUE KEY `uq_ldap_directory_dn_hash` (`ldap_dn_hash`),
                  UNIQUE KEY `uq_ldap_directory_username` (`username`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
             
                """,
                 """
            CREATE TABLE IF NOT EXISTS `ldap_sync_jobs` (
              `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
              `started_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              `finished_at` TIMESTAMP NULL,
              `started_by` VARCHAR(255),
              `status` VARCHAR(32) NOT NULL DEFAULT 'PENDING', -- PENDING / RUNNING / COMPLETED / FAILED
              `total_count` INT DEFAULT 0,
              `processed_count` INT DEFAULT 0,
              `errors_count` INT DEFAULT 0,
              `details` TEXT,
              `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                            """
            ]
        )

    def connect(self):
        try:
            conn = mysql.connector.connect(**self.db_config)
            return conn
        except mysql.connector.Error:
            print("ERROR: Could not connect to MySQL with provided settings.")
            traceback.print_exc()
            raise

    def _execute_statements(self, conn, statements):
        cursor = conn.cursor()
        try:
            for sql in statements:
                s = sql.strip()
                if not s:
                    continue
                first_line = s.splitlines()[0][:160]
                print(f"Executing: {first_line} ...")
                cursor.execute(s)
            conn.commit()
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def _is_already_initialized(self, conn) -> bool:
        cursor = conn.cursor(dictionary=True)
        try:
            q = f"SELECT value_text FROM `{self.init_table}` WHERE key_name = %s LIMIT 1"
            cursor.execute(q, (self.INIT_KEY,))
            row = cursor.fetchone()
            if row and row.get("value_text") and str(row.get("value_text")).lower() in (
                "1",
                "true",
                "yes",
            ):
                return True
            return False
        finally:
            cursor.close()

    def _set_initialized_flag(self, conn):
        cursor = conn.cursor()
        try:
            q = f"""
            INSERT INTO `{self.init_table}` (key_name, value_text)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE value_text = VALUES(value_text), updated_at = CURRENT_TIMESTAMP
            """
            cursor.execute(q, (self.INIT_KEY, "true"))
            conn.commit()
        finally:
            cursor.close()

    def _seed_roles(self, conn):
        cursor = conn.cursor(dictionary=True)
        try:
            for key, display in self.role_inserts:
                cursor.execute("SELECT id FROM roles WHERE role_key = %s", (key,))
                if cursor.fetchone() is None:
                    print(f"Inserting role: {key}")
                    cursor.execute(
                        "INSERT INTO roles (role_key, display_name) VALUES (%s, %s)",
                        (key, display),
                    )
            conn.commit()
        finally:
            cursor.close()

    def initialize_database(self) -> bool:
        print("FEAS: Starting DB initialization...")
        conn = None
        try:
            conn = self.connect()
            self._execute_statements(conn, [self.ddl_statements[0]])
            if self._is_already_initialized(conn):
                print("FEAS: Database already initialized. Skipping.")
                return True
            self._execute_statements(conn, list(self.ddl_statements[1:]))
            self._seed_roles(conn)
            self._set_initialized_flag(conn)
            print("FEAS: Database initialization completed successfully.")
            return True
        except mysql.connector.Error:
            print("FEAS: Database initialization failed due to MySQL error.")
            traceback.print_exc()
            return False
        except Exception:
            print("FEAS: Database initialization failed.")
            traceback.print_exc()
            return False
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass


def initialize_database(db_config: Dict = None) -> bool:
    initializer = DatabaseInitializer(db_config=db_config) if db_config else DatabaseInitializer()
    return initializer.initialize_database()


if __name__ == "__main__":
    if "DJANGO_SETTINGS_MODULE" not in os.environ:
        print("Please set DJANGO_SETTINGS_MODULE to your settings module, e.g:")
        print("  export DJANGO_SETTINGS_MODULE=feas_project.settings")
        sys.exit(2)
    ok = initialize_database()
    if not ok:
        sys.exit(1)
    print("Done.")
