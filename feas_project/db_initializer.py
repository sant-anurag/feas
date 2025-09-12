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

To run manually:
  DJANGO_SETTINGS_MODULE=feas_project.settings python feas_project/db_initializer.py

To call from Django (recommended):
  from feas_project.db_initializer import DatabaseInitializer
  DatabaseInitializer().initialize_database()
"""

import os
import sys
import traceback
from typing import Dict, Tuple

# MySQL connector (as in your uploaded reference)
import mysql.connector
from mysql.connector import errorcode, Error

# Try to import Django settings; require caller to set DJANGO_SETTINGS_MODULE if running standalone
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
    # If the script is run without DJANGO_SETTINGS_MODULE, we re-raise a clear error when executed.
    # When this module is imported from Django runtime (recommended), settings will be available.
    # We'll not raise here to allow import, but higher-level functions will check settings presence.
    settings = None  # type: ignore


class DatabaseInitializer:
    """
    Database initializer class for FEAS.

    Methods:
        get_db_config(): read DB settings from Django settings
        connect(): open mysql.connector connection
        initialize_database(): run DDL + seeders idempotently
    """

    # Default init table name (override via settings.DB_INIT_DONE_TABLE)
    INIT_KEY = "db_initialized"
    DEFAULT_INIT_TABLE = "system_settings"

    def __init__(self, db_config: Dict = None):
        """
        If db_config is given, use it. Otherwise, try to read from Django settings.
        db_config format (mysql.connector.connect kwargs): host, port, user, password, database, etc.
        """
        if db_config:
            self.db_config = db_config
        else:
            self.db_config = self._get_db_config_from_settings()

        # final init table name
        self.init_table = (
            getattr(settings, "DB_INIT_DONE_TABLE", self.DEFAULT_INIT_TABLE)
            if settings is not None
            else self.DEFAULT_INIT_TABLE
        )

        # Compose DDL statements for creating tables (idempotent)
        self.ddl_statements = self._build_ddls(self.init_table)

        # Role seed values
        self.role_inserts = [
            ("ADMIN", "Administrator"),
            ("PDL", "Program Development Lead"),
            ("COE_LEADER", "COE Leader"),
            ("TEAM_LEAD", "Team Lead"),
            ("EMPLOYEE", "Employee"),
        ]

    def _get_db_config_from_settings(self) -> Dict:
        """
        Read DATABASES['default'] from Django settings and convert to mysql.connector kwargs.
        """
        if settings is None:
            raise RuntimeError(
                "Django settings are not available. Set DJANGO_SETTINGS_MODULE or call from inside Django."
            )

        dbs = settings.DATABASES.get("default", {})
        cfg = {
            "host": dbs.get("HOST", "127.0.0.1") or "127.0.0.1",
            "port": int(dbs.get("PORT", 3306) or 3306),
            "user": dbs.get("USER", "") or "",
            "password": dbs.get("PASSWORD", "") or "",
            # mysql.connector expects 'database' key
            "database": dbs.get("NAME", "") or "",
            "charset": "utf8mb4",
            "use_unicode": True,
        }
        return cfg

    def _build_ddls(self, init_table_name: str) -> Tuple[str, ...]:
        """Return ordered DDL statements (tuple) so FK references are respected."""
        return tuple(
            [
                # 1. init/settings table
                f"""
                CREATE TABLE IF NOT EXISTS `{init_table_name}` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `key_name` VARCHAR(128) NOT NULL UNIQUE,
                    `value_text` TEXT,
                    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                # 2. users
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
                # 3. coes
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
                # 4. domains
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
                # 5. projects
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
                # 6. allocations
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
                # 7. weekly_allocations
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
                # 8. notifications
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
                # 9. audit_log
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
                # 10. ldap_sync_history
                """
                CREATE TABLE IF NOT EXISTS `ldap_sync_history` (
                    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
                    `synced_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    `synced_by` VARCHAR(255),
                    `details` TEXT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
                # 11. roles
                """
                CREATE TABLE IF NOT EXISTS `roles` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `role_key` VARCHAR(64) NOT NULL UNIQUE,
                    `display_name` VARCHAR(128) NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """,
            ]
        )

    def connect(self):
        """Open a mysql.connector connection with the configured DB config."""
        try:
            conn = mysql.connector.connect(**self.db_config)
            return conn
        except mysql.connector.Error:
            print("ERROR: Could not connect to MySQL with provided settings.")
            traceback.print_exc()
            raise

    def _execute_statements(self, conn, statements):
        """Execute list of statements in order using a cursor and commit at end."""
        cursor = conn.cursor()
        try:
            for sql in statements:
                s = sql.strip()
                if not s:
                    continue
                # Print a compact log
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
        """Check the init flag in init_table to skip re-initialization."""
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
        """Write the init flag into init_table (upsert style)."""
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
        """Seed canonical roles into roles table if not present."""
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
        """
        Main entry method. Returns True on success, False on failure.
        """
        print("FEAS: Starting DB initialization...")
        conn = None
        try:
            conn = self.connect()

            # Ensure init table exists first
            self._execute_statements(conn, [self.ddl_statements[0]])

            # If already initialized, skip
            if self._is_already_initialized(conn):
                print("FEAS: Database already initialized. Skipping.")
                return True

            # Execute remaining DDL statements (tables)
            self._execute_statements(conn, list(self.ddl_statements[1:]))

            # Seed roles
            self._seed_roles(conn)

            # Set init flag
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


# Convenience function for callers that prefer a simple functional API
def initialize_database(db_config: Dict = None) -> bool:
    """
    Functional wrapper: create DatabaseInitializer and run initialize_database().
    If db_config is provided, it overrides settings-based config.
    """
    initializer = DatabaseInitializer(db_config=db_config) if db_config else DatabaseInitializer()
    return initializer.initialize_database()


# CLI entrypoint (manual run)
if __name__ == "__main__":
    # Ensure DJANGO_SETTINGS_MODULE is set when running manually
    if "DJANGO_SETTINGS_MODULE" not in os.environ:
        print("Please set DJANGO_SETTINGS_MODULE to your settings module, e.g:")
        print("  export DJANGO_SETTINGS_MODULE=feas_project.settings")
        print("Then run this script again.")
        sys.exit(2)

    ok = initialize_database()
    if not ok:
        sys.exit(1)
    print("Done.")
