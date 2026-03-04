"""
Configuration settings for the reporting system.
Database: SQL Server connection using username and password (SQL authentication).
Reads from env: DB_SERVER or DB_HOST, DB_PORT, DB_NAME, DB_USERNAME, DB_PASSWORD.
Optional: DB_DOMAIN (for domain\\user), DB_BACKEND (pymssql | odbc), DB_CONNECT_TIMEOUT.
"""
import os
from typing import Dict, Any

# Load .env from project root (parent of config/) so DB_* is correct regardless of entry point or cwd
_config_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.dirname(_config_dir)
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_project_root, "environment.env"))
    load_dotenv(os.path.join(_project_root, ".env"))  # .env overrides
except ImportError:
    pass

# Database: SQL connection by user & password. All from env.
_db_host = (os.getenv('DB_SERVER') or os.getenv('DB_HOST') or '').strip()
_db_port = os.getenv('DB_PORT', '1433')
_db_name = os.getenv('DB_NAME', '')
_db_domain = (os.getenv('DB_DOMAIN') or os.getenv('DB_Domain') or '').strip()
_db_username = (os.getenv('DB_USERNAME') or '').strip()
_db_password = (os.getenv('DB_PASSWORD') or '').strip()
_db_backend = (os.getenv('DB_BACKEND', 'pymssql') or 'pymssql').strip().lower()
_db_connect_timeout = max(5, min(120, int(os.getenv('DB_CONNECT_TIMEOUT', '10'))))

DATABASE_CONFIG = {
    'server': _db_host or 'localhost',
    'port': _db_port or '1433',
    'database': _db_name or 'NEWDCC-V4-UAT',
    'domain': _db_domain,
    'username': _db_username,
    'password': _db_password,
    'driver': os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server'),
    'encrypt': 'yes',
    'trust_server_certificate': 'yes',
}

# API Configuration (from .env: NODE_API_URL, PYTHON_API_URL, API_TIMEOUT)
API_CONFIG = {
    'node_api_url': os.getenv('NODE_API_URL', os.getenv('NODE_BACKEND_URL', 'https://reporting-system-backend.pianat.ai')),
    'python_api_url': os.getenv('PYTHON_API_URL', os.getenv('PYTHON_API_BASE', 'https://reporting-system-python.pianat.ai')),
    'timeout': int(os.getenv('API_TIMEOUT', '60')),
}

# Export Configuration
EXPORT_CONFIG = {
    'max_rows_per_sheet': 10000,
    'chart_dpi': 150,
    'chart_figsize': (8, 4),
    'default_font_size': 10,
    'page_margins': {
        'top': 1.0,
        'bottom': 1.0,
        'left': 1.0,
        'right': 1.0
    }
}

# File Paths
FILE_PATHS = {
    'fonts': 'fonts',
    'templates': 'templates',
    'outputs': 'outputs'
}

# Default Header Configurations
DEFAULT_HEADER_CONFIGS = {
    'risks': {
        'title': 'RISKS DASHBOARD REPORT',
        'subtitle': 'Comprehensive Analysis Report',
        'showLogo': True,
        'logoSize': 'medium',
        'bankName': 'PIANAT.AI',
        'bankAddress': 'Bank address',
        'bankPhone': 'Bank phone',
        'bankUrl': 'www.website.com',
        'watermarkEnabled': True,
        'watermarkText': 'CONFIDENTIAL',
        'fontColor': '#1F4E79',
        'tableHeaderBgColor': '#E3F2FD',
        'tableHeaderFontColor': '#000000',
        'tableBodyBgColor': '#F5F5F5'
    },
    'controls': {
        'title': 'CONTROLS DASHBOARD REPORT',
        'subtitle': 'Comprehensive Analysis Report',
        'showLogo': True,
        'logoSize': 'medium',
        'bankName': 'PIANAT.AI',
        'bankAddress': 'Bank address',
        'bankPhone': 'Bank phone',
        'bankUrl': 'www.website.com',
        'watermarkEnabled': True,
        'watermarkText': 'CONFIDENTIAL',
        'fontColor': '#1F4E79',
        'tableHeaderBgColor': '#E3F2FD',
        'tableBodyBgColor': '#F5F5F5'
    }
}

def get_database_connection_string() -> str:
    """
    Build ODBC connection string for SQL Server using username and password.
    Uses domain\\username if DB_DOMAIN is set, otherwise username only.
    """
    config = DATABASE_CONFIG
    parts = [
        f"DRIVER={{{config['driver']}}};",
        f"SERVER={config['server']},{config['port']};",
        f"DATABASE={config['database']};",
        f"Encrypt={config['encrypt']};",
        f"TrustServerCertificate={config['trust_server_certificate']};",
    ]
    uid = f"{config['domain']}\\{config['username']}" if config.get('domain') else config['username']
    parts.append(f"UID={uid};")
    parts.append(f"PWD={config['password']};")
    return "".join(parts)


def get_db_connection():
    """
    Return an open SQL Server connection using username and password.
    - DB_BACKEND=pymssql (default): uses pymssql.
    - DB_BACKEND=odbc: uses pyodbc with ODBC Driver.
    """
    config = DATABASE_CONFIG
    if _db_backend == 'pymssql':
        import pymssql
        server = config['server']
        port = int(config['port'])
        user = f"{config['domain']}\\{config['username']}" if config.get('domain') else config['username']
        return pymssql.connect(
            server=server,
            port=port,
            user=user,
            password=config['password'],
            database=config['database'],
            timeout=_db_connect_timeout,
        )
    try:
        import pyodbc
    except ImportError as e:
        raise ImportError(
            "pyodbc required when DB_BACKEND=odbc. Install ODBC driver or set DB_BACKEND=pymssql. "
            f"Original: {e}"
        )
    return pyodbc.connect(get_database_connection_string(), timeout=_db_connect_timeout)


def test_database_connection() -> tuple[bool, str, Dict[str, Any]]:
    """
    Test SQL Server connectivity (user & password). Returns (success, message, details).
    details: server, database, username, table_count, sql_version, error.
    """
    details: Dict[str, Any] = {
        "server": DATABASE_CONFIG.get("server", "N/A"),
        "database": DATABASE_CONFIG.get("database", "N/A"),
        "auth_type": "SQL (user & password)",
        "username": DATABASE_CONFIG.get("username", "N/A"),
    }
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT @@VERSION")
            row = cursor.fetchone()
            details["sql_version"] = (row[0] or "")[:200]
            cursor.execute(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
            )
            row = cursor.fetchone()
            details["table_count"] = int(row[0]) if row and row[0] is not None else 0
        finally:
            cursor.close()
            conn.close()
        return True, "OK", details
    except Exception as e:
        details["error"] = str(e)
        return False, str(e), details

def get_default_header_config(dashboard_type: str) -> Dict[str, Any]:
    """Get default header configuration for a dashboard type"""
    return DEFAULT_HEADER_CONFIGS.get(dashboard_type, DEFAULT_HEADER_CONFIGS['risks']).copy()


def log_database_config():
    """Write current database connection info to debug log (password masked). Call after load_dotenv."""
    try:
        from routes.route_utils import write_debug
        c = DATABASE_CONFIG
        pwd = c.get('password') or ''
        mask = '***' if pwd else '(not set)'
        write_debug(
            f"[DB config] server={c.get('server')}, port={c.get('port')}, database={c.get('database')}, "
            f"username={c.get('username')}, password={mask}, backend={_db_backend}, connect_timeout={_db_connect_timeout}"
        )
    except Exception:
        pass


