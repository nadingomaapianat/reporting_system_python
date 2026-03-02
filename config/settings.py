"""
Configuration settings for the reporting system.
Database: same as Node – from env (DB_HOST or DB_SERVER, DB_PORT, DB_NAME, DB_DOMAIN, DB_USERNAME, DB_PASSWORD).
Supports Windows auth: Trusted_Connection (pyodbc only, DB_BACKEND=odbc) or NTLM with domain\\user + password (pymssql or odbc).
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

# Database: read from env (same vars as Node). DB_SERVER used if DB_HOST not set (e.g. Windows auth .env).
# Prefer DB_SERVER then DB_HOST so Python can use the same .env as the reporting node (Node often uses DB_HOST or DB_SERVER).
_db_host = (os.getenv('DB_SERVER') or os.getenv('DB_HOST') or '').strip()
_db_port = os.getenv('DB_PORT', '1433')
_db_name = os.getenv('DB_NAME', '')
# DB_Domain (adib_backend) and DB_DOMAIN (this project)
_db_domain = (os.getenv('DB_DOMAIN') or os.getenv('DB_Domain') or '').strip()
_db_username = os.getenv('DB_USERNAME', '')
_db_password = os.getenv('DB_PASSWORD', '')
_use_windows_auth = os.getenv('DB_USE_WINDOWS_AUTH', '0').strip().lower() not in ('0', 'false', 'no')
# pymssql = NTLM via FreeTDS (no ODBC driver). odbc = pyodbc + Microsoft ODBC Driver.
_db_backend = (os.getenv('DB_BACKEND', 'pymssql') or 'pymssql').strip().lower()
# Connection timeout in seconds; when DB is unreachable, fail fast to avoid long waits and 504s (default 10s).
_db_connect_timeout = max(5, min(120, int(os.getenv('DB_CONNECT_TIMEOUT', '10'))))

DATABASE_CONFIG = {
    'server': _db_host or 'localhost',
    'port': _db_port or '1433',
    'database': _db_name or '',
    'domain': _db_domain,
    'username': _db_username or '',
    'password': _db_password or '',
    'driver': os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server'),
    'trusted_connection': 'yes' if _use_windows_auth else 'no',
    'encrypt': 'yes',
    'trust_server_certificate': 'yes',
}

# API Configuration (from .env: NODE_API_URL, PYTHON_API_URL, API_TIMEOUT, GRC_BACKEND_URL)
API_CONFIG = {
    'node_api_url': os.getenv('NODE_API_URL', os.getenv('NODE_BACKEND_URL', 'https://reporting-madinetmasr-system-python.comply.now')),
    'python_api_url': os.getenv('PYTHON_API_URL', os.getenv('PYTHON_API_BASE', 'https://reporting-madinetmasr-system-python.comply.now')),
    'timeout': int(os.getenv('API_TIMEOUT', '60')),
    'grc_backend_url': (os.getenv('GRC_BACKEND_URL') or os.getenv('NODE_API_URL') or os.getenv('NODE_BACKEND_URL') or 'https://backend-madinetmasr-compliance.comply.now').rstrip('/'),
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
        'tableHeaderFontColor': '#000000',
        'tableBodyBgColor': '#F5F5F5'
    }
}

def get_database_connection_string() -> str:
    """
    Generate database connection string (same behaviour as Node).
    - Windows auth: Trusted_Connection=yes, no UID/PWD.
    - NTLM/SQL auth: UID=DOMAIN\\user or UID=user, PWD=password (for Docker or when DB_USE_WINDOWS_AUTH=0).
    """
    config = DATABASE_CONFIG
    parts = [
        f"DRIVER={{{config['driver']}}};",
        f"SERVER={config['server']},{config['port']};",
        f"DATABASE={config['database']};",
        f"Encrypt={config['encrypt']};",
        f"TrustServerCertificate={config['trust_server_certificate']};",
    ]
    if config['trusted_connection'] == 'yes':
        parts.append("Trusted_Connection=yes;")
    else:
        # SQL Server auth: UID = username only (no domain)
        uid = config['username'] or ''
        if uid:
            parts.append(f"UID={uid};")
            parts.append(f"PWD={config['password']};")
    return "".join(parts)


def get_db_connection():
    """
    Return an open DB connection. Uses pymssql (NTLM/FreeTDS, no ODBC) when DB_BACKEND=pymssql (default),
    else pyodbc only when DB_BACKEND=odbc. Set DB_BACKEND=odbc only if you need ODBC/Trusted_Connection.
    """
    config = DATABASE_CONFIG
    use_pymssql = _db_backend == 'pymssql'
    if use_pymssql:
        import pymssql
        server = config['server']
        port = int(config['port'])
        # SQL Server auth (normal): user + password only. Windows/NTLM: domain\user + password.
        if config['trusted_connection'] == 'yes' and config.get('domain'):
            user = f"{config['domain']}\\{config['username']}"
        else:
            user = config['username'] or ''
        password = config['password']
        database = config['database']
        return pymssql.connect(
            server=server,
            port=port,
            user=user,
            password=password,
            database=database,
            timeout=_db_connect_timeout,
        )
    # Only import pyodbc when DB_BACKEND=odbc (not used with pymssql)
    try:
        import pyodbc
    except ImportError as e:
        raise ImportError(
            "pyodbc is required when DB_BACKEND=odbc. "
            "Either install ODBC dependencies (unixodbc, msodbcsql18) or set DB_BACKEND=pymssql. "
            f"Original error: {e}"
        )
    return pyodbc.connect(get_database_connection_string(), timeout=_db_connect_timeout)


def log_database_config():
    """Write current database connection info to debug log (password masked). Call after load_dotenv."""
    try:
        from routes.route_utils import write_debug
        c = DATABASE_CONFIG
        pwd = c.get('password') or ''
        mask = '***' if pwd else '(not set)'
        write_debug(
            f"[DB config] server={c.get('server')}, port={c.get('port')}, database={c.get('database')}, "
            f"domain={c.get('domain')}, username={c.get('username')}, password={mask}, "
            f"trusted_connection={c.get('trusted_connection')}, backend={_db_backend}, connect_timeout={_db_connect_timeout}"
        )
    except Exception:
        pass


def test_database_connection() -> tuple[bool, str, Dict[str, Any]]:
    """
    Test database connectivity. Returns (success, message, details).
    details may include: server, database, auth_type, username, table_count, sql_version, error.
    Uses get_db_connection() so it works with both pymssql (NTLM) and odbc.
    """
    details: Dict[str, Any] = {
        "server": DATABASE_CONFIG.get("server", "N/A"),
        "database": DATABASE_CONFIG.get("database", "N/A"),
        "auth_type": "Windows (Trusted_Connection)" if DATABASE_CONFIG.get("trusted_connection") == "yes" else "SQL (username + password)",
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
