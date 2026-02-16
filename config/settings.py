"""
Configuration settings for the reporting system.
Database: same as Node – from env (DB_HOST, DB_PORT, DB_NAME, DB_DOMAIN, DB_USERNAME, DB_PASSWORD).
Supports Windows integrated auth (Trusted_Connection) or NTLM/SQL auth (domain\\user + password).
"""
import os
from typing import Dict, Any, Tuple
from dotenv import load_dotenv

# Database: read from env (same vars as Node). In Docker use NTLM: DB_USE_WINDOWS_AUTH=0, DB_DOMAIN, DB_USERNAME, DB_PASSWORD.
_db_host = os.getenv('DB_HOST', '')
_db_port = os.getenv('DB_PORT', '1433')
_db_name = os.getenv('DB_NAME', '')
_db_domain = (os.getenv('DB_DOMAIN') or '').strip()
_db_username = os.getenv('DB_USERNAME', '')
_db_password = os.getenv('DB_PASSWORD', '')
_use_windows_auth = os.getenv('DB_USE_WINDOWS_AUTH', '1').strip().lower() not in ('0', 'false', 'no')

DATABASE_CONFIG = {
    'server': _db_host or '206.189.57.0',
    'port': _db_port or '1433',
    'database': _db_name or 'NEWDCC-V4-UAT',
    'domain': _db_domain,
    'username': _db_username or 'SA',
    'password': _db_password or 'Nothing_159',
    'driver': os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server'),
    'trusted_connection': 'yes' if _use_windows_auth else 'no',
    'encrypt': 'yes',
    'trust_server_certificate': 'yes',
}

# API Configuration (from .env: NODE_API_URL, PYTHON_API_URL, API_TIMEOUT)
API_CONFIG = {
    'node_api_url': os.getenv('NODE_API_URL', os.getenv('NODE_BACKEND_URL', 'https://grc-reporting-node-uat.adib.co.eg')),
    'python_api_url': os.getenv('PYTHON_API_URL', os.getenv('PYTHON_API_BASE', 'https://grc-reporting-py-uat.adib.co.eg')),
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
        uid = f"{config['domain']}\\{config['username']}" if config.get('domain') else config['username']
        parts.append(f"UID={uid};")
        parts.append(f"PWD={config['password']};")
    return "".join(parts)


def test_database_connection() -> tuple[bool, str, Dict[str, Any]]:
    """
    Test database connectivity. Returns (success, message, details).
    details may include: server, database, auth_type, username, table_count, sql_version, error.
    """
    details: Dict[str, Any] = {
        "server": DATABASE_CONFIG.get("server", "N/A"),
        "database": DATABASE_CONFIG.get("database", "N/A"),
        "auth_type": "Windows (Trusted_Connection)" if DATABASE_CONFIG.get("trusted_connection") == "yes" else "NTLM/SQL (domain\\user + password)",
        "username": DATABASE_CONFIG.get("username", "N/A"),
    }
    try:
        import pyodbc
        conn = pyodbc.connect(get_database_connection_string())
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

def test_database_connection() -> Tuple[bool, str, dict]:
    """
    Test database connection and return status
    Returns: (success: bool, message: str, details: dict)
    """
    try:
        import pyodbc
        connection_string = get_database_connection_string()
        config = DATABASE_CONFIG
        
        # Mask password in connection string for logging
        safe_conn_str = connection_string
        if 'PWD=' in safe_conn_str:
            # Replace password with *** for security
            parts = safe_conn_str.split('PWD=')
            if len(parts) > 1:
                pwd_part = parts[1].split(';')[0]
                safe_conn_str = safe_conn_str.replace(pwd_part, '***')
        
        # Attempt connection
        conn = pyodbc.connect(connection_string, timeout=10)
        cursor = conn.cursor()
        
        # Test query
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        
        # Get database info
        cursor.execute("SELECT DB_NAME()")
        db_name = cursor.fetchone()[0]
        
        # Get the Windows user that is connected (VERIFY Windows Authentication)
        cursor.execute("SELECT SUSER_SNAME(), SYSTEM_USER, USER_NAME()")
        auth_info = cursor.fetchone()
        connected_windows_user = auth_info[0] if auth_info[0] else "Unknown"
        system_user = auth_info[1] if auth_info[1] else "Unknown"
        database_user = auth_info[2] if auth_info[2] else "Unknown"
        
        # Get table count
        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        table_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        # Determine authentication type for display
        if config['use_windows_auth']:
            auth_type = "Windows Authentication (NTLM/Kerberos)"
            username_display = "Current Windows User"
        elif config.get('domain'):
            auth_type = "SQL Server Authentication (NTLM)"
            username_display = f"{config['domain']}\\{config.get('username', 'N/A')}"
        else:
            auth_type = "SQL Server Authentication"
            username_display = config.get('username', 'N/A')
        
        details = {
            "server": f"{config['server']}:{config['port']}",
            "database": db_name,
            "auth_type": auth_type,
            "username": username_display,
            "connected_windows_user": connected_windows_user,  # The actual Windows user connected
            "system_user": system_user,
            "database_user": database_user,
            "table_count": table_count,
            "sql_version": version.split('\n')[0] if version else "Unknown"
        }
        
        return True, "Database connection successful", details
        
    except ImportError:
        return False, "pyodbc module not installed", {}
    except Exception as e:
        error_msg = str(e)
        config = DATABASE_CONFIG
        # Determine authentication type for display
        if config['use_windows_auth']:
            auth_type = "Windows Authentication (NTLM/Kerberos)"
        elif config.get('domain'):
            auth_type = "SQL Server Authentication (NTLM)"
        else:
            auth_type = "SQL Server Authentication"
        
        details = {
            "server": f"{config['server']}:{config['port']}",
            "database": config['database'],
            "auth_type": auth_type,
            "error": error_msg
        }
        
        return False, f"Database connection failed: {error_msg}", details