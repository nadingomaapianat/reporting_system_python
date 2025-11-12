"""
Configuration settings for the reporting system
"""
import os
from typing import Dict, Any

# Load environment variables from environment.env file
def load_env_file(env_file='environment.env'):
    """Load environment variables from a file"""
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith('#'):
                    # Split on first '=' to handle values with '=' in them
                    if '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip()

# Load environment variables on module import
load_env_file()

# Database Configuration - loaded from environment variables
# Supports Windows Authentication (NTLM) similar to NestJS app.module.ts
DATABASE_CONFIG = {
    'server': os.getenv('DB_SERVER', '206.189.57.0'),
    'port': os.getenv('DB_PORT', '1433'),
    'database': os.getenv('DB_NAME', 'NEWDCC-V4-UAT'),
    'driver': os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server'),
    'use_windows_auth': os.getenv('DB_USE_WINDOWS_AUTH', 'yes').lower() == 'yes',
    'domain': os.getenv('DB_DOMAIN', ''),
    'username': os.getenv('DB_USERNAME', ''),
    'password': os.getenv('DB_PASSWORD', ''),
    'encrypt': os.getenv('DB_ENCRYPT', 'yes'),
    'trust_server_certificate': os.getenv('DB_TRUST_SERVER_CERTIFICATE', 'yes'),
    'request_timeout': int(os.getenv('DB_REQUEST_TIMEOUT', '60000')),
    'connect_timeout': int(os.getenv('DB_CONNECT_TIMEOUT', '60000')),
    # Connection pool settings (similar to NestJS)
    'pool_max': int(os.getenv('DB_POOL_MAX', '20')),
    'pool_min': int(os.getenv('DB_POOL_MIN', '5')),
    'pool_acquire': int(os.getenv('DB_POOL_ACQUIRE', '60000')),
    'pool_idle': int(os.getenv('DB_POOL_IDLE', '30000')),
}

# API Configuration - loaded from environment variables
API_CONFIG = {
    'node_api_url': os.getenv('NODE_API_URL', 'http://10.240.53.65:3002'),
    'python_api_url': os.getenv('PYTHON_API_URL', 'http://10.240.53.65:8000'),
    'timeout': int(os.getenv('API_TIMEOUT', '60'))
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
    Generate database connection string with Windows Authentication support
    Similar to NestJS app.module.ts configuration with NTLM authentication
    """
    config = DATABASE_CONFIG
    
    # Build base connection string
    conn_parts = [
        f"DRIVER={{{config['driver']}}};",
        f"SERVER={config['server']},{config['port']};",
        f"DATABASE={config['database']};",
    ]
    
    # Windows Authentication (NTLM) - similar to NestJS app.module.ts
    if config['use_windows_auth']:
        # Use Windows Authentication (uses current Windows user context)
        # This matches NestJS: trustedConnection: true, integratedSecurity: true
        conn_parts.append("Trusted_Connection=yes;")
        
        # Note: For domain-specific authentication with credentials,
        # you typically need to run the Python process under that domain user
        # or use SQL Server Authentication mode with domain\username format
    else:
        # SQL Server Authentication (fallback)
        username = config['username'] or 'SA'
        password = config['password'] or ''
        
        # If domain is provided, format as domain\username
        if config['domain']:
            username = f"{config['domain']}\\{username}"
        
        conn_parts.append(f"UID={username};")
        conn_parts.append(f"PWD={password};")
        conn_parts.append("Trusted_Connection=no;")
    
    # Encryption and certificate settings (matching NestJS)
    conn_parts.append(f"Encrypt={config['encrypt']};")
    conn_parts.append(f"TrustServerCertificate={config['trust_server_certificate']};")
    
    # Timeout settings (matching NestJS dialectOptions)
    # Note: "Connect Timeout" is the standard ODBC parameter name
    conn_parts.append(f"Connect Timeout={config['connect_timeout'] // 1000};")  # Convert ms to seconds
    
    return ''.join(conn_parts)

def get_default_header_config(dashboard_type: str) -> Dict[str, Any]:
    """Get default header configuration for a dashboard type"""
    return DEFAULT_HEADER_CONFIGS.get(dashboard_type, DEFAULT_HEADER_CONFIGS['risks']).copy()
