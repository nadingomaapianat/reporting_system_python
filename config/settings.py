"""
Configuration settings for the reporting system
"""
import os
from typing import Dict, Any

# Database Configuration
DATABASE_CONFIG = {
    'server': '206.189.57.0',
    'port': '1433',
    'database': 'NEWDCC-V4-UAT',
    'username': 'SA',
    'password': 'Nothing_159',
    'driver': 'ODBC Driver 17 for SQL Server',
    'trusted_connection': 'no',
    'encrypt': 'yes',
    'trust_server_certificate': 'yes'
}

# API Configuration
API_CONFIG = {
    'node_api_url': 'http://localhost:3002',
    'python_api_url': 'http://localhost:8000',
    'timeout': 60
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
        'bankAddress': 'King Abdulaziz Road, Riyadh, Saudi Arabia',
        'bankPhone': '+966 11 402 9000',
        'bankUrl': 'www.pianat.ai',
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
        'bankAddress': 'King Abdulaziz Road, Riyadh, Saudi Arabia',
        'bankPhone': '+966 11 402 9000',
        'bankUrl': 'www.pianat.ai',
        'watermarkEnabled': True,
        'watermarkText': 'CONFIDENTIAL',
        'fontColor': '#1F4E79',
        'tableHeaderBgColor': '#E3F2FD',
        'tableBodyBgColor': '#F5F5F5'
    }
}

def get_database_connection_string() -> str:
    """Generate database connection string"""
    config = DATABASE_CONFIG
    return (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']},{config['port']};"
        f"DATABASE={config['database']};"
        f"UID={config['username']};"
        f"PWD={config['password']};"
        f"Trusted_Connection={config['trusted_connection']};"
        f"Encrypt={config['encrypt']};"
        f"TrustServerCertificate={config['trust_server_certificate']};"
    )

def get_default_header_config(dashboard_type: str) -> Dict[str, Any]:
    """Get default header configuration for a dashboard type"""
    return DEFAULT_HEADER_CONFIGS.get(dashboard_type, DEFAULT_HEADER_CONFIGS['risks']).copy()
