#!/usr/bin/env python3
"""
Test database connection from the Python project.
Loads environment.env then uses config.settings (same DB config as the API).
Run from project root: py database_service.py
"""
import os
import sys

# Load env first (same as main.py)
try:
    from dotenv import load_dotenv
    load_dotenv()
    load_dotenv("environment.env")
except ImportError:
    pass

# Now config will see DB_* from environment.env
from config.settings import (
    DATABASE_CONFIG,
    test_database_connection,
)

def main():
    auth_type = "Windows (Trusted_Connection)" if DATABASE_CONFIG.get("trusted_connection") == "yes" else "SQL/NTLM (username+password)"
    print(f"Auth mode: {auth_type}")
    print(f"Server: {DATABASE_CONFIG.get('server')}:{DATABASE_CONFIG.get('port')}, Database: {DATABASE_CONFIG.get('database')}")
    print()

    success, message, details = test_database_connection()
    if success:
        print("OK – database connected")
        print(f"  Auth: {details.get('auth_type', 'N/A')}")
        print(f"  Tables: {details.get('table_count', 0)}")
    else:
        print("FAILED:", message)
        if details.get("error"):
            print("  Error:", details["error"])
        sys.exit(1)

if __name__ == "__main__":
    main()
