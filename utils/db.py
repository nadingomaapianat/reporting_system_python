import os
import pyodbc
import logging

logger = logging.getLogger("db")

def get_connection():
    """
    Connects to SQL Server using NTLM authentication via FreeTDS.
    """

    # Load credentials from environment variables
    DOMAIN = os.getenv("DB_DOMAIN", "ADIBEG")
    USERNAME = os.getenv("DB_USER", "GRCSVC")
    PASSWORD = os.getenv("DB_PASS", "")
    SERVER = os.getenv("DB_SERVER", "10.240.10.202")
    PORT = int(os.getenv("DB_PORT", "5555"))
    DATABASE = os.getenv("DB_NAME", "NEWDCC-V4-UAT")

    conn_str = (
        f"DRIVER={{FreeTDS}};"
        f"SERVER={SERVER};"
        f"PORT={PORT};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        f"Domain={DOMAIN};"
        f"TDS_Version=7.3;"
    )

    logger.info("Connecting to SQL Server with NTLM (FreeTDS)...")

    try:
        conn = pyodbc.connect(conn_str)
        logger.info("Connected successfully!")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise
