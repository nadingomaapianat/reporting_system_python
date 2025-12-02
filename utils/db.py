import os
import pyodbc
import logging

logger = logging.getLogger("db")


def get_connection():
    """
    Connect to SQL Server using NTLM/domain authentication via FreeTDS.
    Uses environment variables for configuration.
    """

    # Load credentials / settings from environment
    DOMAIN = os.getenv("DB_DOMAIN", "ADIBEG")
    USERNAME = os.getenv("DB_USER", "GRCSVC")
    PASSWORD = os.getenv("DB_PASS", "")
    SERVER = os.getenv("DB_SERVER", "10.240.10.202")
    PORT = int(os.getenv("DB_PORT", "5555"))
    DATABASE = os.getenv("DB_NAME", "NEWDCC-V4-UAT")

    # Either use DSN or full connection string; here we use DRIVER + SERVER
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

    logger.info("Connecting to SQL Server with FreeTDS/pyodbc (NTLM)...")

    try:
        conn = pyodbc.connect(conn_str)
        logger.info("Database connection successful")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise
