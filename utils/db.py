import os
import pyodbc
import logging

logger = logging.getLogger("db")


def get_connection():
    """
    Connect to SQL Server using SQL Server Authentication (username + password)
    via FreeTDS + pyodbc.
    """

    # Load credentials / settings from environment (e.g. from .env or Docker env)
    USERNAME = os.getenv("DB_USER", "sql_user")        # SQL login in SQL Server
    PASSWORD = os.getenv("DB_PASS", "")               # SQL login password
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
        f"TDS_Version=7.3;"
    )

    logger.info("Connecting to SQL Server with FreeTDS/pyodbc using SQL auth...")

    try:
        conn = pyodbc.connect(conn_str)
        logger.info("Database connection successful")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise
