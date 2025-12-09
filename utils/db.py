import os
import pyodbc
import logging

logger = logging.getLogger("db")


def get_connection():
    """
    Connect to SQL Server using Active Directory / domain authentication
    via FreeTDS + pyodbc (NTLM/NTLMv2) from Linux.
    """

    # Load credentials / settings from environment (.env or Docker env)
    DOMAIN = os.getenv("DB_DOMAIN", "").strip()
    USERNAME = os.getenv("DB_USER", "").strip()
    PASSWORD = os.getenv("DB_PASS", "")
    SERVER = os.getenv("DB_SERVER", "10.240.10.202")
    PORT = int(os.getenv("DB_PORT", "5555"))
    DATABASE = os.getenv("DB_NAME", "NEWDCC-V4-UAT")

    if not USERNAME or not PASSWORD:
        raise RuntimeError("DB_USER and DB_PASS must be set for AD authentication")

    # Build domain-qualified username if a separate domain is provided
    # FreeTDS expects UID in the form DOMAIN\\username for domain logins. [web:8][web:3]
    if DOMAIN and "\\" not in USERNAME:
        login_user = f"{DOMAIN}\\{USERNAME}"
    else:
        login_user = USERNAME

    conn_str = (
        f"DRIVER={{FreeTDS}};"
        f"SERVER={SERVER};"
        f"PORT={PORT};"
        f"DATABASE={DATABASE};"
        f"UID={login_user};"
        f"PWD={PASSWORD};"
        f"TDS_Version=7.3;"
        f"UseNTLMv2=Yes;"
    )

    logger.info(f"Connecting to SQL Server with FreeTDS/pyodbc using domain login '{login_user}'...")

    try:
        conn = pyodbc.connect(conn_str)
        logger.info("Database connection successful")
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise
