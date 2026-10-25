"""
Error sanitization (CWE-209 remediation).
Never expose stack traces, DB details, or file paths to clients.
"""
import logging
import os
import re

GENERIC_ERROR = "An error occurred while processing your request. Please try again later."
_error_logger = logging.getLogger("reporting.errors")


def is_production() -> bool:
    return os.getenv("NODE_ENV") == "production" or not os.getenv("NODE_ENV")


def sanitize_error_message(message: str, production: bool | None = None) -> str:
    """Return a safe message for client; strip sensitive details."""
    if production is None:
        production = is_production()
    msg = str(message) if message else "Unknown error"

    if production:
        for pattern in (
            r"database\s+['\"]?[\w-]+['\"]?",
            r"table\s+['\"]?[\w.]+['\"]?",
            r"column\s+['\"]?\w+['\"]?",
            r"dbo\.\w+",
            r"[\w/\\]+\.(py|pyc|json|txt|log|env)\b",
            r"[A-Z]:\\[^\s]+",
            r"/[^\s]+\.(py|json|txt|log)",
            r"INSERT\s+statement",
            r"UPDATE\s+statement",
            r"DELETE\s+statement",
            r"Sequelize[\w\s]+error",
            r"at\s+[\w.]+",
        ):
            msg = re.sub(pattern, "[redacted]", msg, flags=re.IGNORECASE)
        msg = re.sub(r"\s+", " ", msg).strip()

    if not msg or len(msg) < 5:
        return GENERIC_ERROR
    return msg


def client_error(exc: BaseException) -> str:
    """Message to put in a response for an unexpected exception.

    Production gets a generic text (the real error is logged); development keeps str(exc).
    """
    _error_logger.error("%s: %s", type(exc).__name__, exc, exc_info=exc)
    return GENERIC_ERROR if is_production() else str(exc)
