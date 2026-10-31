"""
Error sanitization (CWE-209 remediation).
Never expose stack traces, DB details, or file paths to clients.
"""
import os


def is_production() -> bool:
    return os.getenv("NODE_ENV") == "production" or not os.getenv("NODE_ENV")


GENERIC_ERROR_MESSAGE = "An error occurred while processing your request. Please try again later."


def sanitize_error_message(message: str, production: bool | None = None) -> str:
    """
    Return a safe message for client. Production: always the fixed GENERIC_ERROR_MESSAGE.
    Redacting known patterns from the raw text was not enough: a driver error still showed the
    DB login ('SA') and the DB server IP (pen test WAPT09). Development: the message as is.
    """
    if production is None:
        production = is_production()
    if production:
        return GENERIC_ERROR_MESSAGE

    msg = str(message) if message else "Unknown error"
    if not msg or len(msg) < 5:
        return GENERIC_ERROR_MESSAGE
    return msg
