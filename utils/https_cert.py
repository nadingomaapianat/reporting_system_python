"""
Bank security: use same CA cert as new_adib_backend for outbound HTTPS.
Set CERT_PATH or CERTS_PEM_PATH in .env to path to certs.pem (e.g. ./certs.pem).
VERIFY_SSL=false = skip TLS verification (e.g. self-signed / dev).
"""
import os
import ssl
from pathlib import Path

_cached_ssl_context: ssl.SSLContext | None = False  # False = not yet tried
_cached_no_verify_context: ssl.SSLContext | None = False


def is_verify_ssl() -> bool:
    """True = verify certs (default). False when VERIFY_SSL=false or 0."""
    v = (os.getenv("VERIFY_SSL") or "").strip().lower()
    return v not in ("false", "0")


def get_cert_path() -> str | None:
    env_path = os.getenv("CERT_PATH") or os.getenv("CERTS_PEM_PATH")
    if env_path and env_path.strip():
        return env_path.strip()
    default = Path(os.getcwd()) / "certs.pem"
    if default.exists():
        return str(default)
    return None


def get_ssl_context_for_outbound() -> ssl.SSLContext | None:
    """Return SSL context for outbound HTTPS. Respects VERIFY_SSL; when false, returns context that does not verify."""
    global _cached_ssl_context, _cached_no_verify_context

    if not is_verify_ssl():
        if _cached_no_verify_context is False:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            _cached_no_verify_context = ctx
        return _cached_no_verify_context

    if _cached_ssl_context is not False:
        return _cached_ssl_context if _cached_ssl_context else None

    cert_path = get_cert_path()
    if not cert_path:
        _cached_ssl_context = None
        return None
    try:
        ctx = ssl.create_default_context()
        ctx.load_verify_locations(cafile=cert_path)
        _cached_ssl_context = ctx
        return ctx
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning("Failed to load CA cert from %s: %s", cert_path, e)
        _cached_ssl_context = None
        return None


def get_httpx_verify():  # -> bool | ssl.SSLContext
    """For httpx Client/AsyncClient verify=: False when VERIFY_SSL=false, else True or SSL context."""
    if not is_verify_ssl():
        return False
    ctx = get_ssl_context_for_outbound()
    return ctx if ctx is not None else True


def is_allow_self_signed_certs() -> bool:
    """Never disable verification in production."""
    if os.getenv("NODE_ENV") == "production":
        return False
    return os.getenv("ALLOW_SELF_SIGNED_CERTS", "").strip().lower() == "true"
