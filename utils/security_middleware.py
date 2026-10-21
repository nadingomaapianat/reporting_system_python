"""
Bank-grade security middleware: headers, trust proxy, rate limiting.
Aligns with new_adib_backend and reporting_system_node2.
"""
import os
import time
import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response, JSONResponse

logger = logging.getLogger(__name__)

# --- Trust proxy: use X-Forwarded-For when behind reverse proxy ---
TRUST_PROXY = os.getenv("TRUST_PROXY", "1").strip().lower() in ("1", "true", "yes")
try:
    TRUST_PROXY_HOPS = max(1, int(os.getenv("TRUST_PROXY_HOPS", "1")))
except ValueError:
    TRUST_PROXY_HOPS = 1


def get_client_ip(request: Request) -> str:
    """Real client IP when behind proxy (R-WAPT / rate limit)."""
    if not TRUST_PROXY:
        if request.client:
            return request.client.host
        return "0.0.0.0"
    # The client can send its own X-Forwarded-For; the trusted proxy appends the address it
    # saw, so read from the right (TRUST_PROXY_HOPS = number of proxies in front, default 1).
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        hops = [h.strip() for h in forwarded.split(",") if h.strip()]
        if hops:
            return hops[-min(TRUST_PROXY_HOPS, len(hops))]
    if request.client:
        return request.client.host
    return "0.0.0.0"


# --- Rate limiting: 40 requests per minute per IP (same as Node) ---
# /csrf/token exempt so frontend/proxy can fetch CSRF without 429 under load
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX = 40
RATE_LIMIT_EXEMPT_PATHS = ("/csrf/token",)
_store: dict[str, tuple[int, float]] = {}
_store_ttl: float = 0


def _clean_store():
    global _store_ttl
    now = time.time()
    if now - _store_ttl > 60:
        _store_ttl = now
        to_del = [k for k, (_, start) in _store.items() if now - start > RATE_LIMIT_WINDOW]
        for k in to_del:
            del _store[k]


class RateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        if request.method == "OPTIONS":
            return await call_next(request)
        path = request.url.path.rstrip("/") or "/"
        if any(path == p or path.startswith(p + "/") for p in RATE_LIMIT_EXEMPT_PATHS):
            return await call_next(request)
        ip = get_client_ip(request)
        now = time.time()
        _clean_store()
        if ip in _store:
            count, start = _store[ip]
            if now - start >= RATE_LIMIT_WINDOW:
                _store[ip] = (1, now)
            else:
                count += 1
                if count > RATE_LIMIT_MAX:
                    return JSONResponse(
                        status_code=429,
                        content={"detail": "Too many requests. Please try again later."},
                    )
                _store[ip] = (count, start)
        else:
            _store[ip] = (1, now)
        return await call_next(request)


# --- Date query params: many GRC queries write these straight into SQL text ---
DATE_QUERY_PARAMS = ("startDate", "endDate", "start_date", "end_date")


class DateParamsMiddleware(BaseHTTPMiddleware):
    """Refuses a malformed date query param with 400 before any route runs (same as reporting Node)."""

    async def dispatch(self, request: Request, call_next) -> Response:
        from utils.node_grc_query import is_iso_date_value

        for name in DATE_QUERY_PARAMS:
            for value in request.query_params.getlist(name):
                text = value.strip()
                if text and not is_iso_date_value(text):
                    return JSONResponse(status_code=400, content={"detail": f"Invalid {name}"})
        return await call_next(request)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Set security headers (R-WAPT03); remove X-Powered-By."""

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        if hasattr(response, "headers"):
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "SAMEORIGIN"
            response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
            response.headers["Content-Security-Policy"] = (
                "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
                "img-src 'self' data: https:; font-src 'self' data:; connect-src 'self'; "
                "frame-ancestors 'self'; base-uri 'self'; object-src 'none'"
            )
            if "x-powered-by" in response.headers:
                del response.headers["x-powered-by"]
        return response
