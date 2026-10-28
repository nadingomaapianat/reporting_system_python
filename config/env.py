"""
Environment-specific settings (URLs, origins, database, bank branding) come only from the
environment (.env / environment.env / the service's own variables), never from values written in
the source. Hardcoded fallbacks used to point one bank's build at another bank's hosts, and print
"Abu Dhabi Islamic Bank Egypt" on any bank's exported reports. A missing required value now stops
the service with the variable's name instead. Values are never logged.
"""
from __future__ import annotations

import os


def env(name: str, *aliases: str) -> str:
    """First non-empty value of `name` or its aliases, else "" (for optional settings)."""
    for key in (name, *aliases):
        value = (os.getenv(key) or "").strip()
        if value:
            return value
    return ""


def require_env(name: str, *aliases: str) -> str:
    """Like env(), but raises naming the variable when none is set."""
    value = env(name, *aliases)
    if not value:
        alt = f" (or {' / '.join(aliases)})" if aliases else ""
        raise RuntimeError(f"Missing required environment variable {name}{alt}. Set it in .env.")
    return value


# Checked once at startup (main.create_app), so a misconfigured server fails at boot.
REQUIRED_ENV: list[tuple[str, ...]] = [
    ("JWT_SECRET", "JWT_SECRET_KEY"),
    ("DB_HOST", "DB_SERVER"),
    ("DB_PORT",),
    ("DB_NAME",),
    ("NODE_API_URL", "NODE_BACKEND_URL"),
    ("FRONTEND_ORIGIN", "CORS_ORIGINS"),
]


def assert_required_env() -> None:
    """Raises one error listing every missing variable (names only)."""
    missing = [" or ".join(keys) for keys in REQUIRED_ENV if not env(*keys)]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}. Set them in .env.")
