"""
Shared-session check against the main app's `Users` table (DCC / new_adib_backend).

Reporting mints its own 2h JWT from the entry token (IET). The main app cannot individually
revoke that token on admin force-logout / logout-in-another-browser, so instead of relying only
on the `blocked_tokens` list we also check the single server-side session flag the main backend
maintains: it sets `Users.isSessionActive = true` on login and clears `isSessionActive` +
`currentToken` TOGETHER on logout / admin force-logout / force-logout-all / session cleanup.

Once the main session ends, Python stops accepting the user's token on its next request —
the same way the main front and compliance follow a logout. Mirrors the Node UserSessionService.

Both columns are required: `isSessionActive` can be left stale-true when a JWT merely expires,
but `currentToken` is always nulled on a real logout, so the reliable test is
`isSessionActive = 1 AND currentToken IS NOT NULL`.

Read-only here. Fail-open on DB error (a transient outage must not lock everyone out),
consistent with token_blocklist.is_token_blocked.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Optional

logger = logging.getLogger(__name__)

# Short cache so a logout in the main app reaches reporting within seconds, without a DB hit
# on every single request. Override with REPORTING_SESSION_CACHE_MS.
_CACHE_TTL_S = (float(os.environ.get("REPORTING_SESSION_CACHE_MS", "10000")) / 1000.0)
_cache: dict[str, tuple[bool, float]] = {}


def is_user_session_active(user_id: Optional[str]) -> bool:
    """True if the user's main-app session is still active. Fail-open on DB error."""
    if not user_id or not str(user_id).strip():
        return False
    uid = str(user_id).strip()

    now = time.monotonic()
    hit = _cache.get(uid)
    if hit is not None and hit[1] > now:
        return hit[0]

    try:
        from config.settings import get_db_connection, DATABASE_CONFIG

        db_name = (DATABASE_CONFIG.get("database") or "").strip()
        if not db_name:
            logger.warning("[Python Auth][UserSession] DB_NAME empty; skipping session check")
            return True

        conn = get_db_connection()
        try:
            cur = conn.cursor()
            sql = f"""
                SELECT TOP 1 1
                FROM [{db_name}].dbo.Users
                WHERE id = %s
                  AND deletedAt IS NULL
                  AND isSessionActive = 1
                  AND currentToken IS NOT NULL
            """
            cur.execute(sql, (uid,))
            active = cur.fetchone() is not None
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as e:
        # Fail closed: a session that cannot be checked is not treated as active.
        logger.warning(
            "[Python Auth][UserSession] DB query failed (rejecting): %s", e
        )
        return False

    _cache[uid] = (active, now + _CACHE_TTL_S)
    return active
