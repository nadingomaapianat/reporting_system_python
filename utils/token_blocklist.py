"""
Token blocklist check against the shared `blocked_tokens` MSSQL table that DCC's
`new_adib_backend` populates on logout / force-logout.

Schema (Sequelize-managed by DCC):
    dbo.blocked_tokens(id UNIQUEIDENTIFIER PK, token NVARCHAR(MAX), userId UNIQUEIDENTIFIER,
                      blockedAt DATETIME2, expiresAt DATETIME2, createdAt, updatedAt)

This module is read-only here — Python only checks; DCC inserts on revocation.
"""
from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def is_token_blocked(token: Optional[str]) -> bool:
    """
    True if the JWT was revoked (present in `blocked_tokens`) and still within its TTL.

    Fail-closed: if the check cannot run (DB error / no DB name) the token is treated as
    blocked, so logged-out or force-logged-out tokens never work during a DB problem.
    """
    if not token or not str(token).strip():
        return False
    t = str(token).strip()

    try:
        from config.settings import get_db_connection, DATABASE_CONFIG

        db_name = (DATABASE_CONFIG.get("database") or "").strip()
        if not db_name:
            logger.warning("[Python Auth][Blocklist] DB_NAME empty; treating token as blocked")
            return True

        conn = get_db_connection()
        try:
            cur = conn.cursor()
            sql = f"""
                SELECT TOP 1 1
                FROM [{db_name}].dbo.blocked_tokens
                WHERE token = %s
                  AND expiresAt > SYSUTCDATETIME()
            """
            cur.execute(sql, (t,))
            row = cur.fetchone()
            return row is not None
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as e:
        logger.warning(
            "[Python Auth][Blocklist] DB query failed (treating token as blocked): %s", e
        )
        return True
