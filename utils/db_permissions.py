"""
Load DCC permission rows from the same MSSQL database as reporting-node
(`Users` → `Groups.permissions` JSON), matching `AuthService.fetchPermissionsFromDb`.
"""
from __future__ import annotations

import json
import logging
from typing import Any, List, Optional

logger = logging.getLogger(__name__)


def fetch_permissions_for_user_id(user_id: str) -> Optional[List[Any]]:
    """
    Returns permissions array or None if user/group not found / error.
    """
    if not user_id or not str(user_id).strip():
        return None
    uid = str(user_id).strip()
    try:
        from config.settings import get_db_connection, DATABASE_CONFIG

        db_name = (DATABASE_CONFIG.get("database") or "").strip()
        if not db_name:
            logger.warning("[Python Auth] DB_NAME empty; cannot load permissions")
            return None

        conn = get_db_connection()
        try:
            cur = conn.cursor()
            sql = f"""
                SELECT g.permissions AS permissionsJson
                FROM [{db_name}].dbo.Users u
                JOIN [{db_name}].dbo.Groups g ON g.id = u.groupId
                WHERE u.id = %s
                  AND u.deletedAt IS NULL
                  AND g.deletedAt IS NULL
            """
            cur.execute(sql, (uid,))
            row = cur.fetchone()
            if not row:
                logger.warning("[Python Auth] DB permissions lookup: no user/group for user_id=%s", uid)
                return None
            raw = row[0] if row[0] is not None else ""
            if not raw:
                logger.warning(
                    "[Python Auth] Groups.permissions empty for user_id=%s", uid
                )
                return []
            parsed = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(parsed, list):
                logger.info(
                    "[Python Auth] DB permissions loaded: user_id=%s count=%s",
                    uid,
                    len(parsed),
                )
                return parsed
            logger.warning("[Python Auth] permissions JSON is not an array for user_id=%s", uid)
            return []
        finally:
            try:
                conn.close()
            except Exception:
                pass
    except Exception as e:
        logger.warning(
            "[Python Auth] DB permissions query failed for user_id=%s: %s",
            uid,
            e,
        )
        return None


def merge_permissions_into_user(user: dict) -> dict:
    """If JWT has no `permissions` array, hydrate from DB using `id` or `sub`."""
    existing = user.get("permissions")
    if isinstance(existing, list) and len(existing) > 0:
        return user
    uid = user.get("id") or user.get("sub")
    if not uid:
        return user
    loaded = fetch_permissions_for_user_id(str(uid))
    if loaded is not None:
        user = {**user, "permissions": loaded}
    return user
