"""
SIEM activity log: writes a row into the shared `Targets` table (the audit trail the main app,
adib_backend, writes) for every action that reaches this reporting API.

Pure ASGI middleware, added last in main.py so it is the outermost layer: it sees the final
status of everything, including the refusals of CORS, the upload/rate limits, CSRF, the JWT
check and the page-permission check, which answer without reaching a route.

What gets a row:
  - every write (POST/PUT/PATCH/DELETE), success or failure;
  - every export / generated file (Word, PDF, Excel, iXBRL) and download;
  - every FAILED read (4xx/5xx), throttled per user+route;
  - successful reads do NOT (one row per chart/data call would bury the trail).

userId is stored only for the user JWTAuthMiddleware verified (request.state.user). A refused
request with none (an expired or forged token) is still written with an empty userId and the user
its token names quoted in the description. A failed read carrying no token at all is not written.

The caller's IP goes into Targets.ipAddress where that column exists (UB schema), otherwise into
the description (ADIB schema) — detected once at runtime, so the file is the same on both branches.

Logging never affects the request: the insert runs in a worker thread after the response, and a
failure (e.g. the reporting DB login has no INSERT right on Targets) is only logged, at most once
a minute. SIEM_ACTIVITY_LOG=false switches it off. Request bodies are never read or written.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from typing import Any, Dict, Optional

import jwt
from starlette.requests import Request

logger = logging.getLogger(__name__)

_SKIP = (
    re.compile(r"^/csrf(/|$)"),
    re.compile(r"^/(docs|redoc|openapi\.json)(/|$)"),
    re.compile(r"^/favicon\.ico$"),
    re.compile(r"^/health(/|$)"),
)
# Whole path tokens, only consulted for POST (a POST used as a query).
_READ_TOKENS = {
    "search", "filter", "filters", "list", "get", "find", "count", "lookup", "fetch",
    "stats", "statistics", "validate", "preview", "check", "analyze", "columns", "concepts",
    "debug", "extract", "headers",
}
# ...unless the path also says it changes data: POST .../enhanced-bank-check/insert is a write.
_WRITE_TOKENS = {"insert", "save", "create", "update", "delete", "upload", "import", "schedule", "submit", "approve", "reject"}
_EXPORT_TOKENS = {"export", "exports", "download", "downloads", "generate"}
# Leading path segments that do not name the module: /api/reports/bank-check -> "Bank Check".
_PREFIX_SEGMENTS = {"api", "reports", "grc"}
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I)
_THROTTLE_SECONDS = 60
_THROTTLE_MAX_KEYS = 5000
_MAX_CAPTURED_BODY = 16 * 1024

_DONE = {
    "CREATE": "created a record in",
    "UPDATE": "updated a record in",
    "DELETE": "deleted a record from",
    "EXPORT": "exported data from",
    "VIEW": "viewed",
}
_TRIED = {
    "CREATE": "create a record in",
    "UPDATE": "update a record in",
    "DELETE": "delete a record from",
    "EXPORT": "export data from",
    "VIEW": "view",
}
_ACTION_FOR_METHOD = {"GET": "VIEW", "POST": "CREATE", "PUT": "UPDATE", "PATCH": "UPDATE", "DELETE": "DELETE"}


# Raw technical text never reaches the SIEM row: database / driver errors, code stack traces,
# script errors, network error codes and Python exceptions get a plain-language reason (same
# rules as adib_backend src/SIEM/log-text.ts). The technical text stays in the server log.
# Only text that clearly comes from code is rewritten; ordinary messages are left as written.

# Text that only a database / ORM / driver produces.
_DB_TEXT = re.compile(
    r"sequelize|notNull Violation|Validation error:|tedious|mssql|pymssql|pyodbc|RequestError|ConnectionError|"
    r"OperationalError|ProgrammingError|IntegrityError|DatabaseError|\bSQL\b|invalid (column|object) name|"
    r"incorrect syntax|ambiguous column|the (insert|update|delete|merge) statement|statement has been terminated|"
    r"constraint|\bdbo\.|uniqueidentifier|conversion failed when converting|:1433\b",
    re.I,
)
# (pattern, reason) for database text. First match wins.
_DB_KNOWN = [
    (re.compile(r"unique ?constraint|violation of (unique|primary) key|duplicate key", re.I),
     "a record with the same value already exists"),
    (re.compile(r"foreign ?key|conflicted with the (reference|foreign key)|REFERENCE constraint", re.I),
     "the record is linked to other data (missing or still in use)"),
    (re.compile(r"cannot insert the value null|notnull violation|cannot be null|does not allow nulls", re.I),
     "a required field is missing"),
    (re.compile(r"would be truncated|string or binary data|value too long", re.I), "a value is too long"),
    (re.compile(r"conversion failed|error converting|invalid input syntax|out-of-range|arithmetic overflow|"
                r"uniqueidentifier", re.I), "a value has the wrong format"),
    (re.compile(r"timeout|timed out|ETIMEOUT|ECONNREFUSED|ECONNRESET|ESOCKET|connection (is )?closed|"
                r"failed to connect|login failed", re.I), "the database could not be reached"),
    (re.compile(r"deadlock", re.I), "the database was busy, please retry"),
]
_DB_GENERIC = "a database error occurred"
# Sequelize reports a unique-constraint violation as just "Validation error".
_SEQUELIZE_UNIQUE = re.compile(r"(^|:\s*)Validation error\.?\s*$")
# Technical text that is not from the database. (pattern, reason); first match wins.
_TECH_KNOWN = [
    # Directory (LDAP) service.
    (re.compile(r"InvalidCredentialsError|LDAP_INVALID_CREDENTIALS|data 52e\b", re.I),
     "the username or password is not correct"),
    (re.compile(r"\bLDAP\w*Error\b|ldaps?://\S+", re.I), "the directory (LDAP) service could not be reached"),
    # Network / other services.
    (re.compile(r"\b(ECONNREFUSED|ETIMEDOUT|ENOTFOUND|ECONNRESET|EHOSTUNREACH|ENETUNREACH|EAI_AGAIN|EPIPE)\b|"
                r"socket hang up|getaddrinfo|self[- ]signed certificate|unable to verify the first certificate|"
                r"\bCERT_[A-Z_]+|ERR_TLS\w*", re.I), "a connected service could not be reached"),
    (re.compile(r"\bFailed to fetch\b|NetworkError when attempting to fetch|\bLoad failed\b|ERR_NETWORK|Network Error\b"),
     "the server could not be reached from the browser"),
    # Browser script errors.
    (re.compile(r"Loading (CSS )?chunk \S+ failed|ChunkLoadError", re.I), "part of the page failed to load (refresh the page)"),
    # Script errors (browser or server): the sentence around it already says where.
    (re.compile(r"Cannot read propert(y|ies) of (undefined|null)|Cannot set propert(y|ies) of (undefined|null)|"
                r"is not a function\b|is not defined\b|is not iterable\b|Maximum call stack|Minified React error|"
                r"Hydration failed|^Script error\.?$|Unexpected token .* in JSON|JSON\.parse|is not valid JSON", re.I),
     "an unexpected error occurred"),
    # Python exceptions (reporting service).
    (re.compile(r"\b(AttributeError|KeyError|IndexError|NameError|ValueError|ZeroDivisionError|RuntimeError|"
                r"UnboundLocalError|ModuleNotFoundError|ImportError)\b:|'NoneType' object"),
     "an unexpected server error occurred"),
    # Node / JS runtime errors on the server.
    (re.compile(r"\b(TypeError|ReferenceError|RangeError|SyntaxError)\b:"), "an unexpected error occurred"),
]
# One frame of a code stack trace (JS "at f (x.js:1:2)", Python 'File "x.py", line 3', the
# "Traceback (most recent call last):" header, a bare "https://host/chunk.js:1:2345").
_STACK_LINE = re.compile(
    r'^\s*at\s+\S.*(\(.*:\d+:\d+\)|:\d+:\d+|\(native\)|\(<anonymous>\))\s*$|^\s*File ".*", line \d+|'
    r"^\s*Traceback \(most recent call last\):?\s*$|^\s*\S+:\d+:\d+\s*$"
)
_TECH_REMOVED = "(technical details removed — see the server log)"
_TRAILING_ERROR_CLASS = re.compile(r"(^|:\s*)(Uncaught\s+)?\w*(Error|Exception)$")


def _find_technical(line: str):
    """(reason, start of the technical text) for technical text in `line`, or None."""
    m = _SEQUELIZE_UNIQUE.search(line)
    if m:
        return "a record with the same value already exists", m.start()
    if _DB_TEXT.search(line):
        reason = next((r for p, r in _DB_KNOWN if p.search(line)), _DB_GENERIC)
        hits = [x.start() for x in [_DB_TEXT.search(line)] + [p.search(line) for p, _ in _DB_KNOWN] if x]
        return reason, min(hits)
    for pattern, reason in _TECH_KNOWN:
        m = pattern.search(line)
        if m:
            return reason, m.start()
    return None


def _clean_line(line: str) -> str:
    # The request line is a URL, never an error.
    if line.startswith("Request: "):
        return line
    found = _find_technical(line)
    if not found:
        return line
    reason, at = found
    # "<sentence>: <technical text>" keeps the sentence and swaps the technical part. An error
    # class left just before it ("...: TypeError: Cannot read ...") goes too.
    cut = line.rfind(": ", 0, at + 2)  # +2: same as JS lastIndexOf(s, i)
    sentence = _TRAILING_ERROR_CLASS.sub("", line[:cut]) if cut > 0 else ""
    if sentence.strip():
        return f"{sentence}: {reason}"
    if reason == _DB_GENERIC:
        return "A database error occurred (details in the server log)"
    return f"Error: {reason}"


def clean_log_text(description: str) -> str:
    """The description with technical text replaced by a plain-language reason and stack traces
    dropped; the same cleaned line twice in a row is written once."""
    out = []
    removed_stack = False
    for line in description.split("\n"):
        if _STACK_LINE.search(line):
            if not removed_stack:
                out.append(_TECH_REMOVED)
            removed_stack = True
            continue
        cleaned = _clean_line(line)
        if cleaned != line and out and out[-1] == cleaned:
            continue
        out.append(cleaned)
    return "\n".join(out)


def _enabled() -> bool:
    return (os.getenv("SIEM_ACTIVITY_LOG") or "").strip().lower() != "false"


def _tokens_of(path: str) -> list:
    spaced = re.sub(r"([a-z])([A-Z])", r"\1 \2", path).lower()
    return [t for t in re.split(r"[/\-_\s.]+", spaced) if t]


def _module_name(path: str) -> str:
    segments = [s for s in path.split("/") if s]
    segment = next((s for s in segments if s.lower() not in _PREFIX_SEGMENTS), "Home")
    spaced = re.sub(r"([a-z])([A-Z])", r"\1 \2", segment)
    words = [w[:1].upper() + w[1:] for w in re.split(r"[-_\s]+", spaced) if w]
    return f"Reporting {' '.join(words)} Module"[:250]


def _reason(status: int) -> str:
    if status == 401:
        return "not signed in"
    if status == 403:
        return "access denied"
    if status == 404:
        return "not found"
    if status == 429:
        return "too many requests"
    if status >= 500:
        return "server error"
    return "request rejected"


def _describe(who: Optional[str], action: str, method: str, module: str, failed: bool, status: int,
              error: Optional[str]) -> str:
    who = (who or "").strip() or "Unknown user"
    if action == "Logout":
        return f"{who} logged out" if not failed else f"{who} could not log out: {_reason(status)}"
    base = action if action in _DONE else _ACTION_FOR_METHOD.get(method, "CREATE")
    if not failed:
        return f"{who} {_DONE[base]} {module}"
    reason = _reason(status)
    if error and error.strip() and error.strip().lower() != reason:
        reason = f"{reason} ({error.strip()})"
    return f"{who} could not {_TRIED[base]} {module}: {reason}"


def _message_of(body: bytes) -> Optional[str]:
    try:
        data = json.loads(body.decode("utf-8"))
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    m = data.get("message") or data.get("detail") or data.get("error")
    if not m:
        return None
    if isinstance(m, list):
        m = "; ".join(str(x.get("msg", x)) if isinstance(x, dict) else str(x) for x in m)
    return str(m)[:1000]


def _soft_failure(body: bytes) -> bool:
    try:
        data = json.loads(body.decode("utf-8"))
    except Exception:
        return False
    return isinstance(data, dict) and (data.get("success") is False or data.get("isSuccess") is False)


class ActivityLogMiddleware:
    def __init__(self, app):
        self.app = app
        self._recent: Dict[str, float] = {}
        self._last_error_at = 0.0
        self._tasks: set = set()
        # Whether Targets has an ipAddress column (UB schema yes, ADIB no); read on the first insert.
        self._has_ip_column: Optional[bool] = None

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http" or not _enabled():
            await self.app(scope, receive, send)
            return
        method = str(scope.get("method", "")).upper()
        path = scope.get("path", "") or ""
        if method in ("OPTIONS", "HEAD") or any(r.search(path) for r in _SKIP):
            await self.app(scope, receive, send)
            return

        state: Dict[str, Any] = {"status": 500, "json": False, "body": bytearray()}

        async def capture(message):
            if message["type"] == "http.response.start":
                state["status"] = int(message.get("status", 500))
                ctype = b""
                for k, v in message.get("headers") or []:
                    if k.lower() == b"content-type":
                        ctype = v.lower()
                state["json"] = b"json" in ctype
            elif message["type"] == "http.response.body" and state["json"]:
                room = _MAX_CAPTURED_BODY - len(state["body"])
                if room > 0:
                    state["body"].extend((message.get("body") or b"")[:room])
            await send(message)

        try:
            await self.app(scope, receive, capture)
        finally:
            try:
                self._record(scope, method, path, state["status"], bytes(state["body"]))
            except Exception:
                pass  # Logging must never affect the request.

    def _record(self, scope, method: str, path: str, status: int, body: bytes) -> None:
        soft = bool(body) and _soft_failure(body)
        failed = status >= 400 or soft
        is_write = method in ("POST", "PUT", "PATCH", "DELETE")
        tokens = _tokens_of(path)
        is_export = any(t in _EXPORT_TOKENS or t.startswith("generate") for t in tokens)
        is_read = (not is_write) or (
            method == "POST" and any(t in _READ_TOKENS for t in tokens) and not any(t in _WRITE_TOKENS for t in tokens)
        )
        is_logout = any(t.startswith("logout") for t in tokens)
        if not failed and is_read and not is_export and not is_logout:
            return

        user = (scope.get("state") or {}).get("user") or {}
        if not isinstance(user, dict):
            user = {}
        user_id = user.get("id") or None
        # Who a refused token names. Only ever quoted in the description, never stored as userId:
        # a refused request may carry a forged token.
        tokens_sent = [] if user_id else self._candidate_tokens(scope)
        claimed = None if user_id else self._claimed_token_user(tokens_sent)
        # A failed read that carried no token at all names nobody and touched nothing — a tab left
        # open after its session ended, polling. Not written (same rule as the main app).
        if failed and is_read and not is_export and not user_id and not tokens_sent:
            return

        ip = self._client_ip(scope)
        caller = user_id or (claimed or {}).get("id") or ip
        if ((failed and is_read) or status == 429) and self._throttled(f"{caller}|{method}|{path}|{status}"):
            return

        if status == 401:
            action = "UNAUTHORIZED"
        elif status == 403:
            action = "ACCESS_DENIED"
        elif is_logout:
            action = "Logout"
        elif is_export:
            action = "EXPORT"
        elif is_read:
            action = "VIEW"
        elif method == "DELETE":
            action = "DELETE"
        elif method in ("PUT", "PATCH"):
            action = "UPDATE"
        else:
            action = "CREATE"

        module = _module_name(path)
        who = user.get("name") or user.get("username")
        # A 5xx message is technical (it can be the raw database error): the row says "server error".
        error = _message_of(body) if failed and body and status < 500 else None
        lines = [_describe(who, action, method, module, failed, status, error)]
        role = user.get("groupName")
        if role:
            lines.append(f"Role: {role}")
        if claimed and claimed["signature_valid"]:
            name = claimed.get("name")
            lines.append(f"Expired session of {name} ({claimed['id']})" if name else f"Expired session of {claimed['id']}")
        elif claimed:
            lines.append(f"Token claimed user {claimed['id']} (not verified)")
        lines.append(f"Request: {method} {path[:300]} -> {status}")
        ref = _UUID.search(path)

        row = (
            str(user_id) if user_id else None, action, "Failure" if failed else "Success", module,
            clean_log_text("\n".join(lines)), ref.group(0) if ref else None,
        )
        try:
            task = asyncio.get_running_loop().create_task(asyncio.to_thread(self._insert, row, ip))
        except RuntimeError:
            return
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _insert(self, row: tuple, ip: Optional[str]) -> None:
        try:
            from config.settings import DATABASE_CONFIG, _db_backend, get_db_connection

            db_name = (DATABASE_CONFIG.get("database") or "").strip().replace("]", "")
            if not db_name:
                return
            conn = get_db_connection()
            try:
                cur = conn.cursor()
                param = "%s" if _db_backend == "pymssql" else "?"  # pyodbc takes ? placeholders
                if self._has_ip_column is None:
                    cur.execute(f"SELECT COL_LENGTH({param}, 'ipAddress')", (f"[{db_name}].dbo.Targets",))
                    found = cur.fetchone()
                    self._has_ip_column = bool(found and found[0] is not None)
                user_id, action, result, module, description, ref = row
                if not self._has_ip_column and ip:
                    description = description.replace("\nRequest: ", f"\nIP: {ip}\nRequest: ", 1)
                # Same columns Sequelize maintains for the main app's Target model (timestamps +
                # paranoid). userId may be NULL: the main app relaxes the column at boot; until it
                # has, such an insert fails and is only reported below.
                columns = "id, [timestamp], userId, action, result, targetObject, description, refId"
                values = [user_id, action, result, module, description, ref]
                if self._has_ip_column:
                    columns += ", ipAddress"
                    values.append(ip)
                placeholders = ", ".join([param] * len(values))
                sql = (
                    f"INSERT INTO [{db_name}].dbo.Targets ({columns}, createdAt, updatedAt) "
                    f"VALUES (NEWID(), SYSDATETIMEOFFSET(), {placeholders}, SYSDATETIMEOFFSET(), SYSDATETIMEOFFSET())"
                )
                cur.execute(sql, tuple(values))
                conn.commit()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception as e:
            now = time.monotonic()
            if now - self._last_error_at >= 60:
                self._last_error_at = now
                logger.error("[ActivityLog] failed to write SIEM row: %s", e)

    @staticmethod
    def _candidate_tokens(scope) -> list:
        try:
            from utils.auth import candidate_jwt_strings

            return [t.strip() for t in candidate_jwt_strings(Request(scope)) if t and t.strip()]
        except Exception:
            return []

    @staticmethod
    def _claimed_token_user(tokens: list) -> Optional[Dict[str, Any]]:
        """The user a refused token names: signature_valid when one was signed with our secret (an
        expired session), otherwise a forged or garbled token whose id is only a claim."""
        try:
            from utils.auth import JWT_ALGORITHM, JWT_SECRET
        except Exception:
            JWT_ALGORITHM, JWT_SECRET = "HS256", ""
        if JWT_SECRET:
            for raw in tokens:
                try:
                    payload = jwt.decode(raw, JWT_SECRET, algorithms=[JWT_ALGORITHM], options={"verify_exp": False})
                except Exception:
                    continue
                uid = payload.get("id") or payload.get("sub")
                if isinstance(uid, str) and uid:
                    return {"id": uid, "name": payload.get("name") or payload.get("username"), "signature_valid": True}
        for raw in tokens:
            try:
                payload = jwt.decode(raw, options={"verify_signature": False})
            except Exception:
                continue
            uid = payload.get("id") or payload.get("sub") if isinstance(payload, dict) else None
            if isinstance(uid, str) and uid:
                return {"id": uid[:100], "signature_valid": False}
        return None

    @staticmethod
    def _client_ip(scope) -> Optional[str]:
        try:
            from utils.security_middleware import get_client_ip

            ip = get_client_ip(Request(scope))
        except Exception:
            client = scope.get("client")
            ip = client[0] if client else None
        return ip.replace("::ffff:", "") if ip else None

    def _throttled(self, key: str) -> bool:
        now = time.monotonic()
        last = self._recent.get(key)
        if last is not None and now - last < _THROTTLE_SECONDS:
            return True
        if len(self._recent) >= _THROTTLE_MAX_KEYS:
            for k in [k for k, t in self._recent.items() if now - t >= _THROTTLE_SECONDS]:
                del self._recent[k]
            if len(self._recent) >= _THROTTLE_MAX_KEYS:
                self._recent.clear()
        self._recent[key] = now
        return False
