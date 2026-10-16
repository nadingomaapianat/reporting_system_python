"""
Safe pieces for the dynamic report / word-template query builders.

Identifiers (tables, columns) come from the browser, so they are checked against the
database's own INFORMATION_SCHEMA before they reach the SQL text, and quoted. Values are
never put in the SQL text: builders return (sql, params) for cursor.execute(sql, params).
Join types, operators and AND/OR are allow-listed.
"""
from __future__ import annotations

import os
import re
import time
from typing import Dict, List, Optional, Sequence, Tuple

# Never offered to the report builder / SQL box, from new_adib_backend's models:
#   tables  BlockedToken.token, IframeEntryToken.token_hash, IntegrationSetting.payload
#           (encrypted credentials), Target (SIEM audit log — has its own page permission)
#   columns User.password / User.currentToken (live session), GroupModel.permissions
# Overridable (comma-separated) with REPORTING_BLOCKED_TABLES / REPORTING_BLOCKED_COLUMNS.
_DEFAULT_BLOCKED_TABLES = "blocked_tokens,iframe_entry_tokens,integration_settings,Targets"
_DEFAULT_BLOCKED_COLUMNS = "Users.password,Users.currentToken,Groups.permissions"


def _csv(value: str) -> List[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def blocked_tables() -> set:
    return {t.lower() for t in _csv(os.getenv("REPORTING_BLOCKED_TABLES", _DEFAULT_BLOCKED_TABLES))}


def blocked_columns() -> Dict[str, set]:
    """lower(table) -> {lower(column)}"""
    out: Dict[str, set] = {}
    for item in _csv(os.getenv("REPORTING_BLOCKED_COLUMNS", _DEFAULT_BLOCKED_COLUMNS)):
        if "." in item:
            table, column = item.rsplit(".", 1)
            out.setdefault(table.strip().lower(), set()).add(column.strip().lower())
    return out


def is_blocked_table(name: str) -> bool:
    return str(name).split(".")[-1].strip("[]").lower() in blocked_tables()


def visible_columns(table: str, columns: Sequence[str]) -> List[str]:
    hidden = blocked_columns().get(str(table).split(".")[-1].lower(), set())
    return [c for c in columns if str(c).lower() not in hidden]

_SCHEMA_TTL = 300.0
_schema_cache: Tuple[float, Dict[str, Tuple[str, str, Dict[str, str]]]] = (0.0, {})

JOIN_TYPES = {
    "INNER": "INNER", "LEFT": "LEFT", "RIGHT": "RIGHT", "FULL": "FULL",
    "LEFT OUTER": "LEFT OUTER", "RIGHT OUTER": "RIGHT OUTER", "FULL OUTER": "FULL OUTER",
}
COMPARISON_OPERATORS = {"=", "!=", "<>", ">", "<", ">=", "<=", "LIKE", "NOT LIKE"}
LIST_OPERATORS = {"IN", "NOT IN"}
LOGICAL_OPERATORS = {"AND", "OR"}


class QueryBuildError(ValueError):
    """Client sent something the builder cannot turn into safe SQL (-> HTTP 400)."""


def quote_ident(name: str) -> str:
    return "[" + str(name).replace("]", "]]") + "]"


def _load_schema(cursor) -> Dict[str, Tuple[str, str, Dict[str, str]]]:
    """lower('schema.table') and lower('table') for dbo -> (schema, table, {lower col: col})."""
    global _schema_cache
    now = time.monotonic()
    if _schema_cache[0] > now:
        return _schema_cache[1]
    cursor.execute(
        """
        SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS c
        JOIN INFORMATION_SCHEMA.TABLES t
          ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        """
    )
    schema: Dict[str, Tuple[str, str, Dict[str, str]]] = {}
    hidden_tables, hidden_cols = blocked_tables(), blocked_columns()
    for sch, tbl, col in cursor.fetchall():
        if str(tbl).lower() in hidden_tables or str(col).lower() in hidden_cols.get(str(tbl).lower(), set()):
            continue  # blocked: unknown to every builder
        key = f"{sch}.{tbl}".lower()
        entry = schema.setdefault(key, (str(sch), str(tbl), {}))
        entry[2][str(col).lower()] = str(col)
        if str(sch).lower() == "dbo":
            schema[str(tbl).lower()] = entry
    _schema_cache = (now + _SCHEMA_TTL, schema)
    return schema


class SchemaResolver:
    """Resolves client table/column names to real, quoted identifiers."""

    def __init__(self, cursor):
        self._schema = _load_schema(cursor)

    def table(self, name) -> Tuple[str, str, Dict[str, str]]:
        if not isinstance(name, str) or not name.strip():
            raise QueryBuildError("Table name is required")
        raw = name.strip()
        if raw.lower().startswith("dbo."):
            raw = raw[4:]
        entry = self._schema.get(raw.lower())
        if not entry:
            raise QueryBuildError(f"Unknown table: {name}")
        return entry

    def from_ref(self, name) -> str:
        """[schema].[table] for FROM / JOIN."""
        sch, tbl, _ = self.table(name)
        return f"{quote_ident(sch)}.{quote_ident(tbl)}"

    def table_alias(self, name) -> str:
        """[table] — how the existing queries qualify columns."""
        return quote_ident(self.table(name)[1])

    def column(self, table_name, column) -> str:
        _, tbl, cols = self.table(table_name)
        real = cols.get(str(column).strip().lower()) if isinstance(column, str) else None
        if not real:
            raise QueryBuildError(f"Unknown column: {table_name}.{column}")
        return f"{quote_ident(tbl)}.{quote_ident(real)}"

    def column_ref(self, ref, tables: Sequence[str], default_table: Optional[str], qualify_bare: bool) -> str:
        """
        'Table.col' -> [Table].[col]. A bare 'col' must exist in one of `tables`; it is
        qualified with `default_table` when qualify_bare, else emitted as [col] (unqualified),
        matching what each builder did before.
        """
        if not isinstance(ref, str) or not ref.strip():
            raise QueryBuildError("Column name is required")
        ref = ref.strip()
        if "." in ref:
            table_part, column_part = ref.rsplit(".", 1)
            if table_part.lower().startswith("dbo."):
                table_part = table_part[4:]
            return self.column(table_part, column_part)
        if qualify_bare:
            return self.column(default_table, ref)
        for t in tables:
            real = self.table(t)[2].get(ref.lower())
            if real:
                return quote_ident(real)
        raise QueryBuildError(f"Unknown column: {ref}")


def join_type(value) -> str:
    jt = re.sub(r"\s+", " ", str(value or "INNER").strip().upper())
    if jt.endswith(" JOIN"):
        jt = jt[:-5]
    if jt not in JOIN_TYPES:
        raise QueryBuildError(f"Unsupported join type: {value}")
    return JOIN_TYPES[jt]


def logical_operator(value) -> str:
    op = str(value or "AND").strip().upper()
    if op not in LOGICAL_OPERATORS:
        raise QueryBuildError(f"Unsupported logical operator: {value}")
    return op


def condition_sql(column_sql: str, operator, value) -> Tuple[str, List[str]]:
    """`<column> <op> <placeholder(s)>` plus its parameters."""
    op = re.sub(r"\s+", " ", str(operator or "=").strip().upper())
    if op in COMPARISON_OPERATORS:
        return f"{column_sql} {op} %s", [str(value)]
    if op in LIST_OPERATORS:
        items = [v.strip() for v in str(value).split(",") if v.strip()]
        if not items:
            raise QueryBuildError(f"{op} needs at least one value")
        return f"{column_sql} {op} ({', '.join(['%s'] * len(items))})", items
    if op == "BETWEEN":
        parts = [p.strip() for p in re.split(r",|\s+AND\s+", str(value), flags=re.IGNORECASE) if p.strip()]
        if len(parts) != 2:
            raise QueryBuildError("BETWEEN needs two values, e.g. '2025-01-01, 2025-12-31'")
        return f"{column_sql} BETWEEN %s AND %s", parts
    raise QueryBuildError(f"Unsupported operator: {operator}")


def params_or_none(params: List[str]):
    """pymssql formats the SQL text whenever params is not None; pass None when there are none."""
    return tuple(params) if params else None


# Statements and features a read-only query (or filter) never needs. Checked as whole words
# after string literals and [bracketed] names are blanked out.
_SQL_FORBIDDEN_WORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|CREATE|TRUNCATE|EXEC|EXECUTE|INTO|GRANT|REVOKE|DENY"
    r"|OPENROWSET|OPENQUERY|OPENDATASOURCE|OPENXML|BULK|SHUTDOWN|WAITFOR|DBCC|BACKUP|RESTORE|KILL"
    r"|RECONFIGURE|DECLARE|SET|USE|GO)\b|\b(XP|SP)_\w+",
    re.IGNORECASE,
)


def unsafe_sql_reason(sql: str, require_select: bool) -> Optional[str]:
    """
    Why `sql` is not a single read-only statement (require_select=True) or a read-only
    WHERE fragment (require_select=False); None when it is acceptable.
    """
    # Blank out 'string literals' (with '' escapes) and [identifiers] so their text is not inspected.
    masked = re.sub(r"'(?:[^']|'')*'", "''", str(sql))
    masked = re.sub(r"\[[^\]]*\]", "[]", masked)
    if "'" in masked.replace("''", ""):
        return "Unterminated string literal in query"
    if "--" in masked or "/*" in masked or "*/" in masked:
        return "Comments are not allowed in the query"
    body = masked.strip()
    if body.endswith(";"):
        body = body[:-1].rstrip()
    if ";" in body:
        return "Only a single SELECT statement is allowed" if require_select else "Only one filter expression is allowed"
    if require_select and not body.upper().startswith("SELECT"):
        return "Only SELECT queries are allowed"
    bad = _SQL_FORBIDDEN_WORDS.search(body)
    if bad:
        return f"'{bad.group(0).upper()}' is not allowed in a SELECT query"
    return None


def blocked_reference_reason(sql: str) -> Optional[str]:
    """
    Why free SQL (the "Create chart from SQL" box) touches blocked data, or None: a blocked
    table, or a table with hidden columns together with one of them or a * selection.
    """
    text = re.sub(r"'(?:[^']|'')*'", "''", str(sql))      # ignore string literals
    text = re.sub(r"\[([^\]]*)\]", r" \1 ", text)          # [Users] -> Users
    words = {w.lower() for w in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", text)}
    for table in _csv(os.getenv("REPORTING_BLOCKED_TABLES", _DEFAULT_BLOCKED_TABLES)):
        if table.lower() in words:
            return f"Table '{table}' is not available in reports"
    star = "*" in re.sub(r"(?i)\bcount\s*\(\s*\*\s*\)", "", text)
    for item in _csv(os.getenv("REPORTING_BLOCKED_COLUMNS", _DEFAULT_BLOCKED_COLUMNS)):
        if "." not in item:
            continue
        table, column = (part.strip() for part in item.rsplit(".", 1))
        if table.lower() not in words:
            continue
        if column.lower() in words:
            return f"Column '{table}.{column}' is not available in reports"
        if star:
            return f"Select the needed columns of '{table}' by name instead of *"
    return None

