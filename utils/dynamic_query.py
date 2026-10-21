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

# Never offered to the report builder, from new_adib_backend's models:
#   tables  BlockedToken.token, IframeEntryToken.token_hash, IntegrationSetting.payload
#           (encrypted credentials), Target (SIEM audit log — has its own page permission)
#   columns User.password / User.currentToken (live session), GroupModel.permissions
# Overridable (comma-separated) with REPORTING_BLOCKED_TABLES / REPORTING_BLOCKED_COLUMNS.
_DEFAULT_BLOCKED_TABLES = "blocked_tokens,iframe_entry_tokens,integration_settings,Targets"
_DEFAULT_BLOCKED_COLUMNS = "Users.password,Users.currentToken,Groups.permissions"


def _csv(value: str) -> List[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


# Tables the report builder may use (allow-list; REPORTING_ALLOWED_TABLES, comma-separated,
# replaces it). GRC business data only: users / groups / permissions, tokens and settings, the
# audit log, notifications, workflow internals, file-path tables and assets / vulnerabilities
# (scoped elsewhere) are not reportable. Anything not listed is unknown to every builder.
_DEFAULT_ALLOWED_TABLES = (
    "Actionplans, Activities, AdequateFindings, Assertions, BankQuestionControls, "
    "BankQuestionFunctions, BankQuestionRisks, BankQuestions, BankQuestionSurveys, "
    "BusinessUnits, Catalogs, Categories, CBEDomains, CBEFunctions, CobitDomains, "
    "CobitPractices, CobitProcesses, ComplianceControlActions, "
    "ComplianceControlActionsComments, ComplianceGaps, ComplianceMappings, "
    "complianceReferences, Compliances, ComplianceTeam, ControlAssertions, ControlCBEs, "
    "ControlCobits, ControlControlReferences, ControlCosos, ControlDesignTests, "
    "controlDomains, ControlFunctions, ControlIsos, ControlNists, ControlProcesses, "
    "ControlReferences, ControlRequirements, Controls, ControlSurveyAnswer, CosoComponents, "
    "CosoExamples, CosoPoints, CosoPrinciples, Currencies, DocumentCategories, Documents, "
    "DocumentVersions, Domains, EaCapabilities, EaCapabilityControls, EaCapabilityRisks, "
    "EaCapabilitySystemMaps, EffectiveFindings, EventTypes, FinancialImpacts, "
    "FinancialStatementLineItems, FinancialStatements, FunctionProcesses, Functions, "
    "ImpactedAreas, IncidentAccounts, IncidentCategories, IncidentCauses, IncidentEvents, "
    "IncidentRecoveryDetails, Incidents, IncidentSubCategories, Isos, JobTitles, "
    "KriFunctions, KriProcesses, KriRisks, Kris, KriUsers, KriValues, NistCats, NistSubs, "
    "Processes, PublicSurveyFunctions, PublicSurveys, PublicSurveyUsers, RecoverySources, "
    "RemediationActivities, RemediationComments, RemediationCompliances, Remediations, "
    "RemediationTasks, ResidualRiskRatings, Residualrisks, RiskCategories, RiskControls, "
    "RiskFunctions, RiskIncidents, RiskInherentImpacts, RiskProcesses, "
    "RiskResidualAssessments, Risks, RootCauses, Sectors, StatementCategories, "
    "SubBusinessUnits, SubFunctions, SubProcesses, survey_assessment, survey_impacted_area, "
    "Systems"
)


def allowed_tables() -> set:
    return {t.lower() for t in _csv(os.getenv("REPORTING_ALLOWED_TABLES", _DEFAULT_ALLOWED_TABLES))}


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
    """Hidden from the builders: blocked, or not on the allow-list."""
    table = str(name).split(".")[-1].strip("[]").lower()
    return table in blocked_tables() or table not in allowed_tables()


def visible_columns(table: str, columns: Sequence[str]) -> List[str]:
    hidden = blocked_columns().get(str(table).split(".")[-1].lower(), set())
    return [c for c in columns if str(c).lower() not in hidden]

# Row caps so one request cannot pull a whole table into memory (REPORTING_MAX_ROWS overrides).
PREVIEW_MAX_ROWS = 5000


def max_report_rows() -> int:
    try:
        return max(1, int(os.getenv("REPORTING_MAX_ROWS", "50000")))
    except ValueError:
        return 50000


def preview_row_limit(value) -> int:
    """previewLimit from the client: default 1000 when missing or <= 0, at most PREVIEW_MAX_ROWS."""
    try:
        n = int(value) if value not in (None, "") else 1000
    except (TypeError, ValueError):
        n = 1000
    if n <= 0:
        n = 1000
    return min(n, PREVIEW_MAX_ROWS)

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
    hidden_tables, hidden_cols, allowed = blocked_tables(), blocked_columns(), allowed_tables()
    for sch, tbl, col in cursor.fetchall():
        if str(tbl).lower() not in allowed or str(tbl).lower() in hidden_tables or str(col).lower() in hidden_cols.get(str(tbl).lower(), set()):
            continue  # not allowed / blocked: unknown to every builder
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
