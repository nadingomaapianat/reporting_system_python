"""
Export utilities - re-exports for backward compatibility.
Header config is defined in config.settings.
merge_header_config lives in routes.route_utils; re-export here for services that expect it from export_utils.
"""
import re
import uuid as _uuid_mod
from typing import Any, List, Sequence, Tuple, Union
from datetime import datetime, date
from config import get_default_header_config

try:
    from routes.route_utils import merge_header_config
except ImportError:
    # Avoid circular import when route_utils not yet loaded; define a minimal fallback
    def merge_header_config(module_name: str, header_config: dict):
        from config import get_default_header_config
        default_config = get_default_header_config(module_name)
        return {**default_config, **header_config}


# Keys that contain date values (ISO strings or timestamps) to show as readable dates in exports
DATE_KEYS = frozenset({
    'expected_implementation_date', 'implementation_date', 'createdAt', 'created_at',
    'updatedAt', 'updated_at', 'date', 'meeting_date', 'due_date', 'target_date',
    'occurrenceDate', 'reportedDate',
    # SQL aliases with spaces, e.g. AS [Created At]
    'created at', 'updated at', 'deleted at',
})
# Keys that are date+time (show time in exports)
DATETIME_KEYS = frozenset({
    'createdAt', 'created_at', 'updatedAt', 'updated_at',
    'created at', 'updated at', 'deleted at',
})
DATETIME_KEYS_LOWER = frozenset(k.lower() for k in DATETIME_KEYS)


def _try_format_date(value, include_time: bool = False) -> str:
    """Try to format value as DD/MM/YYYY (or DD/MM/YYYY HH:MM if include_time); return None if not a date."""
    try:
        if isinstance(value, (int, float)) and value > 0:
            dt = datetime.utcfromtimestamp(value / 1000.0 if value > 1e12 else value)
            return dt.strftime('%d/%m/%Y %H:%M') if include_time else dt.strftime('%d/%m/%Y')
        if isinstance(value, datetime):
            return value.strftime('%d/%m/%Y %H:%M') if include_time else value.strftime('%d/%m/%Y')
        if isinstance(value, date):
            return value.strftime('%d/%m/%Y')
        if isinstance(value, str):
            s = value.strip()
            if len(s) >= 10 and s[4] == '-' and s[7] == '-':
                if include_time:
                    norm = s.replace('Z', '').split('+')[0].strip().replace('T', ' ').replace('t', ' ')
                    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
                        for n in (norm, norm[:26], norm[:19]):
                            if not n or len(n) < 10:
                                continue
                            try:
                                dt = datetime.strptime(n.strip(), fmt)
                                return dt.strftime('%d/%m/%Y %H:%M')
                            except Exception:
                                continue
                    try:
                        dt = datetime.fromisoformat(s.replace('Z', '').split('+')[0])
                        return dt.strftime('%d/%m/%Y %H:%M')
                    except Exception:
                        pass
                date_part = s.replace('T', ' ').split(' ')[0].strip()[:10]
                dt = datetime.strptime(date_part, '%Y-%m-%d')
                return dt.strftime('%d/%m/%Y')
        if hasattr(value, 'strftime'):
            return value.strftime('%d/%m/%Y %H:%M') if include_time else value.strftime('%d/%m/%Y')
    except Exception:
        pass
    return None


def _column_key_tokens(key: str) -> list:
    """Split column name into lowercase alphanumeric tokens (handles camelCase, spaces, dots)."""
    s = re.sub(r'[\[\]]', ' ', str(key))
    parts = re.split(r'[^a-z0-9]+', s.lower())
    return [p for p in parts if p]


def is_hidden_export_column_key(key: str) -> bool:
    """True for id / UUID / guid columns that should not appear in PDF/Excel exports."""
    k = str(key).strip().lower()
    if not k:
        return False
    if k == 'id':
        return True
    if k.endswith(' id'):
        return True
    if k.endswith('_id'):
        return True
    if k in ('guid', 'uuid', 'rowguid', 'uniqueidentifier'):
        return True
    # token-based: hide ..._uuid, my_uuid, "Row Guid", etc.; avoid matching 'guidance' (no standalone guid/uuid token)
    toks = _column_key_tokens(key)
    if 'uuid' in toks or 'guid' in toks:
        return True
    if any(t.endswith('uuid') or t.endswith('guid') for t in toks):
        return True
    return False


def _normalize_column_label_for_filter(col: Union[str, dict, Any]) -> str:
    """Last segment after '.' for dbo.Table.Id -> Id."""
    if isinstance(col, dict):
        h = col.get('label') or col.get('key') or col.get('name') or ''
    else:
        h = str(col)
    h = h.strip()
    if '.' in h:
        h = h.rsplit('.', 1)[-1].strip()
    return h


def filter_export_columns_rows(
    columns: Sequence,
    data_rows: Sequence[Sequence[Any]],
) -> Tuple[List, List[List[Any]]]:
    """
    Drop hidden id/uuid columns by index so Excel/PDF tables stay aligned.
    Safe when columns is a list of str or dicts with label/key (dynamic reports).
    """
    if not columns or not data_rows:
        return list(columns), [list(r) for r in data_rows]
    keep: List[int] = []
    for i, col in enumerate(columns):
        raw = str(col).strip() if not isinstance(col, dict) else str(col.get('label') or col.get('key') or '')
        norm = _normalize_column_label_for_filter(col)
        if is_hidden_export_column_key(raw) or is_hidden_export_column_key(norm):
            continue
        keep.append(i)
    if len(keep) == len(columns):
        return list(columns), [list(r) for r in data_rows]
    new_cols = [columns[i] for i in keep]
    new_rows: List[List[Any]] = []
    for row in data_rows:
        row = list(row) if row is not None else []
        new_rows.append([row[i] if i < len(row) else '' for i in keep])
    return new_cols, new_rows


def _value_is_uuid_like(value) -> bool:
    """True if value is a standard string or bytes form of a UUID."""
    if isinstance(value, _uuid_mod.UUID):
        return True
    if isinstance(value, (bytes, memoryview)) and len(bytes(value)) == 16:
        return True
    if value is None:
        return False
    s = str(value).strip()
    if len(s) < 32:
        return False
    try:
        _uuid_mod.UUID(s)
        return True
    except Exception:
        pass
    if len(s) == 32 and re.fullmatch(r'[0-9a-fA-F]{32}', s):
        try:
            _uuid_mod.UUID(hex=s)
            return True
        except Exception:
            pass
    return False


def redact_uuid_like_for_export(value) -> str:
    """Replace UUID-like scalars with empty string for exports (no raw ids)."""
    if _value_is_uuid_like(value):
        return ''
    return str(value) if value is not None else ''


def format_cell_value_for_export(key: str, value, _from_bytes: int = 0) -> str:
    """Convert a table cell value to string; format date-like keys as readable DD/MM/YYYY (or with time for createdAt etc.)."""
    if value is None or value == '':
        return 'N/A' if value is None else ''

    if isinstance(value, (bytes, memoryview)):
        b = bytes(value)
        if len(b) == 16:
            # 16-byte SQL uniqueidentifier — do not print as UUID in exports
            return ''
        if _from_bytes < 3:
            try:
                return format_cell_value_for_export(key, b.decode('utf-8'), _from_bytes + 1)
            except Exception:
                pass
        return ''

    key_lower = str(key).lower().strip()
    include_time = key_lower in DATETIME_KEYS_LOWER
    # Format if key looks like a date field
    if key_lower in DATE_KEYS or key_lower.endswith('_date') or key_lower.endswith('date'):
        formatted = _try_format_date(value, include_time=include_time)
        if formatted is not None:
            return formatted
    # Also try to format any value that looks like an ISO date string
    if isinstance(value, str) and len(value) >= 10 and value.strip()[4:5] == '-':
        formatted = _try_format_date(value, include_time=include_time)
        if formatted is not None:
            return formatted
    if _value_is_uuid_like(value):
        return ''
    return str(value)


# Canonical column order taken from the Incidents Catalog (adib-frontend) table.
# Used to normalize every incident export column list (PDF + Excel) to the same order.
# Variant keys for one concept share a slot (e.g. function_name/functionName, net_loss/netLoss).
CATALOG_KEY_ORDER = [
    'code',
    'importance',
    'reportedDate', 'reported_date',
    'occurrenceDate', 'occurrence_date',
    'timeFrame',
    'owner',
    'functionName', 'function_name',
    'categoryName',
    'subCategoryName',
    'title', 'name', 'incident_name', 'incident_title',
    'description',
    'rootCause', 'root_cause',
    'causeName',
    'rcm',
    'kriName',
    'discoveredType',
    'totalLoss',
    'recoveryAmount', 'recovery_amount',
    'netLoss', 'net_loss',
    'financialImpactName', 'financial_impact_name',
    'currencyName',
    'exchangeRate',
    'financialEquivalent',
    'recoveryStatus',
    'eventType',
    'preparerStatus',
    'reviewerStatus',
    'checkerStatus',
    'acceptanceStatus',
]

# Tables that are aggregates (dimension + measure), not incident-row lists — never reordered.
INCIDENT_AGGREGATE_TABLES = {'lossByRiskCategory', 'comprehensiveOperationalLoss'}


def _order_by_catalog(pairs):
    """Reorder a list of (key, label) tuples to CATALOG_KEY_ORDER.

    Reorder-only: columns already present are sorted into catalog order; any key not
    in the catalog is appended last, preserving its original relative order. No column
    is ever added — so PDF's deliberately reduced column sets stay reduced.
    """
    known = [p for p in pairs if p[0] in CATALOG_KEY_ORDER]
    unknown = [p for p in pairs if p[0] not in CATALOG_KEY_ORDER]
    known.sort(key=lambda p: CATALOG_KEY_ORDER.index(p[0]))
    return known + unknown


# Incident table/card export: column order and labels to match UI (IncidentsDashboard tableColumns)
INCIDENT_COLUMNS_UI = [
    ('code', 'Code'),
    ('title', 'Title'),
    ('function_name', 'Function'),
    ('status', 'Status'),
    ('categoryName', 'Category'),
    ('subCategoryName', 'Sub Category'),
    ('owner', 'Owner'),
    ('importance', 'Importance'),
    ('timeFrame', 'Time Frame'),
    ('occurrenceDate', 'Occurrence Date'),
    ('reportedDate', 'Reported Date'),
    ('description', 'Description'),
    ('rootCause', 'Root Cause'),
    ('causeName', 'Cause'),
    ('rcm', 'RCM'),
    ('kriName', 'KRI'),
    ('discoveredType', 'Discovered Type'),
    ('totalLoss', 'Total Loss'),
    ('recoveryAmount', 'Recoveries Amount'),
    ('netLoss', 'Net Loss'),
    ('financialImpactName', 'Financial Impact'),
    ('currencyName', 'Currency'),
    ('exchangeRate', 'Exchange Rate'),
    ('financialEquivalent', 'Financial Equivalent (LYC)'),
    ('recoveryStatus', 'Recovery Status'),
    ('eventType', 'Event Type'),
    ('preparerStatus', 'Incident Status'),
    ('reviewerStatus', 'Review'),
    ('checkerStatus', 'First Approval'),
    ('acceptanceStatus', 'Second Approval'),
    ('createdAt', 'Created At'),
]

# Incident Action Plan table (Actionplans linked to Incidents) — matches IncidentsDashboard incidentActionPlan columns
INCIDENT_ACTION_PLAN_COLUMNS = [
    ('code', 'Code'),
    ('incident_name', 'Incident Name'),
    ('incident_department', 'Incident Department'),
    ('root_cause', 'Root Cause'),
    ('description', 'Description'),
    ('action_taken', 'Action Taken'),
    ('action_owner', 'Action Owner'),
    ('status', 'Status'),
    ('expected_implementation_date', 'Expected Implementation Date'),
]

# Excel only: full field set for Incident Action Plan / Overdue Incidents (matches the on-screen full column set).
# PDF intentionally keeps using INCIDENT_ACTION_PLAN_COLUMNS (compact) above — do not use this list for PDF.
INCIDENT_ACTION_PLAN_COLUMNS_FULL = [
    ('code', 'Code'),
    ('importance', 'Importance'),
    ('reportedDate', 'Reported Date'),
    ('occurrenceDate', 'Occurrence Date'),
    ('timeFrame', 'Time Frame'),
    ('owner', 'Incident Owner'),
    ('incident_name', 'Incident Name'),
    ('incident_department', 'Incident Department'),
    ('categoryName', 'Category'),
    ('subCategoryName', 'Sub Category'),
    ('root_cause', 'Root Cause'),
    ('causeName', 'Cause'),
    ('rcm', 'RCM'),
    ('kriName', 'KRI'),
    ('discoveredType', 'Discovered Type'),
    ('totalLoss', 'Total Loss'),
    ('recoveryAmount', 'Recoveries Amount'),
    ('netLoss', 'Net Loss'),
    ('financialImpactName', 'Financial Impact'),
    ('currencyName', 'Currency'),
    ('exchangeRate', 'Exchange Rate'),
    ('financialEquivalent', 'Financial Equivalent (LYC)'),
    ('recoveryStatus', 'Recovery Status'),
    ('eventType', 'Event Type'),
    ('preparerStatus', 'Preparer Status'),
    ('reviewerStatus', 'Review'),
    ('checkerStatus', 'First Approval'),
    ('acceptanceStatus', 'Second Approval'),
    ('incident_createdAt', 'Incident Created At'),
    ('description', 'Description'),
    ('action_taken', 'Action Taken'),
    ('action_owner', 'Action Owner'),
    ('status', 'Status'),
    ('expected_implementation_date', 'Expected Implementation Date'),
]


def get_incident_action_plan_ordered_keys_full() -> list:
    """Full column order for Incident Action Plan / Overdue Incidents Excel export (matches on-screen table)."""
    return [k for k, _ in INCIDENT_ACTION_PLAN_COLUMNS_FULL]


def get_incident_action_plan_label_full(key: str) -> str:
    """UI label for a full Incident Action Plan column key."""
    for k, label in INCIDENT_ACTION_PLAN_COLUMNS_FULL:
        if k == key:
            return label
    import re
    return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(key)).title()


# Overdue Incidents: same columns as Incident Action Plan (rows filtered server-side)
OVERDUE_INCIDENTS_COLUMNS = [
    ('code', 'Code'),
    ('incident_name', 'Incident Name'),
    ('incident_department', 'Incident Department'),
    ('root_cause', 'Root Cause'),
]


def get_overdue_incidents_ordered_keys() -> list:
    return [k for k, _ in OVERDUE_INCIDENTS_COLUMNS]


def get_overdue_incidents_label(key: str) -> str:
    for k, label in OVERDUE_INCIDENTS_COLUMNS:
        if k == key:
            return label
    import re
    return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(key)).title()


def get_overdue_incidents_cell_value(row: dict, key: str, empty_placeholder: str = 'N/A') -> str:
    v = row.get(key, '')
    if v is None or v == '':
        return empty_placeholder
    return format_cell_value_for_export(key, v)


def get_incident_action_plan_ordered_keys() -> list:
    """Fixed column order for Incident Action Plan exports (matches UI)."""
    return [k for k, _ in INCIDENT_ACTION_PLAN_COLUMNS]


def get_incident_action_plan_label(key: str) -> str:
    """UI label for an Incident Action Plan column key."""
    for k, label in INCIDENT_ACTION_PLAN_COLUMNS:
        if k == key:
            return label
    import re
    return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(key)).title()


def get_incident_action_plan_cell_value(row: dict, key: str, empty_placeholder: str = 'N/A') -> str:
    """Cell value for Incident Action Plan row; dates formatted like other exports."""
    v = row.get(key, '')
    if v is None or v == '':
        return empty_placeholder
    return format_cell_value_for_export(key, v)


# PDF only: subset of columns so the table fits on the page and avoids layout errors
INCIDENT_COLUMNS_PDF = [
    ('code', 'Code'),
    ('title', 'Title'),
    ('function_name', 'Function'),
    ('status', 'Status'),
    ('categoryName', 'Category'),
    ('owner', 'Owner'),
    ('occurrenceDate', 'Occurrence Date'),
    ('createdAt', 'Created At'),
]


def get_incident_ordered_keys_pdf(first_row: dict) -> list:
    """Return ordered list of keys for incident PDF: only the PDF subset, in order. Skips keys missing from data."""
    keys = []
    has_function = 'function_name' in first_row or 'functionName' in first_row
    for k, _ in INCIDENT_COLUMNS_PDF:
        if k == 'functionName':
            continue
        if k in first_row or (k == 'function_name' and has_function):
            keys.append(k)
    return keys


def get_incident_ordered_keys(first_row: dict) -> list:
    """Return ordered list of keys for incident export: UI order first, then any extra keys from data. One 'function_name' covers functionName."""
    ui_keys = [k for k, _ in INCIDENT_COLUMNS_UI]
    data_keys = [k for k in first_row.keys() if not is_hidden_export_column_key(k) and k != 'functionName']
    has_function = 'function_name' in first_row or 'functionName' in first_row
    seen = set()
    ordered = []
    for k in ui_keys:
        if k == 'functionName':
            continue
        if k in data_keys or (k == 'function_name' and has_function):
            ordered.append(k)
            seen.add(k)
    for k in data_keys:
        if k not in seen:
            ordered.append(k)
    return ordered


def get_incident_ordered_keys_full_ui() -> list:
    """Return full list of incident column keys in UI order (all columns as in dashboard). Use for Excel/PDF so export has all UI columns."""
    return [k for k, _ in INCIDENT_COLUMNS_UI if k != 'functionName']


# Excel only: each Incidents table has its own on-screen column order (built independently per table),
# so a single shared list can't match all of them. One dedicated ordered list per table, mirroring
# IncidentsDashboard.tsx's tableColumns exactly (key, label). PDF is unaffected (keeps its own compact lists).
INCIDENT_TABLE_COLUMN_ORDERS = {
    'overallStatuses': [
        ('code', 'Code'), ('occurrenceDate', 'Occurrence Date'), ('reportedDate', 'Reported Date'),
        ('title', 'Title'), ('function_name', 'Function'), ('importance', 'Importance'),
        ('timeFrame', 'Time Frame'), ('owner', 'Owner'), ('categoryName', 'Category'),
        ('subCategoryName', 'Sub Category'), ('description', 'Description'), ('rootCause', 'Root Cause'),
        ('causeName', 'Cause'), ('rcm', 'RCM'), ('kriName', 'KRI'), ('discoveredType', 'Discovered Type'),
        ('totalLoss', 'Total Loss'), ('financialImpactName', 'Financial Impact'), ('currencyName', 'Currency'),
        ('exchangeRate', 'Exchange Rate'), ('financialEquivalent', 'Financial Equivalent (LYC)'),
        ('recoveryStatus', 'Recovery Status'), ('eventType', 'Event Type'),
        ('recoveryAmount', 'Recoveries Amount'), ('netLoss', 'Net Loss'), ('preparerStatus', 'Preparer Status'),
        ('reviewerStatus', 'Review'), ('checkerStatus', 'First Approval'), ('acceptanceStatus', 'Second Approval'),
        ('status', 'Status'), ('createdAt', 'Created At'),
    ],
    'incidentsFinancialDetails': [
        ('code', 'Code'), ('occurrenceDate', 'Occurrence Date'), ('reportedDate', 'Reported Date'),
        ('title', 'Title'), ('function_name', 'Function'), ('importance', 'Importance'),
        ('timeFrame', 'Time Frame'), ('owner', 'Owner'), ('categoryName', 'Category'),
        ('subCategoryName', 'Sub Category'), ('description', 'Description'), ('rootCause', 'Root Cause'),
        ('causeName', 'Cause'), ('rcm', 'RCM'), ('kriName', 'KRI'), ('discoveredType', 'Discovered Type'),
        ('netLoss', 'Net Loss'), ('totalLoss', 'Total Loss'), ('recoveryAmount', 'Recovery Amount'),
        ('grossAmount', 'Gross Amount'), ('financialImpactName', 'Financial Impact'), ('currencyName', 'Currency'),
        ('exchangeRate', 'Exchange Rate'), ('financialEquivalent', 'Financial Equivalent (LYC)'),
        ('eventType', 'Event Type'), ('preparerStatus', 'Preparer Status'), ('reviewerStatus', 'Review'),
        ('checkerStatus', 'First Approval'), ('acceptanceStatus', 'Second Approval'), ('status', 'Status'),
        ('createdAt', 'Created At'),
    ],
    'incidentsWithTimeframe': [
        ('code', 'Code'), ('importance', 'Importance'), ('reportedDate', 'Reported Date'),
        ('occurrenceDate', 'Occurrence Date'), ('owner', 'Owner'), ('incident_name', 'Incident Name'),
        ('function_name', 'Function'), ('categoryName', 'Category'), ('subCategoryName', 'Sub Category'),
        ('description', 'Description'), ('rootCause', 'Root Cause'), ('causeName', 'Cause'), ('rcm', 'RCM'),
        ('kriName', 'KRI'), ('discoveredType', 'Discovered Type'), ('totalLoss', 'Total Loss'),
        ('recoveryAmount', 'Recoveries Amount'), ('netLoss', 'Net Loss'),
        ('financialImpactName', 'Financial Impact'), ('currencyName', 'Currency'),
        ('exchangeRate', 'Exchange Rate'), ('financialEquivalent', 'Financial Equivalent (LYC)'),
        ('recoveryStatus', 'Recovery Status'), ('eventType', 'Event Type'), ('preparerStatus', 'Preparer Status'),
        ('reviewerStatus', 'Review'), ('checkerStatus', 'First Approval'), ('acceptanceStatus', 'Second Approval'),
        ('time_frame', 'Time Frame (days)'), ('createdAt', 'Created At'),
    ],
    'incidentsWithFinancialAndFunction': [
        ('code', 'Code'), ('occurrenceDate', 'Occurrence Date'), ('reportedDate', 'Reported Date'),
        ('title', 'Title'), ('function_name', 'Function'), ('financial_impact_name', 'Financial Impact'),
        ('importance', 'Importance'), ('timeFrame', 'Time Frame'), ('owner', 'Owner'),
        ('categoryName', 'Category'), ('subCategoryName', 'Sub Category'), ('description', 'Description'),
        ('rootCause', 'Root Cause'), ('causeName', 'Cause'), ('rcm', 'RCM'), ('kriName', 'KRI'),
        ('discoveredType', 'Discovered Type'), ('totalLoss', 'Total Loss'), ('currencyName', 'Currency'),
        ('exchangeRate', 'Exchange Rate'), ('financialEquivalent', 'Financial Equivalent (LYC)'),
        ('recoveryStatus', 'Recovery Status'), ('eventType', 'Event Type'),
        ('recoveryAmount', 'Recoveries Amount'), ('netLoss', 'Net Loss'), ('preparerStatus', 'Preparer Status'),
        ('reviewerStatus', 'Review'), ('checkerStatus', 'First Approval'), ('acceptanceStatus', 'Second Approval'),
        ('createdAt', 'Created At'),
    ],
    'lossByRiskCategory': [
        ('riskCategory', 'Risk Category'), ('incidentCount', 'Incident Count'),
        ('totalLoss', 'Total Loss'), ('averageLoss', 'Average Loss'),
    ],
    'comprehensiveOperationalLoss': [
        ('metric', 'Metric'), ('count', 'Count'), ('totalValue', 'Total Value'),
    ],
    'netLossAndRecovery': [
        ('code', 'Code'), ('occurrenceDate', 'Occurrence Date'), ('reportedDate', 'Reported Date'),
        ('incident_title', 'Incident'), ('function_name', 'Function'), ('importance', 'Importance'),
        ('timeFrame', 'Time Frame'), ('owner', 'Owner'), ('categoryName', 'Category'),
        ('subCategoryName', 'Sub Category'), ('description', 'Description'), ('rootCause', 'Root Cause'),
        ('causeName', 'Cause'), ('rcm', 'RCM'), ('kriName', 'KRI'), ('discoveredType', 'Discovered Type'),
        ('totalLoss', 'Total Loss'), ('financialImpactName', 'Financial Impact'), ('currencyName', 'Currency'),
        ('exchangeRate', 'Exchange Rate'), ('financialEquivalent', 'Financial Equivalent (LYC)'),
        ('recoveryStatus', 'Recovery Status'), ('eventType', 'Event Type'), ('net_loss', 'Net Loss'),
        ('recovery_amount', 'Recovery Amount'), ('preparerStatus', 'Preparer Status'), ('reviewerStatus', 'Review'),
        ('checkerStatus', 'First Approval'), ('acceptanceStatus', 'Second Approval'), ('createdAt', 'Created At'),
    ],
}


def get_incident_table_ordered_keys(card_type: str) -> list:
    """Ordered column keys for a specific Incidents table (Excel). Falls back to the generic full-UI order if unknown."""
    order = INCIDENT_TABLE_COLUMN_ORDERS.get(card_type)
    if order is not None:
        return [k for k, _ in order]
    return get_incident_ordered_keys_full_ui()


def get_incident_table_label(card_type: str, key: str) -> str:
    """UI label for a column key within a specific Incidents table (Excel)."""
    order = INCIDENT_TABLE_COLUMN_ORDERS.get(card_type)
    if order is not None:
        for k, label in order:
            if k == key:
                return label
    return get_incident_label(key)


# PDF only: compact, explicit per-table column set for the Incidents dashboard tables (fewer columns than Excel by design).
INCIDENT_TABLE_COLUMN_ORDERS_PDF = {
    'overallStatuses': [
        ('code', 'Code'), ('title', 'Title'), ('function_name', 'Function'), ('status', 'Status'),
    ],
    'incidentsFinancialDetails': [
        ('title', 'Title'), ('rootCause', 'Root Cause'), ('function_name', 'Function'),
        ('netLoss', 'Net Loss'), ('totalLoss', 'Total Loss'), ('recoveryAmount', 'Recovery Amount'),
        ('grossAmount', 'Gross Amount'), ('status', 'Status'),
    ],
    'incidentsWithTimeframe': [
        ('incident_name', 'Incident Name'), ('function_name', 'Function'), ('time_frame', 'Time Frame (days)'),
    ],
    'incidentsWithFinancialAndFunction': [
        ('title', 'Title'), ('financial_impact_name', 'Financial Impact'), ('function_name', 'Function'),
    ],
    'lossByRiskCategory': [
        ('riskCategory', 'Risk Category'), ('incidentCount', 'Incident Count'),
        ('totalLoss', 'Total Loss'), ('averageLoss', 'Average Loss'),
    ],
    'comprehensiveOperationalLoss': [
        ('metric', 'Metric'), ('count', 'Count'), ('totalValue', 'Total Value'),
    ],
    'netLossAndRecovery': [
        ('incident_title', 'Incident'), ('function_name', 'Function'),
        ('net_loss', 'Net Loss'), ('recovery_amount', 'Recovery Amount'),
    ],
}


# --- Normalize every incident column list to the Incidents Catalog order (PDF + Excel) ---
# Reorder-only (never adds columns), so PDF's compact sets stay compact.
# Aggregate tables are left untouched.
INCIDENT_COLUMNS_UI = _order_by_catalog(INCIDENT_COLUMNS_UI)
INCIDENT_COLUMNS_PDF = _order_by_catalog(INCIDENT_COLUMNS_PDF)
INCIDENT_ACTION_PLAN_COLUMNS = _order_by_catalog(INCIDENT_ACTION_PLAN_COLUMNS)
INCIDENT_ACTION_PLAN_COLUMNS_FULL = _order_by_catalog(INCIDENT_ACTION_PLAN_COLUMNS_FULL)
OVERDUE_INCIDENTS_COLUMNS = _order_by_catalog(OVERDUE_INCIDENTS_COLUMNS)
for _tbl, _cols in list(INCIDENT_TABLE_COLUMN_ORDERS.items()):
    if _tbl not in INCIDENT_AGGREGATE_TABLES:
        INCIDENT_TABLE_COLUMN_ORDERS[_tbl] = _order_by_catalog(_cols)
for _tbl, _cols in list(INCIDENT_TABLE_COLUMN_ORDERS_PDF.items()):
    if _tbl not in INCIDENT_AGGREGATE_TABLES:
        INCIDENT_TABLE_COLUMN_ORDERS_PDF[_tbl] = _order_by_catalog(_cols)


def get_incident_table_ordered_keys_pdf(card_type: str, first_row: dict = None) -> list:
    """Ordered column keys for a specific Incidents table (PDF, compact). None if card_type has no dedicated PDF layout."""
    order = INCIDENT_TABLE_COLUMN_ORDERS_PDF.get(card_type)
    if order is None:
        return None
    if first_row is None:
        return [k for k, _ in order]
    return [k for k, _ in order if k in first_row]


def get_incident_table_label_pdf(card_type: str, key: str) -> str:
    """UI label for a column key within a specific Incidents table (PDF)."""
    order = INCIDENT_TABLE_COLUMN_ORDERS_PDF.get(card_type)
    if order is not None:
        for k, label in order:
            if k == key:
                return label
    return get_incident_label(key)


def get_incident_label(key: str) -> str:
    """Return UI label for an incident column key."""
    for k, label in INCIDENT_COLUMNS_UI:
        if k == key:
            return label
    import re
    return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(key)).title()


def get_incident_cell_value(row: dict, key: str, empty_placeholder: str = 'N/A') -> str:
    """Get value for incident table cell; Function from function_name or functionName. Empty/missing -> empty_placeholder (default N/A). Use empty_placeholder='' for Excel so missing columns are blank."""
    if key == 'function_name':
        v = row.get('function_name') or row.get('functionName') or ''
        return format_cell_value_for_export(key, v) if v else empty_placeholder
    v = row.get(key, '')
    if v is None or v == '':
        return empty_placeholder
    return format_cell_value_for_export(key, v)


__all__ = [
    'get_default_header_config',
    'merge_header_config',
    'format_cell_value_for_export',
    'is_hidden_export_column_key',
    'filter_export_columns_rows',
    'redact_uuid_like_for_export',
    'DATE_KEYS',
    'DATETIME_KEYS',
    'INCIDENT_COLUMNS_UI',
    'INCIDENT_COLUMNS_PDF',
    'INCIDENT_TABLE_COLUMN_ORDERS',
    'get_incident_table_ordered_keys',
    'get_incident_table_label',
    'INCIDENT_TABLE_COLUMN_ORDERS_PDF',
    'get_incident_table_ordered_keys_pdf',
    'get_incident_table_label_pdf',
    'INCIDENT_ACTION_PLAN_COLUMNS',
    'INCIDENT_ACTION_PLAN_COLUMNS_FULL',
    'get_incident_action_plan_ordered_keys_full',
    'get_incident_action_plan_label_full',
    'get_incident_ordered_keys',
    'get_incident_ordered_keys_pdf',
    'get_incident_ordered_keys_full_ui',
    'get_incident_label',
    'get_incident_cell_value',
    'get_incident_action_plan_ordered_keys',
    'get_incident_action_plan_label',
    'get_incident_action_plan_cell_value',
    'OVERDUE_INCIDENTS_COLUMNS',
    'get_overdue_incidents_ordered_keys',
    'get_overdue_incidents_label',
    'get_overdue_incidents_cell_value',
]
