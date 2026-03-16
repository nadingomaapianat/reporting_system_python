"""
Export utilities - re-exports for backward compatibility.
Header config is defined in config.settings.
merge_header_config lives in routes.route_utils; re-export here for services that expect it from export_utils.
"""
from datetime import datetime
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
})
# Keys that are date+time (show time in exports)
DATETIME_KEYS = frozenset({'createdAt', 'created_at', 'updatedAt', 'updated_at'})
DATETIME_KEYS_LOWER = frozenset(k.lower() for k in DATETIME_KEYS)


def _try_format_date(value, include_time: bool = False) -> str:
    """Try to format value as DD/MM/YYYY (or DD/MM/YYYY HH:MM if include_time); return None if not a date."""
    try:
        if isinstance(value, (int, float)) and value > 0:
            dt = datetime.utcfromtimestamp(value / 1000.0 if value > 1e12 else value)
            return dt.strftime('%d/%m/%Y %H:%M') if include_time else dt.strftime('%d/%m/%Y')
        if isinstance(value, str):
            s = value.strip()
            if len(s) >= 10 and s[4] == '-' and s[7] == '-':
                if include_time and 'T' in s:
                    try:
                        # Parse full ISO datetime e.g. 2026-03-04T14:30:00.000Z
                        s_iso = s.replace('Z', '').split('+')[0].split('.')[0].strip()
                        dt = datetime.fromisoformat(s_iso)
                        return dt.strftime('%d/%m/%Y %H:%M')
                    except Exception:
                        pass
                date_part = s.split('T')[0].strip()[:10]
                dt = datetime.strptime(date_part, '%Y-%m-%d')
                return dt.strftime('%d/%m/%Y')
        if hasattr(value, 'strftime'):
            return value.strftime('%d/%m/%Y %H:%M') if include_time else value.strftime('%d/%m/%Y')
    except Exception:
        pass
    return None


def format_cell_value_for_export(key: str, value) -> str:
    """Convert a table cell value to string; format date-like keys as readable DD/MM/YYYY (or with time for createdAt etc.)."""
    if value is None or value == '':
        return 'N/A' if value is None else ''
    key_lower = str(key).lower()
    include_time = key_lower in DATETIME_KEYS_LOWER
    # Format if key looks like a date field
    if key_lower in DATE_KEYS or key_lower.endswith('_date') or key_lower.endswith('date'):
        formatted = _try_format_date(value, include_time=include_time)
        if formatted is not None:
            return formatted
    # Also try to format any value that looks like an ISO date string
    if isinstance(value, str) and 'T' in value and value.strip()[4:5] == '-':
        formatted = _try_format_date(value, include_time=include_time)
        if formatted is not None:
            return formatted
    return str(value)


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
    ('totalLoss', 'Total Loss'),
    ('recoveryAmount', 'Recoveries Amount'),
    ('netLoss', 'Net Loss'),
    ('financialImpactName', 'Financial Impact'),
    ('currencyName', 'Currency'),
    ('exchangeRate', 'Exchange Rate'),
    ('recoveryStatus', 'Recovery Status'),
    ('eventType', 'Event Type'),
    ('preparerStatus', 'Incident Status'),
    ('reviewerStatus', 'Review'),
    ('checkerStatus', 'First Approval'),
    ('acceptanceStatus', 'Second Approval'),
    ('createdAt', 'Created At'),
]

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
    data_keys = [k for k in first_row.keys() if str(k).lower() != 'id' and k != 'functionName']
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


__all__ = ['get_default_header_config', 'merge_header_config', 'format_cell_value_for_export', 'DATE_KEYS', 'DATETIME_KEYS', 'INCIDENT_COLUMNS_UI', 'INCIDENT_COLUMNS_PDF', 'get_incident_ordered_keys', 'get_incident_ordered_keys_pdf', 'get_incident_ordered_keys_full_ui', 'get_incident_label', 'get_incident_cell_value']
