"""
Services package for the reporting system
"""
from .api_service import APIService
from .pdf_service import PDFService
from .excel_service import ExcelService

# Optional imports: Services may require pyodbc / drivers
try:
    from .control_service import ControlService
except Exception:
    ControlService = None  # type: ignore

try:
    from .incident_service import IncidentService
except Exception:
    IncidentService = None  # type: ignore

try:
    from .kri_service import KriService as KRIService
except Exception:
    KRIService = None  # type: ignore

try:
    from .risk_service import RiskService
except Exception:
    RiskService = None  # type: ignore

__all__ = [
    'ControlService',
    'IncidentService',
    'KRIService',
    'RiskService',
    'APIService',
    'PDFService',
    'ExcelService'
]
