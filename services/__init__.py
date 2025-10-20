"""
Services package for the reporting system
"""
from .api_service import APIService
from .pdf_service import PDFService
from .excel_service import ExcelService

# Optional import: DatabaseService may require pyodbc / drivers
try:
    from .database_service import DatabaseService
except Exception:
    DatabaseService = None  # type: ignore

__all__ = [
    'DatabaseService',
    'APIService',
    'PDFService',
    'ExcelService'
]
