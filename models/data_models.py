"""
Data models for the reporting system
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime

@dataclass
class RiskData:
    """Risk data model"""
    code: str
    title: str
    inherent_value: str
    created_at: str
    risk_name: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'code': self.code,
            'title': self.title,
            'inherent_value': self.inherent_value,
            'created_at': self.created_at,
            'risk_name': self.risk_name or self.title
        }

@dataclass
class ControlData:
    """Control data model"""
    control_code: str
    control_name: str
    department_name: Optional[str] = None
    status: Optional[str] = None
    created_at: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'control_code': self.control_code,
            'control_name': self.control_name,
            'department_name': self.department_name,
            'status': self.status,
            'created_at': self.created_at
        }

@dataclass
class ChartData:
    """Chart data model"""
    name: str
    value: int
    color: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'value': self.value,
            'color': self.color
        }

@dataclass
class DashboardData:
    """Dashboard data model"""
    cards: List[Dict[str, Any]]
    charts: List[Dict[str, Any]]
    tables: List[Dict[str, Any]]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'cards': self.cards,
            'charts': self.charts,
            'tables': self.tables
        }

@dataclass
class ExportRequest:
    """Export request model"""
    format: str  # 'pdf' or 'excel'
    card_type: Optional[str] = None
    only_card: bool = False
    only_chart: bool = False
    chart_type: Optional[str] = None
    only_overall_table: bool = False
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    header_config: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'format': self.format,
            'card_type': self.card_type,
            'only_card': self.only_card,
            'only_chart': self.only_chart,
            'chart_type': self.chart_type,
            'only_overall_table': self.only_overall_table,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'header_config': self.header_config
        }

@dataclass
class ExportResponse:
    """Export response model"""
    success: bool
    content: Optional[bytes] = None
    filename: Optional[str] = None
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'success': self.success,
            'filename': self.filename,
            'error_message': self.error_message
        }
