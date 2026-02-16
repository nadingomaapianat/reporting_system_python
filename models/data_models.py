"""
Data models for the reporting system.
"""
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict


class ExportRequest(BaseModel):
    """Request model for export operations."""

    model_config = ConfigDict(extra="allow")

    dashboard_type: Optional[str] = None
    header_config: Optional[Dict[str, Any]] = None
    format: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None


class ExportResponse(BaseModel):
    """Response model for export operations."""

    model_config = ConfigDict(extra="allow")

    success: bool = True
    message: Optional[str] = None
    file_path: Optional[str] = None
    download_url: Optional[str] = None


class RiskData(BaseModel):
    """Model for risk-related data."""

    model_config = ConfigDict(extra="allow")


class ControlData(BaseModel):
    """Model for control-related data."""

    model_config = ConfigDict(extra="allow")


class ChartData(BaseModel):
    """Model for chart data."""

    model_config = ConfigDict(extra="allow")


class DashboardData(BaseModel):
    """Model for dashboard data."""

    model_config = ConfigDict(extra="allow")
