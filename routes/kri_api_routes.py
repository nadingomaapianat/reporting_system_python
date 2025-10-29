"""
API routes for the reporting system
"""
import asyncio
import json
from datetime import datetime
import os
import httpx
from fastapi import APIRouter, Query, HTTPException, Request, UploadFile, File, Form
from fastapi.responses import Response, FileResponse, StreamingResponse
from typing import Optional

from services import APIService, PDFService, ExcelService, ControlService, IncidentService, KRIService
from services.bank_check_service import BankCheckService
from services.enhanced_bank_check_service import EnhancedBankCheckService
# Dashboard activity service moved to Node.js backend (NestJS)
DashboardActivityService = None  # type: ignore
from utils.export_utils import get_default_header_config
from models import ExportRequest, ExportResponse
from routes.route_utils import write_debug, parse_header_config, merge_header_config, convert_to_boolean

# Initialize services
api_service = APIService()
pdf_service = PDFService()
excel_service = ExcelService()
control_service = ControlService()
kri_service = KRIService() if KRIService else None
incident_service = IncidentService() if IncidentService else None
dashboard_activity_service = DashboardActivityService() if DashboardActivityService else None
bank_check_service = BankCheckService()
enhanced_bank_check_service = EnhancedBankCheckService()

# db_service points to kri_service for KRI-related database calls
db_service = kri_service or control_service

# Create router
router = APIRouter()





# KRI Export Endpoints
@router.get("/api/grc/kris")
async def get_kris_dashboard(
    startDate: str = Query(None),
    endDate: str = Query(None)
):
    """Return KRIs dashboard data"""
    try:
        data = await api_service.get_kris_data(startDate, endDate)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch KRIs data: {str(e)}")

@router.get("/api/grc/kris/export/pdf")
async def export_kris_pdf(
    onlyCard: str = Query(None),
    onlyChart: str = Query(None),
    onlyOverallTable: str = Query(None),
    startDate: str = Query(None),
    endDate: str = Query(None),
    headerConfig: str = Query(None),
    chartType: str = Query(None)
):
    """Export KRIs dashboard to PDF"""
    try:
        # Convert string parameters to boolean
        only_card = onlyCard and onlyCard.lower() in ['true', '1', 'yes']
        only_chart = onlyChart and onlyChart.lower() in ['true', '1', 'yes']
        only_overall_table = onlyOverallTable and onlyOverallTable.lower() in ['true', '1', 'yes']
        
        # Normalize onlyChart parameter for KRI cards
        if only_chart and chartType:
            if chartType in ['krisByStatus', 'krisByLevel', 'breachedKRIsByDepartment', 'kriAssessmentCount']:
                only_chart = True
            else:
                only_chart = False
        
        # Get header configuration
        config = get_default_header_config("kris")
        if headerConfig:
            try:
                user_config = json.loads(headerConfig)
                config.update(user_config)
            except:
                pass
        
        # Get data
        data = await api_service.get_kris_data(startDate, endDate)
        
        # Generate PDF
        pdf_buffer = await pdf_service.generate_kris_pdf(
            data=data,
            only_card=only_card,
            only_chart=only_chart,
            only_overall_table=only_overall_table,
            card_type=chartType,
            start_date=startDate,
            end_date=endDate,
            header_config=config
        )
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"kris_{timestamp}.pdf"
        
        return Response(
            content=pdf_buffer,
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate PDF: {str(e)}")

@router.get("/api/grc/kris/export/excel")
async def export_kris_excel(
    onlyCard: str = Query(None),
    onlyChart: str = Query(None),
    onlyOverallTable: str = Query(None),
    startDate: str = Query(None),
    endDate: str = Query(None),
    headerConfig: str = Query(None),
    chartType: str = Query(None)
):
    """Export KRIs dashboard to Excel"""
    try:
        # Convert string parameters to boolean
        only_card = onlyCard and onlyCard.lower() in ['true', '1', 'yes']
        only_chart = onlyChart and onlyChart.lower() in ['true', '1', 'yes']
        only_overall_table = onlyOverallTable and onlyOverallTable.lower() in ['true', '1', 'yes']
        
        # Get header configuration
        config = get_default_header_config("kris")
        if headerConfig:
            try:
                user_config = json.loads(headerConfig)
                config.update(user_config)
            except:
                pass
        
        # Get data
        data = await api_service.get_kris_data(startDate, endDate)
        
        # Generate Excel
        excel_buffer = await excel_service.generate_kris_excel(
            kris_data=data,
            only_card=only_card,
            only_chart=only_chart,
            only_overall_table=only_overall_table,
            card_type=chartType,
            start_date=startDate,
            end_date=endDate,
            header_config=config
        )
        
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"kris_{timestamp}.xlsx"
        
        return Response(
            content=excel_buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate Excel: {str(e)}")


