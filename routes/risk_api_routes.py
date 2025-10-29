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

from services import APIService, PDFService, ExcelService, ControlService, IncidentService, RiskService
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
risk_service = RiskService() if RiskService else None
incident_service = IncidentService() if IncidentService else None
dashboard_activity_service = DashboardActivityService() if DashboardActivityService else None
bank_check_service = BankCheckService()
enhanced_bank_check_service = EnhancedBankCheckService()

# db_service now points to risk_service for risk-related database calls
db_service = risk_service or control_service

# Create router
router = APIRouter()





@router.get("/api/grc/risks/export-pdf")
async def export_risks_pdf(
    startDate: str = Query(None),
    endDate: str = Query(None),
    headerConfig: str = Query(None),
    cardType: str = Query(None),
    onlyCard: bool = Query(False),
    onlyChart: bool = Query(False),
    chartType: str = Query(None),
    onlyOverallTable: bool = Query(False)
):
    """Export risks report in PDF format"""
    try:
        # Parse header config
        header_config = {}
        if headerConfig:
            try:
                header_config = json.loads(headerConfig)
            except json.JSONDecodeError as e:
                header_config = {}
        
        # Get default header config for risks
        default_config = get_default_header_config("risks")
        header_config = {**default_config, **header_config}
        
        # Normalize short card names to canonical keys
        if cardType in ['low', 'medium', 'high']:
            cardType = {'low': 'lowRisk', 'medium': 'mediumRisk', 'high': 'highRisk'}[cardType]
        
        # Get risks data
        risks_data = await api_service.get_risks_data(startDate, endDate)
        
        # Resolve a usable "all risks" list from multiple possible keys
        all_risks: list = []
        for key in ['allRisks', 'risks', 'list', 'items', 'data']:
            value = risks_data.get(key)
            if isinstance(value, list) and (len(value) == 0 or isinstance(value[0], dict)):
                all_risks = value
                break
        
        # DB fallback if Node data missing
        if not all_risks and db_service:
            db_rows = await db_service.get_risks_data(startDate, endDate)
            all_risks = [r.to_dict() for r in db_rows]
            risks_data['allRisks'] = all_risks
        
        # For card-specific exports, filter data from resolved list
        if onlyCard and cardType:
            if cardType in ['highRisk', 'mediumRisk', 'lowRisk']:
                def normalize_level(val):
                    if val is None:
                        return None
                    s = str(val).strip().lower()
                    # numeric mapping: 1–3=low, 4=medium, 5+=high
                    if s.isdigit():
                        n = int(s)
                        if n >= 5:
                            return 'high'
                        if n >= 4:
                            return 'medium'
                        return 'low'
                    if 'high' in s:
                        return 'high'
                    if 'medium' in s:
                        return 'medium'
                    if 'low' in s:
                        return 'low'
                    return None

                filtered_risks = []
                for risk in all_risks:
                    level = normalize_level(
                        risk.get('inherent_value')
                        or risk.get('inherent')
                        or risk.get('inherentLevel')
                        or risk.get('inherent_rating')
                    )
                    if (cardType == 'highRisk' and level == 'high') or \
                       (cardType == 'mediumRisk' and level == 'medium') or \
                       (cardType == 'lowRisk' and level == 'low'):
                        filtered_risks.append(risk)
                
                # If still empty, try Node card endpoint
                if not filtered_risks:
                    fetched = await api_service.get_risks_card_data(cardType, startDate, endDate)
                    if isinstance(fetched, list) and fetched:
                        filtered_risks = fetched
                
                risks_data[cardType] = filtered_risks
        
        # Generate PDF
        pdf_content = await pdf_service.generate_risks_pdf(
            risks_data, startDate, endDate, header_config, cardType, onlyCard
        )
        
        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if onlyCard and cardType:
            filename = f"risks_{cardType}_{timestamp}.pdf"
        else:
            filename = f"risks_report_{timestamp}.pdf"
        
        return Response(
            content=pdf_content,
            media_type='application/pdf',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

@router.get("/api/grc/risks/export-excel")
async def export_risks_excel(
    startDate: str = Query(None),
    endDate: str = Query(None),
    headerConfig: str = Query(None),
    cardType: str = Query(None),
    onlyCard: bool = Query(False),
    onlyChart: bool = Query(False),
    chartType: str = Query(None),
    onlyOverallTable: bool = Query(False)
):
    """Export risks report in Excel format"""
    try:
        # Parse header config
        header_config = {}
        if headerConfig:
            try:
                header_config = json.loads(headerConfig)
            except json.JSONDecodeError as e:
                header_config = {}
        
        # Get default header config for risks
        default_config = get_default_header_config("risks")
        header_config = {**default_config, **header_config}

        # Normalize short card names to canonical keys
        if cardType in ['low', 'medium', 'high']:
            cardType = {'low': 'lowRisk', 'medium': 'mediumRisk', 'high': 'highRisk'}[cardType]
        
        # Get risks data
        risks_data = await api_service.get_risks_data(startDate, endDate)
        
        # Resolve a usable "all risks" list from multiple possible keys
        all_risks: list = []
        for key in ['allRisks', 'risks', 'list', 'items', 'data']:
            value = risks_data.get(key)
            if isinstance(value, list) and (len(value) == 0 or isinstance(value[0], dict)):
                all_risks = value
                break
        
        # DB fallback if Node data missing
        if not all_risks and db_service:
            db_rows = await db_service.get_risks_data(startDate, endDate)
            all_risks = [r.to_dict() for r in db_rows]
            risks_data['allRisks'] = all_risks
        
        # For card-specific exports, filter data from resolved list
        if onlyCard and cardType:
            if cardType in ['highRisk', 'mediumRisk', 'lowRisk']:
                def normalize_level(val):
                    if val is None:
                        return None
                    s = str(val).strip().lower()
                    if s.isdigit():
                        n = int(s)
                        if n >= 5:
                            return 'high'
                        if n >= 4:
                            return 'medium'
                        return 'low'
                    if 'high' in s:
                        return 'high'
                    if 'medium' in s:
                        return 'medium'
                    if 'low' in s:
                        return 'low'
                    return None

                filtered_risks = []
                for risk in all_risks:
                    level = normalize_level(
                        risk.get('inherent_value')
                        or risk.get('inherent')
                        or risk.get('inherentLevel')
                        or risk.get('inherent_rating')
                    )
                    if (cardType == 'highRisk' and level == 'high') or \
                       (cardType == 'mediumRisk' and level == 'medium') or \
                       (cardType == 'lowRisk' and level == 'low'):
                        filtered_risks.append(risk)

                # If still empty, try Node card endpoint
                if not filtered_risks:
                    fetched = await api_service.get_risks_card_data(cardType, startDate, endDate)
                    if isinstance(fetched, list) and fetched:
                        filtered_risks = fetched

                risks_data[cardType] = filtered_risks
        
        # Generate Excel
        excel_content = await excel_service.generate_risks_excel(
            risks_data, startDate, endDate, header_config, cardType, onlyCard
        )
        
        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if onlyCard and cardType:
            filename = f"risks_{cardType}_{timestamp}.xlsx"
        else:
            filename = f"risks_report_{timestamp}.xlsx"
        
        return Response(
            content=excel_content,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")



    """Save scheduled report configuration"""
    try:
        body = await request.json()
        report_config = body.get('reportConfig', {})
        schedule = body.get('schedule', {})
        
        # Save to database (you can create a scheduled_reports table)
        import pyodbc
        from config import get_database_connection_string
        
        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        try:
            # Create table if not exists
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='scheduled_reports' and xtype='U')
                CREATE TABLE scheduled_reports (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    report_config NVARCHAR(MAX) NOT NULL,
                    schedule_config NVARCHAR(MAX) NOT NULL,
                    is_active BIT DEFAULT 1,
                    created_at DATETIME2 DEFAULT GETDATE()
                );
            """)
            conn.commit()
            
            # Insert schedule
            import json
            cursor.execute("""
                INSERT INTO scheduled_reports (report_config, schedule_config)
                VALUES (?, ?)
            """, json.dumps(report_config), json.dumps(schedule))
            conn.commit()
            
            return {"success": True, "message": "Schedule saved successfully"}
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save schedule: {str(e)}")


