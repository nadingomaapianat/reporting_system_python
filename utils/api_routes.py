"""
API routes for the reporting system
"""
import json
from datetime import datetime
import os
import httpx
from fastapi import APIRouter, Query, HTTPException, Request, UploadFile, File, Form
from fastapi.responses import Response, FileResponse, StreamingResponse
from typing import Optional

from services import APIService, PDFService, ExcelService, DatabaseService
from services.bank_check_service import BankCheckService
from services.enhanced_bank_check_service import EnhancedBankCheckService
try:
    from services.dashboard_activity_service import DashboardActivityService
except Exception:
    DashboardActivityService = None  # type: ignore
from export_utils import get_default_header_config
from models import ExportRequest, ExportResponse

# Initialize services
api_service = APIService()
pdf_service = PDFService()
excel_service = ExcelService()
db_service = DatabaseService() if DatabaseService else None
dashboard_activity_service = DashboardActivityService() if DashboardActivityService else None
bank_check_service = BankCheckService()
enhanced_bank_check_service = EnhancedBankCheckService()

# Create router
router = APIRouter()
@router.get("/api/grc/incidents")
async def get_incidents_dashboard(
    startDate: str = Query(None),
    endDate: str = Query(None)
):
    """Return incidents dashboard data including statusOverview (details list)"""
    try:
        data = await api_service.get_incidents_data(startDate, endDate)
        # Ensure aggregates exist; if missing, fill from SQL
        if not db_service:
            raise HTTPException(status_code=503, detail="Database service unavailable")
        if not data.get('categoryDistribution'):
            data['categoryDistribution'] = await db_service.get_incidents_by_category(startDate, endDate)
        if not data.get('incidentsByCategory') and data.get('categoryDistribution'):
            data['incidentsByCategory'] = data['categoryDistribution']
        if not data.get('statusDistribution'):
            data['statusDistribution'] = await db_service.get_incidents_by_status(startDate, endDate)
        if not data.get('incidentsByStatus') and data.get('statusDistribution'):
            data['incidentsByStatus'] = data['statusDistribution']
        if not data.get('monthlyTrend'):
            data['monthlyTrend'] = await db_service.get_incidents_monthly_trend(startDate, endDate)

        # Always include statusOverview list for table (like controls)
        status_rows = await db_service.get_incidents_list(startDate, endDate)
        data['statusOverview'] = status_rows
        data['overallStatuses'] = status_rows

        # Derive totals if missing
        if 'totalIncidents' not in data:
            data['totalIncidents'] = len(status_rows)

        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load incidents dashboard: {str(e)}")
# Incidents list (JSON) for frontend table fallback
@router.get("/api/grc/incidents/list")
async def incidents_list(
    startDate: str = Query(None),
    endDate: str = Query(None),
    page: int = Query(1),
    limit: int = Query(1000)
):
    try:
        rows = await db_service.get_incidents_list(startDate, endDate)
        # Simple pagination (frontend expects large page anyway)
        total = len(rows)
        start = max(0, (page - 1) * limit)
        end = start + limit
        return {
            "data": rows[start:end],
            "pagination": {
                "page": page,
                "limit": limit,
                "total": total,
                "totalPages": (total + limit - 1) // limit,
                "hasNext": end < total,
                "hasPrev": start > 0
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load incidents list: {str(e)}")
# Incidents: PDF export
@router.get("/api/grc/incidents/export-pdf")
async def export_incidents_pdf(
    startDate: str = Query(None),
    endDate: str = Query(None),
    headerConfig: str = Query(None),
    cardType: str = Query(None),
    onlyCard: str = Query("False"),
    onlyChart: str = Query("False"),
    chartType: str = Query(None),
    onlyOverallTable: str = Query("False")
):
    try:
        # Convert string parameters to boolean
        onlyCard = onlyCard.lower() in ['true', '1', 'yes']
        onlyChart = onlyChart.lower() in ['true', '1', 'yes']
        onlyOverallTable = onlyOverallTable.lower() in ['true', '1', 'yes']
        
        header_config = {}
        if headerConfig:
            try:
                header_config = json.loads(headerConfig)
            except json.JSONDecodeError:
                header_config = {}
        default_config = get_default_header_config("incidents")
        header_config = {**default_config, **header_config}

        # Normalize chart/table params to cardType
        if onlyChart and chartType in ['byCategory', 'byStatus', 'monthlyTrend', 'netLossAndRecovery', 'topFinancialImpacts']:
            cardType = chartType
            onlyCard = True
        if onlyOverallTable:
            cardType = 'overallStatuses'
            onlyCard = True

        incidents_data = await api_service.get_incidents_data(startDate, endDate)

        # Card-specific fallbacks
        if onlyCard and cardType:
            # totals/list
            if cardType in ['totalIncidents']:
                card_rows = await api_service.get_incidents_card_data('totalIncidents', startDate, endDate)
                if not card_rows:
                    card_rows = await db_service.get_incidents_list(startDate, endDate)
                # reuse risks naming: allRisks style -> use 'list'
                incidents_data['list'] = card_rows
            elif cardType == 'overallStatuses':
                if not incidents_data.get('list'):
                    incidents_data['list'] = await db_service.get_incidents_list(startDate, endDate)
            elif cardType == 'byCategory':
                dist = incidents_data.get('categoryDistribution') or []
                if not dist:
                    incidents_data['categoryDistribution'] = await db_service.get_incidents_by_category(startDate, endDate)
            elif cardType == 'byStatus':
                dist = incidents_data.get('statusDistribution') or []
                if not dist:
                    incidents_data['statusDistribution'] = await db_service.get_incidents_by_status(startDate, endDate)
            elif cardType == 'monthlyTrend':
                trend = incidents_data.get('monthlyTrend') or []
                if not trend:
                    incidents_data['monthlyTrend'] = await db_service.get_incidents_monthly_trend(startDate, endDate)
            elif cardType == 'topFinancialImpacts':
                impacts = incidents_data.get('topFinancialImpacts') or []
                if not impacts:
                    incidents_data['topFinancialImpacts'] = await db_service.get_incidents_top_financial_impacts(startDate, endDate)
            elif cardType == 'netLossAndRecovery':
                net_loss = incidents_data.get('netLossAndRecovery') or []
                if not net_loss:
                    incidents_data['netLossAndRecovery'] = await db_service.get_incidents_net_loss_recovery(startDate, endDate)

        from services import PDFService
        pdf_service_local = PDFService()
        
        # Debug logging
        print(f"DEBUG: onlyCard={onlyCard} (type: {type(onlyCard)})")
        print(f"DEBUG: cardType={cardType} (type: {type(cardType)})")
        print(f"DEBUG: onlyCard and cardType = {onlyCard and cardType}")
        
        pdf_bytes = await pdf_service_local.generate_incidents_pdf(
            incidents_data=incidents_data, 
            start_date=startDate, 
            end_date=endDate, 
            header_config=header_config, 
            card_type=cardType, 
            only_card=onlyCard
        )

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"incidents_{cardType+'_' if onlyCard and cardType else ''}{ts}.pdf"
        return Response(content=pdf_bytes, media_type='application/pdf', headers={'Content-Disposition': f'attachment; filename="{filename}"'})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

# Incidents: Excel export
@router.get("/api/grc/incidents/export-excel")
async def export_incidents_excel(
    startDate: str = Query(None),
    endDate: str = Query(None),
    headerConfig: str = Query(None),
    cardType: str = Query(None),
    onlyCard: str = Query("False"),
    onlyChart: str = Query("False"),
    chartType: str = Query(None),
    onlyOverallTable: str = Query("False")
):
    try:
        # Convert string parameters to boolean
        onlyCard = onlyCard.lower() in ['true', '1', 'yes']
        onlyChart = onlyChart.lower() in ['true', '1', 'yes']
        onlyOverallTable = onlyOverallTable.lower() in ['true', '1', 'yes']
        
        header_config = {}
        if headerConfig:
            try:
                header_config = json.loads(headerConfig)
            except json.JSONDecodeError:
                header_config = {}
        default_config = get_default_header_config("incidents")
        header_config = {**default_config, **header_config}

        # Normalize chart/table params to cardType
        if onlyChart and chartType in ['byCategory', 'byStatus', 'monthlyTrend', 'netLossAndRecovery', 'topFinancialImpacts']:
            cardType = chartType
            onlyCard = True
        if onlyOverallTable:
            cardType = 'overallStatuses'
            onlyCard = True

        incidents_data = await api_service.get_incidents_data(startDate, endDate)

        # Card-specific fallbacks
        if onlyCard and cardType:
            if cardType in ['totalIncidents']:
                card_rows = await api_service.get_incidents_card_data('totalIncidents', startDate, endDate)
                if not card_rows:
                    card_rows = await db_service.get_incidents_list(startDate, endDate)
                incidents_data['list'] = card_rows
            elif cardType == 'overallStatuses':
                if not incidents_data.get('list'):
                    incidents_data['list'] = await db_service.get_incidents_list(startDate, endDate)
            elif cardType == 'byCategory':
                dist = incidents_data.get('categoryDistribution') or []
                if not dist:
                    incidents_data['categoryDistribution'] = await db_service.get_incidents_by_category(startDate, endDate)
            elif cardType == 'byStatus':
                dist = incidents_data.get('statusDistribution') or []
                if not dist:
                    incidents_data['statusDistribution'] = await db_service.get_incidents_by_status(startDate, endDate)
            elif cardType == 'monthlyTrend':
                trend = incidents_data.get('monthlyTrend') or []
                if not trend:
                    incidents_data['monthlyTrend'] = await db_service.get_incidents_monthly_trend(startDate, endDate)
            elif cardType == 'topFinancialImpacts':
                impacts = incidents_data.get('topFinancialImpacts') or []
                if not impacts:
                    incidents_data['topFinancialImpacts'] = await db_service.get_incidents_top_financial_impacts(startDate, endDate)
            elif cardType == 'netLossAndRecovery':
                net_loss = incidents_data.get('netLossAndRecovery') or []
                if not net_loss:
                    incidents_data['netLossAndRecovery'] = await db_service.get_incidents_net_loss_recovery(startDate, endDate)

        from services import ExcelService
        excel_service_local = ExcelService()
        excel_bytes = await excel_service_local.generate_incidents_excel(
            incidents_data=incidents_data, 
            start_date=startDate, 
            end_date=endDate, 
            header_config=header_config, 
            card_type=cardType, 
            only_card=onlyCard
        )

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"incidents_{cardType+'_' if onlyCard and cardType else ''}{ts}.xlsx"
        return Response(content=excel_bytes, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers={'Content-Disposition': f'attachment; filename="{filename}"'})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

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
        
        # Get risks data
        risks_data = await api_service.get_risks_data(startDate, endDate)
        
        # For card-specific exports, filter data from main risks data
        if onlyCard and cardType:
            if cardType in ['highRisk', 'mediumRisk', 'lowRisk']:
                # Filter risks by inherent_value (case-insensitive)
                all_risks = risks_data.get('allRisks', [])
                if cardType == 'highRisk':
                    filtered_risks = [risk for risk in all_risks if risk.get('inherent_value', '').lower() == 'high']
                elif cardType == 'mediumRisk':
                    filtered_risks = [risk for risk in all_risks if risk.get('inherent_value', '').lower() == 'medium']
                elif cardType == 'lowRisk':
                    filtered_risks = [risk for risk in all_risks if risk.get('inherent_value', '').lower() == 'low']
                else:
                    filtered_risks = []
                
                risks_data[f'{cardType}'] = filtered_risks
        
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
        
        # Get risks data
        risks_data = await api_service.get_risks_data(startDate, endDate)
        
        # For card-specific exports, filter data from main risks data
        if onlyCard and cardType:
            if cardType in ['highRisk', 'mediumRisk', 'lowRisk']:
                # Filter risks by inherent_value (case-insensitive)
                all_risks = risks_data.get('allRisks', [])
                if cardType == 'highRisk':
                    filtered_risks = [risk for risk in all_risks if risk.get('inherent_value', '').lower() == 'high']
                elif cardType == 'mediumRisk':
                    filtered_risks = [risk for risk in all_risks if risk.get('inherent_value', '').lower() == 'medium']
                elif cardType == 'lowRisk':
                    filtered_risks = [risk for risk in all_risks if risk.get('inherent_value', '').lower() == 'low']
                else:
                    filtered_risks = []
                
                risks_data[f'{cardType}'] = filtered_risks
        
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

@router.get("/api/grc/controls/export-pdf")
async def export_controls_pdf(
    startDate: str = Query(None),
    endDate: str = Query(None),
    headerConfig: str = Query(None),
    cardType: str = Query(None),
    onlyCard: bool = Query(False),
    onlyChart: bool = Query(False),
    chartType: str = Query(None),
    onlyOverallTable: bool = Query(False)
):
    """Export controls report in PDF format"""
    try:
        # Parse header config
        header_config = {}
        if headerConfig:
            try:
                header_config = json.loads(headerConfig)
            except json.JSONDecodeError as e:
                header_config = {}
        
        # Get default header config for controls
        default_config = get_default_header_config("controls")
        header_config = {**default_config, **header_config}
        
        # Normalize table/chart params → card params to support onlyChart/onlyOverallTable
        if onlyChart and chartType in ['department', 'risk']:
            cardType = chartType
            onlyCard = True
        if onlyOverallTable:
            cardType = 'overallStatuses'
            onlyCard = True

        # Get controls data
        controls_data = await api_service.get_controls_data(startDate, endDate)
        
        # Ensure specific data exists for card-only routes (and add SQL fallbacks)
        if onlyCard and cardType:
            if cardType in ['totalControls', 'unmappedControls', 'pendingPreparer', 'pendingChecker', 'pendingReviewer', 'pendingAcceptance']:
                card_data = await api_service.get_controls_card_data(cardType, startDate, endDate)
                if not card_data:
                    if cardType == 'unmappedControls':
                        card_data = await db_service.get_unmapped_controls(startDate, endDate)
                    elif cardType == 'pendingPreparer':
                        card_data = await db_service.get_pending_controls('preparer', startDate, endDate)
                    elif cardType == 'pendingChecker':
                        card_data = await db_service.get_pending_controls('checker', startDate, endDate)
                    elif cardType == 'pendingReviewer':
                        card_data = await db_service.get_pending_controls('reviewer', startDate, endDate)
                    elif cardType == 'pendingAcceptance':
                        card_data = await db_service.get_pending_controls('acceptance', startDate, endDate)
                    elif cardType == 'totalControls':
                        all_controls = await db_service.execute_query("""
                            SELECT c.code as control_code, c.name as control_name
                            FROM dbo.[Controls] c WHERE c.isDeleted = 0 ORDER BY c.name
                        """)
                        card_data = all_controls
                controls_data[f'{cardType}'] = card_data
            elif cardType == 'overallStatuses':
                # Prefer API aggregate if present
                if not controls_data.get('statusOverview'):
                    # Fallback to DB
                    rows = await db_service.execute_query(
                        """
                        SELECT 
                            c.code as code,
                            c.name as name,
                            c.preparerStatus,
                            c.checkerStatus,
                            c.reviewerStatus,
                            c.acceptanceStatus
                        FROM dbo.[Controls] c
                        WHERE c.isDeleted = 0
                        ORDER BY c.name
                        """
                    )
                    controls_data['statusOverview'] = rows

    # Generate PDF
        pdf_content = await pdf_service.generate_controls_pdf(
            controls_data, startDate, endDate, header_config, cardType, onlyCard
        )
        
        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if onlyCard and cardType:
            filename = f"controls_{cardType}_{timestamp}.pdf"
        else:
            filename = f"controls_report_{timestamp}.pdf"
        
        return Response(
            content=pdf_content,
            media_type='application/pdf',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

@router.get("/api/grc/controls/export-excel")
async def export_controls_excel(
    startDate: str = Query(None),
    endDate: str = Query(None),
    headerConfig: str = Query(None),
    cardType: str = Query(None),
    onlyCard: bool = Query(False),
    onlyChart: bool = Query(False),
    chartType: str = Query(None),
    onlyOverallTable: bool = Query(False)
):
    """Export controls report in Excel format"""
    try:
        # Parse header config
        header_config = {}
        if headerConfig:
            try:
                header_config = json.loads(headerConfig)
            except json.JSONDecodeError as e:
                header_config = {}
        
        # Get default header config for controls
        default_config = get_default_header_config("controls")
        header_config = {**default_config, **header_config}
        
        # Normalize table/chart params → card params for backend handling
        # This lets URLs with onlyChart=true&chartType=... or onlyOverallTable work
        if onlyChart and chartType in ['department', 'risk']:
            cardType = chartType
            onlyCard = True
        if onlyOverallTable:
            cardType = 'overallStatuses'
            onlyCard = True

        # Get controls data
        controls_data = await api_service.get_controls_data(startDate, endDate)
        
        # For card-specific exports, fetch specific card data (large page size)
        if onlyCard and cardType:
            if cardType in ['totalControls', 'unmappedControls', 'pendingPreparer', 'pendingChecker', 'pendingReviewer', 'pendingAcceptance']:
                # Try Node API first
                card_data = await api_service.get_controls_card_data(cardType, startDate, endDate)
                # If empty, fallback to direct SQL for reliability
                if not card_data:
                    if cardType == 'unmappedControls':
                        card_data = await db_service.get_unmapped_controls(startDate, endDate)
                    elif cardType == 'pendingPreparer':
                        card_data = await db_service.get_pending_controls('preparer', startDate, endDate)
                    elif cardType == 'pendingChecker':
                        card_data = await db_service.get_pending_controls('checker', startDate, endDate)
                    elif cardType == 'pendingReviewer':
                        card_data = await db_service.get_pending_controls('reviewer', startDate, endDate)
                    elif cardType == 'pendingAcceptance':
                        card_data = await db_service.get_pending_controls('acceptance', startDate, endDate)
                    elif cardType == 'totalControls':
                        # For total, return all controls with code/name minimal set
                        all_controls = await db_service.execute_query("""
                            SELECT c.code as control_code, c.name as control_name
                            FROM dbo.[Controls] c WHERE c.isDeleted = 0 ORDER BY c.name
                        """)
                        card_data = all_controls
                controls_data[f'{cardType}'] = card_data
            elif cardType == 'overallStatuses':
                if not controls_data.get('statusOverview'):
                    rows = await db_service.execute_query(
                        """
                        SELECT 
                            c.code as code,
                            c.name as name,
                            c.preparerStatus,
                            c.checkerStatus,
                            c.reviewerStatus,
                            c.acceptanceStatus
                        FROM dbo.[Controls] c
                        WHERE c.isDeleted = 0
                        ORDER BY c.name
                        """
                    )
                    controls_data['statusOverview'] = rows
        
        # Ensure chart data is present for chart-only exports
        if onlyCard and cardType in ['department', 'risk']:
            # department chart expects departmentDistribution: [{name, value}]
            if cardType == 'department':
                dist = controls_data.get('departmentDistribution', []) or []
                if not dist:
                    rows = await db_service.get_controls_by_department(startDate, endDate)
                    # rows fields: department_name, control_count
                    controls_data['departmentDistribution'] = [
                        { 'name': (r.get('department_name') or r.get('name') or 'Unknown'), 'value': r.get('control_count', 0) }
                        for r in rows
                    ]
            # risk response chart expects statusDistribution: [{name, value}]
            if cardType == 'risk':
                dist = controls_data.get('statusDistribution', []) or []
                if not dist:
                    rows = await db_service.get_controls_by_risk_response(startDate, endDate)
                    # rows fields: risk_response_type, control_count
                    controls_data['statusDistribution'] = [
                        { 'name': (r.get('risk_response_type') or r.get('name') or 'Unknown'), 'value': r.get('control_count', 0) }
                        for r in rows
                    ]

        # Generate Excel
        excel_content = await excel_service.generate_controls_excel(
            controls_data, startDate, endDate, header_config, cardType, onlyCard
        )
        
        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if onlyCard and cardType:
            filename = f"controls_{cardType}_{timestamp}.xlsx"
        else:
            filename = f"controls_report_{timestamp}.xlsx"
        
        return Response(
            content=excel_content,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

# Dashboard Activity Endpoints
@router.get("/api/dashboard-activity")
async def get_dashboard_activities(user_id: str = Query("default_user")):
    """Get dashboard activity data from database"""
    try:
        # Initialize table if it doesn't exist
        await dashboard_activity_service.create_activity_table()
        
        # Initialize default activities if none exist
        await dashboard_activity_service.initialize_default_activities()
        
        # Get activities from database
        activities = await dashboard_activity_service.get_dashboard_activities(user_id)
        
        return activities
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch dashboard activities: {str(e)}")

@router.post("/api/dashboard-activity")
async def update_dashboard_activity(request: Request):
    """Update dashboard activity in database"""
    try:
        # Parse JSON body
        body = await request.json()
        
        # Extract parameters from request body
        dashboard_id = body.get('dashboard_id')
        user_id = body.get('user_id', 'default_user')
        card_count = body.get('card_count', 0)
        
        if not dashboard_id:
            raise HTTPException(status_code=400, detail="dashboard_id is required")
        
        # Initialize table if it doesn't exist
        await dashboard_activity_service.create_activity_table()
        
        # Update activity
        activity = await dashboard_activity_service.update_dashboard_activity(
            dashboard_id, user_id, card_count
        )
        
        return activity
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update dashboard activity: {str(e)}")

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

# -------------------------------
# Export logging (Excel/PDF) APIs
# -------------------------------
@router.post("/api/exports/log")
async def log_report_export(request: Request):
    """Log an export (excel/pdf/word/zip) with title and src for later download listing."""
    try:
        import pyodbc
        from config import get_database_connection_string

        body = await request.json()
        title = (body.get("title") or "").strip() or "Untitled Report"
        src = (body.get("src") or "").strip()
        fmt = (body.get("format") or "").strip().lower() or "unknown"
        dashboard = (body.get("dashboard") or "").strip() or "general"

        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        try:
            # Ensure table exists
            cursor.execute(
                """
                IF NOT EXISTS (
                  SELECT * FROM INFORMATION_SCHEMA.TABLES 
                  WHERE TABLE_NAME = 'report_exports' AND TABLE_SCHEMA='dbo'
                )
                BEGIN
                  CREATE TABLE dbo.report_exports (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    title NVARCHAR(255) NOT NULL,
                    src NVARCHAR(1024) NULL,
                    format NVARCHAR(20) NOT NULL,
                    dashboard NVARCHAR(100) NULL,
                    created_at DATETIME2 DEFAULT GETDATE()
                  )
                END
                """
            )
            conn.commit()

            cursor.execute(
                """
                INSERT INTO dbo.report_exports (title, src, format, dashboard)
                VALUES (?, ?, ?, ?)
                """,
                (title, src, fmt, dashboard)
            )
            conn.commit()

            new_id = cursor.execute("SELECT @@IDENTITY").fetchone()[0]
            return {"success": True, "id": int(new_id)}
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        return {"success": False, "error": str(e)}

@router.get("/api/exports/recent")
async def list_recent_exports(limit: int = Query(50)):
    """Return recent report exports (newest first)."""
    try:
        import pyodbc
        from config import get_database_connection_string

        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                IF NOT EXISTS (
                  SELECT * FROM INFORMATION_SCHEMA.TABLES 
                  WHERE TABLE_NAME = 'report_exports' AND TABLE_SCHEMA='dbo'
                )
                BEGIN
                  CREATE TABLE dbo.report_exports (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    title NVARCHAR(255) NOT NULL,
                    src NVARCHAR(1024) NULL,
                    format NVARCHAR(20) NOT NULL,
                    dashboard NVARCHAR(100) NULL,
                    created_at DATETIME2 DEFAULT GETDATE()
                  )
                END
                """
            )
            conn.commit()

            cursor.execute(
                """
                SELECT TOP (?) id, title, src, format, dashboard, created_at
                FROM dbo.report_exports
                ORDER BY created_at DESC, id DESC
                """,
                (limit,)
            )
            rows = cursor.fetchall()
            exports = [
                {
                    "id": int(r[0]),
                    "title": r[1],
                    "src": r[2],
                    "format": r[3],
                    "dashboard": r[4],
                    "created_at": r[5].isoformat() if hasattr(r[5], 'isoformat') else str(r[5])
                }
                for r in rows
            ]
            return {"success": True, "exports": exports}
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        return {"success": False, "error": str(e), "exports": []}

@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

# Bank Check Processing: upload PDF + form and return generated files
@router.post("/api/reports/bank-check")
async def create_bank_check_report(
    request: Request,
    file: UploadFile = File(...),
    bankName: str = Form(None),
    cost: str = Form(None),
    date: str = Form(None),
    daysRemaining: str = Form(None),
    format: str = Form("excel")
):
    try:
        content = await file.read()
        excel_bytes, word_bytes = bank_check_service.process(content, {
            "bankName": bankName,
            "cost": cost,
            "date": date,
            "daysRemaining": daysRemaining,
        })

        # allow query param override: ?format=excel|word|zip
        fmt = request.query_params.get('format') or (format or 'excel')
        fmt = (fmt or 'excel').lower()

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        if fmt == 'excel':
            filename = f"bank_check_{ts}.xlsx"
            return Response(
                content=excel_bytes,
                media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                headers={'Content-Disposition': f'attachment; filename="{filename}"'}
            )
        elif fmt == 'word':
            filename = f"bank_check_{ts}.docx"
            return Response(
                content=word_bytes,
                media_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                headers={'Content-Disposition': f'attachment; filename="{filename}"'}
            )
        else:
            # package into a simple zip in-memory
            import zipfile
            from io import BytesIO
            buffer = BytesIO()
            with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(f"bank_check_{ts}.xlsx", excel_bytes)
                zf.writestr(f"bank_check_{ts}.docx", word_bytes)
            buffer.seek(0)
            filename = f"bank_check_{ts}.zip"
            return Response(content=buffer.getvalue(), media_type='application/zip', headers={'Content-Disposition': f'attachment; filename="{filename}"'})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create bank check report: {str(e)}")

# Debug: return OCR text and parsed fields without generating files
@router.post("/api/reports/bank-check/debug")
async def debug_bank_check(
    file: UploadFile = File(...)
):
    try:
        content = await file.read()
        webhook_url = os.getenv('OCR_WEBHOOK_URL')
        if webhook_url:
            async with httpx.AsyncClient(timeout=60) as client:
                files = {
                    'file': (file.filename or 'upload.bin', content, file.content_type or 'application/octet-stream')
                }
                resp = await client.post(webhook_url, files=files)
                if resp.status_code == 200:
                    data = resp.json()
                    # Best-effort mapping for UI
                    mapped_fields = {
                        'bankName': data.get('bankName') or data.get('bank') or data.get('bank_name') or '',
                        'branch': data.get('branch') or '',
                        'currency': data.get('currency') or data.get('ccy') or '',
                        'amountNumeric': data.get('amountNumeric') or data.get('amount') or data.get('amount_number') or '',
                        'amountWords': data.get('amountWords') or data.get('amount_text') or '',
                        'date': data.get('date') or data.get('checkDate') or '',
                        'payee': data.get('payee') or data.get('to') or data.get('beneficiary') or '',
                    }
                    text_value = data.get('text') or data.get('textSnippet') or ''
                    return {
                        'textSnippet': text_value[:1000] if text_value else '',
                        'textLength': len(text_value or ''),
                        'fields': mapped_fields,
                        'diagnostics': {
                            'source': 'webhook',
                            'webhookUrl': webhook_url,
                            'status': resp.status_code
                        }
                    }
                else:
                    raise HTTPException(status_code=502, detail=f"OCR webhook failed with status {resp.status_code}")

        # Fallback: local OCR if webhook not configured
        full_text = bank_check_service.extract_text(content)
        fields = bank_check_service.extract_fields_from_pdf(content)
        diag = bank_check_service.diagnose_extraction(content)
        return {
            "textSnippet": (full_text[:1000] if full_text else ""),
            "textLength": len(full_text or ""),
            "fields": fields,
            "diagnostics": diag
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Debug extraction failed: {str(e)}")

# Enhanced Bank Check Processing with professional reports
@router.post("/api/reports/enhanced-bank-check")
async def create_enhanced_bank_check_report(
    request: Request
):
    """Process bank check records and generate professional Excel/Word reports"""
    try:
        # Check if it's a file upload or JSON data
        content_type = request.headers.get('content-type', '')
        
        if 'multipart/form-data' in content_type:
            # Handle file upload
            form = await request.form()
            file = form.get('file')
            if not file:
                raise HTTPException(status_code=400, detail="No file provided")
            
            content = await file.read()
            excel_bytes, word_bytes, data = await enhanced_bank_check_service.process_check(content, file.filename or 'check.pdf')
            
            # Create ZIP with both files
            import zipfile
            from io import BytesIO
            buffer = BytesIO()
            with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                zf.writestr(f"bank_check_analysis_{ts}.xlsx", excel_bytes)
                zf.writestr(f"bank_check_report_{ts}.docx", word_bytes)
            buffer.seek(0)
            
            filename = f"bank_check_reports_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            return Response(content=buffer.getvalue(), media_type='application/zip', headers={'Content-Disposition': f'attachment; filename="{filename}"'})
        
        else:
            # Handle JSON data with records
            body = await request.json()
            records = body.get('records', [])
            format_type = body.get('format', 'both')
            
            if not records:
                raise HTTPException(status_code=400, detail="No records provided")
            
            if format_type == 'word':
                # Return only Word document
                _, word_bytes = await enhanced_bank_check_service.process_records(records)
                filename = f"bank_check_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.docx"
                return Response(content=word_bytes, media_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document', headers={'Content-Disposition': f'attachment; filename="{filename}"'})
            else:
                # Return only Excel document
                excel_bytes, _ = await enhanced_bank_check_service.process_records(records)
                filename = f"bank_check_records_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                return Response(content=excel_bytes, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers={'Content-Disposition': f'attachment; filename="{filename}"'})
                
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Enhanced bank check processing failed: {str(e)}")

@router.post("/api/reports/enhanced-bank-check/preview")
async def preview_enhanced_bank_check(
    file: UploadFile = File(...)
):
    """Preview extracted data from bank check without generating files"""
    try:
        content = await file.read()
        data = await enhanced_bank_check_service.extract_check_data(content, file.filename or 'check.pdf')
        return {
            "success": True,
            "data": data,
            "extraction_summary": {
                "total_fields": 10,
                "extracted_fields": sum(1 for v in data.values() if v and v != ''),
                "missing_fields": sum(1 for v in data.values() if not v or v == ''),
                "success_rate": f"{(sum(1 for v in data.values() if v and v != '') / 10 * 100):.1f}%"
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Preview failed: {str(e)}")

@router.post("/api/reports/enhanced-bank-check/extract-headers")
async def extract_excel_headers(
    file: UploadFile = File(...)
):
    """Extract headers from Excel template file"""
    try:
        content = await file.read()
        headers = await enhanced_bank_check_service.extract_excel_headers(content, file.filename or 'template.xlsx')
        return {
            "success": True,
            "headers": headers,
            "count": len(headers)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Header extraction failed: {str(e)}")

@router.post("/api/reports/enhanced-bank-check/insert")
async def insert_check_record(
    request: Request
):
    """Insert check record into database"""
    try:
        import pyodbc
        from config import get_database_connection_string
        
        body = await request.json()
        record = body.get('record', {})
        table_name = body.get('table_name', 'bank_checks')
        
        if not record:
            return {
                "success": False,
                "error": "No record data provided"
            }
        
        # Use existing SQL Server connection
        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        try:
            # Get table columns to validate the record
            columns_query = """
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = ? AND COLUMN_NAME != 'id'
                ORDER BY ORDINAL_POSITION
            """
            cursor.execute(columns_query, table_name)
            columns = cursor.fetchall()
            
            if not columns:
                return {
                    "success": False,
                    "error": f"Table '{table_name}' not found"
                }
            
            # Prepare insert statement
            column_names = [col[0] for col in columns]
            placeholders = ', '.join(['?' for _ in column_names])
            column_list = ', '.join([f'[{col}]' for col in column_names])
            
            insert_query = f"""
                INSERT INTO [{table_name}] ({column_list})
                VALUES ({placeholders})
            """
            
            # Prepare values in the correct order
            values = []
            for col_name in column_names:
                value = record.get(col_name, None)
                if value is None and any(col[0] == col_name and col[2] == 'NO' for col in columns):
                    # Required field is missing
                    return {
                        "success": False,
                        "error": f"Required field '{col_name}' is missing"
                    }
                values.append(value)
            
            # Execute insert
            cursor.execute(insert_query, values)
            conn.commit()
            
            # Get the inserted record ID
            record_id = cursor.execute("SELECT @@IDENTITY").fetchone()[0]
            
            return {
                "success": True,
                "message": f"Record inserted successfully into {table_name}",
                "record_id": record_id,
                "table_name": table_name
            }
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"[EnhancedBankCheck] Insert error: {e}")
        return {
            "success": False,
            "error": f"Database insertion failed: {str(e)}"
        }

@router.get("/api/reports/enhanced-bank-check/tables")
async def get_database_tables():
    """Get all tables from SQL Server database"""
    try:
        import pyodbc
        from config import get_database_connection_string
        
        # Use existing SQL Server connection
        connection_string = get_database_connection_string()
        print(f"[EnhancedBankCheck] Connecting to SQL Server with: {connection_string[:50]}...")
        
        # Connect to SQL Server database
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        try:
            # Get all tables from the database
            tables_query = """
                SELECT 
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    TABLE_TYPE
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME;
            """
            
            cursor.execute(tables_query)
            tables_result = cursor.fetchall()
            
            tables = []
            for table in tables_result:  # Get ALL tables from database
                table_name = table[1]  # TABLE_NAME
                schema_name = table[0]  # TABLE_SCHEMA
                full_table_name = f"{schema_name}.{table_name}" if schema_name != 'dbo' else table_name
                
                # Get columns for each table
                columns_query = """
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        IS_NULLABLE,
                        COLUMN_DEFAULT
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                    ORDER BY ORDINAL_POSITION;
                """
                
                cursor.execute(columns_query, schema_name, table_name)
                columns_result = cursor.fetchall()
                column_names = [col[0] for col in columns_result]  # COLUMN_NAME
                
                tables.append({
                    "id": f"{schema_name}_{table_name}",
                    "name": table_name,
                    "schema": schema_name,
                    "full_name": full_table_name,
                    "fields": column_names,
                    "field_count": len(column_names)
                })
            
            print(f"[EnhancedBankCheck] Found {len(tables)} tables in SQL Server database")
            
            return {
                "success": True,
                "tables": tables,
                "count": len(tables)
            }
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"[EnhancedBankCheck] SQL Server connection error: {e}")
        # Fallback to sample data if database connection fails
        sample_tables = [
            {
                "id": "dbo_bank_checks",
                "name": "bank_checks",
                "schema": "dbo",
                "full_name": "bank_checks",
                "fields": ["id", "bank_name", "date", "payee_name", "amount_value", "amount_text", "currency", "status_note", "issuer_signature", "created_at"],
                "field_count": 10
            },
            {
                "id": "dbo_customer_records", 
                "name": "customer_records",
                "schema": "dbo",
                "full_name": "customer_records",
                "fields": ["id", "customer_name", "project_name", "building_number", "apartment_number", "check_number", "due_date", "collection_date", "remaining_days", "collection_status", "total_receivables", "created_at"],
                "field_count": 12
            }
        ]
        
        return {
            "success": True,
            "tables": sample_tables,
            "count": len(sample_tables),
            "note": "Using fallback data - SQL Server connection failed",
            "error": str(e)
        }

@router.get("/api/reports/enhanced-bank-check/test-db")
async def test_database_connection():
    """Test SQL Server database connection"""
    try:
        import pyodbc
        from config import get_database_connection_string
        
        # Use existing SQL Server connection
        connection_string = get_database_connection_string()
        print(f"[DB Test] Attempting SQL Server connection...")
        
        # Test connection
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Test query
        cursor.execute("SELECT @@VERSION")
        result = cursor.fetchone()[0]
        
        # Get table count
        cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        table_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "message": "SQL Server connection successful",
            "version": result,
            "table_count": table_count,
            "database": "NEWDCC-V4-UAT"
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "database": "NEWDCC-V4-UAT"
        }

@router.post("/api/reports/enhanced-bank-check/check-table")
async def check_table_exists(request: Request):
    """Check if a table exists in the database"""
    try:
        import pyodbc
        from config import get_database_connection_string
        
        body = await request.json()
        table_name = body.get('table_name', '').strip()
        
        if not table_name:
            return {
                "success": False,
                "error": "Table name is required"
            }
        
        # Use existing SQL Server connection
        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        try:
            # Check if table exists
            check_query = """
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = ? AND TABLE_TYPE = 'BASE TABLE'
            """
            
            cursor.execute(check_query, table_name)
            exists = cursor.fetchone()[0] > 0
            
            if exists:
                # Get table info if it exists
                info_query = """
                    SELECT 
                        TABLE_SCHEMA,
                        TABLE_NAME,
                        (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
                         WHERE TABLE_SCHEMA = t.TABLE_SCHEMA AND TABLE_NAME = t.TABLE_NAME) as COLUMN_COUNT
                    FROM INFORMATION_SCHEMA.TABLES t
                    WHERE TABLE_NAME = ? AND TABLE_TYPE = 'BASE TABLE'
                """
                cursor.execute(info_query, table_name)
                table_info = cursor.fetchone()
                
                return {
                    "success": True,
                    "exists": True,
                    "table_name": table_name,
                    "schema": table_info[0] if table_info else 'dbo',
                    "column_count": table_info[2] if table_info else 0,
                    "message": f"Table '{table_name}' already exists"
                }
            else:
                return {
                    "success": True,
                    "exists": False,
                    "table_name": table_name,
                    "message": f"Table '{table_name}' does not exist - can be created"
                }
                
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

@router.post("/api/reports/enhanced-bank-check/create-table")
async def create_table(request: Request):
    """Create a new table in the database"""
    try:
        import pyodbc
        from config import get_database_connection_string
        
        body = await request.json()
        table_name = body.get('table_name', '').strip()
        fields = body.get('fields', [])
        record_data = body.get('record_data', {})
        
        if not table_name:
            return {
                "success": False,
                "error": "Table name is required"
            }
        
        if not fields:
            return {
                "success": False,
                "error": "At least one field is required"
            }
        
        # Use existing SQL Server connection
        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        try:
            # First check if table exists
            check_query = """
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = ? AND TABLE_TYPE = 'BASE TABLE'
            """
            
            cursor.execute(check_query, table_name)
            exists = cursor.fetchone()[0] > 0
            
            if exists:
                return {
                    "success": False,
                    "error": f"Table '{table_name}' already exists",
                    "exists": True
                }
            
            # Create table with fields
            # Add ID column as primary key
            columns = ["id INT IDENTITY(1,1) PRIMARY KEY"]
            
            for field in fields:
                field_name = field.get('name', '').strip()
                field_type = field.get('type', 'NVARCHAR(255)')
                is_required = field.get('required', False)
                
                if field_name:
                    # Sanitize field name (remove spaces, special chars)
                    clean_name = ''.join(c for c in field_name if c.isalnum() or c in '_')
                    if not clean_name:
                        clean_name = f"field_{len(columns)}"
                    
                    null_constraint = "NOT NULL" if is_required else "NULL"
                    columns.append(f"[{clean_name}] {field_type} {null_constraint}")
            
            # Add created_at timestamp
            columns.append("[created_at] DATETIME2 DEFAULT GETDATE()")
            
            create_query = f"""
                CREATE TABLE [{table_name}] (
                    {', '.join(columns)}
                )
            """
            
            cursor.execute(create_query)
            conn.commit()
            
            # Insert record data if provided
            record_id = None
            if record_data:
                try:
                    # Get table columns to validate the record
                    columns_query = """
                        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_NAME = ? AND COLUMN_NAME != 'id'
                        ORDER BY ORDINAL_POSITION
                    """
                    cursor.execute(columns_query, table_name)
                    columns = cursor.fetchall()
                    
                    if columns:
                        # Prepare insert statement
                        column_names = [col[0] for col in columns]
                        placeholders = ', '.join(['?' for _ in column_names])
                        column_list = ', '.join([f'[{col}]' for col in column_names])
                        
                        insert_query = f"""
                            INSERT INTO [{table_name}] ({column_list})
                            VALUES ({placeholders})
                        """
                        
                        # Prepare values in the correct order
                        values = []
                        for col_name in column_names:
                            # Find matching field by name (case-insensitive)
                            field_name = None
                            for field in fields:
                                clean_field_name = ''.join(c for c in field.get('name', '') if c.isalnum() or c in '_')
                                if clean_field_name.lower() == col_name.lower():
                                    field_name = field.get('name', '')
                                    break
                            
                            if field_name and field_name in record_data:
                                value = record_data[field_name]
                            else:
                                value = None
                            
                            if value is None and any(col[0] == col_name and col[2] == 'NO' for col in columns):
                                # Required field is missing, skip insertion
                                print(f"[EnhancedBankCheck] Warning: Required field '{col_name}' is missing, skipping record insertion")
                                record_id = None
                                break
                            values.append(value)
                        
                        if values:  # Only insert if we have valid values
                            cursor.execute(insert_query, values)
                            conn.commit()
                            
                            # Get the inserted record ID
                            record_id = cursor.execute("SELECT @@IDENTITY").fetchone()[0]
                            print(f"[EnhancedBankCheck] Record inserted with ID: {record_id}")
                        
                except Exception as insert_error:
                    print(f"[EnhancedBankCheck] Error inserting record: {insert_error}")
                    # Don't fail the table creation if record insertion fails
                    pass
            
            return {
                "success": True,
                "message": f"Table '{table_name}' created successfully" + (f" and record inserted (ID: {record_id})" if record_id else ""),
                "table_name": table_name,
                "field_count": len(fields),
                "created_fields": [f.get('name', '') for f in fields],
                "record_id": record_id,
                "record_inserted": record_id is not None
            }
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
