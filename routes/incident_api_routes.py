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

from services import APIService, PDFService, ExcelService, ControlService, IncidentService
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
incident_service = IncidentService() if IncidentService else None
dashboard_activity_service = DashboardActivityService() if DashboardActivityService else None
bank_check_service = BankCheckService()
enhanced_bank_check_service = EnhancedBankCheckService()

# db_service points to incident_service for incident-related database calls
db_service = incident_service or control_service

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
        if not incident_service:
            raise HTTPException(status_code=503, detail="Database service unavailable")
        if not data.get('categoryDistribution'):
            data['categoryDistribution'] = await incident_service.get_incidents_by_category(startDate, endDate)
        if not data.get('incidentsByCategory') and data.get('categoryDistribution'):
            data['incidentsByCategory'] = data['categoryDistribution']
        if not data.get('statusDistribution'):
            data['statusDistribution'] = await incident_service.get_incidents_by_status(startDate, endDate)
        if not data.get('incidentsByStatus') and data.get('statusDistribution'):
            data['incidentsByStatus'] = data['statusDistribution']
        if not data.get('monthlyTrend'):
            data['monthlyTrend'] = await incident_service.get_incidents_monthly_trend(startDate, endDate)

        # Always include statusOverview list for table (like controls)
        status_rows = await incident_service.get_incidents_list(startDate, endDate)
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
        rows = await incident_service.get_incidents_list(startDate, endDate)
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

        incidents_data = await incident_service.get_incidents_data(startDate, endDate)

        # Card-specific fallbacks
        if onlyCard and cardType:
            # totals/list
            if cardType in ['totalIncidents']:
                card_rows = await api_service.get_incidents_card_data('totalIncidents', startDate, endDate)
                if not card_rows:
                    card_rows = await incident_service.get_incidents_list(startDate, endDate)
                # reuse risks naming: allRisks style -> use 'list'
                incidents_data['list'] = card_rows
            elif cardType == 'overallStatuses':
                if not incidents_data.get('list'):
                    incidents_data['list'] = await incident_service.get_incidents_list(startDate, endDate)
            elif cardType == 'byCategory':
                dist = incidents_data.get('categoryDistribution') or []
                if not dist:
                    incidents_data['categoryDistribution'] = await incident_service.get_incidents_by_category(startDate, endDate)
            elif cardType == 'byStatus':
                dist = incidents_data.get('statusDistribution') or []
                if not dist:
                    incidents_data['statusDistribution'] = await incident_service.get_incidents_by_status(startDate, endDate)
            elif cardType == 'monthlyTrend':
                trend = incidents_data.get('monthlyTrend') or []
                if not trend:
                    incidents_data['monthlyTrend'] = await incident_service.get_incidents_monthly_trend(startDate, endDate)
            elif cardType == 'topFinancialImpacts':
                impacts = incidents_data.get('topFinancialImpacts') or []
                if not impacts:
                    incidents_data['topFinancialImpacts'] = await incident_service.get_incidents_top_financial_impacts(startDate, endDate)
            elif cardType == 'netLossAndRecovery':
                net_loss = incidents_data.get('netLossAndRecovery') or []
                if not net_loss:
                    incidents_data['netLossAndRecovery'] = await incident_service.get_incidents_net_loss_recovery(startDate, endDate)

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


        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


