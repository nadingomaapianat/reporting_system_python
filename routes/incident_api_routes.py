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

from services import APIService, PDFService, ExcelService, IncidentService
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
incident_service = IncidentService() if IncidentService else None
dashboard_activity_service = DashboardActivityService() if DashboardActivityService else None
bank_check_service = BankCheckService()
enhanced_bank_check_service = EnhancedBankCheckService()

# db_service points to incident_service for incident-related database calls
db_service = incident_service 

# Create router
router = APIRouter()


# Incidents: PDF export


@router.get("/api/grc/incidents/export-pdf")
async def export_incidents_pdf(
    request: Request,
    startDate: str = Query(None),
    endDate: str = Query(None),
    headerConfig: str = Query(None),
    cardType: str = Query(None),
    onlyCard: str = Query("False"),
    onlyChart: str = Query("False"),
    chartType: str = Query(None),
    onlyOverallTable: str = Query("False"),
    tableType: str = Query(None)
):  
    """Export incidents report in PDF format (service-backed like controls)."""
    try:
        write_debug(f"[INCIDENTS PDF] startDate={startDate} endDate={endDate}")
        write_debug(f"[INCIDENTS PDF] cardType={cardType} onlyCard={onlyCard} onlyChart={onlyChart}")
        write_debug(f"[INCIDENTS PDF] chartType={chartType} onlyOverallTable={onlyOverallTable} tableType={tableType}")

        # Parse and merge header configuration
        header_config = parse_header_config(headerConfig)
        # Allow chartType as separate query param
        renderType = request.query_params.get('renderType')
        if renderType:
            try:
                header_config["chartType"] = renderType
            except Exception:
                header_config = {"chartType": renderType}
        elif chartType:
            try:
                header_config["chartType"] = chartType
            except Exception:
                header_config = {"chartType": chartType}
        header_config = merge_header_config("incidents", header_config)
        
        # Normalize parameters
        if chartType and not cardType:
            cardType = chartType
        if onlyOverallTable and tableType:
            cardType = tableType

        # Normalize booleans
        try:
            only_card_bool = convert_to_boolean(onlyCard)
        except Exception:
            only_card_bool = str(onlyCard).lower() == 'true'
        
        try:
            only_chart_bool = convert_to_boolean(onlyChart)
        except Exception:
            only_chart_bool = str(onlyChart).lower() == 'true'
        
        try:
            only_overall_table_bool = convert_to_boolean(onlyOverallTable)
        except Exception:
            only_overall_table_bool = str(onlyOverallTable).lower() == 'true'

        # Require cardType
        if not cardType:
            raise HTTPException(status_code=400, detail="cardType or chartType is required for exports")

        write_debug(f"[INCIDENTS PDF] normalized cardType={cardType}")

        # Fetch data via incident_service
        if not incident_service:
            raise HTTPException(status_code=500, detail="Incident service not available")

        data = None
        # Metrics
        if cardType == 'totalIncidents':
            data = await incident_service.get_incidents_list(startDate, endDate)
        elif cardType == 'pendingPreparer':
            write_debug(f"[INCIDENTS PDF] fetching pending preparer incidents for {startDate} to {endDate}")
            data = await incident_service.get_incidents_by_status('pendingPreparer', startDate, endDate)
        elif cardType == 'pendingChecker':
            data = await incident_service.get_incidents_by_status('pendingChecker', startDate, endDate)
        elif cardType == 'pendingReviewer':
            data = await incident_service.get_incidents_by_status('pendingReviewer', startDate, endDate)
        elif cardType == 'pendingAcceptance':
            data = await incident_service.get_incidents_by_status('pendingAcceptance', startDate, endDate)
            
      
        # Charts
        elif cardType == 'byCategory':
            data = await incident_service.get_incidents_by_category(startDate, endDate)
        
        elif cardType == 'byStatus':
            data = await incident_service.get_incidents_by_status_distribution(startDate, endDate)
        
        elif cardType == 'monthlyTrend':
            data = await incident_service.get_incidents_monthly_trend(startDate, endDate)
       
        elif cardType == 'incidentsTimeSeries':
            data = await incident_service.get_incidents_time_series(startDate, endDate)
        
        elif cardType == 'topFinancialImpacts':
            data = await incident_service.get_incidents_top_financial_impacts(startDate, endDate)
       
       
        elif cardType == 'incidentsByEventType':
            data = await incident_service.get_incidents_by_event_type(startDate, endDate)
       
       
        elif cardType == 'incidentsByFinancialImpact':
            data = await incident_service.get_incidents_by_financial_impact(startDate, endDate)
        
        elif cardType == 'netLossAndRecovery':
            data = await incident_service.get_incidents_net_loss_recovery(startDate, endDate)
       
        #tables
        elif cardType == 'overallStatuses':
            data = await incident_service.get_incidents_status_overview(startDate, endDate)
        elif cardType == 'incidentsFinancialDetails':
            data = await incident_service.get_incidents_financial_details(startDate, endDate)
        elif cardType == 'incidentsWithTimeframe':
            data = await incident_service.get_incidents_with_timeframe(startDate, endDate)
        elif cardType == 'incidentsWithFinancialAndFunction':
            data = await incident_service.get_incidents_with_financial_and_function(startDate, endDate)
          
        """
        elif cardType == 'createdDeletedIncidentsPerQuarter':
            data = await incident_service.get_created_deleted_incidents_per_quarter(startDate, endDate)
        elif cardType == 'quarterlyIncidentCreationTrends':
            data = await incident_service.get_quarterly_incident_creation_trends(startDate, endDate)
        elif cardType == 'incidentApprovalStatusDistribution':
            data = await incident_service.get_incident_approval_status_distribution(startDate, endDate)
        
        # Tables
        elif cardType == 'incidentsPerDepartment':
            data = await incident_service.get_incidents_per_department(startDate, endDate)
        elif cardType == 'incidentsPerBusinessProcess':
            data = await incident_service.get_incidents_per_business_process(startDate, endDate)
        elif cardType == 'inherentResidualIncidentComparison':
            data = await incident_service.get_inherent_residual_incident_comparison(startDate, endDate)
        elif cardType == 'highResidualIncidentOverview':
            data = await incident_service.get_high_residual_incident_overview(startDate, endDate)
        elif cardType == 'incidentsAndControlsCount':
            data = await incident_service.get_incidents_and_controls_count(startDate, endDate)
        elif cardType == 'controlsAndIncidentCount':
            data = await incident_service.get_controls_and_incident_count(startDate, endDate)
        elif cardType == 'allIncidents':
            data = await incident_service.get_incidents_details(startDate, endDate)
        """

        incidents_data = {cardType: data}
        write_debug(f"incidents_data: {incidents_data}")
        try:
            data_len = len(data) if isinstance(data, list) else (len(data.keys()) if isinstance(data, dict) else 1)
            write_debug(f"[INCIDENTS PDF] data_type={type(data).__name__} data_len={data_len}")
        except Exception:
            pass
        
        # Generate PDF
        try:
            pdf_content = await pdf_service.generate_incidents_pdf(  # ← FIXED: Proper indentation
                incidents_data, startDate, endDate, header_config, cardType, only_card_bool, only_overall_table_bool, only_chart_bool
        )
        except Exception as gen_err:
            write_debug(f"[INCIDENTS PDF] generate_incidents_pdf error: {gen_err}")
            raise
        
        # Filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"incidents_{cardType}_{timestamp}.pdf"  # ← FIXED: Proper indentation
        
        return Response(
            content=pdf_content,
            media_type='application/pdf',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        write_debug(f"[INCIDENTS PDF] Export failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

@router.get("/api/grc/incidents/export-excel")
async def export_incidents_excel(
    request: Request,
    startDate: str = Query(None),
    endDate: str = Query(None),
    headerConfig: str = Query(None),
    cardType: str = Query(None),
    onlyCard: str = Query("False"),
    onlyChart: str = Query("False"),
    chartType: str = Query(None),
    onlyOverallTable: str = Query("False"),
    tableType: str = Query(None)
):
    """Export incidents report in Excel format (service-backed like controls)."""
   
    try:
        write_debug(f"Exporting incidents report in Excel format for {startDate} to {endDate}")
        write_debug(f"cardType: {cardType}")
        write_debug(f"onlyCard: {onlyCard}")
        write_debug(f"onlyChart: {onlyChart}")
        write_debug(f"chartType: {chartType}")
        write_debug(f"onlyOverallTable: {onlyOverallTable}")
        write_debug(f"tableType: {tableType}")

        # Parse and merge header configuration
        header_config = parse_header_config(headerConfig)
        # Allow chartType as separate query param
        renderType = request.query_params.get('renderType')
        if renderType:
            try:
                header_config["chartType"] = renderType
            except Exception:
                header_config = {"chartType": renderType}
        elif chartType:
            try:
                header_config["chartType"] = chartType
            except Exception:
                header_config = {"chartType": chartType}
        header_config = merge_header_config("incidents", header_config)

        # Normalize parameters
        if chartType and not cardType:
            cardType = chartType
        if onlyOverallTable and tableType:
            cardType = tableType

        # Normalize booleans
        try:
            only_card_bool = convert_to_boolean(onlyCard)
        except Exception:
            only_card_bool = str(onlyCard).lower() == 'true'
        
        try:
            only_chart_bool = convert_to_boolean(onlyChart)
        except Exception:
            only_chart_bool = str(onlyChart).lower() == 'true'
        
        try:
            only_overall_table_bool = convert_to_boolean(onlyOverallTable)
        except Exception:
            only_overall_table_bool = str(onlyOverallTable).lower() == 'true'

        if not cardType:
            raise HTTPException(status_code=400, detail="cardType or chartType is required for exports")

        
        if not incident_service:
            raise HTTPException(status_code=500, detail="Incident service not available")

        data = None
        
        if cardType == 'totalIncidents':
            data = await incident_service.get_incidents_list(startDate, endDate)
        elif cardType == 'pendingPreparer':
            write_debug(f"[INCIDENTS PDF] fetching pending preparer incidents for {startDate} to {endDate}")
            data = await incident_service.get_incidents_by_status('pendingPreparer', startDate, endDate)
        elif cardType == 'pendingChecker':
            data = await incident_service.get_incidents_by_status('pendingChecker', startDate, endDate)
        elif cardType == 'pendingReviewer':
            data = await incident_service.get_incidents_by_status('pendingReviewer', startDate, endDate)
        elif cardType == 'pendingAcceptance':
            data = await incident_service.get_incidents_by_status('pendingAcceptance', startDate, endDate)
            
      
        # Charts
        elif cardType == 'byCategory':
            data = await incident_service.get_incidents_by_category(startDate, endDate)
        elif cardType == 'byStatus':
            data = await incident_service.get_incidents_by_status_distribution(startDate, endDate)
        elif cardType == 'monthlyTrend':
            data = await incident_service.get_incidents_monthly_trend(startDate, endDate)
        elif cardType == 'incidentsTimeSeries':
            data = await incident_service.get_incidents_time_series(startDate, endDate)
        elif cardType == 'topFinancialImpacts':
            data = await incident_service.get_incidents_top_financial_impacts(startDate, endDate)
        
        elif cardType == 'incidentsByEventType':
            data = await incident_service.get_incidents_by_event_type(startDate, endDate)
        elif cardType == 'incidentsByFinancialImpact':
            data = await incident_service.get_incidents_by_financial_impact(startDate, endDate)
        elif cardType == 'netLossAndRecovery':
            data = await incident_service.get_incidents_net_loss_recovery(startDate, endDate)
        # Pending buckets and totals/list
      
        #tables
        elif cardType == 'overallStatuses':
            data = await incident_service.get_incidents_status_overview(startDate, endDate)
        elif cardType == 'incidentsFinancialDetails':
            data = await incident_service.get_incidents_financial_details(startDate, endDate)
        elif cardType == 'incidentsWithTimeframe':
            data = await incident_service.get_incidents_with_timeframe(startDate, endDate)
        elif cardType == 'incidentsWithFinancialAndFunction':
            data = await incident_service.get_incidents_with_financial_and_function(startDate, endDate)

       
        elif cardType == 'incidentsReduced':
            data = await incident_service.get_incidents_reduced(startDate, endDate)
        elif cardType == 'newIncidents':
            write_debug(f"[INCIDENTS PDF] fetching new incidents for {startDate} to {endDate}")
            data = await incident_service.get_new_incidents(startDate, endDate)


        
        else:
            # Fallback to aggregated API data
            incidents_data = await api_service.get_incidents_data(startDate, endDate)
            data = incidents_data.get(cardType) or incidents_data.get('statusOverview') or []

        incidents_data_wrapped = {cardType or 'overallStatuses': data}

        excel_bytes = await excel_service.generate_incidents_excel(
            incidents_data=incidents_data_wrapped,
            start_date=startDate,
            end_date=endDate,
            header_config=header_config,
            card_type=cardType or 'overallStatuses',
            only_card=only_card_bool,
            only_overall_table=only_overall_table_bool,
            only_chart=only_chart_bool
        )

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"incidents_{(cardType or 'overallStatuses')}_{ts}.xlsx"
        return Response(content=excel_bytes, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers={'Content-Disposition': f'attachment; filename=\"{filename}\"'})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")


