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
risk_service = RiskService() if RiskService else None
incident_service = IncidentService() if IncidentService else None
dashboard_activity_service = DashboardActivityService() if DashboardActivityService else None
bank_check_service = BankCheckService()
enhanced_bank_check_service = EnhancedBankCheckService()

# db_service now points to risk_service for risk-related database calls
db_service = risk_service 

# Create router
router = APIRouter()


@router.get("/api/grc/risks/export-pdf")
async def export_risks_pdf(
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
    """Export risks report in PDF format (service-backed like controls)."""
    try:
        write_debug(f"[RISKS PDF] startDate={startDate} endDate={endDate}")
        write_debug(f"[RISKS PDF] cardType={cardType} onlyCard={onlyCard} onlyChart={onlyChart}")
        write_debug(f"[RISKS PDF] chartType={chartType} onlyOverallTable={onlyOverallTable} tableType={tableType}")

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
        header_config = merge_header_config("risks", header_config)
        
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

        write_debug(f"[RISKS PDF] normalized cardType={cardType}")

        # Fetch data via risk_service
        if not risk_service:
            raise HTTPException(status_code=500, detail="Risk service not available")

        data = None
        # Metrics
        if cardType == 'total':
            data = await risk_service.get_total_risks(startDate, endDate)
        elif cardType == 'high':
            data = await risk_service.get_high_risks(startDate, endDate)
        elif cardType == 'medium':
            write_debug(f"[RISKS PDF] fetching medium risks for {startDate} to {endDate}")
            data = await risk_service.get_medium_risks(startDate, endDate)
            write_debug(f"[RISKS PDF] medium risks data: {data}")
        elif cardType == 'low':
            data = await risk_service.get_low_risks(startDate, endDate)
        elif cardType == 'risksReduced':
            data = await risk_service.get_risks_reduced(startDate, endDate)
        elif cardType == 'newRisks':
            write_debug(f"[RISKS PDF] fetching new risks for {startDate} to {endDate}")
            data = await risk_service.get_new_risks(startDate, endDate)

        # Charts
        elif cardType == 'risksByCategory':
            data = await risk_service.get_risks_by_category(startDate, endDate)
        elif cardType == 'risksByEventType':
            data = await risk_service.get_risks_by_event_type_chart(startDate, endDate)
        elif cardType == 'createdDeletedRisksPerQuarter':
            data = await risk_service.get_created_deleted_risks_per_quarter(startDate, endDate)
        elif cardType == 'quarterlyRiskCreationTrends':
            data = await risk_service.get_quarterly_risk_creation_trends(startDate, endDate)
        elif cardType == 'riskApprovalStatusDistribution':
            data = await risk_service.get_risk_approval_status_distribution(startDate, endDate)
        elif cardType == 'riskDistributionByFinancialImpact':
            data = await risk_service.get_risk_distribution_by_financial_impact(startDate, endDate)
        # Tables
        elif cardType == 'risksPerDepartment':
            data = await risk_service.get_risks_per_department(startDate, endDate)
        elif cardType == 'risksPerBusinessProcess':
            data = await risk_service.get_risks_per_business_process(startDate, endDate)
        elif cardType == 'inherentResidualRiskComparison':
            data = await risk_service.get_inherent_residual_risk_comparison(startDate, endDate)
        elif cardType == 'highResidualRiskOverview':
            data = await risk_service.get_high_residual_risk_overview(startDate, endDate)
        elif cardType == 'risksAndControlsCount':
            data = await risk_service.get_risks_and_controls_count(startDate, endDate)
        elif cardType == 'controlsAndRiskCount':
            data = await risk_service.get_controls_and_risk_count(startDate, endDate)
        elif cardType == 'allRisks':
            data = await risk_service.get_risks_details(startDate, endDate)

        risks_data = {cardType: data}
        write_debug(f"risks_data: {risks_data}")
        try:
            data_len = len(data) if isinstance(data, list) else (len(data.keys()) if isinstance(data, dict) else 1)
            write_debug(f"[RISKS PDF] data_type={type(data).__name__} data_len={data_len}")
        except Exception:
            pass
        
        # Generate PDF
        try:
            pdf_content = await pdf_service.generate_risks_pdf(  # ← FIXED: Proper indentation
                risks_data, startDate, endDate, header_config, cardType, only_card_bool, only_overall_table_bool, only_chart_bool
        )
        except Exception as gen_err:
            write_debug(f"[RISKS PDF] generate_risks_pdf error: {gen_err}")
            raise
        
        # Filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"risks_{cardType}_{timestamp}.pdf"  # ← FIXED: Proper indentation
        
        return Response(
            content=pdf_content,
            media_type='application/pdf',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        write_debug(f"[RISKS PDF] Export failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

@router.get("/api/grc/risks/export-excel")
async def export_risks_excel(
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
    """Export risks report in Excel format (service-backed like controls)."""
   
    try:
        write_debug(f"Exporting risks report in Excel format for {startDate} to {endDate}")
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
        header_config = merge_header_config("risks", header_config)

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

        
        if not risk_service:
            raise HTTPException(status_code=500, detail="Risk service not available")

        data = None
        # Metrics (counts) or lists when onlyCard is requested
        if cardType == 'total':
            data = await risk_service.get_total_risks(startDate, endDate)
        elif cardType == 'high':
            data = await risk_service.get_high_risks(startDate, endDate)
        elif cardType == 'medium':
            data = await risk_service.get_medium_risks(startDate, endDate)
        elif cardType == 'low':
            data = await risk_service.get_low_risks(startDate, endDate)
        elif cardType == 'risksReduced':
            data = await risk_service.get_risks_reduced(startDate, endDate)
        elif cardType == 'newRisks':
            data = await risk_service.get_new_risks(startDate, endDate)
        # Charts
        elif cardType == 'risksByCategory':
            data = await risk_service.get_risks_by_category(startDate, endDate)
        elif cardType == 'risksByEventType':
            data = await risk_service.get_risks_by_event_type_chart(startDate, endDate)
        elif cardType == 'createdDeletedRisksPerQuarter':
            data = await risk_service.get_created_deleted_risks_per_quarter(startDate, endDate)
        elif cardType == 'quarterlyRiskCreationTrends':
            data = await risk_service.get_quarterly_risk_creation_trends(startDate, endDate)
        elif cardType == 'riskApprovalStatusDistribution':
            data = await risk_service.get_risk_approval_status_distribution(startDate, endDate)
        elif cardType == 'riskDistributionByFinancialImpact':
            data = await risk_service.get_risk_distribution_by_financial_impact(startDate, endDate)
        # Tables
        elif cardType == 'risksPerDepartment':
            data = await risk_service.get_risks_per_department(startDate, endDate)
        elif cardType == 'risksPerBusinessProcess':
            data = await risk_service.get_risks_per_business_process(startDate, endDate)
        elif cardType == 'inherentResidualRiskComparison':
            data = await risk_service.get_inherent_residual_risk_comparison(startDate, endDate)
        elif cardType == 'highResidualRiskOverview':
            data = await risk_service.get_high_residual_risk_overview(startDate, endDate)
        elif cardType == 'risksAndControlsCount':
            data = await risk_service.get_risks_and_controls_count(startDate, endDate)
        elif cardType == 'controlsAndRiskCount':
            data = await risk_service.get_controls_and_risk_count(startDate, endDate)
        elif cardType == 'allRisks':
            data = await risk_service.get_risks_details(startDate, endDate)

        risks_data = {cardType: data}
        write_debug(f"risks_data: {risks_data}")
        
        # Generate Excel
        excel_content = await excel_service.generate_risks_excel(
            risks_data, startDate, endDate, header_config, cardType, only_card_bool, only_overall_table_bool, only_chart_bool
        )
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"risks_{cardType}_{timestamp}.xlsx"
        
        return Response(
            content=excel_content,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")