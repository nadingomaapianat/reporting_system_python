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
from routes.route_utils import write_debug, parse_header_config, merge_header_config, convert_to_boolean, save_and_log_export, extract_user_and_function_params
from utils.order_by_function import apply_order_by_function_deep, order_by_function_from_request
from utils.node_grc_query import grc_iso_date_param

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
    tableType: str = Query(None),
    functionId: str = Query(None),
    functionIds: Optional[str] = Query(
        None,
        description="Comma-separated function IDs (multi-select); mirrors Node dashboard filters",
    ),
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
        start_date_q = grc_iso_date_param(startDate)
        end_date_q = grc_iso_date_param(endDate)
        write_debug(f"[RISKS PDF] dates for SQL: start={start_date_q!r} end={end_date_q!r} (query raw: {startDate!r} / {endDate!r})")

        # Extract user and function parameters
        user_id, group_name, function_id = extract_user_and_function_params(request)
        if functionId:
            # Clean functionId using the same logic as extract_user_and_function_params
            from routes.route_utils import clean_function_id
            function_id = clean_function_id(functionId)
        function_ids = functionIds
        write_debug(f"[RISKS PDF] user_id={user_id}, group_name={group_name}, function_id={function_id}, functionIds={functionIds!r}")

        # Fetch data via risk_service
        if not risk_service:
            raise HTTPException(status_code=500, detail="Risk service not available")

        data = None
        # Metrics
        if cardType == 'total':
            data = await risk_service.get_total_risks(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'high':
            data = await risk_service.get_high_risks(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'medium':
            write_debug(f"[RISKS PDF] fetching medium risks for {start_date_q} to {end_date_q}")
            data = await risk_service.get_medium_risks(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
            write_debug(f"[RISKS PDF] medium risks data: {data}")
        elif cardType == 'low':
            data = await risk_service.get_low_risks(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType in ('reduction', 'risksReduced'):
            data = await risk_service.get_risks_reduced(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'newRisks':
            write_debug(f"[RISKS PDF] fetching new risks for {start_date_q} to {end_date_q}")
            data = await risk_service.get_new_risks(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)

        # Charts
        elif cardType == 'risksByCategory':
            data = await risk_service.get_risks_by_category(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'risksByEventType':
            data = await risk_service.get_risks_by_event_type_chart(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'createdDeletedRisksPerQuarter':
            data = await risk_service.get_created_deleted_risks_per_quarter(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'quarterlyRiskCreationTrends':
            data = await risk_service.get_quarterly_risk_creation_trends(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'riskApprovalStatusDistribution':
            data = await risk_service.get_risk_approval_status_distribution(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'riskDistributionByFinancialImpact':
            data = await risk_service.get_risk_distribution_by_financial_impact(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        # Tables
        elif cardType == 'risksPerDepartment':
            data = await risk_service.get_risks_per_department(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'risksPerBusinessProcess':
            data = await risk_service.get_risks_per_business_process(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'inherentResidualRiskComparison':
            data = await risk_service.get_inherent_residual_risk_comparison(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'highResidualRiskOverview':
            data = await risk_service.get_high_residual_risk_overview(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'risksAndControlsCount':
            data = await risk_service.get_risks_and_controls_count(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'controlsAndRiskCount':
            data = await risk_service.get_controls_and_risk_count(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'allRisks':
            data = await risk_service.get_risks_details(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)

        risks_data = {cardType: data}
        if order_by_function_from_request(request):
            risks_data = apply_order_by_function_deep(risks_data)
        write_debug(f"risks_data: {risks_data}")
        try:
            data_len = len(data) if isinstance(data, list) else (len(data.keys()) if isinstance(data, dict) else 1)
            write_debug(f"[RISKS PDF] data_type={type(data).__name__} data_len={data_len}")
        except Exception:
            pass
        
        # Generate PDF
        try:
            pdf_content = await pdf_service.generate_risks_pdf(
                risks_data, start_date_q or startDate or "", end_date_q or endDate or "", header_config, cardType, only_card_bool, only_overall_table_bool, only_chart_bool
        )
        except Exception as gen_err:
            write_debug(f"[RISKS PDF] generate_risks_pdf error: {gen_err}")
            raise
        
        # Get user from request headers (if available)
        created_by = request.headers.get('X-User-Name') or request.headers.get('Authorization') or "System"
        
        # Save file and log to database
        export_info = await save_and_log_export(
            content=pdf_content,
            file_extension='pdf',
            dashboard='risks',
            card_type=cardType,
            header_config=header_config,
            created_by=created_by,
            date_range={'startDate': start_date_q or startDate, 'endDate': end_date_q or endDate},
            request=request
        )
        
        filename = export_info['filename']
        
        return Response(
            content=pdf_content,
            media_type='application/pdf',
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"',
                'X-Export-Src': export_info['relative_path'],
                'X-Export-Id': str(export_info.get('export_id', ''))
            }
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
    tableType: str = Query(None),
    functionId: str = Query(None),
    functionIds: Optional[str] = Query(
        None,
        description="Comma-separated function IDs (multi-select); mirrors Node dashboard filters",
    ),
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

        start_date_q = grc_iso_date_param(startDate)
        end_date_q = grc_iso_date_param(endDate)
        write_debug(f"[RISKS EXCEL] dates for SQL: start={start_date_q!r} end={end_date_q!r} (raw: {startDate!r} / {endDate!r})")

        # Extract user and function parameters
        user_id, group_name, function_id = extract_user_and_function_params(request)
        if functionId:
            # Clean functionId using the same logic as extract_user_and_function_params
            from routes.route_utils import clean_function_id
            function_id = clean_function_id(functionId)
        function_ids = functionIds
        write_debug(f"[RISKS EXCEL] user_id={user_id}, group_name={group_name}, function_id={function_id}, functionIds={functionIds!r}")
        
        if not risk_service:
            raise HTTPException(status_code=500, detail="Risk service not available")

        data = None
        # Metrics (counts) or lists when onlyCard is requested
        if cardType == 'total':
            data = await risk_service.get_total_risks(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'high':
            data = await risk_service.get_high_risks(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'medium':
            data = await risk_service.get_medium_risks(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'low':
            data = await risk_service.get_low_risks(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType in ('reduction', 'risksReduced'):
            data = await risk_service.get_risks_reduced(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'newRisks':
            data = await risk_service.get_new_risks(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        # Charts
        elif cardType == 'risksByCategory':
            data = await risk_service.get_risks_by_category(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'risksByEventType':
            data = await risk_service.get_risks_by_event_type_chart(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'createdDeletedRisksPerQuarter':
            data = await risk_service.get_created_deleted_risks_per_quarter(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'quarterlyRiskCreationTrends':
            data = await risk_service.get_quarterly_risk_creation_trends(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'riskApprovalStatusDistribution':
            data = await risk_service.get_risk_approval_status_distribution(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'riskDistributionByFinancialImpact':
            data = await risk_service.get_risk_distribution_by_financial_impact(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        # Tables
        elif cardType == 'risksPerDepartment':
            data = await risk_service.get_risks_per_department(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'risksPerBusinessProcess':
            data = await risk_service.get_risks_per_business_process(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'inherentResidualRiskComparison':
            data = await risk_service.get_inherent_residual_risk_comparison(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'highResidualRiskOverview':
            data = await risk_service.get_high_residual_risk_overview(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'risksAndControlsCount':
            data = await risk_service.get_risks_and_controls_count(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'controlsAndRiskCount':
            data = await risk_service.get_controls_and_risk_count(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)
        elif cardType == 'allRisks':
            data = await risk_service.get_risks_details(start_date_q, end_date_q, user_id=user_id, group_name=group_name, function_id=function_id, function_ids=function_ids)

        risks_data = {cardType: data}
        if order_by_function_from_request(request):
            risks_data = apply_order_by_function_deep(risks_data)
        write_debug(f"risks_data: {risks_data}")
        
        # Generate Excel
        excel_content = await excel_service.generate_risks_excel(
            risks_data, start_date_q or startDate or "", end_date_q or endDate or "", header_config, cardType, only_card_bool, only_overall_table_bool, only_chart_bool
        )
        
        # Get user from request headers (if available)
        created_by = request.headers.get('X-User-Name') or request.headers.get('Authorization') or "System"
        
        # Save file and log to database
        export_info = await save_and_log_export(
            content=excel_content,
            file_extension='xlsx',
            dashboard='risks',
            card_type=cardType,
            header_config=header_config,
            created_by=created_by,
            date_range={'startDate': start_date_q or startDate, 'endDate': end_date_q or endDate},
            request=request
        )
        
        filename = export_info['filename']
        
        return Response(
            content=excel_content,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"',
                'X-Export-Src': export_info['relative_path'],
                'X-Export-Id': str(export_info.get('export_id', ''))
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")