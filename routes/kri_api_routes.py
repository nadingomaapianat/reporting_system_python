"""
API routes for the reporting system
"""
import asyncio
import json
from datetime import datetime
import os
import httpx
from fastapi import APIRouter, Query, HTTPException, Request, UploadFile, File, Form
from fastapi import Request
from fastapi.responses import Response, FileResponse, StreamingResponse
from typing import Optional

from services import APIService, PDFService, ExcelService, ControlService, IncidentService, KRIService
from services.bank_check_service import BankCheckService
from services.enhanced_bank_check_service import EnhancedBankCheckService
# Dashboard activity service moved to Node.js backend (NestJS)
DashboardActivityService = None  # type: ignore
from utils.export_utils import get_default_header_config
from models import ExportRequest, ExportResponse
from routes.route_utils import write_debug, parse_header_config, merge_header_config, convert_to_boolean, save_and_log_export, extract_user_and_function_params

# Initialize services
api_service = APIService()
pdf_service = PDFService()
excel_service = ExcelService()

# Initialize KRI service
try:
    from services.kri_service import KriService
    kri_service = KriService()
except Exception as e:
    write_debug(f"Failed to initialize KRI service: {e}")
    import traceback
    traceback.print_exc()
    kri_service = None

incident_service = IncidentService() if IncidentService else None
dashboard_activity_service = DashboardActivityService() if DashboardActivityService else None
bank_check_service = BankCheckService()
enhanced_bank_check_service = EnhancedBankCheckService()

# db_service points to kri_service for KRI-related database calls
db_service = kri_service

# Create router
router = APIRouter()

# Display names for KRI cards, charts, and tables (used for PDF/Excel title and filenames)
KRI_DISPLAY_NAMES = {
    "totalKris": "Total KRIs",
    "pendingPreparer": "KRIs Pending Preparer",
    "pendingChecker": "KRIs Pending Checker",
    "pendingReviewer": "KRIs Pending Reviewer",
    "pendingAcceptance": "KRIs Pending Acceptance",
    "krisByStatus": "KRIs by Status",
    "krisByLevel": "KRIs by Risk Level",
    "breachedKRIsByDepartment": "Breached KRIs by Function",
    "kriAssessmentCount": "KRI Assessment Count by Function",
    "kriCountsByFrequency": "KRIs by Frequency",
    "kriCountsByMonthYear": "KRIs Count by Month/Year",
    "kriRisksByKriName": "Risks by KRI Name",
    "kriOverdueStatusCounts": "KRIs Overdue Status",
    "kriMonthlyAssessment": "Monthly KRI Assessments (stacked)",
    "deletedKrisPerMonth": "Deleted KRIs Per Month",
    "overallKris": "Overall KRI Statuses",
    "kriStatus": "Overall KRI Statuses",
    "allKrisSubmittedByFunction": "KRIs Submission Status by Function",
    "activeKrisDetails": "Active KRIs Details",
    "overdueKrisByDepartment": "Overdue KRIs by Function",
    "kriWithoutLinkedRisks": "KRIs Without Linked Risks",
    "krisWithoutLinkedRisks": "KRIs Without Linked Risks",
    "kriRiskRelationships": "KRI to Risk Relationships",
    "kriDetailsWithActionPlans": "KRI Details & Action Plans",
}


@router.api_route("/api/grc/kris/export/pdf", methods=["GET", "POST"])
async def export_kris_pdf(
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
    functionId: str = Query(None)
):
    """Export KRIs dashboard to PDF (GET or POST with optional body.totalKrisList or body.kriDetailsWithActionPlans)."""
    kri_details_override = None
    total_kris_list_override = None
    if request.method == "POST":
        try:
            body = await request.json()
            if isinstance(body, dict):
                if "totalKrisList" in body:
                    total_kris_list_override = body.get("totalKrisList")
                    if not isinstance(total_kris_list_override, list):
                        total_kris_list_override = []
                if "kriDetailsWithActionPlans" in body:
                    kri_details_override = body.get("kriDetailsWithActionPlans")
                    if not isinstance(kri_details_override, list):
                        kri_details_override = []
        except Exception:
            pass
    global kri_service
    try:
        write_debug(f"[KRIS PDF] startDate={startDate} endDate={endDate}")
        write_debug(f"[KRIS PDF] cardType={cardType} onlyCard={onlyCard} onlyChart={onlyChart}")
        write_debug(f"[KRIS PDF] chartType={chartType} onlyOverallTable={onlyOverallTable} tableType={tableType}")

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
        header_config = merge_header_config("kris", header_config)
        
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

        # Set report title to the display name for this card/chart/table (PDF title and Excel sheet name)
        header_config["title"] = KRI_DISPLAY_NAMES.get(
            cardType, header_config.get("title", "KRI Report")
        )

        write_debug(f"[KRIS PDF] normalized cardType={cardType}")

        # Extract user and function parameters
        user_id, group_name, function_id = extract_user_and_function_params(request)
        if functionId:
            # Clean functionId using the same logic as extract_user_and_function_params
            from routes.route_utils import clean_function_id
            function_id = clean_function_id(functionId)
        write_debug(f"[KRIS PDF] user_id={user_id}, group_name={group_name}, function_id={function_id}")

        # Fetch data via kri_service - reinitialize if needed
        if not kri_service:
            write_debug("[KRIS PDF] ERROR: kri_service is None - attempting to reinitialize")
            try:
                from services.kri_service import KriService
                kri_service = KriService()
                write_debug("[KRIS PDF] Successfully reinitialized kri_service")
            except Exception as init_err:
                write_debug(f"[KRIS PDF] Failed to reinitialize kri_service: {init_err}")
                import traceback
                traceback.print_exc()
                raise HTTPException(status_code=500, detail=f"KRI service not available: {str(init_err)}")
        
        if not kri_service:
            raise HTTPException(status_code=500, detail="KRI service not available after initialization attempt")

        data = None
        
        # Status counts (metrics) - return lists for card export
        if cardType == 'totalKris' or cardType == 'krisList':
            if total_kris_list_override is not None:
                data = total_kris_list_override
                write_debug(f"[KRIS PDF] using totalKrisList from POST body, len={len(data)}")
            else:
                write_debug('jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj')
                data = await kri_service.get_kris_list(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'pendingPreparer':
            data = await kri_service.get_kris_by_status_detail('pendingPreparer', startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'pendingChecker':
            data = await kri_service.get_kris_by_status_detail('pendingChecker', startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'pendingReviewer':
            data = await kri_service.get_kris_by_status_detail('pendingReviewer', startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'pendingAcceptance':
            data = await kri_service.get_kris_by_status_detail('pendingAcceptance', startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'approved':
            data = await kri_service.get_kris_by_status_detail('Approved', startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        
        # Charts
        elif cardType == 'krisByStatus':
            data = await kri_service.get_kris_by_status(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'krisByLevel':
            data = await kri_service.get_kris_by_level_detailed(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'breachedKRIsByDepartment':
            data = await kri_service.get_breached_kris_by_department_detailed(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'kriAssessmentCount':
            data = await kri_service.get_kri_assessment_count_detailed(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'kriMonthlyAssessment':
            data = await kri_service.get_kri_monthly_assessment(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'newlyCreatedKrisPerMonth':
            data = await kri_service.get_newly_created_kris_per_month(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'deletedKrisPerMonth':
            data = await kri_service.get_deleted_kris_per_month(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'kriOverdueStatusCounts':
            data = await kri_service.get_kri_overdue_status_counts(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'kriCountsByMonthYear':
            data = await kri_service.get_kri_counts_by_month_year(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'kriCountsByFrequency':
            data = await kri_service.get_kri_counts_by_frequency(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'kriRisksByKriName':
            data = await kri_service.get_kri_risks_by_kri_name(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        
        # Tables
        elif cardType == 'overallKris' or cardType == 'kriStatus':
            data = await kri_service.get_overall_kri_statuses(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'kriHealth':
            data = await kri_service.get_kri_health(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'activeKrisDetails':
            data = await kri_service.get_active_kris_details(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'overdueKrisByDepartment':
            data = await kri_service.get_overdue_kris_by_department(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'allKrisSubmittedByFunction':
            data = await kri_service.get_all_kris_submitted_by_function(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'kriRiskRelationships':
            data = await kri_service.get_kri_risk_relationships(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'kriWithoutLinkedRisks' or cardType == 'krisWithoutLinkedRisks':
            data = await kri_service.get_kris_without_linked_risks(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'kriDetailsWithActionPlans':
            # Use POST body if provided (frontend fetches from Node with auth); else fetch from Node
            if kri_details_override is not None:
                data = kri_details_override
                write_debug(f"[KRIS PDF] using kriDetailsWithActionPlans from POST body, len={len(data)}")
            else:
                forward_headers = {}
                if request.headers.get("authorization"):
                    forward_headers["Authorization"] = request.headers.get("authorization")
                if request.headers.get("cookie"):
                    forward_headers["Cookie"] = request.headers.get("cookie")
                dashboard = await api_service.get_kris_data(
                    start_date=startDate,
                    end_date=endDate,
                    user_id=user_id,
                    group_name=group_name,
                    function_id=function_id,
                    headers=forward_headers if forward_headers else None,
                )
                data = dashboard.get("kriDetailsWithActionPlans") or []
        else:
            raise HTTPException(status_code=400, detail=f"Unknown cardType: {cardType}")

        kris_data = {cardType: data}
        write_debug(f"kris_data: {kris_data}")
        try:
            data_len = len(data) if isinstance(data, list) else (len(data.keys()) if isinstance(data, dict) else 1)
            write_debug(f"[KRIS PDF] data_type={type(data)} data_len={data_len}")
        except Exception:
            write_debug(f"[KRIS PDF] data_type={type(data)} data_len=N/A")
        
        # Generate PDF
        try:
            pdf_content = await pdf_service.generate_kris_pdf(
                kris_data, startDate, endDate, header_config, cardType, only_card_bool, only_overall_table_bool, only_chart_bool
            )
        except Exception as gen_err:
            write_debug(f"[KRIS PDF] generate_kris_pdf error: {gen_err}")
            raise
        
        # Get user from request headers (if available)
        created_by = request.headers.get('X-User-Name') or request.headers.get('Authorization') or "System"
        
        # Save file and log to database
        export_info = await save_and_log_export(
            content=pdf_content,
            file_extension='pdf',
            dashboard='kris',
            card_type=cardType,
            header_config=header_config,
            created_by=created_by,
            date_range={'startDate': startDate, 'endDate': endDate},
            request=request
        )
        
        filename = export_info['filename']
        
        return Response(
            content=pdf_content,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                'X-Export-Src': export_info['relative_path'],
                'X-Export-Id': str(export_info.get('export_id', ''))
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        write_debug(f"[KRIS PDF] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to generate PDF: {str(e)}")

@router.api_route("/api/grc/kris/export/excel", methods=["GET", "POST"])
async def export_kris_excel(
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
    functionId: str = Query(None)
):
    """Export KRIs dashboard to Excel (GET or POST with optional body.totalKrisList or body.kriDetailsWithActionPlans)."""
    kri_details_override = None
    total_kris_list_override = None
    if request.method == "POST":
        try:
            body = await request.json()
            if isinstance(body, dict):
                if "totalKrisList" in body:
                    total_kris_list_override = body.get("totalKrisList")
                    if not isinstance(total_kris_list_override, list):
                        total_kris_list_override = []
                if "kriDetailsWithActionPlans" in body:
                    kri_details_override = body.get("kriDetailsWithActionPlans")
                    if not isinstance(kri_details_override, list):
                        kri_details_override = []
        except Exception:
            pass
    global kri_service
    try:
        write_debug(f"Exporting KRIs report in Excel format for {startDate} to {endDate}")
        write_debug(f"cardType: {cardType}")
        write_debug(f"onlyCard: {onlyCard}")
        write_debug(f"onlyChart: {onlyChart}")
        write_debug(f"chartType: {chartType}")

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
        header_config = merge_header_config("kris", header_config)

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

        # Set report title to the display name for this card/chart/table (PDF title and Excel sheet name)
        header_config["title"] = KRI_DISPLAY_NAMES.get(
            cardType, header_config.get("title", "KRI Report")
        )

        # Extract user and function parameters (needed for kriDetailsWithActionPlans and filtering)
        user_id, group_name, function_id = extract_user_and_function_params(request)
        if functionId:
            from routes.route_utils import clean_function_id
            function_id = clean_function_id(functionId)

        if not kri_service:
            write_debug("[KRIS EXCEL] ERROR: kri_service is None - attempting to reinitialize")
            try:
                from services.kri_service import KriService
                kri_service = KriService()
                write_debug("[KRIS EXCEL] Successfully reinitialized kri_service")
            except Exception as init_err:
                write_debug(f"[KRIS EXCEL] Failed to reinitialize kri_service: {init_err}")
                import traceback
                traceback.print_exc()
                raise HTTPException(status_code=500, detail=f"KRI service not available: {str(init_err)}")
        
        if not kri_service:
            raise HTTPException(status_code=500, detail="KRI service not available after initialization attempt")

        data = None
        
        # Status counts (metrics) - return lists for card export
        if cardType == 'totalKris' or cardType == 'krisList':
            if total_kris_list_override is not None:
                data = total_kris_list_override
                write_debug(f"[KRIS EXCEL] using totalKrisList from POST body, len={len(data)}")
            else:
                data = await kri_service.get_kris_list(startDate, endDate, user_id=user_id, group_name=group_name, function_id=function_id)
        elif cardType == 'pendingPreparer':
            data = await kri_service.get_kris_by_status_detail('pendingPreparer', startDate, endDate)
        elif cardType == 'pendingChecker':
            data = await kri_service.get_kris_by_status_detail('pendingChecker', startDate, endDate)
        elif cardType == 'pendingReviewer':
            data = await kri_service.get_kris_by_status_detail('pendingReviewer', startDate, endDate)
        elif cardType == 'pendingAcceptance':
            data = await kri_service.get_kris_by_status_detail('pendingAcceptance', startDate, endDate)
        elif cardType == 'approved':
            data = await kri_service.get_kris_by_status_detail('Approved', startDate, endDate)
        
        # Charts
        elif cardType == 'krisByStatus':
            data = await kri_service.get_kris_by_status(startDate, endDate)
        elif cardType == 'krisByLevel':
            data = await kri_service.get_kris_by_level_detailed(startDate, endDate)
        elif cardType == 'breachedKRIsByDepartment':
            data = await kri_service.get_breached_kris_by_department_detailed(startDate, endDate)
        elif cardType == 'kriAssessmentCount':
            data = await kri_service.get_kri_assessment_count_detailed(startDate, endDate)
        elif cardType == 'kriMonthlyAssessment':
            data = await kri_service.get_kri_monthly_assessment(startDate, endDate)
        elif cardType == 'newlyCreatedKrisPerMonth':
            data = await kri_service.get_newly_created_kris_per_month(startDate, endDate)
        elif cardType == 'deletedKrisPerMonth':
            data = await kri_service.get_deleted_kris_per_month(startDate, endDate)
        elif cardType == 'kriOverdueStatusCounts':
            data = await kri_service.get_kri_overdue_status_counts(startDate, endDate)
        elif cardType == 'kriCountsByMonthYear':
            data = await kri_service.get_kri_counts_by_month_year(startDate, endDate)
        elif cardType == 'kriCountsByFrequency':
            data = await kri_service.get_kri_counts_by_frequency(startDate, endDate)
        elif cardType == 'kriRisksByKriName':
            data = await kri_service.get_kri_risks_by_kri_name(startDate, endDate)
        
        # Tables
        elif cardType == 'overallKris' or cardType == 'kriStatus':
            data = await kri_service.get_overall_kri_statuses(startDate, endDate)
        elif cardType == 'kriHealth':
            data = await kri_service.get_kri_health(startDate, endDate)
        elif cardType == 'activeKrisDetails':
            data = await kri_service.get_active_kris_details(startDate, endDate)
        elif cardType == 'overdueKrisByDepartment':
            data = await kri_service.get_overdue_kris_by_department(startDate, endDate)
        elif cardType == 'allKrisSubmittedByFunction':
            data = await kri_service.get_all_kris_submitted_by_function(startDate, endDate)
        elif cardType == 'kriRiskRelationships':
            data = await kri_service.get_kri_risk_relationships(startDate, endDate)
        elif cardType == 'kriWithoutLinkedRisks' or cardType == 'krisWithoutLinkedRisks':
            data = await kri_service.get_kris_without_linked_risks(startDate, endDate)
        elif cardType == 'kriDetailsWithActionPlans':
            if kri_details_override is not None:
                data = kri_details_override
                write_debug(f"[KRIS EXCEL] using kriDetailsWithActionPlans from POST body, len={len(data)}")
            else:
                forward_headers = {}
                if request.headers.get("authorization"):
                    forward_headers["Authorization"] = request.headers.get("authorization")
                if request.headers.get("cookie"):
                    forward_headers["Cookie"] = request.headers.get("cookie")
                dashboard = await api_service.get_kris_data(
                    start_date=startDate,
                    end_date=endDate,
                    user_id=user_id,
                    group_name=group_name,
                    function_id=function_id,
                    headers=forward_headers if forward_headers else None,
                )
                data = dashboard.get("kriDetailsWithActionPlans") or []
        else:
            raise HTTPException(status_code=400, detail=f"Unknown cardType: {cardType}")

        kris_data = {cardType: data}
        write_debug(f"KRIs Excel export - card_type={cardType}, data type={type(data)}, len={len(data) if isinstance(data, list) else 'N/A'}")
        
        # Generate Excel
        excel_content = await excel_service.generate_kris_excel(
            kris_data, startDate, endDate, header_config, cardType, only_card_bool, only_overall_table_bool, only_chart_bool
        )
        
        # Get user from request headers (if available)
        created_by = request.headers.get('X-User-Name') or request.headers.get('Authorization') or "System"
        
        # Save file and log to database
        export_info = await save_and_log_export(
            content=excel_content,
            file_extension='xlsx',
            dashboard='kris',
            card_type=cardType,
            header_config=header_config,
            created_by=created_by,
            date_range={'startDate': startDate, 'endDate': endDate},
            request=request
        )
        
        filename = export_info['filename']
        
        return Response(
            content=excel_content,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                'X-Export-Src': export_info['relative_path'],
                'X-Export-Id': str(export_info.get('export_id', ''))
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        write_debug(f"[KRIS EXCEL] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to generate Excel: {str(e)}")


