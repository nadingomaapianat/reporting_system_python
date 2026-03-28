"""
GRC Comply export routes: fetch dashboard from Node (JWT + filters) and render PDF/Excel in Python.
Export cardType slugs match reporting_system_frontend2 ComplyChartsDashboard chartTypeMap / tableTypeMap.
"""
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response

from services import APIService, PDFService, ExcelService
from routes.route_utils import (
    write_debug,
    parse_header_config,
    merge_header_config,
    convert_to_boolean,
    save_and_log_export,
    extract_user_and_function_params,
)
from utils.order_by_function import apply_order_by_function_deep, order_by_function_from_request

api_service = APIService()
pdf_service = PDFService()
excel_service = ExcelService()

router = APIRouter()

# Frontend slug -> Node dashboard key (GrcComplyService reportNames), must stay aligned with ComplyChartsDashboard.tsx
COMPLY_CARD_TO_NODE_REPORT: Dict[str, str] = {
    "surveysByStatus": "Surveys by Status",
    "complianceByStatus": "Compliance per complianceStatus",
    "complianceByProgress": "Compliance per progressStatus",
    "complianceByApproval": "Compliance per approval_status",
    "avgScorePerSurvey": "Average Score Per Survey",
    "complianceByControlCategory": "Compliance by Control Category",
    "topFailedControls": "Top Failed Controls",
    "controlsPerCategory": "Controls no. per category",
    "risksPerCategory": "Risks no. per category",
    "impactedAreasTrend": "Impacted Areas Trend Over Time",
    "questionsPerType": "Questions no. per type",
    "questionsPerReferences": "Questions no. per References",
    "controlNosPerDomains": "Control Nos. per Domains",
    "surveyCompletionRate": "Survey Completion Rate",
    "bankQuestionsDetails": "Bank Questions details",
    "risksPerCategoryDetails": "Risks per category details",
    "controlsPerCategoryDetails": "Controls per category details",
    "controlsPerDomainsDetails": "Controls per domains Details",
    "questionsPerCategory": "Questions Per Category",
    "complianceDetails": "Compliance Details",
    "impactedAreasByControls": "Impacted Areas by Number of Linked Controls",
    "surveyParticipationByDepartment": "Survey Participation by Department",
    "activeFunctions": "Most Active vs Least Active Functions (Answer Count)",
    "surveyCoverageByCategory": "Survey Coverage by Category (How many categories included per survey)",
    "complianceManagementDetails": "Compliance managment details",
    "complianceWithoutEvidence": "Compliance controls without evidence",
}


def _forward_auth_headers(request: Request) -> dict:
    out: Dict[str, str] = {}
    if request.headers.get("cookie"):
        out["Cookie"] = request.headers.get("cookie")
    if request.headers.get("authorization"):
        out["Authorization"] = request.headers.get("authorization")
    return out


def _order_by_params_from_request(request: Request) -> Dict[str, str]:
    """Mirror Node order-by-function query params on GRC dashboards."""
    out: Dict[str, str] = {}
    for key in ("orderByFunction", "orderByFunctionAsc"):
        v = request.query_params.get(key)
        if v is not None and str(v).strip() != "":
            out[key] = str(v)
    return out


def _coerce_list_rows(raw: Any) -> List[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("data", "items", "results"):
            inner = raw.get(key)
            if isinstance(inner, list):
                return inner
    return []


def _node_report_key_for_card(card_type: str) -> str:
    return COMPLY_CARD_TO_NODE_REPORT.get(card_type, card_type)


@router.api_route("/api/grc/comply/export-pdf", methods=["GET", "POST"])
async def export_comply_pdf(
    request: Request,
    startDate: Optional[str] = Query(None),
    endDate: Optional[str] = Query(None),
    headerConfig: Optional[str] = Query(None),
    cardType: Optional[str] = Query(None),
    onlyCard: str = Query("False"),
    onlyChart: str = Query("False"),
    chartType: Optional[str] = Query(None),
    onlyOverallTable: str = Query("False"),
    tableType: Optional[str] = Query(None),
    functionId: Optional[str] = Query(None),
    functionIds: Optional[str] = Query(None),
):
    try:
        write_debug(
            f"[COMPLY PDF] startDate={startDate} endDate={endDate} cardType={cardType} "
            f"onlyCard={onlyCard} onlyChart={onlyChart} onlyOverallTable={onlyOverallTable} tableType={tableType}"
        )

        header_config = parse_header_config(headerConfig)
        render_type = request.query_params.get("renderType")
        if render_type:
            header_config["chartType"] = render_type
        elif chartType:
            header_config["chartType"] = chartType
        header_config = merge_header_config("comply", header_config)

        only_card_bool = convert_to_boolean(onlyCard)
        only_chart_bool = convert_to_boolean(onlyChart)
        only_overall_table_bool = convert_to_boolean(onlyOverallTable)

        if chartType and not cardType:
            cardType = chartType
        if only_overall_table_bool and tableType:
            cardType = tableType

        if not cardType:
            raise HTTPException(status_code=400, detail="cardType, chartType, or tableType is required")

        user_id, group_name, function_id = extract_user_and_function_params(request)
        if functionId:
            from routes.route_utils import clean_function_id

            function_id = clean_function_id(functionId)

        fwd = _forward_auth_headers(request)
        dashboard = await api_service.get_comply_dashboard(
            start_date=startDate,
            end_date=endDate,
            user_id=user_id,
            group_name=group_name,
            function_id=function_id,
            function_ids_csv=functionIds,
            headers=fwd,
        )
        node_key = _node_report_key_for_card(cardType)
        series = _coerce_list_rows(dashboard.get(node_key) if isinstance(dashboard, dict) else None)
        comply_data: Dict[str, Any] = {cardType: series}
        if order_by_function_from_request(request):
            comply_data = apply_order_by_function_deep(comply_data)

        pdf_content = await pdf_service.generate_comply_pdf(
            comply_data,
            startDate or "",
            endDate or "",
            header_config,
            cardType,
            only_card_bool,
            only_overall_table_bool,
            only_chart_bool,
        )

        created_by = request.headers.get("X-User-Name") or request.headers.get("Authorization") or "System"
        export_info = await save_and_log_export(
            content=pdf_content,
            file_extension="pdf",
            dashboard="comply",
            card_type=cardType,
            header_config=header_config,
            created_by=created_by,
            date_range={"startDate": startDate, "endDate": endDate},
            request=request,
        )
        filename = export_info["filename"]
        return Response(
            content=pdf_content,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "X-Export-Src": export_info["relative_path"],
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        write_debug(f"[COMPLY PDF] error: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.api_route("/api/grc/comply/export-excel", methods=["GET", "POST"])
async def export_comply_excel(
    request: Request,
    startDate: Optional[str] = Query(None),
    endDate: Optional[str] = Query(None),
    headerConfig: Optional[str] = Query(None),
    cardType: Optional[str] = Query(None),
    onlyCard: str = Query("False"),
    onlyChart: str = Query("False"),
    chartType: Optional[str] = Query(None),
    onlyOverallTable: str = Query("False"),
    tableType: Optional[str] = Query(None),
    functionId: Optional[str] = Query(None),
    functionIds: Optional[str] = Query(None),
):
    try:
        write_debug(
            f"[COMPLY EXCEL] startDate={startDate} endDate={endDate} cardType={cardType} "
            f"onlyCard={onlyCard} onlyChart={onlyChart} onlyOverallTable={onlyOverallTable} tableType={tableType}"
        )

        header_config = parse_header_config(headerConfig)
        render_type = request.query_params.get("renderType")
        if render_type:
            header_config["chartType"] = render_type
        elif chartType:
            header_config["chartType"] = chartType
        header_config = merge_header_config("comply", header_config)

        only_card_bool = convert_to_boolean(onlyCard)
        only_chart_bool = convert_to_boolean(onlyChart)
        only_overall_table_bool = convert_to_boolean(onlyOverallTable)

        if chartType and not cardType:
            cardType = chartType
        if only_overall_table_bool and tableType:
            cardType = tableType

        if not cardType:
            raise HTTPException(status_code=400, detail="cardType, chartType, or tableType is required")

        user_id, group_name, function_id = extract_user_and_function_params(request)
        if functionId:
            from routes.route_utils import clean_function_id

            function_id = clean_function_id(functionId)

        fwd = _forward_auth_headers(request)
        ob_params = _order_by_params_from_request(request)
        dashboard = await api_service.get_comply_dashboard(
            start_date=startDate,
            end_date=endDate,
            user_id=user_id,
            group_name=group_name,
            function_id=function_id,
            function_ids_csv=functionIds,
            headers=fwd,
            extra_query=ob_params if ob_params else None,
        )
        node_key = _node_report_key_for_card(cardType)
        series = _coerce_list_rows(dashboard.get(node_key) if isinstance(dashboard, dict) else None)
        comply_data: Dict[str, Any] = {cardType: series}
        if order_by_function_from_request(request):
            comply_data = apply_order_by_function_deep(comply_data)

        excel_content = await excel_service.generate_comply_excel(
            comply_data,
            startDate or "",
            endDate or "",
            header_config,
            cardType,
            only_card_bool,
            only_overall_table_bool,
            only_chart_bool,
        )

        created_by = request.headers.get("X-User-Name") or request.headers.get("Authorization") or "System"
        export_info = await save_and_log_export(
            content=excel_content,
            file_extension="xlsx",
            dashboard="comply",
            card_type=cardType,
            header_config=header_config,
            created_by=created_by,
            date_range={"startDate": startDate, "endDate": endDate},
            request=request,
        )
        filename = export_info["filename"]
        return Response(
            content=excel_content,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "X-Export-Src": export_info["relative_path"],
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        write_debug(f"[COMPLY EXCEL] error: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e)) from e
