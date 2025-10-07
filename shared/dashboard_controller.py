from fastapi import APIRouter, Query, HTTPException
from typing import Optional, Dict, Any
from .controls_dashboard import ControlsDashboardService
from .chart_registry import chart_registry, ChartConfig

# Create router
router = APIRouter()

# Initialize dashboard service
dashboard_service = ControlsDashboardService()

@router.get("/dashboard")
async def get_dashboard(
    start_date: Optional[str] = Query(None, description="Start date for filtering (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date for filtering (YYYY-MM-DD)")
) -> Dict[str, Any]:
    """Get complete dashboard data"""
    try:
        return await dashboard_service.get_controls_dashboard(start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/charts")
async def get_all_charts(
    start_date: Optional[str] = Query(None, description="Start date for filtering (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date for filtering (YYYY-MM-DD)")
) -> Dict[str, Any]:
    """Get all charts data"""
    try:
        config = dashboard_service.get_config()
        charts = config.get('charts', [])
        results = {}
        
        for chart in charts:
            chart_data = await dashboard_service.get_chart_data(chart['id'], start_date, end_date)
            results[chart['id']] = chart_data
        
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/charts/{chart_id}")
async def get_chart(
    chart_id: str,
    start_date: Optional[str] = Query(None, description="Start date for filtering (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date for filtering (YYYY-MM-DD)")
) -> Dict[str, Any]:
    """Get specific chart data"""
    try:
        chart_data = await dashboard_service.get_chart_data(chart_id, start_date, end_date)
        return {chart_id: chart_data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/metrics")
async def get_all_metrics(
    start_date: Optional[str] = Query(None, description="Start date for filtering (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date for filtering (YYYY-MM-DD)")
) -> Dict[str, Any]:
    """Get all metrics data"""
    try:
        config = dashboard_service.get_config()
        metrics = config.get('metrics', [])
        results = {}
        
        for metric in metrics:
            metric_data = await dashboard_service.get_metric_data(metric['id'], start_date, end_date)
            results[metric['id']] = metric_data['value']
        
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/metrics/{metric_id}")
async def get_metric(
    metric_id: str,
    start_date: Optional[str] = Query(None, description="Start date for filtering (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date for filtering (YYYY-MM-DD)")
) -> Dict[str, Any]:
    """Get specific metric data"""
    try:
        metric_data = await dashboard_service.get_metric_data(metric_id, start_date, end_date)
        return metric_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Card-specific endpoints for modals
@router.get("/cards/total")
async def get_total_controls(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    start_date: Optional[str] = Query(None, description="Start date for filtering (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date for filtering (YYYY-MM-DD)")
) -> Dict[str, Any]:
    """Get total controls data with pagination"""
    try:
        return await dashboard_service.get_total_controls(page, limit, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/cards/pending-preparer")
async def get_pending_preparer_controls(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    start_date: Optional[str] = Query(None, description="Start date for filtering (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date for filtering (YYYY-MM-DD)")
) -> Dict[str, Any]:
    """Get pending preparer controls data with pagination"""
    try:
        return await dashboard_service.get_pending_preparer_controls(page, limit, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/cards/pending-checker")
async def get_pending_checker_controls(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    start_date: Optional[str] = Query(None, description="Start date for filtering (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date for filtering (YYYY-MM-DD)")
) -> Dict[str, Any]:
    """Get pending checker controls data with pagination"""
    try:
        return await dashboard_service.get_pending_checker_controls(page, limit, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/cards/pending-reviewer")
async def get_pending_reviewer_controls(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    start_date: Optional[str] = Query(None, description="Start date for filtering (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date for filtering (YYYY-MM-DD)")
) -> Dict[str, Any]:
    """Get pending reviewer controls data with pagination"""
    try:
        return await dashboard_service.get_pending_reviewer_controls(page, limit, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/cards/pending-acceptance")
async def get_pending_acceptance_controls(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Items per page"),
    start_date: Optional[str] = Query(None, description="Start date for filtering (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date for filtering (YYYY-MM-DD)")
) -> Dict[str, Any]:
    """Get pending acceptance controls data with pagination"""
    try:
        return await dashboard_service.get_pending_acceptance_controls(page, limit, start_date, end_date)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Chart registry endpoints
@router.post("/charts/add")
async def add_chart(chart_data: Dict[str, Any]) -> Dict[str, Any]:
    """Add a new chart to the registry"""
    try:
        chart = ChartConfig(
            id=chart_data['id'],
            name=chart_data['name'],
            chart_type=chart_data['chart_type'],
            sql=chart_data['sql'],
            x_field=chart_data.get('x_field', 'name'),
            y_field=chart_data.get('y_field', 'value'),
            label_field=chart_data.get('label_field', 'name'),
            config=chart_data.get('config')
        )
        
        success = chart_registry.add_chart(chart)
        if success:
            return {
                'success': True,
                'message': f"Chart '{chart.name}' added successfully",
                'chart_id': chart.id
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to add chart")
            
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/charts/list")
async def list_charts() -> Dict[str, Any]:
    """List all charts in the registry"""
    try:
        charts = chart_registry.list_charts()
        return {
            'success': True,
            'charts': charts,
            'count': len(charts)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/charts/{chart_id}")
async def remove_chart(chart_id: str) -> Dict[str, Any]:
    """Remove a chart from the registry"""
    try:
        success = chart_registry.remove_chart(chart_id)
        if success:
            return {
                'success': True,
                'message': f"Chart '{chart_id}' removed successfully"
            }
        else:
            raise HTTPException(status_code=404, detail=f"Chart '{chart_id}' not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
