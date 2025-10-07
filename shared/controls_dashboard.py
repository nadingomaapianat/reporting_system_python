from typing import Dict, Any, Optional
from .base_dashboard import BaseDashboardService
from .dashboard_templates import DashboardTemplates

class ControlsDashboardService(BaseDashboardService):
    """Controls Dashboard Service - extends BaseDashboardService"""
    
    def get_config(self) -> Dict[str, Any]:
        """Return controls dashboard configuration"""
        return DashboardTemplates.get_controls_dashboard_config()
    
    async def get_controls_dashboard(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """Get controls dashboard data"""
        return await self.get_dashboard_data(start_date, end_date)
    
    # Individual card data methods (for modals)
    async def get_total_controls(self, page: int = 1, limit: int = 10, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """Get total controls data with pagination"""
        return await self._get_paginated_data('total_controls', page, limit, start_date, end_date)
    
    async def get_pending_preparer_controls(self, page: int = 1, limit: int = 10, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """Get pending preparer controls data with pagination"""
        return await self._get_paginated_data('pending_preparer', page, limit, start_date, end_date)
    
    async def get_pending_checker_controls(self, page: int = 1, limit: int = 10, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """Get pending checker controls data with pagination"""
        return await self._get_paginated_data('pending_checker', page, limit, start_date, end_date)
    
    async def get_pending_reviewer_controls(self, page: int = 1, limit: int = 10, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """Get pending reviewer controls data with pagination"""
        return await self._get_paginated_data('pending_reviewer', page, limit, start_date, end_date)
    
    async def get_pending_acceptance_controls(self, page: int = 1, limit: int = 10, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """Get pending acceptance controls data with pagination"""
        return await self._get_paginated_data('pending_acceptance', page, limit, start_date, end_date)
    
    async def _get_paginated_data(self, metric_id: str, page: int, limit: int, start_date: Optional[str], end_date: Optional[str]) -> Dict[str, Any]:
        """Get paginated data for a specific metric"""
        config = self.get_config()
        metrics = config.get('metrics', [])
        
        metric_config = next((m for m in metrics if m['id'] == metric_id), None)
        if not metric_config:
            return {'data': [], 'pagination': {'page': page, 'limit': limit, 'total': 0, 'totalPages': 0, 'hasNext': False, 'hasPrev': False}}
        
        date_filter = self._build_date_filter(start_date, end_date)
        
        # Convert count query to data query
        data_query = metric_config['sql'].replace('COUNT(*) as total', '*').replace('{dateFilter}', date_filter)
        count_query = metric_config['sql'].replace('{dateFilter}', date_filter)
        
        # Add pagination
        offset = (page - 1) * limit
        paginated_query = f"{data_query} ORDER BY id OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY"
        
        try:
            if self.database_service:
                data = await self.database_service.query(paginated_query)
                count_result = await self.database_service.query(count_query)
                total = count_result[0].get('total', 0) if count_result else 0
            else:
                # Mock data for testing
                data = [
                    {'id': i, 'name': f'Control {i}', 'code': f'CTRL-{i:03d}'}
                    for i in range(1, min(limit + 1, 11))
                ]
                total = 100
            
            total_pages = (total + limit - 1) // limit
            
            return {
                'data': [
                    {
                        'control_code': row.get('code', f'CTRL-{row.get("id", 0):03d}'),
                        'control_name': row.get('name', f'Control {row.get("id", 0)}'),
                        **row
                    }
                    for row in data
                ],
                'pagination': {
                    'page': page,
                    'limit': limit,
                    'total': total,
                    'totalPages': total_pages,
                    'hasNext': page < total_pages,
                    'hasPrev': page > 1
                }
            }
            
        except Exception as error:
            print(f"Error fetching paginated data for {metric_id}: {error}")
            return {
                'data': [],
                'pagination': {
                    'page': page,
                    'limit': limit,
                    'total': 0,
                    'totalPages': 0,
                    'hasNext': False,
                    'hasPrev': False
                }
            }
