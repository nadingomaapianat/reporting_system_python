from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from datetime import datetime
import asyncio
import httpx
from fastapi import HTTPException

@dataclass
class ChartConfig:
    id: str
    name: str
    chart_type: str  # 'bar', 'pie', 'line', 'area', 'scatter'
    sql: str
    x_field: str = 'name'
    y_field: str = 'value'
    label_field: str = 'name'
    config: Optional[Dict[str, Any]] = None

@dataclass
class MetricConfig:
    id: str
    name: str
    sql: str
    color: str = 'blue'
    icon: str = 'chart-bar'

@dataclass
class TableConfig:
    id: str
    name: str
    sql: str
    columns: List[Dict[str, Any]]
    pagination: bool = True

class BaseDashboardService(ABC):
    """Base class for all dashboard services - handles common functionality"""
    
    def __init__(self, database_service=None):
        self.database_service = database_service
    
    @abstractmethod
    def get_config(self) -> Dict[str, Any]:
        """Return dashboard configuration with charts, metrics, and tables"""
        pass
    
    async def get_dashboard_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """Get all dashboard data - charts, metrics, and tables"""
        config = self.get_config()
        date_filter = self._build_date_filter(start_date, end_date)
        
        try:
            # Execute all queries in parallel
            results = {}
            
            # Get metrics
            if 'metrics' in config:
                metrics_results = await self._get_metrics_data(config['metrics'], date_filter)
                results.update(metrics_results)
            
            # Get charts
            if 'charts' in config:
                charts_results = await self._get_charts_data(config['charts'], date_filter)
                results.update(charts_results)
            
            # Get tables
            if 'tables' in config:
                tables_results = await self._get_tables_data(config['tables'], date_filter)
                results.update(tables_results)
            
            return results
            
        except Exception as error:
            print(f"Error fetching dashboard data: {error}")
            raise HTTPException(status_code=500, detail=f"Error fetching dashboard data: {str(error)}")
    
    async def get_chart_data(self, chart_id: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get specific chart data"""
        config = self.get_config()
        charts = config.get('charts', [])
        
        chart_config = next((c for c in charts if c['id'] == chart_id), None)
        if not chart_config:
            raise HTTPException(status_code=404, detail=f"Chart {chart_id} not found")
        
        date_filter = self._build_date_filter(start_date, end_date)
        query = chart_config['sql'].replace('{dateFilter}', date_filter)
        
        try:
            if self.database_service:
                data = await self.database_service.query(query)
            else:
                # Mock data for testing
                data = self._generate_mock_data(chart_config)
            
            return self._format_chart_data(data, chart_config)
            
        except Exception as error:
            print(f"Error fetching chart {chart_id}: {error}")
            raise HTTPException(status_code=500, detail=f"Error fetching chart data: {str(error)}")
    
    async def get_metric_data(self, metric_id: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """Get specific metric data"""
        config = self.get_config()
        metrics = config.get('metrics', [])
        
        metric_config = next((m for m in metrics if m['id'] == metric_id), None)
        if not metric_config:
            raise HTTPException(status_code=404, detail=f"Metric {metric_id} not found")
        
        date_filter = self._build_date_filter(start_date, end_date)
        query = metric_config['sql'].replace('{dateFilter}', date_filter)
        
        try:
            if self.database_service:
                data = await self.database_service.query(query)
                value = data[0].get('total', data[0].get('count', 0)) if data else 0
            else:
                # Mock data for testing
                value = 100
            
            return {
                'id': metric_id,
                'value': value,
                'name': metric_config['name'],
                'color': metric_config.get('color', 'blue'),
                'icon': metric_config.get('icon', 'chart-bar')
            }
            
        except Exception as error:
            print(f"Error fetching metric {metric_id}: {error}")
            raise HTTPException(status_code=500, detail=f"Error fetching metric data: {str(error)}")
    
    async def _get_metrics_data(self, metrics: List[Dict[str, Any]], date_filter: str) -> Dict[str, Any]:
        """Get all metrics data"""
        results = {}
        
        for metric in metrics:
            try:
                query = metric['sql'].replace('{dateFilter}', date_filter)
                
                if self.database_service:
                    data = await self.database_service.query(query)
                    value = data[0].get('total', data[0].get('count', 0)) if data else 0
                else:
                    # Mock data for testing
                    value = 100
                
                results[metric['id']] = value
                
            except Exception as error:
                print(f"Error fetching metric {metric['id']}: {error}")
                results[metric['id']] = 0
        
        return results
    
    async def _get_charts_data(self, charts: List[Dict[str, Any]], date_filter: str) -> Dict[str, Any]:
        """Get all charts data"""
        results = {}
        
        for chart in charts:
            try:
                query = chart['sql'].replace('{dateFilter}', date_filter)
                
                if self.database_service:
                    data = await self.database_service.query(query)
                else:
                    # Mock data for testing
                    data = self._generate_mock_data(chart)
                
                results[chart['id']] = self._format_chart_data(data, chart)
                
            except Exception as error:
                print(f"Error fetching chart {chart['id']}: {error}")
                results[chart['id']] = []
        
        return results
    
    async def _get_tables_data(self, tables: List[Dict[str, Any]], date_filter: str) -> Dict[str, Any]:
        """Get all tables data"""
        results = {}
        
        for table in tables:
            try:
                query = table['sql'].replace('{dateFilter}', date_filter)
                
                if self.database_service:
                    data = await self.database_service.query(query)
                else:
                    # Mock data for testing
                    data = self._generate_mock_table_data(table)
                
                results[table['id']] = data
                
            except Exception as error:
                print(f"Error fetching table {table['id']}: {error}")
                results[table['id']] = []
        
        return results
    
    def _build_date_filter(self, start_date: Optional[str], end_date: Optional[str]) -> str:
        """Build date filter for SQL queries"""
        if not start_date and not end_date:
            return ''
        
        filter_parts = []
        
        if start_date:
            filter_parts.append(f"created_at >= '{start_date}'")
        
        if end_date:
            filter_parts.append(f"created_at <= '{end_date} 23:59:59'")
        
        return ' AND ' + ' AND '.join(filter_parts) if filter_parts else ''
    
    def _format_chart_data(self, data: List[Dict[str, Any]], chart_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Format chart data for frontend"""
        return [
            {
                'name': row.get(chart_config['x_field'], ''),
                'value': row.get(chart_config['y_field'], 0),
                'label': row.get(chart_config.get('label_field', chart_config['x_field']), '')
            }
            for row in data
        ]
    
    def _generate_mock_data(self, chart_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate mock data for testing"""
        chart_type = chart_config.get('chart_type', 'bar')
        
        if chart_type == 'bar':
            return [
                {'name': 'Category A', 'value': 100},
                {'name': 'Category B', 'value': 150},
                {'name': 'Category C', 'value': 200},
                {'name': 'Category D', 'value': 120}
            ]
        elif chart_type == 'pie':
            return [
                {'name': 'Active', 'value': 60},
                {'name': 'Inactive', 'value': 30},
                {'name': 'Pending', 'value': 10}
            ]
        elif chart_type == 'line':
            return [
                {'name': '2024-01', 'value': 100},
                {'name': '2024-02', 'value': 120},
                {'name': '2024-03', 'value': 150},
                {'name': '2024-04', 'value': 180}
            ]
        else:
            return [
                {'name': 'Item 1', 'value': 50},
                {'name': 'Item 2', 'value': 75},
                {'name': 'Item 3', 'value': 100}
            ]
    
    def _generate_mock_table_data(self, table_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate mock table data for testing"""
        return [
            {'id': 1, 'name': 'Sample Item 1', 'status': 'Active', 'value': 100},
            {'id': 2, 'name': 'Sample Item 2', 'status': 'Inactive', 'value': 200},
            {'id': 3, 'name': 'Sample Item 3', 'status': 'Pending', 'value': 150}
        ]
