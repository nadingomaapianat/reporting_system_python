from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import json
import os

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

class ChartRegistry:
    """Registry for managing charts - similar to Node.js version"""
    
    def __init__(self, storage_file: str = "charts.json"):
        self.storage_file = storage_file
        self.charts: Dict[str, ChartConfig] = {}
        self.load_charts()
    
    def add_chart(self, chart: ChartConfig) -> bool:
        """Add a new chart to the registry"""
        try:
            self.charts[chart.id] = chart
            self.save_charts()
            return True
        except Exception as e:
            print(f"Error adding chart {chart.id}: {e}")
            return False
    
    def get_chart(self, chart_id: str) -> Optional[ChartConfig]:
        """Get a specific chart by ID"""
        return self.charts.get(chart_id)
    
    def get_all_charts(self) -> List[ChartConfig]:
        """Get all charts"""
        return list(self.charts.values())
    
    def remove_chart(self, chart_id: str) -> bool:
        """Remove a chart from the registry"""
        try:
            if chart_id in self.charts:
                del self.charts[chart_id]
                self.save_charts()
                return True
            return False
        except Exception as e:
            print(f"Error removing chart {chart_id}: {e}")
            return False
    
    def list_charts(self) -> List[Dict[str, str]]:
        """List all charts with basic info"""
        return [
            {
                'id': chart.id,
                'name': chart.name,
                'type': chart.chart_type
            }
            for chart in self.charts.values()
        ]
    
    def save_charts(self):
        """Save charts to file"""
        try:
            charts_data = {
                chart_id: {
                    'id': chart.id,
                    'name': chart.name,
                    'chart_type': chart.chart_type,
                    'sql': chart.sql,
                    'x_field': chart.x_field,
                    'y_field': chart.y_field,
                    'label_field': chart.label_field,
                    'config': chart.config
                }
                for chart_id, chart in self.charts.items()
            }
            
            with open(self.storage_file, 'w') as f:
                json.dump(charts_data, f, indent=2)
                
        except Exception as e:
            print(f"Error saving charts: {e}")
    
    def load_charts(self):
        """Load charts from file"""
        try:
            if os.path.exists(self.storage_file):
                with open(self.storage_file, 'r') as f:
                    charts_data = json.load(f)
                
                self.charts = {
                    chart_id: ChartConfig(
                        id=data['id'],
                        name=data['name'],
                        chart_type=data['chart_type'],
                        sql=data['sql'],
                        x_field=data.get('x_field', 'name'),
                        y_field=data.get('y_field', 'value'),
                        label_field=data.get('label_field', 'name'),
                        config=data.get('config')
                    )
                    for chart_id, data in charts_data.items()
                }
        except Exception as e:
            print(f"Error loading charts: {e}")
            self.charts = {}

# Global registry instance
chart_registry = ChartRegistry()

# Convenience functions
def add_chart(chart: ChartConfig) -> bool:
    """Add a chart to the global registry"""
    return chart_registry.add_chart(chart)

def get_chart(chart_id: str) -> Optional[ChartConfig]:
    """Get a chart from the global registry"""
    return chart_registry.get_chart(chart_id)

def get_all_charts() -> List[ChartConfig]:
    """Get all charts from the global registry"""
    return chart_registry.get_all_charts()

def remove_chart(chart_id: str) -> bool:
    """Remove a chart from the global registry"""
    return chart_registry.remove_chart(chart_id)

def list_charts() -> List[Dict[str, str]]:
    """List all charts from the global registry"""
    return chart_registry.list_charts()
