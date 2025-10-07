from typing import Dict, List, Any
from .chart_registry import ChartConfig

class DashboardTemplates:
    """Templates for common dashboard configurations"""
    
    @staticmethod
    def get_chart_template(template_name: str, **kwargs) -> ChartConfig:
        """Get a chart template by name"""
        templates = {
            'department_distribution': ChartConfig(
                id=kwargs.get('id', 'department_distribution'),
                name=kwargs.get('name', 'Distribution by Department'),
                chart_type='bar',
                sql=f"SELECT department_name as name, COUNT(*) as value FROM {kwargs.get('table_name', 'your_table')} WHERE 1=1 {{dateFilter}} GROUP BY department_name ORDER BY COUNT(*) DESC",
                x_field='name',
                y_field='value'
            ),
            
            'status_distribution': ChartConfig(
                id=kwargs.get('id', 'status_distribution'),
                name=kwargs.get('name', 'Distribution by Status'),
                chart_type='pie',
                sql=f"SELECT {kwargs.get('status_field', 'status')} as name, COUNT(*) as value FROM {kwargs.get('table_name', 'your_table')} WHERE 1=1 {{dateFilter}} GROUP BY {kwargs.get('status_field', 'status')}",
                x_field='name',
                y_field='value'
            ),
            
            'monthly_trend': ChartConfig(
                id=kwargs.get('id', 'monthly_trend'),
                name=kwargs.get('name', 'Monthly Trend'),
                chart_type='line',
                sql=f"SELECT FORMAT({kwargs.get('date_field', 'created_at')}, 'yyyy-MM') as name, COUNT(*) as value FROM {kwargs.get('table_name', 'your_table')} WHERE 1=1 {{dateFilter}} GROUP BY FORMAT({kwargs.get('date_field', 'created_at')}, 'yyyy-MM') ORDER BY name",
                x_field='name',
                y_field='value'
            ),
            
            'risk_distribution': ChartConfig(
                id=kwargs.get('id', 'risk_distribution'),
                name=kwargs.get('name', 'Risk Level Distribution'),
                chart_type='bar',
                sql=f"SELECT {kwargs.get('risk_field', 'risk_level')} as name, COUNT(*) as value FROM {kwargs.get('table_name', 'your_table')} WHERE 1=1 {{dateFilter}} GROUP BY {kwargs.get('risk_field', 'risk_level')} ORDER BY COUNT(*) DESC",
                x_field='name',
                y_field='value'
            ),
            
            'category_distribution': ChartConfig(
                id=kwargs.get('id', 'category_distribution'),
                name=kwargs.get('name', 'Category Distribution'),
                chart_type='pie',
                sql=f"SELECT {kwargs.get('category_field', 'category')} as name, COUNT(*) as value FROM {kwargs.get('table_name', 'your_table')} WHERE 1=1 {{dateFilter}} GROUP BY {kwargs.get('category_field', 'category')}",
                x_field='name',
                y_field='value'
            ),
            
            'financial_summary': ChartConfig(
                id=kwargs.get('id', 'financial_summary'),
                name=kwargs.get('name', 'Financial Summary'),
                chart_type='bar',
                sql=f"SELECT {kwargs.get('group_field', 'category')} as name, SUM({kwargs.get('amount_field', 'amount')}) as value FROM {kwargs.get('table_name', 'your_table')} WHERE 1=1 {{dateFilter}} GROUP BY {kwargs.get('group_field', 'category')} ORDER BY SUM({kwargs.get('amount_field', 'amount')}) DESC",
                x_field='name',
                y_field='value'
            )
        }
        
        if template_name not in templates:
            raise ValueError(f"Template '{template_name}' not found. Available templates: {list(templates.keys())}")
        
        return templates[template_name]
    
    @staticmethod
    def get_controls_dashboard_config() -> Dict[str, Any]:
        """Get controls dashboard configuration"""
        return {
            'name': 'Controls Dashboard',
            'table_name': 'dbo.[Controls]',
            'date_field': 'createdAt',
            'metrics': [
                {
                    'id': 'total_controls',
                    'name': 'Total Controls',
                    'sql': 'SELECT COUNT(*) as total FROM dbo.[Controls] WHERE 1=1 {dateFilter}',
                    'color': 'blue',
                    'icon': 'chart-bar'
                },
                {
                    'id': 'pending_preparer',
                    'name': 'Pending Preparer',
                    'sql': 'SELECT COUNT(*) as total FROM dbo.[Controls] WHERE preparerStatus != "approved" AND 1=1 {dateFilter}',
                    'color': 'orange',
                    'icon': 'clock'
                },
                {
                    'id': 'pending_checker',
                    'name': 'Pending Checker',
                    'sql': 'SELECT COUNT(*) as total FROM dbo.[Controls] WHERE checkerStatus != "approved" AND 1=1 {dateFilter}',
                    'color': 'purple',
                    'icon': 'check-circle'
                },
                {
                    'id': 'pending_reviewer',
                    'name': 'Pending Reviewer',
                    'sql': 'SELECT COUNT(*) as total FROM dbo.[Controls] WHERE reviewerStatus != "approved" AND 1=1 {dateFilter}',
                    'color': 'indigo',
                    'icon': 'document-check'
                },
                {
                    'id': 'pending_acceptance',
                    'name': 'Pending Acceptance',
                    'sql': 'SELECT COUNT(*) as total FROM dbo.[Controls] WHERE acceptanceStatus != "approved" AND 1=1 {dateFilter}',
                    'color': 'red',
                    'icon': 'exclamation-triangle'
                }
            ],
            'charts': [
                {
                    'id': 'controls_by_department',
                    'name': 'Controls by Department',
                    'chart_type': 'bar',
                    'sql': 'SELECT department_name as name, COUNT(*) as value FROM dbo.[Controls] WHERE 1=1 {dateFilter} GROUP BY department_name ORDER BY COUNT(*) DESC',
                    'x_field': 'name',
                    'y_field': 'value'
                },
                {
                    'id': 'controls_by_risk_response',
                    'name': 'Controls by Risk Response Type',
                    'chart_type': 'pie',
                    'sql': 'SELECT risk_response as name, COUNT(*) as value FROM dbo.[Controls] WHERE 1=1 {dateFilter} GROUP BY risk_response',
                    'x_field': 'name',
                    'y_field': 'value'
                }
            ],
            'tables': [
                {
                    'id': 'overall_statuses',
                    'name': 'Overall Control Statuses',
                    'sql': 'SELECT id, name, preparerStatus, checkerStatus, reviewerStatus, acceptanceStatus FROM dbo.[Controls] WHERE 1=1 {dateFilter} ORDER BY name',
                    'columns': [
                        {'key': 'id', 'label': 'ID', 'type': 'text'},
                        {'key': 'name', 'label': 'Control Name', 'type': 'text'},
                        {'key': 'preparerStatus', 'label': 'Preparer Status', 'type': 'status'},
                        {'key': 'checkerStatus', 'label': 'Checker Status', 'type': 'status'},
                        {'key': 'reviewerStatus', 'label': 'Reviewer Status', 'type': 'status'},
                        {'key': 'acceptanceStatus', 'label': 'Acceptance Status', 'type': 'status'}
                    ],
                    'pagination': True
                }
            ]
        }
    
    @staticmethod
    def get_incidents_dashboard_config() -> Dict[str, Any]:
        """Get incidents dashboard configuration"""
        return {
            'name': 'Incidents Dashboard',
            'table_name': 'dbo.[Incidents]',
            'date_field': 'createdAt',
            'metrics': [
                {
                    'id': 'total_incidents',
                    'name': 'Total Incidents',
                    'sql': 'SELECT COUNT(*) as total FROM dbo.[Incidents] WHERE 1=1 {dateFilter}',
                    'color': 'red',
                    'icon': 'exclamation-triangle'
                },
                {
                    'id': 'pending_preparer',
                    'name': 'Pending Preparer',
                    'sql': 'SELECT COUNT(*) as total FROM dbo.[Incidents] WHERE preparerStatus != "approved" AND 1=1 {dateFilter}',
                    'color': 'orange',
                    'icon': 'clock'
                },
                {
                    'id': 'total_recovery',
                    'name': 'Total Recovery',
                    'sql': 'SELECT SUM(recovery_amount) as total FROM dbo.[Incidents] WHERE 1=1 {dateFilter}',
                    'color': 'green',
                    'icon': 'currency-dollar'
                }
            ],
            'charts': [
                {
                    'id': 'incidents_by_category',
                    'name': 'Incidents by Category',
                    'chart_type': 'bar',
                    'sql': 'SELECT category_name as name, COUNT(*) as value FROM dbo.[Incidents] WHERE 1=1 {dateFilter} GROUP BY category_name ORDER BY COUNT(*) DESC',
                    'x_field': 'name',
                    'y_field': 'value'
                },
                {
                    'id': 'incidents_by_status',
                    'name': 'Incidents by Status',
                    'chart_type': 'pie',
                    'sql': 'SELECT status as name, COUNT(*) as value FROM dbo.[Incidents] WHERE 1=1 {dateFilter} GROUP BY status',
                    'x_field': 'name',
                    'y_field': 'value'
                }
            ],
            'tables': [
                {
                    'id': 'net_loss_recovery',
                    'name': 'Net Loss and Recovery',
                    'sql': 'SELECT id, title, function_name, net_loss, recovery_amount FROM dbo.[Incidents] WHERE 1=1 {dateFilter} ORDER BY net_loss DESC',
                    'columns': [
                        {'key': 'id', 'label': 'ID', 'type': 'text'},
                        {'key': 'title', 'label': 'Incident Title', 'type': 'text'},
                        {'key': 'function_name', 'label': 'Function', 'type': 'text'},
                        {'key': 'net_loss', 'label': 'Net Loss', 'type': 'currency'},
                        {'key': 'recovery_amount', 'label': 'Recovery Amount', 'type': 'currency'}
                    ],
                    'pagination': True
                }
            ]
        }
    
    @staticmethod
    def get_risks_dashboard_config() -> Dict[str, Any]:
        """Get risks dashboard configuration"""
        return {
            'name': 'Risks Dashboard',
            'table_name': 'dbo.[Risks]',
            'date_field': 'createdAt',
            'metrics': [
                {
                    'id': 'total_risks',
                    'name': 'Total Risks',
                    'sql': 'SELECT COUNT(*) as total FROM dbo.[Risks] WHERE 1=1 {dateFilter}',
                    'color': 'red',
                    'icon': 'exclamation-triangle'
                },
                {
                    'id': 'pending_risks',
                    'name': 'Pending Risks',
                    'sql': 'SELECT COUNT(*) as total FROM dbo.[Risks] WHERE status != "approved" AND 1=1 {dateFilter}',
                    'color': 'yellow',
                    'icon': 'clock'
                },
                {
                    'id': 'approved_risks',
                    'name': 'Approved Risks',
                    'sql': 'SELECT COUNT(*) as total FROM dbo.[Risks] WHERE status = "approved" AND 1=1 {dateFilter}',
                    'color': 'green',
                    'icon': 'check-circle'
                }
            ],
            'charts': [
                {
                    'id': 'risks_by_level',
                    'name': 'Risks by Level',
                    'chart_type': 'bar',
                    'sql': 'SELECT risk_level as name, COUNT(*) as value FROM dbo.[Risks] WHERE 1=1 {dateFilter} GROUP BY risk_level ORDER BY COUNT(*) DESC',
                    'x_field': 'name',
                    'y_field': 'value'
                },
                {
                    'id': 'risks_by_category',
                    'name': 'Risks by Category',
                    'chart_type': 'pie',
                    'sql': 'SELECT category as name, COUNT(*) as value FROM dbo.[Risks] WHERE 1=1 {dateFilter} GROUP BY category',
                    'x_field': 'name',
                    'y_field': 'value'
                }
            ],
            'tables': [
                {
                    'id': 'risk_statuses',
                    'name': 'Risk Statuses',
                    'sql': 'SELECT id, name, status, risk_level FROM dbo.[Risks] WHERE 1=1 {dateFilter} ORDER BY name',
                    'columns': [
                        {'key': 'id', 'label': 'ID', 'type': 'text'},
                        {'key': 'name', 'label': 'Risk Name', 'type': 'text'},
                        {'key': 'status', 'label': 'Status', 'type': 'status'},
                        {'key': 'risk_level', 'label': 'Risk Level', 'type': 'text'}
                    ],
                    'pagination': True
                }
            ]
        }
