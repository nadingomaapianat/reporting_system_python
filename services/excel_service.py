import asyncio
import aiohttp
from typing import Dict, Any, Optional, List
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter
from openpyxl.drawing.image import Image
import io
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

class ExcelService:
    def __init__(self):
        self.api_service = None
        self.database_service = None

    def set_services(self, api_service, database_service):
        self.api_service = api_service
        self.database_service = database_service

    async def generate_excel_report(self, report_data: Dict[str, Any], report_config: Dict[str, Any]) -> bytes:
        """Generate Excel report from data and configuration using reusable functions"""
        try:
            from utils.api_routes import generate_excel_report
            
            # For now, generate a basic report
            data_rows = [['Controls Dashboard Report', 'Generated Successfully']]
            columns = [{'key': 'title', 'label': 'Title'}, {'key': 'status', 'label': 'Status'}]
            
            return generate_excel_report(columns, data_rows, report_config)
            
        except Exception as e:
            print(f"Error generating Excel report: {e}")
            raise e

    async def _add_number_of_controls_per_component_content(self, ws, controls_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add number of controls per component content to Excel worksheet"""
        ws['A4'] = "Number of Controls per Component"
        
        # Get the data
        data = controls_data.get('numberOfControlsPerComponent', [])
        if not data:
            ws['A5'] = "No data available."
            return
        
        # Add headers
        headers = ['Component', 'Controls Count']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=5, column=i, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD') if header_config else '#E3F2FD'
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, end_color=header_bg_color, fill_type='solid')
        
        # Add data rows
        for i, item in enumerate(data, 6):
            ws.cell(row=i, column=1, value=item.get('name', 'N/A'))
            ws.cell(row=i, column=2, value=item.get('value', 0))
        
        # Set column widths
        column_widths = [30, 15]
        for i, width in enumerate(column_widths, 1):
            ws.column_dimensions[get_column_letter(i)].width = width

    async def generate_controls_excel(self, controls_data: Dict[str, Any], startDate: str, endDate: str, header_config: Dict[str, Any], cardType: str = None, onlyCard: bool = False, onlyOverallTable: bool = False, onlyChart: bool = False) -> bytes:
        """Generate Excel report for controls dashboard using normal export pattern"""
        try:
            from utils.api_routes import generate_excel_report
            
            # For chart exports, generate chart image + data table
            if onlyChart and cardType:
                # Get the data from controls_data
                data = controls_data.get(cardType, [])
                
                # Define columns and process data based on cardType
                if cardType in ['numberOfControlsPerComponent', 'quarterlyControlCreationTrend', 'controlsByType', 'department', 'risk', 'antiFraudDistribution', 'controlsPerLevel', 'controlExecutionFrequency', 'numberOfControlsByIcofrStatus', 'numberOfFocusPointsPerPrinciple', 'numberOfFocusPointsPerComponent', 'actionPlansStatus']:
                    # All chart types use the same format: name/value pairs
                    columns = [{'key': 'name', 'label': 'Name'}, {'key': 'value', 'label': 'Value'}]
                    data_rows = []
                    chart_data = []
                    
                    if data and len(data) > 0:  # Only process if data exists
                        for item in data:
                            data_rows.append([
                                item.get('name', 'N/A'),
                                str(item.get('value', 0))
                            ])
                            chart_data.append({
                                'name': item.get('name', 'N/A'),
                                'value': item.get('value', 0)
                            })
                    else:
                        data_rows.append(['No data available', 'No data available'])
                        chart_data = [{'name': 'No data available', 'value': 0}]
                    
                    # Add chart_data to header_config for chart generation
                    header_config['chart_data'] = chart_data
                    
                    # Set chart type based on cardType
                    if cardType == 'risk':
                        header_config['chart_type'] = 'pie'
                    elif cardType == 'quarterlyControlCreationTrend':
                        header_config['chart_type'] = 'line'
                    elif cardType in ['department', 'controlsPerLevel', 'controlExecutionFrequency', 'numberOfFocusPointsPerPrinciple', 'numberOfFocusPointsPerComponent']:
                        header_config['chart_type'] = 'bar'
                    elif cardType in ['controlsByType', 'antiFraudDistribution', 'numberOfControlsByIcofrStatus', 'actionPlansStatus']:
                        header_config['chart_type'] = 'pie'
                    else:
                        header_config['chart_type'] = 'bar'
                else:
                    # Default columns for other chart types
                    columns = [{'key': 'data', 'label': 'Data'}]
                    data_rows = [['No data available']]
                
                return generate_excel_report(columns, data_rows, header_config)
            
            # For table exports, use the normal pattern like other tables
            elif onlyOverallTable and cardType:
                # Get the data from controls_data
                data = controls_data.get(cardType, [])
                
                # Define columns based on cardType (as strings for Excel/PDF generation)
                if cardType == 'controlsNotMappedToAssertions':
                    columns = ['#', 'Control Name', 'Function Name']
                    # Convert data to rows format
                    data_rows = []
                    if data:  # Only process if data exists
                        for i, item in enumerate(data, 1):
                            data_rows.append([
                                str(i),
                                item.get('Control Name', 'N/A'),
                                item.get('Function Name', 'N/A')
                            ])
                    else:
                        # If no data from API, try to fetch directly from database
                        try:
                            from services.database_service import DatabaseService
                            db_service = DatabaseService()
                            query = '''
                            SELECT 
                                c.name AS [Control Name], 
                                f.name AS [Function Name]
                            FROM [NEWDCC-V4-UAT].dbo.Controls c
                            LEFT JOIN [NEWDCC-V4-UAT].dbo.ControlFunctions cf ON cf.control_id = c.id 
                            LEFT JOIN [NEWDCC-V4-UAT].dbo.Functions f ON f.id = cf.function_id 
                            WHERE c.icof_id IS NULL AND c.isDeleted = 0
                            ORDER BY c.createdAt DESC
                            '''
                            db_data = await db_service.execute_query(query)
                            if db_data:
                                for i, item in enumerate(db_data, 1):
                                    data_rows.append([
                                        str(i),
                                        item.get('Control Name', 'N/A'),
                                        item.get('Function Name', 'N/A')
                                    ])
                            else:
                                data_rows.append(['1', 'No data available', 'No data available'])
                        except Exception as e:
                            print(f"DEBUG: Error fetching from database: {e}")
                            data_rows.append(['1', 'No data available', 'No data available'])
                        
                elif cardType == 'controlsNotMappedToPrinciples':
                    columns = ['#', 'Control Name', 'Function Name']
                    # Convert data to rows format
                    data_rows = []
                    if data:  # Only process if data exists
                        for i, item in enumerate(data, 1):
                            data_rows.append([
                                str(i),
                                item.get('Control Name', 'N/A'),
                                item.get('Function Name', 'N/A')
                            ])
                    else:
                        # If no data from API, try to fetch directly from database
                        try:
                            from services.database_service import DatabaseService
                            db_service = DatabaseService()
                            query = '''
                            SELECT 
                                c.name AS [Control Name], 
                                f.name AS [Function Name]
                            FROM [NEWDCC-V4-UAT].dbo.Controls c
                            LEFT JOIN [NEWDCC-V4-UAT].dbo.ControlFunctions cf ON cf.control_id = c.id 
                            LEFT JOIN [NEWDCC-V4-UAT].dbo.Functions f ON f.id = cf.function_id 
                            LEFT JOIN [NEWDCC-V4-UAT].dbo.ControlCosos ccx ON ccx.control_id = c.id AND ccx.deletedAt IS NULL 
                            WHERE ccx.control_id IS NULL AND c.isDeleted = 0
                            ORDER BY c.createdAt DESC
                            '''
                            db_data = await db_service.execute_query(query)
                            if db_data:
                                for i, item in enumerate(db_data, 1):
                                    data_rows.append([
                                        str(i),
                                        item.get('Control Name', 'N/A'),
                                        item.get('Function Name', 'N/A')
                                    ])
                            else:
                                data_rows.append(['1', 'No data available', 'No data available'])
                        except Exception as e:
                            print(f"DEBUG: Error fetching from database: {e}")
                            data_rows.append(['1', 'No data available', 'No data available'])
                elif cardType == 'controlsTestingApprovalCycle':
                    columns = [{'key': 'index', 'label': '#'}, {'key': 'Code', 'label': 'Code'}, {'key': 'Control Name', 'label': 'Control Name'}, {'key': 'Business Unit', 'label': 'Business Unit'}, {'key': 'Preparer Status', 'label': 'Preparer Status'}, {'key': 'Checker Status', 'label': 'Checker Status'}, {'key': 'Reviewer Status', 'label': 'Reviewer Status'}, {'key': 'Acceptance Status', 'label': 'Acceptance Status'}]
                    # Convert data to rows format
                    data_rows = []
                    for i, item in enumerate(data, 1):
                        data_rows.append([
                            str(i),
                            item.get('Code', 'N/A'),
                            item.get('Control Name', 'N/A'),
                            item.get('Business Unit', 'N/A'),
                            item.get('Preparer Status', 'N/A'),
                            item.get('Checker Status', 'N/A'),
                            item.get('Reviewer Status', 'N/A'),
                            item.get('Acceptance Status', 'N/A')
                        ])
                else:
                    # Default columns for other table types
                    columns = [{'key': 'data', 'label': 'Data'}]
                    data_rows = [['No data available']]
                
                return generate_excel_report(columns, data_rows, header_config)
            
            elif onlyCard and cardType:
                # Generic card-only rendering: table from list of dicts
                data = controls_data.get(cardType, [])
                if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                    columns = ['#'] + list(data[0].keys())
                    data_rows = []
                    for i, item in enumerate(data, 1):
                        row = [str(i)] + [str(item.get(col, '')) for col in columns[1:]]
                        data_rows.append(row)
                else:
                    columns = ['Data']
                    data_rows = [['No data available']]
                return generate_excel_report(columns, data_rows, header_config)
            
            else:
                # Generate basic report for other cases
                from openpyxl import Workbook
                wb = Workbook()
                ws = wb.active
                ws.title = header_config.get('title', 'Controls Report')
                ws['A1'] = 'Controls Dashboard Report'
                ws['A2'] = 'Generated Successfully'
                
                from io import BytesIO
                output = BytesIO()
                wb.save(output)
                return output.getvalue()
        except Exception as e:
            print(f"Error generating controls Excel: {e}")
            raise e
    
    async def _add_controls_card_content(self, ws, card_type: str, controls_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add content for different control card types"""
        if card_type == 'actionPlansStatus':
            await self._add_action_plans_status_content(ws, controls_data, header_config)
        elif card_type == 'numberOfControlsPerComponent':
            await self._add_number_of_controls_per_component_content(ws, controls_data, header_config)
        elif card_type == 'controlsNotMappedToAssertions':
            await self._add_controls_not_mapped_to_assertions_content(ws, controls_data, header_config)
        elif card_type == 'controlsNotMappedToPrinciples':
            await self._add_controls_not_mapped_to_principles_content(ws, controls_data, header_config)

    async def _add_action_plans_status_content(self, ws, controls_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add action plans status content to Excel worksheet"""
        ws['A4'] = "Action Plans Status"
        
        # Get the data
        data = controls_data.get('actionPlansStatus', [])
        if not data:
            ws['A5'] = "No data available."
            return
        
        # Add headers
        headers = ['Status', 'Count']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=5, column=i, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, end_color=header_bg_color, fill_type='solid')
        
        # Add data rows
        for i, item in enumerate(data, 6):
            ws.cell(row=i, column=1, value=item.get('status', 'N/A'))
            ws.cell(row=i, column=2, value=item.get('value', 0))
        
        # Set column widths
        column_widths = [30, 20]
        for i, width in enumerate(column_widths, 1):
            ws.column_dimensions[get_column_letter(i)].width = width

    async def _add_controls_not_mapped_to_assertions_content(self, ws, controls_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add controls not mapped to assertions content to Excel worksheet"""
        ws['A4'] = "Controls not mapped to any Assertions"
        
        # Get the data
        data = controls_data.get('controlsNotMappedToAssertions', [])
        if not data:
            ws['A5'] = "No data available."
            return
        
        # Add headers
        headers = ['#', 'Control Name', 'Department']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=5, column=i, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, end_color=header_bg_color, fill_type='solid')
        
        # Add data rows
        for i, item in enumerate(data, 6):
            ws.cell(row=i, column=1, value=i-5)  # Index
            ws.cell(row=i, column=2, value=item.get('Control Name', 'N/A'))
            ws.cell(row=i, column=3, value=item.get('Department', 'N/A'))
        
        # Set column widths
        column_widths = [5, 50, 20]
        for i, width in enumerate(column_widths, 1):
            ws.column_dimensions[get_column_letter(i)].width = width

    async def _add_controls_not_mapped_to_principles_content(self, ws, controls_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add controls not mapped to principles content to Excel worksheet"""
        ws['A4'] = "Controls not mapped to any Principles"
        
        # Get the data
        data = controls_data.get('controlsNotMappedToPrinciples', [])
        if not data:
            ws['A5'] = "No data available."
            return
        
        # Add headers
        headers = ['#', 'Control Name', 'Function Name']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=5, column=i, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, end_color=header_bg_color, fill_type='solid')
        
        # Add data rows
        for i, item in enumerate(data, 6):
            ws.cell(row=i, column=1, value=i-5)  # Index
            ws.cell(row=i, column=2, value=item.get('Control Name', 'N/A'))
            ws.cell(row=i, column=3, value=item.get('Function Name', 'N/A'))
        
        # Set column widths
        column_widths = [5, 50, 20]
        for i, width in enumerate(column_widths, 1):
            ws.column_dimensions[get_column_letter(i)].width = width

    async def generate_risks_excel(self, risks_data: Dict[str, Any], start_date: str, end_date: str, header_config: Dict[str, Any], card_type: str = None, only_card: bool = False) -> bytes:
        """Generate risks Excel report"""
        try:
            from utils.api_routes import generate_excel_report
            
            if only_card and card_type:
                # Generate card-specific report
                if card_type in risks_data:
                    data = risks_data[card_type]
                    if isinstance(data, list) and len(data) > 0:
                        # Get column names from first item
                        columns = list(data[0].keys()) if data else ['No Data']
                        data_rows = []
                        for i, item in enumerate(data, 1):
                            data_rows.append([str(i)] + [str(item.get(col, 'N/A')) for col in columns])
                        columns = ['#'] + columns
                    else:
                        columns = ['Data']
                        data_rows = [['No data available']]
                else:
                    columns = ['Data']
                    data_rows = [['No data available']]
            else:
                # Generate basic report for other cases
                data_rows = [['Risks Dashboard Report', 'Generated Successfully']]
                columns = [{'key': 'title', 'label': 'Title'}, {'key': 'status', 'label': 'Status'}]
            
            return generate_excel_report(columns, data_rows, header_config)
                
        except Exception as e:
            print(f"Error generating risks Excel: {e}")
            raise e