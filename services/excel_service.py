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
            from routes.route_utils import generate_excel_report
            
            # For now, generate a basic report
            data_rows = [['Controls Dashboard Report', 'Generated Successfully']]
            columns = ['Title', 'Status']
            
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
            if isinstance(item, dict):
                ws.cell(row=i, column=1, value=item.get('name', 'N/A'))
                ws.cell(row=i, column=2, value=item.get('value', 0))
            else:
                # Handle list/tuple format
                ws.cell(row=i, column=1, value=str(item[0]) if isinstance(item, (list, tuple)) and len(item) > 0 else 'N/A')
                ws.cell(row=i, column=2, value=str(item[1]) if isinstance(item, (list, tuple)) and len(item) > 1 else 0)
        
        # Set column widths
        column_widths = [30, 15]
        for i, width in enumerate(column_widths, 1):
            ws.column_dimensions[get_column_letter(i)].width = width

    async def generate_controls_excel(self, controls_data: Dict[str, Any], startDate: str, endDate: str, header_config: Dict[str, Any], cardType: str = None, onlyCard: bool = False, onlyOverallTable: bool = False, onlyChart: bool = False) -> bytes:
        """Generate Excel report for controls dashboard - matches PDF format exactly"""
        try:
            from routes.route_utils import generate_excel_report
            import re
            
            # Get the data from controls_data
            data = controls_data.get(cardType, []) if controls_data else []
            from routes.route_utils import write_debug
            write_debug(f"Excel export - cardType={cardType}, data type={type(data)}, len={len(data) if isinstance(data, list) else 'N/A'}")
            
            columns = []
            data_rows = []
            
            # Define column name formatter (same as PDF)
            def format_column_name(key):
                """Convert snake_case or camelCase to Title Case"""
                formatted = re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(key))
                return formatted.title()
            
            # CHART EXPORT - same logic as PDF
            if onlyChart and cardType:
                if isinstance(data, list) and data:
                    first_item = data[0] if data else {}
                    if isinstance(first_item, dict):
                        keys = list(first_item.keys())
                        if len(keys) >= 2:
                            key1 = keys[0]
                            key2 = keys[-1]
                            label1 = format_column_name(key1)
                            label2 = format_column_name(key2)
                            columns = [label1, label2]
                            
                            for item in data:
                                name = item.get(key1, "N/A")
                                value = item.get(key2, 0)
                                data_rows.append([name, str(value)])
                        else:
                            key1 = keys[0]
                            columns = [format_column_name(key1), "Value"]
                            for item in data:
                                name = item.get(key1, "N/A")
                                data_rows.append([name, 0])
                else:
                    data_rows = [["No data available", "0"]]
                    columns = ["Label", "Value"]
                
                # Add chart type configuration (same as PDF)
                default_type_by_card = {
                    "quarterlyControlCreationTrend": "line",
                    "controlsByType": "pie",
                    "antiFraudDistribution": "pie",
                    "controlsPerLevel": "bar",
                    "controlExecutionFrequency": "bar",
                    "numberOfControlsPerComponent": "bar",
                    "departmentDistribution": "bar",
                    "numberOfControlsByIcofrStatus": "pie",
                }
                
                # Priority: renderType/chartType from query > cardType default > "bar"
                chart_type = header_config.get("chartType") or header_config.get("chart_type")
                if chart_type and chart_type in {"bar", "line", "pie"}:
                    write_debug(f"Using provided chart type: {chart_type}")
                else:
                    chart_type = default_type_by_card.get(cardType, "bar")
                    write_debug(f"Using default chart type for {cardType}: {chart_type}")
                
                header_config['chart_type'] = chart_type
                
                # Extract chart data for visualization (labels and values from data_rows)
                chart_labels = []
                chart_values = []
                for row in data_rows:
                    if len(row) >= 2:
                        chart_labels.append(str(row[0]))  # First column (label)
                        try:
                            chart_values.append(float(row[1]))  # Second column (value)
                        except:
                            chart_values.append(0)
                
                # Add chart data to header_config
                if len(chart_labels) > 0 and len(chart_values) > 0:
                    header_config['chart_data'] = {
                        'labels': chart_labels,
                        'values': chart_values
                    }
                    write_debug(f"Chart export: Prepared chart_data with {len(chart_labels)} items, type={chart_type}")
                
                return generate_excel_report(columns, data_rows, header_config)
            
            # TABLE EXPORT - same logic as PDF (dynamic column extraction)
            elif onlyOverallTable and cardType:
                # Generic table builder: derive columns from keys of first row and add index column
                if isinstance(data, list) and len(data) > 0:
                    first_item = data[0]
                    if isinstance(first_item, dict):
                        raw_keys = list(first_item.keys())
                        columns = ['#'] + [format_column_name(k) for k in raw_keys]
                        data_rows = []
                        for i, row in enumerate(data, 1):
                            if isinstance(row, dict):
                                values = [str(row.get(k, '')) for k in raw_keys]
                                data_rows.append([str(i)] + values)
                            elif isinstance(row, (list, tuple)):
                                vals = [str(v) for v in row]
                                data_rows.append([str(i)] + vals)
                            else:
                                data_rows.append([str(i), str(row)])
                    elif isinstance(first_item, (list, tuple)):
                        num_cols = len(first_item)
                        columns = ['#'] + [f'C{idx+1}' for idx in range(num_cols)]
                        data_rows = []
                        for i, row in enumerate(data, 1):
                            vals = [str(v) for v in (row if isinstance(row, (list, tuple)) else [row])]
                            data_rows.append([str(i)] + vals)
                    else:
                        columns = ['#', 'Value']
                        data_rows = [[str(i+1), str(v)] for i, v in enumerate(data)]
                else:
                    columns = ['#', 'Value']
                    data_rows = [['1', 'No data available']]
                
                write_debug(f"About to call generate_excel_report with chart_data={header_config.get('chart_data') is not None}")
                write_debug(f"header_config keys: {list(header_config.keys())}")
                result = generate_excel_report(columns, data_rows, header_config)
                write_debug(f"Excel report generated, returning {len(result) if result else 0} bytes")
                return result
            
            # CARD SUMMARY EXPORT - same logic as PDF
            elif onlyCard and cardType:
                # Handle both dict and list data
                if isinstance(data, list) and data:
                    first_item = data[0] if data else {}
                    
                    if isinstance(first_item, str):
                        columns = ["#", "Control Name"]
                        data_rows = []
                        for i, item in enumerate(data, 1):
                            data_rows.append([str(i), str(item)])
                    elif isinstance(first_item, dict):
                        if cardType in ['pendingPreparer', 'pendingChecker', 'pendingReviewer', 'pendingAcceptance', 'testsPendingPreparer', 'testsPendingChecker', 'testsPendingReviewer', 'testsPendingAcceptance','unmappedControls','unmappedIcofrControls','unmappedNonIcofrControls','totalControls']:
                            columns = ["#", "Control Code", "Control Name"]
                            data_rows = []
                            for i, item in enumerate(data, 1):
                                if isinstance(item, dict):
                                    data_rows.append([
                                        str(i),
                                        item.get('control_code', item.get('code', 'N/A')),
                                        item.get('control_name', item.get('name', 'N/A'))
                                    ])
                                else:
                                    data_rows.append([str(i), 'N/A', 'N/A'])
                        else:
                            columns = ["#", "Code", "Name"]
                            data_rows = []
                            for i, item in enumerate(data, 1):
                                if isinstance(item, dict):
                                    data_rows.append([
                                        str(i),
                                        item.get('code', 'N/A'),
                                        item.get('name', 'N/A')
                                    ])
                                else:
                                    data_rows.append([str(i), 'N/A', 'N/A'])
                elif isinstance(data, dict):
                    columns = ["Metric", "Value"]
                    data_rows = [[key, str(value)] for key, value in data.items()]
                else:
                    columns = ["Metric", "Value"]
                    data_rows = [["No data available", "N/A"]]
                
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
            if isinstance(item, dict):
                ws.cell(row=i, column=1, value=item.get('status', 'N/A'))
                ws.cell(row=i, column=2, value=item.get('value', 0))
            else:
                # Handle list/tuple format
                ws.cell(row=i, column=1, value=str(item[0]) if isinstance(item, (list, tuple)) and len(item) > 0 else 'N/A')
                ws.cell(row=i, column=2, value=str(item[1]) if isinstance(item, (list, tuple)) and len(item) > 1 else 0)
        
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
            if isinstance(item, dict):
                ws.cell(row=i, column=2, value=item.get('Control Name', 'N/A'))
                ws.cell(row=i, column=3, value=item.get('Department', 'N/A'))
            elif isinstance(item, (list, tuple)):
                ws.cell(row=i, column=2, value=str(item[0]) if len(item) > 0 else 'N/A')
                ws.cell(row=i, column=3, value=str(item[1]) if len(item) > 1 else 'N/A')
            else:
                ws.cell(row=i, column=2, value='N/A')
                ws.cell(row=i, column=3, value='N/A')
        
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
            if isinstance(item, dict):
                ws.cell(row=i, column=2, value=item.get('Control Name', 'N/A'))
                ws.cell(row=i, column=3, value=item.get('Function Name', 'N/A'))
            elif isinstance(item, (list, tuple)):
                ws.cell(row=i, column=2, value=str(item[0]) if len(item) > 0 else 'N/A')
                ws.cell(row=i, column=3, value=str(item[1]) if len(item) > 1 else 'N/A')
            else:
                ws.cell(row=i, column=2, value='N/A')
                ws.cell(row=i, column=3, value='N/A')
        
        # Set column widths
        column_widths = [5, 50, 20]
        for i, width in enumerate(column_widths, 1):
            ws.column_dimensions[get_column_letter(i)].width = width

    async def generate_risks_excel(self, risks_data: Dict[str, Any], start_date: str, end_date: str, header_config: Dict[str, Any], card_type: str = None, only_card: bool = False) -> bytes:
        """Generate risks Excel report"""
        try:
            from routes.route_utils import generate_excel_report
            
            if only_card and card_type:
                # Generate card-specific report
                if card_type in risks_data:
                    data = risks_data[card_type]
                    if isinstance(data, list) and len(data) > 0:
                        # Get column names from first item
                        if isinstance(data[0], dict):
                            columns = list(data[0].keys()) if data else ['No Data']
                            data_rows = []
                            for i, item in enumerate(data, 1):
                                if isinstance(item, dict):
                                    data_rows.append([str(i)] + [str(item.get(col, 'N/A')) for col in columns])
                                elif isinstance(item, (list, tuple)):
                                    data_rows.append([str(i)] + [str(val) for val in item])
                        else:
                            columns = ['Data']
                            data_rows = [['No data available']]
                        columns = ['#'] + columns if len(columns) > 0 else columns
                    else:
                        columns = ['Data']
                        data_rows = [['No data available']]
                else:
                    columns = ['Data']
                    data_rows = [['No data available']]
            else:
                # Generate basic report for other cases
                data_rows = [['Risks Dashboard Report', 'Generated Successfully']]
                columns = ['Title', 'Status']
            
            return generate_excel_report(columns, data_rows, header_config)
                
        except Exception as e:
            print(f"Error generating risks Excel: {e}")
            raise e