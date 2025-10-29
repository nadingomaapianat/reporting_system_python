import asyncio
import aiohttp
from typing import Dict, Any, Optional, List
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.units import inch
from reportlab.platypus import Image
from reportlab.lib.utils import ImageReader
import io
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
from datetime import datetime

# Import PDF utilities
from utils.pdf_utils import shape_text_for_arabic, generate_pdf_report

def write_debug(msg):
    """Write debug message to file and stderr with timestamp"""
    from datetime import datetime
    timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    msg_with_time = f"[{timestamp}] {msg}"
    with open('debug_log.txt', 'a', encoding='utf-8') as f:
        f.write(f"{msg_with_time}\n")
        f.flush()
    import sys
    sys.stderr.write(f"{msg_with_time}\n")
    sys.stderr.flush()

class PDFService:
    def __init__(self):
        self.api_service = None
        self.database_service = None

    def set_services(self, api_service, database_service):
        self.api_service = api_service
        self.database_service = database_service

    async def generate_pdf_report(self, report_data: Dict[str, Any], report_config: Dict[str, Any]) -> bytes:
        """Generate PDF report from data and configuration"""
        try:
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=A4)
            content = []
            
            # Add title
            styles = getSampleStyleSheet()
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=16,
                spaceAfter=30,
                alignment=1
            )
            
            content.append(Paragraph("Controls Dashboard Report", title_style))
            content.append(Spacer(1, 20))
            
            # Add basic content
            content.append(Paragraph("Report generated successfully.", styles['Normal']))
            
            doc.build(content)
            buffer.seek(0)
            
            return buffer.getvalue()
            
        except Exception as e:
            print(f"Error generating PDF report: {e}")
            raise e


    async def generate_controls_pdf(self, controls_data: Dict[str, Any], startDate: str, endDate: str, header_config: Dict[str, Any], cardType: str = None, onlyCard: bool = False, onlyOverallTable: bool = False, onlyChart: bool = False) -> bytes:
        
        """
        Generate PDF report for controls dashboard.
        Simplified to use provided data instead of fetching or reconstructing.
        Each cardType data is passed directly from caller.
        """
        # generate_pdf_report is already imported at top of file from pdf_utils
        import re
        write_debug(f"DEBUG: ===== generate_controls_pdf START =====")

        try:
            # Basic info logs
            write_debug(f"  - cardType: {cardType}")
            write_debug(f"  - onlyCard: {onlyCard}")
            write_debug(f"  - onlyChart: {onlyChart}")
            write_debug(f"  - onlyOverallTable: {onlyOverallTable}")


            if not controls_data:
                raise ValueError("No controls_data provided")

            data = controls_data.get(cardType, [])
            
            # Safety defaults
            columns = []
            data_rows = []

            # CHART EXPORT
            if onlyChart and cardType:
                chart_data = {"labels": [], "values": []}

                if isinstance(data, list) and data:
                    # Dynamically extract keys from the first item
                    first_item = data[0] if data else {}
                    if isinstance(first_item, dict):
                        keys = list(first_item.keys())
                        
                        # For most queries, we expect 2 columns: a label/key and a count/value
                        if len(keys) >= 2:
                            # First key is usually the label/name column
                            # Last key is usually the count/value column
                            key1 = keys[0]
                            key2 = keys[-1]
                            
                            # Create human-readable column headers
                            def format_column_name(key):
                                # Convert snake_case or camelCase to Title Case
                                # Replace underscores/camelCase with spaces
                                formatted = re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', key)
                                return formatted.title()
                            
                            label1 = format_column_name(key1)
                            label2 = format_column_name(key2)
                            columns = [label1, label2]
                            
                            
                            
                            for item in data:
                                name = item.get(key1, "N/A")
                                value = item.get(key2, 0)
                                chart_data["labels"].append(name)
                                chart_data["values"].append(value)
                                data_rows.append([name, str(value)])
                        else:
                            # Fallback if unexpected structure: build single-column names safely
                            write_debug(f"  - Unexpected key count: {len(keys)}, using first key only")
                            key1 = keys[0]
                            columns = [format_column_name(key1), "Value"]
                            # Do NOT append duplicate rows; leave values unknown
                            for item in data:
                                name = item.get(key1, "N/A")
                                chart_data["labels"].append(name)
                                # Keep value as 0 to avoid N/A duplicates in table
                                chart_data["values"].append(0)
                                data_rows.append([name, 0])
                    else:
                        
                        data_rows.append(["No data available", "0"])
                        columns = ["Label", "Value"]
                else:
                    data_rows.append(["No data available", "0"])
                    columns = ["Label", "Value"]
                
                # Decide chart type: header_config overrides mapping
                chart_type_override = None
                try:
                    chart_type_override = header_config.get("chartType") or header_config.get("chart_type")
                except Exception:
                    chart_type_override = None

                # Map cardType to default chart types (matching frontend config)
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

                # If override is not a known matplotlib type, fall back to mapping
                valid_types = {"bar", "line", "pie"}
                if chart_type_override and chart_type_override in valid_types:
                    resolved_chart_type = chart_type_override
                else:
                    # Also handle when chartType was sent as a card key (e.g., 'quarterlyControlCreationTrend')
                    resolved_chart_type = default_type_by_card.get(cardType or chart_type_override, "bar")

                header_config["chart_data"] = chart_data
                header_config["chart_type"] = resolved_chart_type
            
            # TABLE EXPORT
            elif onlyOverallTable:
                write_debug(f"DEBUG: ===== onlyOverallTable START =====")
                write_debug(f"  - data type: {type(data)}")
                
                # Generic table builder: derive columns from keys of first row and add index column
                if isinstance(data, list) and len(data) > 0:
                    first_item = data[0]
                    if isinstance(first_item, dict):
                        raw_keys = list(first_item.keys())
                        # Human-readable column labels
                        def nice(k: str) -> str:
                            return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(k)).title()
                        columns = ['#'] + [nice(k) for k in raw_keys]
                        data_rows = []
                        for i, row in enumerate(data, 1):
                            values = [str(row.get(k, '')) for k in raw_keys]
                            data_rows.append([str(i)] + values)
                    elif isinstance(first_item, (list, tuple)):
                        # If rows are arrays, label columns generically C1..Cn
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

            # CARD SUMMARY EXPORT
            elif onlyCard and cardType:
                write_debug(f"DEBUG: ===== onlyCard START =====")
                write_debug(f"  - data: {data}")
                write_debug(f"  - data type: {type(data)}")
                
                # Handle both dict and list data
                if isinstance(data, list) and data:
                    first_item = data[0] if data else {}
                    
                    # Check if data is list of strings (for totalControls)
                    if isinstance(first_item, str):
                        write_debug(f"  - Data is list of strings")
                        write_debug(f"  - Creating columns: ['#', 'Control Name']")
                        columns = ["#", "Control Name"]
                        data_rows = []
                        for i, item in enumerate(data, 1):
                            data_rows.append([
                                str(i),
                                str(item)  # Just the control name
                            ])
                        write_debug(f"  - Created {len(data_rows)} rows")
                    elif isinstance(first_item, dict):
                        # Data is list of dicts
                        write_debug(f"  - Data is list of dicts")
                        write_debug(f"  - first_item keys: {list(first_item.keys())}")
                        # Determine columns based on cardType
                        if cardType in ['pendingPreparer', 'pendingChecker', 'pendingReviewer', 'pendingAcceptance', 'testsPendingPreparer', 'testsPendingChecker', 'testsPendingReviewer', 'testsPendingAcceptance','unmappedControls','unmappedIcofrControls','unmappedNonIcofrControls','totalControls']:
                            write_debug(f"  - Creating columns: ['#', 'Control Code', 'Control Name']")
                            columns = ["#", "Control Code", "Control Name"]
                            data_rows = []
                            for i, item in enumerate(data, 1):
                                data_rows.append([
                                    str(i),
                                    item.get('control_code', item.get('code', 'N/A')),
                                    item.get('control_name', item.get('name', 'N/A'))
                                ])
                            write_debug(f"  - Created {len(data_rows)} rows with columns: {columns}")
                        else:
                            # Generic handling for other card types
                            write_debug(f"  - Generic handling")
                            columns = ["#", "Code", "Name"]
                            data_rows = []
                            for i, item in enumerate(data, 1):
                                data_rows.append([
                                    str(i),
                                    item.get('code', 'N/A'),
                                    item.get('name', 'N/A')
                                ])
                            write_debug(f"  - Created {len(data_rows)} rows with columns: {columns}")
                elif isinstance(data, dict):
                    columns = ["Metric", "Value"]
                    data_rows = [[key, str(value)] for key, value in data.items()]
                else:
                    columns = ["Metric", "Value"]
                    data_rows = [["No data available", "N/A"]]

            # Apply default PDF design settings before generation
            # Uses centralized configuration system from utils/export_utils.py
            # All defaults are defined in ONE place - modify them there!
            
            from utils.export_utils import merge_header_config
            
            # Merge user config with centralized defaults
            final_config = merge_header_config(header_config, "controls")
            
            write_debug(f"DEBUG: Using PDF config: {final_config}")
            
            # Generate PDF with merged config
            result = generate_pdf_report(columns, data_rows, final_config)
            if not result:
                raise ValueError("PDF generation returned None")

            write_debug(f"DEBUG: PDF generated successfully for {cardType}")
            return result
                
        except Exception as e:
            write_debug(f"ERROR: Failed to generate PDF for {cardType} - {str(e)}")
            import traceback
            traceback.print_exc()
            raise e

        

    async def generate_risks_pdf(self, risks_data: Dict[str, Any], start_date: str, end_date: str, header_config: Dict[str, Any], card_type: str = None, only_card: bool = False) -> bytes:
        """Generate risks PDF report using the same reusable function as Controls dashboard"""
        try:
            from pdf_report_utils import generate_pdf_report
            # Ensure a proper risks header if none was provided
            if not header_config:
                from export_utils import get_default_header_config
                header_config = get_default_header_config("risks")
            
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
            
            return generate_pdf_report(columns, data_rows, header_config)
                
        except Exception as e:
            print(f"Error generating risks PDF: {e}")
            # Fallback to simple PDF generation
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=A4)
            content = [Paragraph("Error generating report", getSampleStyleSheet()['Normal'])]
            doc.build(content)
            buffer.seek(0)
            return buffer.getvalue()
