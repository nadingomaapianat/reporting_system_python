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

    async def generate_incidents_pdf(self, incidents_data: Dict[str, Any], start_date: str, end_date: str, header_config: Dict[str, Any], card_type: str = None, only_card: bool = False, only_overall_table: bool = False, only_chart: bool = False) -> bytes:
        """Generate incidents PDF report mirroring risks/controls behavior."""
        import re
        write_debug("DEBUG: ===== generate_incidents_pdf START =====")
        try:
            if not header_config:
                from utils.export_utils import get_default_header_config
                header_config = get_default_header_config("incidents")

            write_debug(f"  - card_type: {card_type}")
            write_debug(f"  - only_card: {only_card}")
            write_debug(f"  - only_chart: {only_chart}")
            write_debug(f"  - only_overall_table: {only_overall_table}")

            if not incidents_data:
                raise ValueError("No incidents_data provided")

            # Resolve data by card_type
            data = []
            if card_type:
                data = incidents_data.get(card_type) or incidents_data.get('list') or []

            columns: List[str] = []
            data_rows: List[List[str]] = []

            # CHART EXPORT
            if only_chart and card_type:
                chart_data = {"labels": [], "values": []}
                if isinstance(data, list) and data:
                    first_item = data[0]
                    if isinstance(first_item, dict):
                        keys = list(first_item.keys())
                        if len(keys) >= 2:
                            key1 = keys[0]
                            key2 = keys[-1]
                            def fmt(k: str) -> str:
                                return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', k).title()
                            columns = [fmt(key1), fmt(key2)]
                            for item in data:
                                name = item.get(key1, "N/A")
                                value = item.get(key2, 0)
                                chart_data["labels"].append(name)
                                chart_data["values"].append(value)
                                data_rows.append([name, str(value)])
                        else:
                            columns = ["Label", "Value"]
                            for item in data:
                                name = str(item)
                                chart_data["labels"].append(name)
                                chart_data["values"].append(0)
                                data_rows.append([name, "0"])
                    else:
                        columns = ["Label", "Value"]
                        data_rows.append([str(first_item), "0"])
                else:
                    columns = ["Label", "Value"]
                    data_rows.append(["No data available", "0"])

                default_type_by_card = {
                    "byCategory": "bar",
                    "byStatus": "pie",
                    "monthlyTrend": "line",
                    "netLossAndRecovery": "bar",
                    "topFinancialImpacts": "bar",
                    "incidentsByEventType": "pie",
                    "incidentsByFinancialImpact": "pie",
                }
                chart_type_override = None
                try:
                    chart_type_override = header_config.get("chartType") or header_config.get("chart_type")
                except Exception:
                    chart_type_override = None
                valid_types = {"bar", "line", "pie"}
                if chart_type_override in valid_types:
                    resolved_chart_type = chart_type_override
                else:
                    resolved_chart_type = default_type_by_card.get(card_type or chart_type_override, "bar")
                header_config["chart_data"] = chart_data
                header_config["chart_type"] = resolved_chart_type

            # TABLE EXPORT
            elif only_overall_table:
                write_debug("DEBUG: ===== incidents only_overall_table START =====")
                # Use incidents list for overall table
                table_rows = []
                if card_type == 'overallStatuses':
                    table_rows = incidents_data.get('overallStatuses') or incidents_data.get('statusOverview') or []
                else:
                    table_rows = incidents_data.get(card_type) or []

                if isinstance(table_rows, list) and table_rows:
                    first_item = table_rows[0]
                    if isinstance(first_item, dict):
                        raw_keys = list(first_item.keys())
                        def nice(k: str) -> str:
                            return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(k)).title()
                        columns = ['#'] + [nice(k) for k in raw_keys]
                        for i, row in enumerate(table_rows, 1):
                            values = [str(row.get(k, '')) for k in raw_keys]
                            data_rows.append([str(i)] + values)
                    elif isinstance(first_item, (list, tuple)):
                        num_cols = len(first_item)
                        columns = ['#'] + [f'C{idx+1}' for idx in range(num_cols)]
                        for i, row in enumerate(table_rows, 1):
                            vals = [str(v) for v in (row if isinstance(row, (list, tuple)) else [row])]
                            data_rows.append([str(i)] + vals)
                    else:
                        columns = ['#', 'Value']
                        data_rows = [["1", str(first_item)]]
                else:
                    columns = ['#', 'Value']
                    data_rows = [["1", 'No data available']]

            # CARD SUMMARY EXPORT
            elif only_card and card_type:
                write_debug("DEBUG: ===== incidents only_card START =====")
                # Build a simple table from the card data
                if isinstance(data, list) and data:
                    first_item = data[0]
                    if isinstance(first_item, dict):
                        def nice(k: str) -> str:
                            return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(k)).title()
                        raw_keys = list(first_item.keys())
                        columns = ['#'] + [nice(k) for k in raw_keys]
                        for i, item in enumerate(data, 1):
                            values = [str(item.get(k, 'N/A')) for k in raw_keys]
                            data_rows.append([str(i)] + values)
                    else:
                        columns = ['#', 'Value']
                        data_rows = [["1", str(first_item)]]
                elif isinstance(data, dict):
                    columns = ["Metric", "Value"]
                    data_rows = [[k, str(v)] for k, v in data.items()]
                else:
                    columns = ["Metric", "Value"]
                    data_rows = [["No data available", "N/A"]]
            else:
                # Default simple report
                data_rows = [['Incidents Dashboard Report', 'Generated Successfully']]
                columns = ['Title', 'Status']

            # Merge header config for incidents
            from utils.export_utils import merge_header_config
            final_config = merge_header_config(header_config, "incidents")
            write_debug(f"DEBUG: Using PDF config (incidents): {final_config}")

            result = generate_pdf_report(columns, data_rows, final_config)
            if not result:
                raise ValueError("PDF generation returned None")
            write_debug(f"DEBUG: Incidents PDF generated successfully for {card_type}")
            return result

        except Exception as e:
            write_debug(f"ERROR: Failed to generate incidents PDF for {card_type} - {str(e)}")
            import traceback
            traceback.print_exc()
            # Fallback simple PDF
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=A4)
            content = [Paragraph("Error generating incidents report", getSampleStyleSheet()['Normal'])]
            doc.build(content)
            buffer.seek(0)
            return buffer.getvalue()

    async def generate_kris_pdf(self, data: Dict[str, Any], start_date: str, end_date: str, header_config: Dict[str, Any], card_type: str = None, only_card: bool = False, only_overall_table: bool = False, only_chart: bool = False) -> bytes:
        """Generate KRI PDF report mirroring incidents behavior."""
        import re
        write_debug("DEBUG: ===== generate_kris_pdf START =====")
        try:
            if not header_config:
                from utils.export_utils import get_default_header_config
                header_config = get_default_header_config("kris")

            write_debug(f"  - card_type: {card_type}")
            write_debug(f"  - only_card: {only_card}")
            write_debug(f"  - only_chart: {only_chart}")
            write_debug(f"  - only_overall_table: {only_overall_table}")

            if not data:
                raise ValueError("No KRI data provided")

            # Resolve data by card_type
            kris_data = []
            if card_type:
                kris_data = data.get(card_type) or data.get('list') or []

            columns: List[str] = []
            data_rows: List[List[str]] = []

            # CHART EXPORT
            if only_chart and card_type:
                chart_data = {"labels": [], "values": []}
                if isinstance(kris_data, list) and kris_data:
                    first_item = kris_data[0]
                    if isinstance(first_item, dict):
                        keys = list(first_item.keys())
                        if len(keys) >= 2:
                            key1 = keys[0]
                            key2 = keys[-1]
                            def fmt(k: str) -> str:
                                return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', k).title()
                            columns = [fmt(key1), fmt(key2)]
                            for item in kris_data:
                                name = item.get(key1, "N/A")
                                value = item.get(key2, 0)
                                chart_data["labels"].append(name)
                                chart_data["values"].append(value)
                                data_rows.append([name, str(value)])
                        else:
                            columns = ["Label", "Value"]
                            for item in kris_data:
                                name = str(item)
                                chart_data["labels"].append(name)
                                chart_data["values"].append(0)
                                data_rows.append([name, "0"])
                    else:
                        columns = ["Label", "Value"]
                        data_rows.append([str(first_item), "0"])
                else:
                    columns = ["Label", "Value"]
                    data_rows.append(["No data available", "0"])

                default_type_by_card = {
                    "krisByStatus": "pie",
                    "krisByLevel": "pie",
                    "breachedKRIsByDepartment": "bar",
                    "kriAssessmentCount": "bar",
                }
                chart_type_override = None
                try:
                    chart_type_override = header_config.get("chartType") or header_config.get("chart_type")
                except Exception:
                    chart_type_override = None
                valid_types = {"bar", "line", "pie"}
                if chart_type_override in valid_types:
                    resolved_chart_type = chart_type_override
                else:
                    resolved_chart_type = default_type_by_card.get(card_type or chart_type_override, "bar")
                header_config["chart_data"] = chart_data
                header_config["chart_type"] = resolved_chart_type

            # TABLE EXPORT
            elif only_overall_table:
                write_debug("DEBUG: ===== kris only_overall_table START =====")
                table_rows = data.get(card_type) or []

                if isinstance(table_rows, list) and table_rows:
                    first_item = table_rows[0]
                    if isinstance(first_item, dict):
                        raw_keys = list(first_item.keys())
                        def nice(k: str) -> str:
                            return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(k)).title()
                        columns = ['#'] + [nice(k) for k in raw_keys]
                        for i, row in enumerate(table_rows, 1):
                            values = [str(row.get(k, '')) for k in raw_keys]
                            data_rows.append([str(i)] + values)
                    elif isinstance(first_item, (list, tuple)):
                        num_cols = len(first_item)
                        columns = ['#'] + [f'C{idx+1}' for idx in range(num_cols)]
                        for i, row in enumerate(table_rows, 1):
                            vals = [str(v) for v in (row if isinstance(row, (list, tuple)) else [row])]
                            data_rows.append([str(i)] + vals)
                    else:
                        columns = ['#', 'Value']
                        data_rows = [["1", str(first_item)]]
                else:
                    columns = ['#', 'Value']
                    data_rows = [["1", 'No data available']]

            # CARD SUMMARY EXPORT
            elif only_card and card_type:
                write_debug("DEBUG: ===== kris only_card START =====")
                # Build a simple table from the card data
                if isinstance(kris_data, list) and kris_data:
                    first_item = kris_data[0]
                    if isinstance(first_item, dict):
                        def nice(k: str) -> str:
                            return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(k)).title()
                        raw_keys = list(first_item.keys())
                        columns = ['#'] + [nice(k) for k in raw_keys]
                        for i, item in enumerate(kris_data, 1):
                            values = [str(item.get(k, 'N/A')) for k in raw_keys]
                            data_rows.append([str(i)] + values)
                    else:
                        columns = ['#', 'Value']
                        data_rows = [["1", str(first_item)]]
                elif isinstance(kris_data, dict):
                    columns = ["Metric", "Value"]
                    data_rows = [[k, str(v)] for k, v in kris_data.items()]
                else:
                    columns = ["Metric", "Value"]
                    data_rows = [["No data available", "N/A"]]
            else:
                # Default simple report
                data_rows = [['KRIs Dashboard Report', 'Generated Successfully']]
                columns = ['Title', 'Status']

            # Merge header config for kris
            from utils.export_utils import merge_header_config
            final_config = merge_header_config(header_config, "kris")
            write_debug(f"DEBUG: Using PDF config (kris): {final_config}")

            result = generate_pdf_report(columns, data_rows, final_config)
            if not result:
                raise ValueError("PDF generation returned None")
            write_debug(f"DEBUG: KRIs PDF generated successfully for {card_type}")
            return result

        except Exception as e:
            write_debug(f"ERROR: Failed to generate KRIs PDF for {card_type} - {str(e)}")
            import traceback
            traceback.print_exc()
            # Fallback simple PDF
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=A4)
            content = [Paragraph("Error generating KRIs report", getSampleStyleSheet()['Normal'])]
            doc.build(content)
            buffer.seek(0)
            return buffer.getvalue()

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

        

    async def generate_risks_pdf(self, risks_data: Dict[str, Any], start_date: str, end_date: str, header_config: Dict[str, Any], card_type: str = None, only_card: bool = False, only_overall_table: bool = False, only_chart: bool = False) -> bytes:
        """Generate risks PDF report with the same structure as controls PDF generation."""
        import re
        write_debug("DEBUG: ===== generate_risks_pdf START =====")
        try:
            # Ensure header_config has risks defaults (handle None or empty dict)
            if not header_config or (isinstance(header_config, dict) and len(header_config) == 0):
                from utils.export_utils import get_default_header_config
                header_config = get_default_header_config("risks")
            
            write_debug(f"  - header_config tableHeaderBgColor (before merge): {header_config.get('tableHeaderBgColor', 'NOT SET')}")
            write_debug(f"  - card_type: {card_type}")
            write_debug(f"  - only_card: {only_card}")
            write_debug(f"  - only_chart: {only_chart}")
            write_debug(f"  - only_overall_table: {only_overall_table}")

            if not risks_data:
                raise ValueError("No risks_data provided")

            data = risks_data.get(card_type, [])

            columns: List[str] = []
            data_rows: List[List[str]] = []

            # CHART EXPORT (mirrors controls behavior)
            if only_chart and card_type:
                chart_data = {"labels": [], "values": []}
                if isinstance(data, list) and data:
                    first_item = data[0]
                    if isinstance(first_item, dict):
                        keys = list(first_item.keys())
                        def fmt(k: str) -> str:
                            return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', k).title()
                        
                        # Special handling for charts with multiple value columns
                        if card_type == "createdDeletedRisksPerQuarter" and len(keys) >= 3:
                            # For created/deleted charts, include all numeric columns
                            label_key = "name"
                            value_keys = [k for k in keys if k != label_key]
                            write_debug(f"[RISKS PDF] Multi-column chart: label_key={label_key}, value_keys={value_keys}")
                            columns = [fmt(label_key)] + [fmt(k) for k in value_keys]
                            chart_data["labels"] = []
                            chart_data["values"] = []
                            for item in data:
                                name = item.get(label_key, "N/A")
                                row = [name]
                                values_for_chart = []
                                for vk in value_keys:
                                    val = item.get(vk, 0)
                                    row.append(str(val))
                                    values_for_chart.append(val)
                                chart_data["labels"].append(name)
                                # Use first value series for chart (created)
                                chart_data["values"].append(values_for_chart[0] if values_for_chart else 0)
                                data_rows.append(row)
                            write_debug(f"[RISKS PDF] Generated {len(data_rows)} rows, columns={columns}")
                        elif len(keys) >= 2:
                            key1 = keys[0]
                            key2 = keys[-1]
                            columns = [fmt(key1), fmt(key2)]
                            for item in data:
                                name = item.get(key1, "N/A")
                                value = item.get(key2, 0)
                                chart_data["labels"].append(name)
                                chart_data["values"].append(value)
                                data_rows.append([name, str(value)])
                        else:
                            columns = ["Label", "Value"]
                            for item in data:
                                name = str(item)
                                chart_data["labels"].append(name)
                                chart_data["values"].append(0)
                                data_rows.append([name, "0"])
                    else:
                        columns = ["Label", "Value"]
                        data_rows.append([str(first_item), "0"])
                else:
                    columns = ["Label", "Value"]
                    data_rows.append(["No data available", "0"])

                # Risk default chart types (fallbacks)
                default_type_by_card = {
                    "risksByCategory": "bar",
                    "risksByEventType": "pie",
                    "createdDeletedRisksPerQuarter": "bar",
                    "quarterlyRiskCreationTrends": "line",
                    "riskApprovalStatusDistribution": "pie",
                    "riskDistributionByFinancialImpact": "pie",
                }
                chart_type_override = None
                try:
                    chart_type_override = header_config.get("chartType") or header_config.get("chart_type")
                except Exception:
                    chart_type_override = None
                valid_types = {"bar", "line", "pie"}
                if chart_type_override in valid_types:
                    resolved_chart_type = chart_type_override
                else:
                    resolved_chart_type = default_type_by_card.get(card_type or chart_type_override, "bar")
                header_config["chart_data"] = chart_data
                header_config["chart_type"] = resolved_chart_type

            # TABLE EXPORT
            elif only_overall_table:
                write_debug("DEBUG: ===== only_overall_table START =====")
                write_debug(f"  - data type: {type(data)}")
                if isinstance(data, list) and data:
                    first_item = data[0]
                    if isinstance(first_item, dict):
                        # Special formatting for allRisks table
                        if card_type == 'allRisks':
                            columns = ['#', 'Risk Name', 'Risk Description', 'Event', 'Inherent Value', 'Frequency', 'Financial Impact']
                            def map_frequency(val):
                                mapping = {1: 'Once in Three Years', 2: 'Annually', 3: 'Half Yearly', 4: 'Quarterly', 5: 'Monthly'}
                                try:
                                    num = int(val)
                                except Exception:
                                    return str(val)
                                return mapping.get(num, str(val))
                            def map_financial(val):
                                mapping = {1: '0 - 10,000', 2: '10,000 - 100,000', 3: '100,000 - 1,000,000', 4: '1,000,000 - 10,000,000', 5: '> 10,000,000'}
                                try:
                                    num = int(val)
                                except Exception:
                                    return str(val)
                                return mapping.get(num, str(val))
                            for i, row in enumerate(data, 1):
                                data_rows.append([
                                    str(i),
                                    str(row.get('RiskName', '')),
                                    str(row.get('RiskDesc', '')),
                                    str(row.get('RiskEventName', 'Unknown')),
                                    str(row.get('InherentValue', '')),
                                    map_frequency(row.get('InherentFrequency', '')),
                                    map_financial(row.get('InherentFinancialValue', '')),
                                ])
                        else:
                            raw_keys = list(first_item.keys())
                            def nice(k: str) -> str:
                                return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(k)).title()
                            columns = ['#'] + [nice(k) for k in raw_keys]
                            for i, row in enumerate(data, 1):
                                values = [str(row.get(k, '')) for k in raw_keys]
                                data_rows.append([str(i)] + values)
                    elif isinstance(first_item, (list, tuple)):
                        num_cols = len(first_item)
                        columns = ['#'] + [f'C{idx+1}' for idx in range(num_cols)]
                        for i, row in enumerate(data, 1):
                            vals = [str(v) for v in (row if isinstance(row, (list, tuple)) else [row])]
                            data_rows.append([str(i)] + vals)
                    else:
                        columns = ['#', 'Value']
                        data_rows = [["1", str(first_item)]]
                else:
                    columns = ['#', 'Value']
                    data_rows = [["1", 'No data available']]

            # CARD SUMMARY EXPORT
            elif only_card and card_type:
                write_debug("DEBUG: ===== only_card START =====")
                write_debug(f"  - data type: {type(data)}")
                if isinstance(data, list) and data:
                    first_item = data[0]
                    if isinstance(first_item, dict):
                        def nice(k: str) -> str:
                            return re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', str(k)).title()
                        raw_keys = list(first_item.keys())
                        columns = ['#'] + [nice(k) for k in raw_keys]
                        for i, item in enumerate(data, 1):
                            values = [str(item.get(k, 'N/A')) for k in raw_keys]
                            data_rows.append([str(i)] + values)
                    else:
                        columns = ['#', 'Value']
                        data_rows = [["1", str(first_item)]]
                elif isinstance(data, dict):
                    columns = ["Metric", "Value"]
                    data_rows = [[k, str(v)] for k, v in data.items()]
                else:
                    columns = ["Metric", "Value"]
                    data_rows = [["No data available", "N/A"]]
            else:
                # Default simple report
                data_rows = [['Risks Dashboard Report', 'Generated Successfully']]
                columns = ['Title', 'Status']

            # Merge header config for risks
            from utils.export_utils import merge_header_config
            write_debug(f"  - header_config tableHeaderBgColor (before merge_header_config): {header_config.get('tableHeaderBgColor', 'NOT SET')}")
            final_config = merge_header_config(header_config, "risks")
            write_debug(f"  - final_config tableHeaderBgColor: {final_config.get('tableHeaderBgColor', 'NOT SET')}")
            write_debug(f"DEBUG: Using PDF config (risks): {final_config}")

            result = generate_pdf_report(columns, data_rows, final_config)
            if not result:
                raise ValueError("PDF generation returned None")
            write_debug(f"DEBUG: PDF generated successfully for {card_type}")
            return result
                
        except Exception as e:
            write_debug(f"ERROR: Failed to generate risks PDF for {card_type} - {str(e)}")
            import traceback
            traceback.print_exc()
            # Fallback simple PDF
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=A4)
            content = [Paragraph("Error generating report", getSampleStyleSheet()['Normal'])]
            doc.build(content)
            buffer.seek(0)
            return buffer.getvalue()
