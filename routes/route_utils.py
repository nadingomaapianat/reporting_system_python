"""
Common utilities for API routes

Note: The large report generation functions (generate_excel_report, generate_word_report, 
generate_pdf_report) are intentionally kept in individual route files due to their size 
(500+ lines each) and specific module dependencies. Only truly common, reusable utilities 
are centralized here.
"""
from typing import Dict, Any, Optional, List
import json
import sys
from datetime import datetime


def write_debug(msg: str) -> None:
    """Write debug message to file with timestamp"""
    from datetime import datetime
    timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    msg_with_time = f"[{timestamp}] {msg}"
    with open('debug_log.txt', 'a', encoding='utf-8') as f:
        f.write(f"{msg_with_time}\n")
        f.flush()
    sys.stderr.write(f"{msg_with_time}\n")
    sys.stderr.flush()


def parse_header_config(headerConfig: Optional[str]) -> Dict[str, Any]:
    """Parse header configuration from JSON string"""
    header_config = {}
    if headerConfig:
        try:
            header_config = json.loads(headerConfig)
        except json.JSONDecodeError:
            header_config = {}
    return header_config


def merge_header_config(module_name: str, header_config: Dict[str, Any]) -> Dict[str, Any]:
    """Merge custom header config with default config"""
    from utils.export_utils import get_default_header_config
    default_config = get_default_header_config(module_name)
    return {**default_config, **header_config}


def convert_to_boolean(value: Any) -> bool:
    """Convert string/boolean value to boolean"""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ['true', '1', 'yes']
    return bool(value)


def generate_filename(module_name: str, card_type: Optional[str] = None, 
                     start_date: Optional[str] = None, end_date: Optional[str] = None,
                     extension: str = "pdf") -> str:
    """Generate filename for exports"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename_parts = [module_name]
    
    if card_type:
        filename_parts.append(card_type)
    
    if start_date and end_date:
        filename_parts.append(f"{start_date}_to_{end_date}")
    
    filename_parts.append(timestamp)
    filename = "_".join(filename_parts) + f".{extension}"
    
    return filename


def get_all_items_from_data(data: Dict[str, Any], possible_keys: List[str]) -> list:
    """Get all items from data dictionary using multiple possible keys"""
    for key in possible_keys:
        value = data.get(key)
        if isinstance(value, list) and (len(value) == 0 or isinstance(value[0], dict)):
            return value
    return []


def hex_to_rgb_for_excel(hex_color: str) -> str:
    """Convert hex color to RGB string for Excel openpyxl"""
    if hex_color.startswith('#'):
        hex_color = hex_color[1:]
    return hex_color


def hex_to_rgb_for_word(hex_color: str):
    """Convert hex color to RGBColor for Word python-docx"""
    from docx.shared import RGBColor
    if hex_color.startswith('#'):
        hex_color = hex_color[1:]
    try:
        r = int(hex_color[0:2], 16)
        g = int(hex_color[2:4], 16)
        b = int(hex_color[4:6], 16)
        return RGBColor(r, g, b)
    except:
        return RGBColor(31, 78, 121)  # Default color


def hex_to_color_for_pdf(hex_color: str):
    """Convert hex color to ReportLab color"""
    from reportlab.lib import colors
    if hex_color.startswith('#'):
        hex_color = hex_color[1:]
    try:
        r = int(hex_color[0:2], 16) / 255.0
        g = int(hex_color[2:4], 16) / 255.0
        b = int(hex_color[4:6], 16) / 255.0
        return colors.Color(r, g, b)
    except:
        return colors.HexColor(f"#{hex_color}")

def build_dynamic_sql_query(tables, joins, columns, where_conditions, time_filter):
    """Build SQL query from dynamic report configuration"""
    # Start with SELECT clause
    select_columns = ', '.join(columns) if columns else '*'
    sql = f"SELECT {select_columns}"
    
    # Add FROM clause with first table
    if not tables:
        raise ValueError("At least one table is required")
    
    sql += f" FROM {tables[0]}"
    
    # Add JOINs
    for join in joins:
        if join.get('leftTable') and join.get('rightTable') and join.get('leftColumn') and join.get('rightColumn'):
            join_type = join.get('type', 'INNER')
            sql += f" {join_type} JOIN {join['rightTable']} ON {join['leftTable']}.{join['leftColumn']} = {join['rightTable']}.{join['rightColumn']}"
    
    # Add WHERE clause
    where_clauses = []
    
    # Add time filter
    if time_filter and time_filter.get('column') and time_filter.get('startDate') and time_filter.get('endDate'):
        where_clauses.append(f"{time_filter['column']} BETWEEN '{time_filter['startDate']}' AND '{time_filter['endDate']}'")
    
    # Add custom WHERE conditions
    for i, condition in enumerate(where_conditions):
        if condition.get('column') and condition.get('value'):
            operator = condition.get('operator', '=')
            value = condition.get('value', '')
            logical_op = condition.get('logicalOperator', 'AND') if i > 0 else ''
            
            if logical_op:
                where_clauses.append(f" {logical_op} {condition['column']} {operator} '{value}'")
            else:
                where_clauses.append(f"{condition['column']} {operator} '{value}'")
    
    if where_clauses:
        sql += " WHERE " + " ".join(where_clauses)
    
    return sql

def generate_excel_report(columns, data_rows, header_config=None):
    """Generate Excel report from dynamic data with full header configuration support"""
    from io import BytesIO
    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
    from openpyxl.utils import get_column_letter
    import os
    import base64
    
    write_debug(f"generate_excel_report called with columns={columns}, data_rows count={len(data_rows)}")
    
    # Get default header config if none provided
    if not header_config:
        write_debug("header_config is None, creating default")
        from utils.export_utils import get_default_header_config
        header_config = get_default_header_config("dynamic")
    else:
        write_debug(f"header_config provided with keys={list(header_config.keys())}")
        write_debug(f"chart_data present? {header_config.get('chart_data') is not None}")
        if header_config.get('chart_data'):
            write_debug(f"chart_data has {len(header_config['chart_data'].get('labels', []))} labels")
    
    wb = Workbook()
    ws = wb.active
    ws.title = header_config.get('title', 'Dynamic Report')
    
    # Extract ALL configuration values from header modal configuration
    # Basic report settings
    include_header = header_config.get("includeHeader", True)
    title = header_config.get("title", "Dynamic Report")
    subtitle = header_config.get("subtitle", "")
    icon = header_config.get("icon", "chart-line")
    
    # Date and time settings
    show_date = header_config.get("showDate", True)
    footer_show_date = header_config.get("footerShowDate", True)
    
    # Bank information settings
    show_bank_info = header_config.get("showBankInfo", True)
    bank_info_location = header_config.get("bankInfoLocation", "top")  # top, bottom, none
    bank_info_align = (
        header_config.get("bankInfoAlign")
        or header_config.get("logoPosition", "center")
        or "center"
    ).lower()  # left, center, right
    bank_name = header_config.get("bankName", "")
    bank_address = header_config.get("bankAddress", "")
    bank_phone = header_config.get("bankPhone", "")
    bank_website = header_config.get("bankWebsite", "")
    
    # Logo settings
    show_logo = header_config.get("showLogo", True)
    logo_base64 = header_config.get("logoBase64", "")
    logo_position = header_config.get("logoPosition", "left")
    logo_height = header_config.get("logoHeight", 36)
    logo_file = header_config.get("logoFile", None)
    
    # Color and styling settings
    font_color = header_config.get("fontColor", "#1F4E79")
    table_header_bg_color = header_config.get("tableHeaderBgColor", "#1F4E79")
    table_body_bg_color = header_config.get("tableBodyBgColor", "#FFFFFF")
    background_color = header_config.get("backgroundColor", "#FFFFFF")
    border_style = header_config.get("borderStyle", "solid")
    border_color = header_config.get("borderColor", "#E5E7EB")
    border_width = header_config.get("borderWidth", 1)
    
    # Font and size settings
    font_size = header_config.get("fontSize", "medium")
    padding = header_config.get("padding", 20)
    margin = header_config.get("margin", 72)  # 1 inch = 72 points
    
    # Excel specific settings
    excel_auto_fit_columns = header_config.get("excelAutoFitColumns", True)
    excel_zebra_stripes = header_config.get("excelZebraStripes", True)
    excel_fit_to_width = header_config.get("excelFitToWidth", True)
    excel_freeze_top_row = header_config.get("excelFreezeTopRow", True)
    
    # Watermark settings
    watermark_enabled = header_config.get("watermarkEnabled", False)
    watermark_text = header_config.get("watermarkText", "CONFIDENTIAL")
    watermark_opacity = header_config.get("watermarkOpacity", 10)
    watermark_diagonal = header_config.get("watermarkDiagonal", True)
    
    # Footer settings
    footer_show_confidentiality = header_config.get("footerShowConfidentiality", True)
    footer_confidentiality_text = header_config.get("footerConfidentialityText", "Confidential Report - Internal Use Only")
    footer_show_page_numbers = header_config.get("footerShowPageNumbers", True)
    footer_align = header_config.get("footerAlign", "center")
    
    # Page settings
    show_page_numbers = header_config.get("showPageNumbers", True)
    location = header_config.get("location", "top")
    
    # Convert hex colors to RGB for openpyxl
    def hex_to_rgb(hex_color):
        if hex_color.startswith('#'):
            hex_color = hex_color[1:]
        return hex_color
    
    font_color_rgb = hex_to_rgb(font_color)
    header_bg_rgb = hex_to_rgb(table_header_bg_color)
    body_bg_rgb = hex_to_rgb(table_body_bg_color)
    background_rgb = hex_to_rgb(background_color)
    border_rgb = hex_to_rgb(border_color)
    
    current_row = 1
    
    # Only add header if includeHeader is True
    if include_header:
        # Add logo if available
        if show_logo and logo_base64:
            try:
                import base64
                from PIL import Image as PILImage
                from openpyxl.drawing.image import Image as XLImage
                
                logo_bytes = base64.b64decode(logo_base64.split(',')[-1])
                logo_buf = BytesIO(logo_bytes)
                pil_img = PILImage.open(logo_buf)
                
                # Resize logo based on configuration
                desired_h = min(logo_height, 64)
                w, h = pil_img.size
                if h > 0:
                    scale = desired_h / h
                    new_w = int(w * scale)
                    max_w = 180
                    if new_w > max_w:
                        new_w = max_w
                        scale = new_w / w
                        desired_h = int(h * scale)
                    pil_img = pil_img.resize((new_w, desired_h), PILImage.Resampling.LANCZOS)
                
                # Save to BytesIO for openpyxl
                logo_buf = BytesIO()
                pil_img.save(logo_buf, format='PNG')
                logo_buf.seek(0)
                
                # Create openpyxl image
                xl_image = XLImage(logo_buf)
                
                # Position logo based on logoPosition
                if logo_position == 'left':
                    ws.add_image(xl_image, 'A1')
                elif logo_position == 'center':
                    ws.add_image(xl_image, 'C1')
                elif logo_position == 'right':
                    ws.add_image(xl_image, 'E1')
                
                current_row += 2  # Leave space for logo
            except Exception as e:
                pass  # Continue without logo if there's an error
        
        # Add bank information at top if configured
        if show_bank_info and bank_name and bank_info_location == "top":
            # Resolve Excel horizontal alignment from bank_info_align
            _xl_bank_align = 'center'
            if bank_info_align == 'left':
                _xl_bank_align = 'left'
            elif bank_info_align == 'right':
                _xl_bank_align = 'right'
            ws.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=len(columns))
            ws.cell(row=current_row, column=1, value=bank_name)
            ws.cell(row=current_row, column=1).font = Font(size=12, bold=True, color=font_color_rgb)
            ws.cell(row=current_row, column=1).alignment = Alignment(horizontal=_xl_bank_align)
            current_row += 1
            
            if bank_address:
                ws.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=len(columns))
                ws.cell(row=current_row, column=1, value=bank_address)
                ws.cell(row=current_row, column=1).font = Font(size=10, color=font_color_rgb)
                ws.cell(row=current_row, column=1).alignment = Alignment(horizontal=_xl_bank_align)
                current_row += 1
                
            if bank_phone or bank_website:
                contact_info = []
                if bank_phone:
                    contact_info.append(f"Tel: {bank_phone}")
                if bank_website:
                    contact_info.append(f"Web: {bank_website}")
                
                ws.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=len(columns))
                ws.cell(row=current_row, column=1, value=" | ".join(contact_info))
                ws.cell(row=current_row, column=1).font = Font(size=10, color=font_color_rgb)
                ws.cell(row=current_row, column=1).alignment = Alignment(horizontal=_xl_bank_align)
                current_row += 1
        
        # Add title
        ws.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=len(columns))
        ws.cell(row=current_row, column=1, value=title)
        ws.cell(row=current_row, column=1).font = Font(size=16, bold=True, color=font_color_rgb)
        ws.cell(row=current_row, column=1).alignment = Alignment(horizontal='center', vertical='center')
        current_row += 1
        
        # Add subtitle
        if subtitle:
            ws.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=len(columns))
            ws.cell(row=current_row, column=1, value=subtitle)
            ws.cell(row=current_row, column=1).font = Font(size=12, italic=True, color=font_color_rgb)
            ws.cell(row=current_row, column=1).alignment = Alignment(horizontal='center')
            current_row += 1
        
        # Add generation date
        if show_date:
            ws.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=len(columns))
            ws.cell(row=current_row, column=1, value=f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            ws.cell(row=current_row, column=1).font = Font(size=10, italic=True, color=font_color_rgb)
            ws.cell(row=current_row, column=1).alignment = Alignment(horizontal='center')
            current_row += 1
        
        # Add empty row for spacing
        current_row += 1
    
    # Table headers
    header_row = current_row
    for idx, col in enumerate(columns, start=1):
        cell = ws.cell(row=header_row, column=idx, value=col)
        cell.font = Font(bold=True, color='FFFFFF')
        cell.fill = PatternFill(start_color=header_bg_rgb, end_color=header_bg_rgb, fill_type='solid')
        cell.alignment = Alignment(horizontal='center', vertical='center')
    
    # Data rows
    for row_idx, row_data in enumerate(data_rows, start=header_row + 1):
        for col_idx, value in enumerate(row_data, start=1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.alignment = Alignment(vertical='top')
            
            # Apply zebra stripes if enabled
            if excel_zebra_stripes and (row_idx - header_row) % 2 == 0:
                cell.fill = PatternFill(start_color=body_bg_rgb, end_color=body_bg_rgb, fill_type='solid')

    # Optional footer totals row
    footer_totals_cols = header_config.get("tableFooterTotals", []) or []
    if isinstance(footer_totals_cols, list) and len(footer_totals_cols) > 0:
        name_to_index = {str(col): idx for idx, col in enumerate(columns)}
        totals = [None] * len(columns)
        for col_name in footer_totals_cols:
            if str(col_name) in name_to_index:
                idx = name_to_index[str(col_name)]
                total_value = 0.0
                for row_data in data_rows:
                    try:
                        val = row_data[idx]
                        if val is None or val == "":
                            continue
                        total_value += float(str(val).replace(',', ''))
                    except Exception:
                        pass
                totals[idx] = total_value
        totals_row = ws.max_row + 1
        for i in range(len(columns)):
            cell = ws.cell(row=totals_row, column=i + 1)
            if i == 0:
                cell.value = "Total"
                cell.alignment = Alignment(horizontal='left', vertical='center')
            elif totals[i] is not None:
                cell.value = totals[i]
                cell.number_format = '#,##0.00'
                cell.alignment = Alignment(horizontal='right', vertical='center')
            else:
                cell.alignment = Alignment(horizontal='right', vertical='center')
            # Style totals row
            cell.fill = PatternFill(start_color=header_bg_rgb, end_color=header_bg_rgb, fill_type='solid')
            cell.font = Font(bold=True, color='FFFFFF')
    
    # Freeze top row if enabled
    if excel_freeze_top_row:
        ws.freeze_panes = f"A{header_row + 1}"
    
    # Generate chart if chart_data is provided (right side)
    chart_data = header_config.get('chart_data')
    chart_type = header_config.get('chart_type', 'bar')
    write_debug(f"Checking for chart - chart_data={'present' if chart_data else 'missing'}, chart_type={chart_type}")
    if chart_data and chart_data.get('labels') and chart_data.get('values'):
        try:
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
            from io import BytesIO
            from openpyxl.drawing.image import Image as XLImage
            
            write_debug(f"Generating Excel chart with {len(chart_data['labels'])} labels, type={chart_type}")
            
            # Create chart
            fig, ax = plt.subplots(figsize=(8, 5))
            
            if chart_type == 'bar':
                ax.barh(chart_data['labels'], chart_data['values'], color='#4472C4')
                ax.set_xlabel('Controls Count')
                ax.set_ylabel('Component')
            elif chart_type == 'pie':
                ax.pie(chart_data['values'], labels=chart_data['labels'], autopct='%1.1f%%')
            elif chart_type == 'line':
                ax.plot(chart_data['labels'], chart_data['values'], marker='o')
                ax.set_xlabel('Labels')
                ax.set_ylabel('Values')
            
            ax.set_title(title if title else 'Chart')
            plt.tight_layout()
            
            # Save chart to buffer
            chart_buffer = BytesIO()
            plt.savefig(chart_buffer, format='png', dpi=150, bbox_inches='tight')
            chart_buffer.seek(0)
            plt.close()
            
            # Add chart to Excel on the right side
            img = XLImage(chart_buffer)
            img.width = 500
            img.height = 300
            # Position chart to the right of the table, starting from the first data row (not header)
            chart_col = len(columns) + 2  # Start after the table columns + 1 space
            chart_start_row = header_row + 1  # Start chart on first data row, not header row
            cell_address = f'{get_column_letter(chart_col)}{chart_start_row}'
            write_debug(f"Adding chart to Excel at cell {cell_address} (header_row={header_row}, chart_start_row={chart_start_row})")
            ws.add_image(img, cell_address)
            write_debug("Chart added successfully")
        except Exception as e:
            write_debug(f"Error generating chart for Excel: {e}")
            import traceback
            traceback.print_exc()
            pass
    else:
        write_debug(f"Skipping chart - chart_data={chart_data}, labels={chart_data.get('labels') if chart_data else None}, values={chart_data.get('values') if chart_data else None}")
    
    # Auto-fit columns if enabled
    if excel_auto_fit_columns:
        for col_idx in range(1, len(columns) + 1):
            max_length = 0
            column_letter = get_column_letter(col_idx)
            for row_idx in range(1, ws.max_row + 1):
                try:
                    cell = ws.cell(row=row_idx, column=col_idx)
                    if hasattr(cell, 'value') and cell.value:
                        cell_length = len(str(cell.value))
                        if cell_length > max_length:
                            max_length = cell_length
                except:
                    pass
            adjusted_width = min(max_length + 3, 50)
            ws.column_dimensions[column_letter].width = adjusted_width
    else:
        # Set default column width
        for i in range(1, len(columns) + 1):
            ws.column_dimensions[get_column_letter(i)].width = 15
    
    # Apply fit to width if enabled
    if excel_fit_to_width:
        ws.page_setup.fitToWidth = 1
        ws.page_setup.fitToHeight = 0
    
    # Add watermark if enabled
    if watermark_enabled:
        try:
            from utils.export_utils import add_watermark_to_excel_sheet
            add_watermark_to_excel_sheet(ws, header_config)
        except Exception as e:
            pass  # Continue without watermark if there's an error
    
    # Add bank information at bottom if configured
    if show_bank_info and bank_name and bank_info_location == "bottom":
        # Add some spacing before bank info
        current_row += 2
        
        ws.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=len(columns))
        ws.cell(row=current_row, column=1, value=bank_name)
        ws.cell(row=current_row, column=1).font = Font(size=12, bold=True, color=font_color_rgb)
        ws.cell(row=current_row, column=1).alignment = Alignment(horizontal='center')
        current_row += 1
        
        if bank_address:
            ws.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=len(columns))
            ws.cell(row=current_row, column=1, value=bank_address)
            ws.cell(row=current_row, column=1).font = Font(size=10, color=font_color_rgb)
            ws.cell(row=current_row, column=1).alignment = Alignment(horizontal='center')
            current_row += 1
            
        if bank_phone or bank_website:
            contact_info = []
            if bank_phone:
                contact_info.append(f"Tel: {bank_phone}")
            if bank_website:
                contact_info.append(f"Web: {bank_website}")
            
            ws.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=len(columns))
            ws.cell(row=current_row, column=1, value=" | ".join(contact_info))
            ws.cell(row=current_row, column=1).font = Font(size=10, color=font_color_rgb)
            ws.cell(row=current_row, column=1).alignment = Alignment(horizontal='center')
            current_row += 1

    # Configure page setup and headers/footers
    if show_page_numbers:
        ws.page_setup.orientation = ws.ORIENTATION_LANDSCAPE if len(columns) > 6 else ws.ORIENTATION_PORTRAIT
    
    # Add footer if configured
    if footer_show_date or footer_show_confidentiality or footer_show_page_numbers:
        footer_text = []
        if footer_show_date:
            footer_text.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if footer_show_confidentiality:
            footer_text.append(footer_confidentiality_text)
        if footer_show_page_numbers:
            footer_text.append("Page &P of &N")
        
        if footer_align == "center":
            ws.HeaderFooter.oddFooter.center.text = " | ".join(footer_text)
        elif footer_align == "left":
            ws.HeaderFooter.oddFooter.left.text = " | ".join(footer_text)
        elif footer_align == "right":
            ws.HeaderFooter.oddFooter.right.text = " | ".join(footer_text)
    
    # Save to file
    base_dir = os.path.dirname(os.path.dirname(__file__))
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    date_folder = datetime.now().strftime('%Y-%m-%d')
    reports_export_dir = os.path.join(base_dir, "reports_export", date_folder)
    os.makedirs(reports_export_dir, exist_ok=True)
    filename = f"dynamic_report_{ts}.xlsx"
    file_path = os.path.join(reports_export_dir, filename)
    
    wb.save(file_path)
    
    # Save export record to database (best-effort)
    try:
        import pyodbc
        from config import get_database_connection_string
        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                IF NOT EXISTS (
                  SELECT * FROM INFORMATION_SCHEMA.TABLES 
                  WHERE TABLE_NAME = 'report_exports' AND TABLE_SCHEMA='dbo'
                )
                BEGIN
                  CREATE TABLE dbo.report_exports (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    title NVARCHAR(255) NOT NULL,
                    src NVARCHAR(1024) NULL,
                    format NVARCHAR(20) NOT NULL,
                    dashboard NVARCHAR(100) NULL,
                    created_at DATETIME2 DEFAULT GETDATE()
                  )
                END
                """
            )
            conn.commit()
            export_title = header_config.get("title", "Dynamic Report") if header_config else "Dynamic Report"
            export_title = f"{export_title} - {datetime.now().strftime('%Y-%m-%d')}"
            export_dashboard = header_config.get("dashboard", "dynamic") if header_config else "dynamic"
            cursor.execute(
                """
                INSERT INTO dbo.report_exports (title, src, format, dashboard)
                VALUES (?, ?, ?, ?)
                """,
                (export_title, file_path, 'excel', export_dashboard)
            )
            conn.commit()
        finally:
            cursor.close()
            conn.close()
    except Exception:
        pass

    with open(file_path, 'rb') as f:
        content = f.read()
    
    return content

def generate_word_report(columns, data_rows, header_config=None):
    """Generate Word report from dynamic data with full header configuration support"""
    from io import BytesIO
    from docx import Document
    from docx.shared import Pt, RGBColor
    from docx.enum.table import WD_TABLE_ALIGNMENT, WD_ALIGN_VERTICAL
    from docx.enum.text import WD_PARAGRAPH_ALIGNMENT
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement
    import os
    
    # Get default header config if none provided
    if not header_config:
        from utils.export_utils import get_default_header_config
        header_config = get_default_header_config("dynamic")
    
    doc = Document()
    
    # Extract ALL configuration values from header modal configuration
    # Basic report settings
    include_header = header_config.get("includeHeader", True)
    title = header_config.get("title", "Dynamic Report")
    subtitle = header_config.get("subtitle", "")
    icon = header_config.get("icon", "chart-line")
    
    # Date and time settings
    show_date = header_config.get("showDate", True)
    footer_show_date = header_config.get("footerShowDate", True)
    
    # Bank information settings
    show_bank_info = header_config.get("showBankInfo", True)
    bank_info_location = header_config.get("bankInfoLocation", "top")  # top, bottom, none
    bank_info_align = (
        header_config.get("bankInfoAlign")
        or header_config.get("logoPosition", "center")
        or "center"
    ).lower()  # left, center, right
    bank_name = header_config.get("bankName", "")
    bank_address = header_config.get("bankAddress", "")
    bank_phone = header_config.get("bankPhone", "")
    bank_website = header_config.get("bankWebsite", "")
    
    # Logo settings
    show_logo = header_config.get("showLogo", True)
    logo_base64 = header_config.get("logoBase64", "")
    logo_position = header_config.get("logoPosition", "left")
    logo_height = header_config.get("logoHeight", 36)
    logo_file = header_config.get("logoFile", None)
    
    # Color and styling settings
    font_color = header_config.get("fontColor", "#1F4E79")
    table_header_bg_color = header_config.get("tableHeaderBgColor", "#1F4E79")
    table_body_bg_color = header_config.get("tableBodyBgColor", "#FFFFFF")
    background_color = header_config.get("backgroundColor", "#FFFFFF")
    border_style = header_config.get("borderStyle", "solid")
    border_color = header_config.get("borderColor", "#E5E7EB")
    border_width = header_config.get("borderWidth", 1)
    
    # Font and size settings
    font_size = header_config.get("fontSize", "medium")
    padding = header_config.get("padding", 20)
    margin = header_config.get("margin", 72)  # 1 inch = 72 points
    
    # Watermark settings
    watermark_enabled = header_config.get("watermarkEnabled", False)
    watermark_text = header_config.get("watermarkText", "CONFIDENTIAL")
    watermark_opacity = header_config.get("watermarkOpacity", 10)
    watermark_diagonal = header_config.get("watermarkDiagonal", True)
    
    # Footer settings
    footer_show_confidentiality = header_config.get("footerShowConfidentiality", True)
    footer_confidentiality_text = header_config.get("footerConfidentialityText", "Confidential Report - Internal Use Only")
    footer_show_page_numbers = header_config.get("footerShowPageNumbers", True)
    footer_align = header_config.get("footerAlign", "center")
    
    # Page settings
    show_page_numbers = header_config.get("showPageNumbers", True)
    location = header_config.get("location", "top")
    
    # Convert hex colors to RGB for python-docx
    def hex_to_rgb(hex_color):
        if hex_color.startswith('#'):
            hex_color = hex_color[1:]
        try:
            r = int(hex_color[0:2], 16)
            g = int(hex_color[2:4], 16)
            b = int(hex_color[4:6], 16)
            return RGBColor(r, g, b)
        except:
            return RGBColor(31, 78, 121)  # Default color
    
    font_color_rgb = hex_to_rgb(font_color)
    header_bg_color_rgb = hex_to_rgb(table_header_bg_color)
    body_bg_color_rgb = hex_to_rgb(table_body_bg_color)
    background_color_rgb = hex_to_rgb(background_color)
    border_color_rgb = hex_to_rgb(border_color)
    
    # Set document margins
    sections = doc.sections
    for section in sections:
        section.top_margin = Pt(margin)
        section.bottom_margin = Pt(margin)
        section.left_margin = Pt(margin)
        section.right_margin = Pt(margin)
    
    # Only add header if includeHeader is True
    if include_header:
        # Add logo image into document header if configured
        if show_logo and (logo_base64 or logo_file):
            try:
                img_bytes_io = None
                if logo_base64:
                    b64_data = logo_base64
                    if ',' in b64_data:
                        b64_data = b64_data.split(',')[1]
                    img_bytes_io = BytesIO(base64.b64decode(b64_data))
                elif logo_file and os.path.exists(logo_file):
                    with open(logo_file, 'rb') as lf:
                        img_bytes_io = BytesIO(lf.read())
                if img_bytes_io:
                    header = doc.sections[0].header
                    # Use existing header paragraph if available, else add one
                    header_para = header.paragraphs[0] if header.paragraphs else header.add_paragraph()
                    run = header_para.add_run()
                    # Preserve aspect ratio by setting only height
                    run.add_picture(img_bytes_io, height=Pt(float(logo_height)))
                    if logo_position == 'left':
                        header_para.alignment = WD_PARAGRAPH_ALIGNMENT.LEFT
                    elif logo_position == 'right':
                        header_para.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT
                    else:
                        header_para.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
            except Exception:
                pass
        # Add bank information at top if configured
        if show_bank_info and bank_name and bank_info_location == "top":
            bank_para = doc.add_paragraph()
            bank_run = bank_para.add_run(f"🏦 {bank_name}")
            bank_run.font.size = Pt(12)
            bank_run.font.bold = True
            bank_run.font.color.rgb = font_color_rgb
            if bank_info_align == 'left':
                bank_para.alignment = WD_PARAGRAPH_ALIGNMENT.LEFT
            elif bank_info_align == 'right':
                bank_para.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT
            else:
                bank_para.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
            
            if bank_address:
                address_para = doc.add_paragraph()
                address_run = address_para.add_run(bank_address)
                address_run.font.size = Pt(10)
                address_run.font.color.rgb = font_color_rgb
                if bank_info_align == 'left':
                    address_para.alignment = WD_PARAGRAPH_ALIGNMENT.LEFT
                elif bank_info_align == 'right':
                    address_para.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT
                else:
                    address_para.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
            
            if bank_phone or bank_website:
                contact_info = []
                if bank_phone:
                    contact_info.append(f"Tel: {bank_phone}")
                if bank_website:
                    contact_info.append(f"Web: {bank_website}")
                
                contact_para = doc.add_paragraph()
                contact_run = contact_para.add_run(" | ".join(contact_info))
                contact_run.font.size = Pt(10)
                contact_run.font.color.rgb = font_color_rgb
                if bank_info_align == 'left':
                    contact_para.alignment = WD_PARAGRAPH_ALIGNMENT.LEFT
                elif bank_info_align == 'right':
                    contact_para.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT
                else:
                    contact_para.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
            
            # Add spacing
            doc.add_paragraph()
        
        # Add title
        title_para = doc.add_paragraph()
        title_run = title_para.add_run(title)
        title_run.font.size = Pt(16)
        title_run.font.bold = True
        title_run.font.color.rgb = font_color_rgb
        title_para.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
        
        # Add subtitle
        if subtitle:
            subtitle_para = doc.add_paragraph()
            subtitle_run = subtitle_para.add_run(subtitle)
            subtitle_run.font.size = Pt(12)
            subtitle_run.font.italic = True
            subtitle_run.font.color.rgb = font_color_rgb
            subtitle_para.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
        
        # Add generation date
        if show_date:
            date_para = doc.add_paragraph()
            current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            date_run = date_para.add_run(f"Generated on: {current_date}")
            date_run.font.size = Pt(10)
            date_run.font.italic = True
            date_run.font.color.rgb = font_color_rgb
            date_para.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
        
        # Add spacing
        doc.add_paragraph()
    
    # Create table with configuration-based styling
    table = doc.add_table(rows=1, cols=len(columns))
    table.style = 'Table Grid'
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    
    # Helper function to shade table cells
    def shade_cell(cell, fill_color):
        tcPr = cell._tc.get_or_add_tcPr()
        shd = OxmlElement('w:shd')
        shd.set(qn('w:val'), 'clear')
        shd.set(qn('w:color'), 'auto')
        shd.set(qn('w:fill'), fill_color.replace('#', ''))
        tcPr.append(shd)
    
    # Headers
    hdr_cells = table.rows[0].cells
    for i, col in enumerate(columns):
        hdr_cells[i].text = str(col)
        # Style header cell
        for paragraph in hdr_cells[i].paragraphs:
            for run in paragraph.runs:
                run.font.bold = True
                run.font.color.rgb = RGBColor(255, 255, 255)  # White text
            paragraph.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
        hdr_cells[i].vertical_alignment = WD_ALIGN_VERTICAL.CENTER
        # Apply header background color
        shade_cell(hdr_cells[i], table_header_bg_color)
    
    # Data rows
    for row_idx, row_data in enumerate(data_rows):
        row_cells = table.add_row().cells
        for i, value in enumerate(row_data):
            row_cells[i].text = str(value)
            # Style data cell
            for paragraph in row_cells[i].paragraphs:
                paragraph.alignment = WD_PARAGRAPH_ALIGNMENT.LEFT
            row_cells[i].vertical_alignment = WD_ALIGN_VERTICAL.TOP
            
            # Apply alternating row colors if needed
            if row_idx % 2 == 0:
                shade_cell(row_cells[i], table_body_bg_color)

    # Optional footer totals row for Word
    footer_totals_cols = header_config.get("tableFooterTotals", []) or []
    if isinstance(footer_totals_cols, list) and len(footer_totals_cols) > 0:
        totals_row = table.add_row().cells
        for i in range(len(columns)):
            totals_row[i].text = ""
        totals_row[0].text = "Total"
        name_to_index = {str(col): idx for idx, col in enumerate(columns)}
        for col_name in footer_totals_cols:
            if str(col_name) in name_to_index:
                idx = name_to_index[str(col_name)]
                s = 0.0
                for r in data_rows:
                    try:
                        val = r[idx]
                        if val is None or val == "":
                            continue
                        s += float(str(val).replace(',', ''))
                    except Exception:
                        pass
                totals_row[idx].text = f"{s:,.2f}"
        # Bold and align totals row
        for i in range(len(columns)):
            for paragraph in totals_row[i].paragraphs:
                for run in paragraph.runs:
                    run.font.bold = True
                paragraph.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT if i > 0 else WD_PARAGRAPH_ALIGNMENT.LEFT
    
    # Add bank information at bottom if configured
    if show_bank_info and bank_name and bank_info_location == "bottom":
        doc.add_paragraph()  # Add spacing
        
        bank_para = doc.add_paragraph()
        bank_run = bank_para.add_run(f"🏦 {bank_name}")
        bank_run.font.size = Pt(12)
        bank_run.font.bold = True
        bank_run.font.color.rgb = font_color_rgb
        if bank_info_align == 'left':
            bank_para.alignment = WD_PARAGRAPH_ALIGNMENT.LEFT
        elif bank_info_align == 'right':
            bank_para.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT
        else:
            bank_para.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
        
        if bank_address:
            address_para = doc.add_paragraph()
            address_run = address_para.add_run(bank_address)
            address_run.font.size = Pt(10)
            address_run.font.color.rgb = font_color_rgb
            if bank_info_align == 'left':
                address_para.alignment = WD_PARAGRAPH_ALIGNMENT.LEFT
            elif bank_info_align == 'right':
                address_para.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT
            else:
                address_para.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
        
        if bank_phone or bank_website:
            contact_info = []
            if bank_phone:
                contact_info.append(f"Tel: {bank_phone}")
            if bank_website:
                contact_info.append(f"Web: {bank_website}")
            
            contact_para = doc.add_paragraph()
            contact_run = contact_para.add_run(" | ".join(contact_info))
            contact_run.font.size = Pt(10)
            contact_run.font.color.rgb = font_color_rgb
            if bank_info_align == 'left':
                contact_para.alignment = WD_PARAGRAPH_ALIGNMENT.LEFT
            elif bank_info_align == 'right':
                contact_para.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT
            else:
                contact_para.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER

    # Add footer if configured
    if footer_show_date or footer_show_confidentiality or footer_show_page_numbers:
        doc.add_paragraph()  # Add spacing
        
        footer_text = []
        if footer_show_date:
            current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            footer_text.append(f"Generated on: {current_date}")
        if footer_show_confidentiality:
            footer_text.append(footer_confidentiality_text)
        if footer_show_page_numbers:
            footer_text.append("Page &P of &N")
        
        if footer_text:
            footer_para = doc.add_paragraph()
            footer_run = footer_para.add_run(" | ".join(footer_text))
            footer_run.font.size = Pt(8)
            footer_run.font.color.rgb = RGBColor(128, 128, 128)  # Gray color
            
            if footer_align == "center":
                footer_para.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
            elif footer_align == "left":
                footer_para.alignment = WD_PARAGRAPH_ALIGNMENT.LEFT
            elif footer_align == "right":
                footer_para.alignment = WD_PARAGRAPH_ALIGNMENT.RIGHT
    
    # Add watermark if enabled (single centered line)
    if watermark_enabled:
        try:
            watermark_para = doc.add_paragraph()
            if watermark_diagonal:
                spaced_text = " ".join(watermark_text)
                watermark_run = watermark_para.add_run(spaced_text)
            else:
                watermark_run = watermark_para.add_run(watermark_text)
            watermark_run.font.size = Pt(48)
            watermark_run.font.color.rgb = RGBColor(200, 200, 200)
            watermark_para.alignment = WD_PARAGRAPH_ALIGNMENT.CENTER
            watermark_para.space_after = Pt(0)
            watermark_para.space_before = Pt(0)
        except Exception:
            pass
    
    # Save to file
    base_dir = os.path.dirname(os.path.dirname(__file__))
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    date_folder = datetime.now().strftime('%Y-%m-%d')
    reports_export_dir = os.path.join(base_dir, "reports_export", date_folder)
    os.makedirs(reports_export_dir, exist_ok=True)
    filename = f"dynamic_report_{ts}.docx"
    file_path = os.path.join(reports_export_dir, filename)
    
    doc.save(file_path)
    
    # Save export record to database (best-effort)
    try:
        import pyodbc
        from config import get_database_connection_string
        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                IF NOT EXISTS (
                  SELECT * FROM INFORMATION_SCHEMA.TABLES 
                  WHERE TABLE_NAME = 'report_exports' AND TABLE_SCHEMA='dbo'
                )
                BEGIN
                  CREATE TABLE dbo.report_exports (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    title NVARCHAR(255) NOT NULL,
                    src NVARCHAR(1024) NULL,
                    format NVARCHAR(20) NOT NULL,
                    dashboard NVARCHAR(100) NULL,
                    created_at DATETIME2 DEFAULT GETDATE()
                  )
                END
                """
            )
            conn.commit()
            export_title = header_config.get("title", "Dynamic Report") if header_config else "Dynamic Report"
            export_title = f"{export_title} - {datetime.now().strftime('%Y-%m-%d')}"
            export_dashboard = header_config.get("dashboard", "dynamic") if header_config else "dynamic"
            cursor.execute(
                """
                INSERT INTO dbo.report_exports (title, src, format, dashboard)
                VALUES (?, ?, ?, ?)
                """,
                (export_title, file_path, 'word', export_dashboard)
            )
            conn.commit()
        finally:
            cursor.close()
            conn.close()
    except Exception:
        pass

    with open(file_path, 'rb') as f:
        content = f.read()
    
    return Response(
        content=content,
        media_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        headers={
            'Content-Disposition': f'attachment; filename="{filename}"',
            'X-Export-Src': file_path
        }
    )

def generate_pdf_report(columns, data_rows, header_config=None):
    """Generate PDF report from dynamic data with full header configuration support"""
    from reportlab.lib.pagesizes import letter, A4
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    import os
    import base64
    from io import BytesIO
    try:
        from PIL import Image as PILImage
    except Exception:
        PILImage = None
    
    # Import Arabic text support
    from pdf_utils import shape_text_for_arabic, ARABIC_FONT_NAME
    
    # Get default header config if none provided
    if not header_config:
        from utils.export_utils import get_default_header_config
        header_config = get_default_header_config("dynamic")
    
    base_dir = os.path.dirname(os.path.dirname(__file__))
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    date_folder = datetime.now().strftime('%Y-%m-%d')
    reports_export_dir = os.path.join(base_dir, "reports_export", date_folder)
    os.makedirs(reports_export_dir, exist_ok=True)
    filename = f"dynamic_report_{ts}.pdf"
    file_path = os.path.join(reports_export_dir, filename)
    
    # Extract ALL configuration values from header modal configuration
    # Basic report settings
    include_header = header_config.get("includeHeader", True)
    title = header_config.get("title", "Dynamic Report")
    subtitle = header_config.get("subtitle", "")
    icon = header_config.get("icon", "chart-line")
    
    # Date and time settings
    show_date = header_config.get("showDate", True)
    footer_show_date = header_config.get("footerShowDate", True)
    
    # Bank information settings
    show_bank_info = header_config.get("showBankInfo", True)
    bank_info_location = header_config.get("bankInfoLocation", "top")  # top, bottom, none
    bank_info_align = (
        header_config.get("bankInfoAlign")
        or header_config.get("logoPosition", "center")
        or "center"
    ).lower()
    bank_name = header_config.get("bankName", "")
    bank_address = header_config.get("bankAddress", "")
    bank_phone = header_config.get("bankPhone", "")
    bank_website = header_config.get("bankWebsite", "")
    
    # Logo settings
    show_logo = header_config.get("showLogo", True)
    logo_base64 = header_config.get("logoBase64", "")
    logo_position = header_config.get("logoPosition", "left")
    logo_height = header_config.get("logoHeight", 36)
    logo_file = header_config.get("logoFile", None)
    
    # Color and styling settings
    font_color = header_config.get("fontColor", "#1F4E79")
    table_header_bg_color = header_config.get("tableHeaderBgColor", "#1F4E79")
    table_body_bg_color = header_config.get("tableBodyBgColor", "#FFFFFF")
    background_color = header_config.get("backgroundColor", "#FFFFFF")
    border_style = header_config.get("borderStyle", "solid")
    border_color = header_config.get("borderColor", "#E5E7EB")
    border_width = header_config.get("borderWidth", 1)
    
    # Font and size settings
    font_size = header_config.get("fontSize", "medium")
    padding = header_config.get("padding", 20)
    margin = header_config.get("margin", 72)  # 1 inch = 72 points
    
    # Watermark settings
    watermark_enabled = header_config.get("watermarkEnabled", False)
    watermark_text = header_config.get("watermarkText", "CONFIDENTIAL")
    watermark_opacity = header_config.get("watermarkOpacity", 10)
    watermark_diagonal = header_config.get("watermarkDiagonal", True)
    
    # Footer settings
    footer_show_confidentiality = header_config.get("footerShowConfidentiality", True)
    footer_confidentiality_text = header_config.get("footerConfidentialityText", "Confidential Report - Internal Use Only")
    footer_show_page_numbers = header_config.get("footerShowPageNumbers", True)
    footer_align = header_config.get("footerAlign", "center")
    
    # Page settings
    show_page_numbers = header_config.get("showPageNumbers", True)
    location = header_config.get("location", "top")
    
    # Convert hex colors to ReportLab colors
    def hex_to_color(hex_color):
        if hex_color.startswith('#'):
            hex_color = hex_color[1:]
        try:
            r = int(hex_color[0:2], 16) / 255.0
            g = int(hex_color[2:4], 16) / 255.0
            b = int(hex_color[4:6], 16) / 255.0
            return colors.Color(r, g, b)
        except:
            return colors.HexColor(f"#{hex_color}")
    
    font_color_rl = hex_to_color(font_color)
    header_bg_color_rl = hex_to_color(table_header_bg_color)
    body_bg_color_rl = hex_to_color(table_body_bg_color)
    background_color_rl = hex_to_color(background_color)
    border_color_rl = hex_to_color(border_color)
    
    # Choose page size based on number of columns
    page_size = A4 if len(columns) <= 6 else letter
    
    # Create document with margins
    doc = SimpleDocTemplate(
        file_path, 
        pagesize=page_size,
        rightMargin=margin,
        leftMargin=margin,
        topMargin=margin,
        bottomMargin=margin
    )
    
    styles = getSampleStyleSheet()
    story = []
    
    # Create custom styles based on configuration
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Title'],
        fontSize=16,
        textColor=font_color_rl,
        alignment=TA_CENTER,
        spaceAfter=12
    )
    
    subtitle_style = ParagraphStyle(
        'CustomSubtitle',
        parent=styles['Normal'],
        fontSize=12,
        textColor=font_color_rl,
        alignment=TA_CENTER,
        spaceAfter=6
    )
    
    # Resolve text alignment for bank info
    _pdf_align = TA_CENTER
    if bank_info_align == 'left':
        _pdf_align = TA_LEFT
    elif bank_info_align == 'right':
        _pdf_align = TA_RIGHT

    bank_style = ParagraphStyle(
        'BankInfo',
        parent=styles['Normal'],
        fontSize=10,
        textColor=font_color_rl,
        alignment=_pdf_align,
        spaceAfter=3
    )
    
    date_style = ParagraphStyle(
        'DateInfo',
        parent=styles['Normal'],
        fontSize=10,
        textColor=font_color_rl,
        alignment=TA_CENTER,
        spaceAfter=6
    )
    
    # Only add header if includeHeader is True
    if include_header:
        # Add logo image if configured
        if show_logo and (logo_base64 or logo_file):
            try:
                img_stream = None
                if logo_base64:
                    b64_data = logo_base64
                    if ',' in b64_data:
                        b64_data = b64_data.split(',')[1]
                    img_bytes = base64.b64decode(b64_data)
                    img_stream = BytesIO(img_bytes)
                elif logo_file and os.path.exists(logo_file):
                    with open(logo_file, 'rb') as lf:
                        img_stream = BytesIO(lf.read())
                if img_stream:
                    width_arg = None
                    height_arg = None
                    if PILImage is not None:
                        img_stream.seek(0)
                        pil_img = PILImage.open(img_stream)
                        orig_w, orig_h = pil_img.size
                        if orig_h > 0:
                            scale = float(logo_height) / float(orig_h)
                            width_arg = orig_w * scale
                            height_arg = logo_height
                        img_stream.seek(0)
                    rl_img = RLImage(img_stream, width=width_arg, height=height_arg)
                    if logo_position == 'left':
                        rl_img.hAlign = 'LEFT'
                    elif logo_position == 'right':
                        rl_img.hAlign = 'RIGHT'
                    else:
                        rl_img.hAlign = 'CENTER'
                    story.append(rl_img)
                    story.append(Spacer(1, 6))
            except Exception:
                pass
        # Add bank information at top if configured
        if show_bank_info and bank_name and bank_info_location == "top":
            story.append(Paragraph(f"🏦 {bank_name}", bank_style))
            if bank_address:
                story.append(Paragraph(bank_address, bank_style))
            if bank_phone or bank_website:
                contact_info = []
                if bank_phone:
                    contact_info.append(f"Tel: {bank_phone}")
                if bank_website:
                    contact_info.append(f"Web: {bank_website}")
                story.append(Paragraph(" | ".join(contact_info), bank_style))
            story.append(Spacer(1, 12))
        
        # Add title
        story.append(Paragraph(title, title_style))
        
        # Add subtitle
        if subtitle:
            story.append(Paragraph(subtitle, subtitle_style))
        
        # Add generation date
        if show_date:
            current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            story.append(Paragraph(f"Generated on: {current_date}", date_style))
        
        story.append(Spacer(1, 20))
    
    # Generate chart if chart_data is provided
    chart_data = header_config.get('chart_data')
    chart_type = header_config.get('chart_type', 'bar')
    if chart_data and chart_data.get('labels') and chart_data.get('values'):
        try:
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
            from io import BytesIO
            
            # Create chart
            fig, ax = plt.subplots(figsize=(8, 5))
            
            if chart_type == 'bar':
                ax.barh(chart_data['labels'], chart_data['values'], color='#4472C4')
                ax.set_xlabel('Controls Count')
                ax.set_ylabel('Component')
            elif chart_type == 'pie':
                ax.pie(chart_data['values'], labels=chart_data['labels'], autopct='%1.1f%%')
            
            ax.set_title(title if title else 'Chart')
            plt.tight_layout()
            
            # Save chart to buffer
            chart_buffer = BytesIO()
            plt.savefig(chart_buffer, format='png', dpi=150, bbox_inches='tight')
            chart_buffer.seek(0)
            plt.close()
            
            # Add chart to PDF
            chart_img = RLImage(chart_buffer, width=6*inch, height=3.5*inch)
            chart_img.hAlign = 'CENTER'
            story.append(chart_img)
            story.append(Spacer(1, 20))
        except Exception as e:
            print(f"Error generating chart: {e}")
            pass
    
    # Table data with Arabic text support and multi-line text handling
    # Create styles for table cells
    styles = getSampleStyleSheet()
    
    # Header style
    header_style = ParagraphStyle(
        'TableHeader',
        parent=styles['Normal'],
        fontSize=12,
        fontName=ARABIC_FONT_NAME or 'Helvetica-Bold',
        alignment=TA_CENTER,
        textColor=colors.whitesmoke,
        spaceAfter=6,
        spaceBefore=6
    )
    
    # Data cell style
    data_style = ParagraphStyle(
        'TableCell',
        parent=styles['Normal'],
        fontSize=10,
        fontName=ARABIC_FONT_NAME or 'Helvetica',
        alignment=TA_CENTER,
        spaceAfter=4,
        spaceBefore=4,
        leading=12
    )
    
    # Process columns with Paragraph objects for multi-line support
    processed_columns = [Paragraph(shape_text_for_arabic(str(col)), header_style) for col in columns]
    
    # Process data rows with Paragraph objects for multi-line support
    processed_data_rows = []
    for row in data_rows:
        processed_row = [Paragraph(shape_text_for_arabic(str(cell)), data_style) for cell in row]
        processed_data_rows.append(processed_row)
    
    table_data = [processed_columns] + processed_data_rows

    # Optional footer totals row for PDF
    footer_totals_cols = header_config.get("tableFooterTotals", []) or []
    if isinstance(footer_totals_cols, list) and len(footer_totals_cols) > 0:
        name_to_index = {str(col): idx for idx, col in enumerate(columns)}
        totals_row = [Paragraph("", data_style)] * len(columns)
        totals_row[0] = Paragraph(shape_text_for_arabic("Total"), data_style)
        for col_name in footer_totals_cols:
            if str(col_name) in name_to_index:
                idx = name_to_index[str(col_name)]
                s = 0.0
                for r in data_rows:
                    try:
                        val = r[idx]
                        if val is None or val == "":
                            continue
                        s += float(str(val).replace(',', ''))
                    except Exception:
                        pass
                totals_row[idx] = Paragraph(shape_text_for_arabic(f"{s:,.2f}"), data_style)
        table_data.append(totals_row)
    
    # Create table with configuration-based styling and column widths
    # Calculate column widths to leave some margin
    num_cols = len(table_data[0]) if table_data else 1
    available_width = page_size[0] - (margin * 2)  # Subtract left and right margins
    col_width = available_width / num_cols if num_cols > 0 else available_width
    col_widths = [col_width] * num_cols
    
    table = Table(table_data, repeatRows=1, colWidths=col_widths)
    
    # Build table style based on configuration with Arabic font support
    table_style = [
        # Header row styling
        ('BACKGROUND', (0, 0), (-1, 0), header_bg_color_rl),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), ARABIC_FONT_NAME or 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('TOPPADDING', (0, 0), (-1, 0), 12),
        
        # Data rows styling
        ('BACKGROUND', (0, 1), (-1, -1), body_bg_color_rl),
        ('FONTNAME', (0, 1), (-1, -1), ARABIC_FONT_NAME or 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
        ('TOPPADDING', (0, 1), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
        
        # Grid lines
        ('GRID', (0, 0), (-1, -1), border_width, border_color_rl),
        
        # Alternating row colors if needed
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey])
    ]

    # If totals row exists, style it like a bold footer
    if isinstance(footer_totals_cols, list) and len(footer_totals_cols) > 0:
        last_row_index = len(table_data) - 1
        table_style += [
            ('BACKGROUND', (0, last_row_index), (-1, last_row_index), header_bg_color_rl),
            ('TEXTCOLOR', (0, last_row_index), (-1, last_row_index), colors.whitesmoke),
            ('FONTNAME', (0, last_row_index), (-1, last_row_index), 'Helvetica-Bold'),
        ]
    
    table.setStyle(TableStyle(table_style))
    story.append(table)
    
    # Add watermark if enabled
    if watermark_enabled:
        try:
            from utils.export_utils import add_watermark_to_pdf
            add_watermark_to_pdf(story, header_config)
        except Exception as e:
            pass  # Continue without watermark if there's an error
    
    # Add bank information at bottom if configured
    if show_bank_info and bank_name and bank_info_location == "bottom":
        story.append(Spacer(1, 20))
        story.append(Paragraph(f"🏦 {bank_name}", bank_style))
        if bank_address:
            story.append(Paragraph(bank_address, bank_style))
        if bank_phone or bank_website:
            contact_info = []
            if bank_phone:
                contact_info.append(f"Tel: {bank_phone}")
            if bank_website:
                contact_info.append(f"Web: {bank_website}")
            story.append(Paragraph(" | ".join(contact_info), bank_style))

    # Add footer elements
    if footer_show_date or footer_show_confidentiality or footer_show_page_numbers:
        story.append(Spacer(1, 20))
        
        footer_style = ParagraphStyle(
            'Footer',
            parent=styles['Normal'],
            fontSize=8,
            textColor=colors.grey,
            alignment=TA_CENTER
        )
        
        footer_text = []
        if footer_show_date:
            current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            footer_text.append(f"Generated on: {current_date}")
        if footer_show_confidentiality:
            footer_text.append(footer_confidentiality_text)
        if footer_show_page_numbers:
            footer_text.append("Page &P of &N")
        
        if footer_text:
            story.append(Paragraph(" | ".join(footer_text), footer_style))
    
    # Build the PDF
    doc.build(story)
    
    # Save export record to database (best-effort)
    try:
        import pyodbc
        from config import get_database_connection_string
        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                IF NOT EXISTS (
                  SELECT * FROM INFORMATION_SCHEMA.TABLES 
                  WHERE TABLE_NAME = 'report_exports' AND TABLE_SCHEMA='dbo'
                )
                BEGIN
                  CREATE TABLE dbo.report_exports (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    title NVARCHAR(255) NOT NULL,
                    src NVARCHAR(1024) NULL,
                    format NVARCHAR(20) NOT NULL,
                    dashboard NVARCHAR(100) NULL,
                    created_at DATETIME2 DEFAULT GETDATE()
                  )
                END
                """
            )
            conn.commit()
            export_title = header_config.get("title", "Dynamic Report") if header_config else "Dynamic Report"
            export_title = f"{export_title} - {datetime.now().strftime('%Y-%m-%d')}"
            export_dashboard = header_config.get("dashboard", "dynamic") if header_config else "dynamic"
            cursor.execute(
                """
                INSERT INTO dbo.report_exports (title, src, format, dashboard)
                VALUES (?, ?, ?, ?)
                """,
                (export_title, file_path, 'pdf', export_dashboard)
            )
            conn.commit()
        finally:
            cursor.close()
            conn.close()
    except Exception:
        pass

    with open(file_path, 'rb') as f:
        content = f.read()
    
    return content
