"""
Comprehensive PDF Generation Utilities - Dynamic PDF Generator with All Features
Supports: Arabic text, tables, charts, logos, watermarks, custom styling, and more
"""
import os
import sys
import base64
import re
from datetime import datetime
from io import BytesIO
from typing import Dict, Any, Optional, List
from pathlib import Path

# ReportLab imports
from reportlab.lib.pagesizes import A4, letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

# Arabic font support
try:
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    import arabic_reshaper
    from bidi.algorithm import get_display
    ARABIC_AVAILABLE = True
except Exception:
    ARABIC_AVAILABLE = False

# Register Arabic font
ARABIC_FONT_NAME = None
if ARABIC_AVAILABLE:
    try:
        # Try bundled fonts first
        fonts_dir = Path(__file__).parent / 'fonts'
        preferred = [
            fonts_dir / 'NotoNaskhArabic-Regular.ttf',
            fonts_dir / 'Amiri-Regular.ttf',
            fonts_dir / 'Tahoma.ttf',
        ]
        
        candidates = []
        for p in preferred:
            if p.exists():
                candidates.append(p)
        
        if fonts_dir.exists():
            candidates.extend([p for p in fonts_dir.glob('*.ttf') if p not in candidates])
        
        # Windows system fonts as fallback
        windows_fonts = Path('C:/Windows/Fonts')
        if windows_fonts.exists():
            candidates.extend([
                windows_fonts / 'tahoma.ttf',
                windows_fonts / 'segoeui.ttf',
                windows_fonts / 'arial.ttf',
            ])
        
        for fpath in candidates:
            try:
                pdfmetrics.registerFont(TTFont('ArabicMain', str(fpath)))
                ARABIC_FONT_NAME = 'ArabicMain'
                break
            except Exception:
                continue
    except Exception:
        ARABIC_FONT_NAME = None

# Fallback Tahoma registration for Windows
try:
    if not ARABIC_FONT_NAME:
        tahoma_paths = [
            'C:/Windows/Fonts/tahoma.ttf',
            'C:/Windows/Fonts/TAHOMA.TTF',
            'C:/Windows/Fonts/Tahoma.ttf',
        ]
        for path in tahoma_paths:
            if os.path.exists(path):
                pdfmetrics.registerFont(TTFont('Tahoma', path))
                ARABIC_FONT_NAME = 'Tahoma'
                ARABIC_AVAILABLE = True
                break
except Exception:
    pass

def shape_text_for_arabic(text: str) -> str:
    """Shape Arabic text for proper display"""
    if not text:
        return text
    
    if not ARABIC_AVAILABLE:
        return text
    
    try:
        # Check if text contains Arabic characters
        arabic_chars = any('\u0600' <= char <= '\u06FF' for char in text)
        if not arabic_chars:
            return text
        
        # Process Arabic text
        reshaped_text = arabic_reshaper.reshape(text)
        bidi_text = get_display(reshaped_text)
        return bidi_text
    except Exception:
        return text

def format_column_name(key: str) -> str:
    """Convert snake_case or camelCase to Title Case"""
    formatted = re.sub(r'[_]|([a-z])([A-Z])', r'\1 \2', key)
    return formatted.title()

def hex_to_color(hex_color: str):
    """Convert hex color to ReportLab color object"""
    if hex_color.startswith('#'):
        hex_color = hex_color[1:]
    try:
        r = int(hex_color[0:2], 16) / 255.0
        g = int(hex_color[2:4], 16) / 255.0
        b = int(hex_color[4:6], 16) / 255.0
        return colors.Color(r, g, b)
    except:
        return colors.HexColor(f"#{hex_color}")

def get_writer():
    """Get debug writer function"""
    def write_debug(msg: str):
        timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        msg_with_time = f"[{timestamp}] {msg}"
        with open('debug_log.txt', 'a', encoding='utf-8') as f:
            f.write(f"{msg_with_time}\n")
            f.flush()
        sys.stderr.write(f"{msg_with_time}\n")
        sys.stderr.flush()
    return write_debug

write_debug = get_writer()

def generate_pdf_report(
    columns: List[str], 
    data_rows: List[List[Any]], 
    header_config: Optional[Dict[str, Any]] = None,
    return_file_path: bool = False
) -> bytes:
    """
    Comprehensive PDF generator with all features:
    - Arabic text support
    - Custom logos and bank info
    - Charts and graphs
    - Watermarks
    - Custom colors and styling
    - Dynamic table sizing
    - Multi-line text support
    """
    
    # Get default config if none provided (uses centralized config system)
    if not header_config:
        from utils.export_utils import get_default_header_config
        header_config = get_default_header_config("dynamic")
    
    # Extract configuration values
    include_header = header_config.get("includeHeader", True)
    title = header_config.get("title", "Report")
    subtitle = header_config.get("subtitle", "")
    
    # Margins
    right_margin = header_config.get('rightMargin', 0)  # No margin for maximum table width
    left_margin = header_config.get('leftMargin', 0)  # No margin for maximum table width
    top_margin = header_config.get('topMargin', 72)
    bottom_margin = header_config.get('bottomMargin', 72)
    
    # Colors
    font_color = header_config.get("fontColor", "#1F4E79")
    table_header_bg_color = header_config.get("tableHeaderBgColor", "#1F4E79")
    table_body_bg_color = header_config.get("tableBodyBgColor", "#FFFFFF")
    border_color = header_config.get("borderColor", "#000000")
    
    # Bank info
    show_bank_info = header_config.get("showBankInfo", True)
    bank_name = header_config.get("bankName", "")
    bank_address = header_config.get("bankAddress", "")
    bank_phone = header_config.get("bankPhone", "")
    bank_website = header_config.get("bankWebsite", "")
    
    # Logo
    show_logo = header_config.get("showLogo", True)
    logo_base64 = header_config.get("logoBase64", "")
    logo_position = header_config.get("logoPosition", "left")
    logo_height = header_config.get("logoHeight", 36)
    
    # Convert colors
    font_color_rl = hex_to_color(font_color)
    header_bg_color_rl = hex_to_color(table_header_bg_color)
    body_bg_color_rl = hex_to_color(table_body_bg_color)
    border_color_rl = hex_to_color(border_color)
    
    # Create document
    buffer = BytesIO()
    page_size = A4 if len(columns) <= 6 else letter
    
    doc = SimpleDocTemplate(
        buffer,
        pagesize=page_size,
        rightMargin=right_margin,
        leftMargin=left_margin,
        topMargin=top_margin,
        bottomMargin=bottom_margin
    )
    
    styles = getSampleStyleSheet()
    story = []
    
    # HEADER SECTION
    if include_header:
        # Logo
        if show_logo and logo_base64:
            try:
                img_bytes = base64.b64decode(logo_base64.split(',')[-1])
                img_stream = BytesIO(img_bytes)
                
                try:
                    from PIL import Image as PILImage
                    pil_img = PILImage.open(img_stream)
                    orig_w, orig_h = pil_img.size
                    if orig_h > 0:
                        scale = float(logo_height) / float(orig_h)
                        width = orig_w * scale
                        height = logo_height
                    img_stream.seek(0)
                except:
                    width = None
                    height = logo_height
                
                logo_img = RLImage(img_stream, width=width, height=height)
                if logo_position == 'left':
                    logo_img.hAlign = 'LEFT'
                elif logo_position == 'right':
                    logo_img.hAlign = 'RIGHT'
                else:
                    logo_img.hAlign = 'CENTER'
                story.append(logo_img)
                story.append(Spacer(1, 12))
            except:
                pass
        
        # Bank info
        if show_bank_info and bank_name:
            bank_style = ParagraphStyle(
                'BankInfo',
                parent=styles['Normal'],
                fontSize=11,
                textColor=font_color_rl,
                alignment=TA_CENTER,
                spaceAfter=3
            )
            story.append(Paragraph(shape_text_for_arabic(bank_name), bank_style))
            if bank_address:
                story.append(Paragraph(shape_text_for_arabic(bank_address), bank_style))
            if bank_phone or bank_website:
                contact = []
                if bank_phone:
                    contact.append(f"Tel: {bank_phone}")
                if bank_website:
                    contact.append(f"Web: {bank_website}")
                story.append(Paragraph(" | ".join(contact), bank_style))
            story.append(Spacer(1, 12))
        
        # Title
        title_style = ParagraphStyle(
            'Title',
            parent=styles['Title'],
            fontSize=18,
            textColor=font_color_rl,
            alignment=TA_CENTER,
            spaceAfter=12,
            fontName=ARABIC_FONT_NAME or 'Helvetica-Bold'
        )
        story.append(Paragraph(shape_text_for_arabic(title), title_style))
        
        # Subtitle
        if subtitle:
            subtitle_style = ParagraphStyle(
                'Subtitle',
                parent=styles['Normal'],
                fontSize=12,
                textColor=font_color_rl,
                alignment=TA_CENTER,
                spaceAfter=12,
                fontName=ARABIC_FONT_NAME or 'Helvetica'
            )
            story.append(Paragraph(shape_text_for_arabic(subtitle), subtitle_style))
        
        # Date
        if header_config.get("showDate", True):
            date_style = ParagraphStyle(
                'Date',
                parent=styles['Normal'],
                fontSize=10,
                textColor=colors.grey,
                alignment=TA_CENTER,
                spaceAfter=12
            )
            current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            story.append(Paragraph(f"Generated on: {current_date}", date_style))
        
        story.append(Spacer(1, 20))
    
    # CHART SECTION (if chart data provided)
    chart_data = header_config.get('chart_data')
    chart_type = header_config.get('chart_type', 'bar')
    try:
        write_debug(f"PDF chart precheck: chart_type={chart_type}, has_chart_data={bool(chart_data)}")
        if isinstance(chart_data, dict):
            write_debug(f"PDF chart precheck lengths: labels={len(chart_data.get('labels', []) if chart_data else [])}, values={len(chart_data.get('values', []) if chart_data else [])}")
    except Exception:
        pass
    if chart_data and chart_data.get('labels') and chart_data.get('values'):
        try:
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
            
            # Debug chart data
            try:
                write_debug(f"PDF chart: type={chart_type}, labels_count={len(chart_data.get('labels', []))}, values_count={len(chart_data.get('values', []))}")
                if chart_data.get('labels'):
                    write_debug(f"PDF chart sample label[0]: {str(chart_data['labels'][0])}")
                if chart_data.get('values'):
                    write_debug(f"PDF chart sample value[0]: {chart_data['values'][0]}")
            except Exception:
                pass

            fig, ax = plt.subplots(figsize=(8, 5))
            
            # Sanitize labels/values
            raw_labels = chart_data.get('labels') or []
            raw_values = chart_data.get('values') or []
            labels = [str(x) for x in raw_labels]
            values: List[float] = []
            for v in raw_values:
                try:
                    # Convert Decimals/strings to float
                    values.append(float(v))
                except Exception:
                    values.append(0.0)

            # Ensure matching lengths
            min_len = min(len(labels), len(values))
            labels = labels[:min_len]
            values = values[:min_len]

            # Guard: if all zeros, avoid pie errors and still render axes
            has_positive = any(v > 0 for v in values)

            if chart_type == 'bar':
                ax.barh(labels, values, color='#4472C4')
                ax.set_xlabel('Count')
                ax.set_ylabel('Category')
                ax.set_xlim(left=0)
            elif chart_type == 'line':
                # Line chart over categorical x-axis
                x = list(range(len(labels)))
                ax.plot(x, values, marker='o', color='#4472C4')
                ax.set_xticks(x)
                ax.set_xticklabels(labels, rotation=45, ha='right')
                ax.set_ylabel('Count')
                ax.grid(True, linestyle='--', alpha=0.3)
            elif chart_type == 'pie':
                if not has_positive:
                    # Render a placeholder to avoid empty chart
                    values = [1]
                    labels = ['No Data']
                ax.pie(values, labels=labels, autopct='%1.1f%%')
            
            plt.tight_layout()
            
            chart_buffer = BytesIO()
            plt.savefig(chart_buffer, format='png', dpi=150, bbox_inches='tight')
            chart_buffer.seek(0)
            plt.close()
            
            chart_img = RLImage(chart_buffer, width=6*inch, height=3.5*inch)
            chart_img.hAlign = 'CENTER'
            story.append(chart_img)
            story.append(Spacer(1, 20))
        except Exception as e:
            write_debug(f"Error generating chart: {e}")
    
    # TABLE SECTION
    if data_rows and len(data_rows) > 0:
        # Convert columns to headers
        if columns and len(columns) > 0 and isinstance(columns[0], dict):
            column_headers = [col.get('label', col.get('key', 'Unknown')) for col in columns]
        else:
            column_headers = columns if columns else ['Data']
        
        # Create styles for table cells with Arabic support
        header_style = ParagraphStyle(
            'TableHeader',
            parent=styles['Normal'],
            fontSize=12,
            alignment=TA_CENTER,
            textColor=colors.whitesmoke,
            leading=14,
            fontName=ARABIC_FONT_NAME or 'Helvetica-Bold'
        )
        
        cell_style = ParagraphStyle(
            'TableCell',
            parent=styles['Normal'],
            fontSize=9,
            alignment=TA_LEFT,
            leading=11,
            fontName=ARABIC_FONT_NAME or 'Helvetica'
        )
        
        # Process headers
        processed_headers = [
            Paragraph(shape_text_for_arabic(str(h)), header_style) 
            for h in column_headers
        ]
        
        # Process data rows with defensive truncation for extremely long cells
        max_cell_chars = int(header_config.get('maxCellChars', 300))
        processed_rows = []
        for row in data_rows:
            out_row = []
            for cell in row:
                text = '' if cell is None else str(cell)
                if len(text) > max_cell_chars:
                    text = text[:max_cell_chars] + '…'
                out_row.append(Paragraph(shape_text_for_arabic(text), cell_style))
            processed_rows.append(out_row)
        
        # Calculate column widths (full width with margins)
        # Add small buffer to compensate for any internal reportlab padding
        available_width = page_size[0] - (left_margin + right_margin) + 20
        num_cols = len(column_headers)
        # Weighted widths: give any column with 'name' more width; '#' minimal
        weights: List[float] = []
        for label in column_headers:
            label_str = str(label)
            lower = label_str.lower().strip()
            if lower == '#':
                weights.append(0.6)
            elif 'name' in lower:
                weights.append(2.0)
            else:
                weights.append(1.0)
        total_weight = sum(weights) if sum(weights) > 0 else float(num_cols)
        col_widths = [available_width * (w / total_weight) for w in weights]

        # Split very large tables into multiple smaller tables to prevent oversized flowables
        max_rows_per_table = int(header_config.get('maxRowsPerTable', 20))
        total_rows = len(processed_rows)
        start_idx = 0
        batch_num = 0
        # Defensive: if rows extremely large, lower batch size further
        if total_rows > 300 and max_rows_per_table >= 20:
            max_rows_per_table = 15

        while start_idx < total_rows:
            end_idx = min(start_idx + max_rows_per_table, total_rows)
            batch_rows = processed_rows[start_idx:end_idx]
            table_data = [processed_headers] + batch_rows

            table = Table(table_data, colWidths=col_widths, repeatRows=1)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), header_bg_color_rl),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), ARABIC_FONT_NAME or 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 5),  # Reduced from 10 to 5
                ('TOPPADDING', (0, 0), (-1, 0), 5),  # Reduced from 10 to 5
                ('BACKGROUND', (0, 1), (-1, -1), body_bg_color_rl),
                ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
                ('FONTNAME', (0, 1), (-1, -1), ARABIC_FONT_NAME or 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
                ('TOPPADDING', (0, 1), (-1, -1), 2),  # Reduced from 4 to 2
                ('BOTTOMPADDING', (0, 1), (-1, -1), 2),  # Reduced from 4 to 2
                ('LEFTPADDING', (0, 0), (-1, -1), 2),  # Reduced from 3 to 2
                ('RIGHTPADDING', (0, 0), (-1, -1), 2),  # Reduced from 3 to 2
                ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
                ('GRID', (0, 0), (-1, -1), 0.5, border_color_rl),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ]))

            # Append table directly; widths already span available width and rows are chunked
            story.append(table)
            # Add a small spacer and a page break between batches (except after last batch)
            start_idx = end_idx
            batch_num += 1
            if start_idx < total_rows:
                story.append(Spacer(1, 12))
                story.append(PageBreak())
    else:
        try:
            write_debug("PDF chart: no chart_data or empty labels/values - skipping chart rendering")
        except Exception:
            pass
        story.append(Paragraph("No data available", styles['Normal']))
    
    # FOOTER SECTION
    story.append(Spacer(1, 20))
    
    footer_items = []
    if header_config.get('footerShowDate', True):
        footer_items.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if header_config.get('footerShowConfidentiality', True):
        footer_items.append(header_config.get('footerConfidentialityText', 'Confidential Report - Internal Use Only'))
    
    if footer_items:
        footer_style = ParagraphStyle(
            'Footer',
            parent=styles['Normal'],
            fontSize=8,
            textColor=colors.grey,
            alignment=TA_CENTER
        )
        story.append(Paragraph(" | ".join(footer_items), footer_style))
    
    # WATERMARK (if enabled)
    watermark_callback = None
    if header_config.get('watermarkEnabled', False):
        def draw_watermark(canv, _doc):
            try:
                wm_text = shape_text_for_arabic(header_config.get('watermarkText', 'CONFIDENTIAL'))
                canv.saveState()
                opacity = max(0.05, min(0.3, header_config.get('watermarkOpacity', 10) / 100.0))
                gray = 0.6 + (0.4 * (1 - opacity))
                canv.setFillColorRGB(gray, gray, gray)
                font_name = ARABIC_FONT_NAME if ARABIC_FONT_NAME else 'Helvetica'
                canv.setFont(font_name, 48)
                page_width, page_height = page_size
                canv.translate(page_width / 2.0, page_height / 2.0)
                if header_config.get('watermarkDiagonal', True):
                    canv.rotate(45)
                canv.drawCentredString(0, 0, wm_text)
                canv.restoreState()
            except:
                pass
        
        watermark_callback = draw_watermark
    
    # Build PDF
    if watermark_callback:
        doc.build(story, onFirstPage=watermark_callback, onLaterPages=watermark_callback)
    else:
        doc.build(story)
    
    buffer.seek(0)
    return buffer.getvalue()

