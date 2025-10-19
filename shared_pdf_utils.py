"""
Shared PDF utilities for consistent report generation across all dashboards
"""
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, BaseDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, PageBreak, KeepInFrame, Frame, PageTemplate
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.piecharts import Pie
from reportlab.graphics import renderPDF
import io
import base64
from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path

# Import Arabic font handling
try:
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    import arabic_reshaper
    from bidi.algorithm import get_display
    ARABIC_AVAILABLE = True
except Exception:
    ARABIC_AVAILABLE = False

# Try to register an Arabic-capable font if available (prefer bundled fonts)
ARABIC_FONT_NAME = None
if ARABIC_AVAILABLE:
    try:
        candidates = []
        fonts_dir = Path(__file__).parent / 'fonts'
        # Strongly preferred bundled fonts (put one of these in shared_pdf_utils.py/fonts)
        preferred = [
            fonts_dir / 'NotoNaskhArabic-Regular.ttf',
            fonts_dir / 'Amiri-Regular.ttf',
            fonts_dir / 'Tahoma.ttf',
        ]
        for p in preferred:
            if p.exists():
                candidates.append(p)
        # Any other .ttf in fonts dir
        if fonts_dir.exists():
            candidates.extend([p for p in fonts_dir.glob('*.ttf') if p not in candidates])

        # Windows system fonts (last resort)
        windows_fonts = Path('C:/Windows/Fonts')
        if windows_fonts.exists():
            candidates.extend([
                windows_fonts / 'tahoma.ttf',
                windows_fonts / 'segoeui.ttf',
                windows_fonts / 'arial.ttf',
                windows_fonts / 'times.ttf',
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

def shape_text_for_arabic(text: str) -> str:
    """Shape Arabic text for proper display"""
    if not text:
        return text
    
    # If Arabic processing is not available, return text as-is
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
        pass
        return bidi_text
    except Exception as e:
        pass
        return text

def create_standard_document(buffer: io.BytesIO, pagesize=A4) -> SimpleDocTemplate:
    """Create a standard document template with consistent margins"""
    return SimpleDocTemplate(buffer, pagesize=pagesize, rightMargin=72, leftMargin=72, topMargin=72, bottomMargin=72)

def create_standard_styles() -> Dict[str, Any]:
    """Create standard paragraph styles for all dashboards"""
    styles = getSampleStyleSheet()
    
    # Set default Arabic-capable font for body text if available
    if ARABIC_FONT_NAME:
        try:
            styles['Normal'].fontName = ARABIC_FONT_NAME
        except Exception:
            pass
    
    # Add CardTitle style
    styles.add(ParagraphStyle(
        'CardTitle',
        parent=styles['Heading1'],
        fontSize=16,
        textColor=colors.HexColor('#1F4E79'),
        alignment=1,  # Center alignment
        spaceAfter=12
    ))
    if ARABIC_FONT_NAME:
        try:
            styles['CardTitle'].fontName = ARABIC_FONT_NAME
        except Exception:
            pass
    
    return styles

def create_header_styles(header_config: Dict[str, Any]) -> Dict[str, ParagraphStyle]:
    """Create consistent header styles for all dashboards"""
    styles = create_standard_styles()
    
    # Get font color from header config
    font_color = colors.HexColor(header_config.get('fontColor', '#1F4E79'))
    
    # Standard font size mapping
    font_size_map = {'small': 14, 'medium': 18, 'large': 22}
    title_font_size = font_size_map.get(str(header_config.get('fontSize', 'medium')).lower(), 18)
    
    # Title style
    title_style = ParagraphStyle(
        'StandardTitle',
        parent=styles['Title'],
        fontSize=title_font_size,
        textColor=font_color,
        alignment=TA_CENTER,
        spaceAfter=12
    )
    if ARABIC_FONT_NAME:
        title_style.fontName = ARABIC_FONT_NAME
    
    # Subtitle style
    subtitle_style = ParagraphStyle(
        'StandardSubtitle',
        parent=styles['Normal'],
        fontSize=12,
        textColor=font_color,
        alignment=TA_CENTER,
        spaceAfter=20
    )
    if ARABIC_FONT_NAME:
        subtitle_style.fontName = ARABIC_FONT_NAME
    
    # Bank info style
    bank_style = ParagraphStyle(
        'StandardBankInfo',
        parent=styles['Normal'],
        fontSize=10,
        textColor=colors.black,
        alignment=TA_LEFT,  # Standard left alignment
        spaceAfter=6,
        leftIndent=0,
        rightIndent=0
    )
    if ARABIC_FONT_NAME:
        bank_style.fontName = ARABIC_FONT_NAME
    
    return {
        'title': title_style,
        'subtitle': subtitle_style,
        'bank': bank_style
    }

def add_standard_logo_and_bank_info(story: List, header_config: Dict[str, Any]) -> None:
    """Add logo and bank info with consistent positioning for all dashboards"""
    if not header_config.get('showBankInfo', True):
        return
    
    # Get bank information
    bank_name = header_config.get('bankName', 'PIANAT.AI')
    bank_address = header_config.get('bankAddress', 'King Abdulaziz Road, Riyadh, Saudi Arabia')
    bank_phone = header_config.get('bankPhone', '+966 11 402 9000')
    bank_website = header_config.get('bankWebsite', 'www.pianat.ai')
    
    # Get logo position
    logo_position = header_config.get('logoPosition', 'left').lower()
    
    # Add logo if available
    if header_config.get('showLogo', True) and header_config.get('logoBase64'):
        try:
            import base64
            from reportlab.platypus import Image as RLImage
            
            logo_base64 = header_config['logoBase64']
            img_bytes = base64.b64decode(logo_base64.split(',')[-1])
            img_buf = io.BytesIO(img_bytes)
            
            # Create ReportLab image with standard size
            logo_img = RLImage(img_buf, width=1.2*inch, height=0.4*inch)
            
            # Add logo with consistent positioning
            if logo_position == 'left':
                # Create a table to force left alignment
                logo_table_data = [[logo_img, ""]]
                logo_table = Table(logo_table_data, colWidths=[1.5*inch, 4*inch])
                logo_table.setStyle(TableStyle([
                    ('ALIGN', (0, 0), (0, 0), 'LEFT'),
                    ('ALIGN', (1, 0), (1, 0), 'LEFT'),
                    ('VALIGN', (0, 0), (0, 0), 'TOP'),
                    ('LEFTPADDING', (0, 0), (0, 0), 0),
                    ('RIGHTPADDING', (0, 0), (0, 0), 0),
                    ('TOPPADDING', (0, 0), (0, 0), 0),
                    ('BOTTOMPADDING', (0, 0), (0, 0), 0),
                ]))
                story.append(logo_table)
            else:
                # Center or right alignment
                story.append(logo_img)
                
        except Exception as e:
            pass
    
    # Add bank info with consistent styling
    bank_styles = create_header_styles(header_config)
    bank_style = bank_styles['bank']
    
    story.append(Paragraph(bank_name, bank_style))
    story.append(Paragraph(bank_address, bank_style))
    story.append(Paragraph(f"Tel: {bank_phone} | Web: {bank_website}", bank_style))

def add_standard_title_and_subtitle(story: List, header_config: Dict[str, Any]) -> None:
    """Add title and subtitle with consistent styling for all dashboards"""
    # Get title and subtitle
    title_text = shape_text_for_arabic(header_config.get('title', 'Dashboard Report'))
    subtitle_text = shape_text_for_arabic(header_config.get('subtitle', ''))
    
    # Create styles
    bank_styles = create_header_styles(header_config)
    title_style = bank_styles['title']
    subtitle_style = bank_styles['subtitle']
    
    # Add title
    story.append(Paragraph(title_text, title_style))
    
    # Add subtitle if provided
    if subtitle_text:
        story.append(Paragraph(subtitle_text, subtitle_style))

def create_standard_watermark_callback(header_config: Dict[str, Any]):
    """Create standard watermark callback for all dashboards"""
    def _draw_watermark(canv, _doc):
        try:
            if not header_config.get('watermarkEnabled', False):
                return
            wm_text = str(header_config.get('watermarkText', 'CONFIDENTIAL'))
            wm_text = shape_text_for_arabic(wm_text)
            canv.saveState()
            # opacity via fill color approximation
            opacity = max(0.05, min(0.3, (header_config.get('watermarkOpacity', 10) or 10) / 100.0))
            gray = 0.6 + (0.4 * (1 - opacity))
            canv.setFillColorRGB(gray, gray, gray)
            font_name = ARABIC_FONT_NAME if ARABIC_FONT_NAME else 'Helvetica'
            canv.setFont(font_name, 48)
            page_width, page_height = A4
            canv.translate(page_width / 2.0, page_height / 2.0)
            if header_config.get('watermarkDiagonal', True):
                canv.rotate(45)
            canv.drawCentredString(0, 0, wm_text)
            canv.restoreState()
        except Exception as e:
            pass
    
    return _draw_watermark

def create_standard_footer_elements(header_config: Dict[str, Any]) -> List:
    """Create standard footer elements for all dashboards"""
    footer_items = []
    
    if header_config.get('footerShowDate', True):
        footer_items.append(f"Generated on: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}")
    
    if header_config.get('footerShowConfidentiality', True):
        footer_items.append(str(header_config.get('footerConfidentialityText', 'Confidential Report - Internal Use Only')))
    
    footer_elements = []
    if footer_items:
        footer_style = ParagraphStyle(
            'StandardFooter',
            parent=getSampleStyleSheet()['Normal'],
            fontSize=8,
            textColor=colors.grey,
            alignment=TA_CENTER
        )
        if ARABIC_FONT_NAME:
            footer_style.fontName = ARABIC_FONT_NAME
        
        for line in footer_items:
            footer_elements.append(Paragraph(shape_text_for_arabic(line), footer_style))
    
    return footer_elements

def create_standard_table_style(header_config: Dict[str, Any], num_cols: int) -> TableStyle:
    """Create standard table styling for all dashboards"""
    # Get colors from header config
    table_header_bg = colors.HexColor(header_config.get('tableHeaderBgColor', '#1F4E79'))
    table_body_bg = colors.HexColor(header_config.get('tableBodyBgColor', '#FFFFFF'))
    
    table_style = [
        ('BACKGROUND', (0, 0), (-1, 0), table_header_bg),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), ARABIC_FONT_NAME or 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), table_body_bg),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]
    
    if ARABIC_FONT_NAME:
        table_style.append(('FONTNAME', (0, 1), (-1, -1), ARABIC_FONT_NAME))
    
    return TableStyle(table_style)

def generate_standard_pdf_report(
    data: Dict[str, Any], 
    dashboard_type: str,
    start_date: str = None, 
    end_date: str = None, 
    header_config: Dict[str, Any] = None,
    card_type: str = None, 
    only_card: bool = False
) -> bytes:
    """Generate a standard PDF report using shared utilities"""
    buffer = io.BytesIO()
    
    # Create standard document
    doc = create_standard_document(buffer)
    
    # Create content list
    story = []
    
    # Add standard header elements
    add_standard_logo_and_bank_info(story, header_config)
    add_standard_title_and_subtitle(story, header_config)
    
    # Add dashboard-specific content (to be implemented by each dashboard)
    # This is where each dashboard would add its specific content
    
    # Add standard footer
    footer_elements = create_standard_footer_elements(header_config)
    story.extend(footer_elements)
    
    # Create watermark callback
    watermark_callback = create_standard_watermark_callback(header_config)
    
    # Build PDF with watermark
    doc.build(story, onFirstPage=watermark_callback, onLaterPages=watermark_callback)
    
    buffer.seek(0)
    return buffer.getvalue()
