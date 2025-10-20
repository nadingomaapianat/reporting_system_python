"""
PDF Report Utilities - Reusable functions for PDF generation
"""
from io import BytesIO
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import inch
from datetime import datetime
import os
import base64

def generate_pdf_report(columns, data_rows, header_config=None):
    """Generate PDF report from dynamic data with full header configuration support"""
    
    # Get default header config if none provided
    if not header_config:
        from export_utils import get_default_header_config
        header_config = get_default_header_config("dynamic")
    
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=72, leftMargin=72, topMargin=72, bottomMargin=18)
    story = []
    
    # Create styles
    styles = getSampleStyleSheet()
    
    # Add header
    if header_config.get('includeHeader', True):
        # Title
        title_style = ParagraphStyle(
            'Header',
            parent=styles['Title'],
            fontSize=16,
            textColor=colors.HexColor(header_config.get('fontColor', '#1F4E79')),
            alignment=1,  # Center alignment
            spaceAfter=12
        )
        story.append(Paragraph(header_config.get('title', 'Dashboard Report'), title_style))
        
        # Subtitle
        if header_config.get('subtitle'):
            subtitle_style = ParagraphStyle(
                'Subtitle',
                parent=styles['Normal'],
                fontSize=12,
                textColor=colors.HexColor(header_config.get('fontColor', '#1F4E79')),
                alignment=1,
                spaceAfter=20
            )
            story.append(Paragraph(header_config.get('subtitle', ''), subtitle_style))
        
        # Bank info
        bank_name = header_config.get('bankName', '')
        bank_address = header_config.get('bankAddress', '')
        bank_phone = header_config.get('bankPhone', '')
        bank_website = header_config.get('bankWebsite', '')
        
        if bank_name:
            bank_style = ParagraphStyle(
                'Bank',
                parent=styles['Normal'],
                fontSize=10,
                textColor=colors.HexColor(header_config.get('fontColor', '#1F4E79')),
                alignment=1,
                spaceAfter=6
            )
            story.append(Paragraph(bank_name, bank_style))
            if bank_address:
                story.append(Paragraph(bank_address, bank_style))
            if bank_phone and bank_website:
                story.append(Paragraph(f"Tel: {bank_phone} | Web: {bank_website}", bank_style))
        
        # Date
        if header_config.get('showDate', True):
            current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            date_style = ParagraphStyle(
                'Date',
                parent=styles['Normal'],
                fontSize=10,
                textColor=colors.grey,
                alignment=1,
                spaceAfter=20
            )
            story.append(Paragraph(f"Generated on: {current_date}", date_style))
        
        story.append(Spacer(1, 20))
    
    # Add table
    if data_rows and len(data_rows) > 0:
        # Convert columns to strings if they're dictionaries
        if columns and len(columns) > 0 and isinstance(columns[0], dict):
            column_headers = [col.get('label', col.get('key', 'Unknown')) for col in columns]
        else:
            column_headers = columns if columns else ['Data']
        
        table_data = [column_headers] + data_rows
        table = Table(table_data, repeatRows=1)
        
        # Table styling
        table_header_bg = colors.HexColor(header_config.get('tableHeaderBgColor', '#1F4E79'))
        table_body_bg = colors.HexColor(header_config.get('tableBodyBgColor', '#FFFFFF'))
        
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), table_header_bg),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), table_body_bg),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
        ]))
        story.append(table)
    else:
        story.append(Paragraph("No data available", styles['Normal']))
    
    # Add footer
    if header_config.get('footerShowDate', True) or header_config.get('footerShowConfidentiality', True):
        story.append(Spacer(1, 20))
        footer_style = ParagraphStyle(
            'Footer',
            parent=styles['Normal'],
            fontSize=8,
            textColor=colors.grey,
            alignment=1
        )
        
        if header_config.get('footerShowDate', True):
            current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            story.append(Paragraph(f"Generated on: {current_date}", footer_style))
        
        if header_config.get('footerShowConfidentiality', True):
            confidentiality_text = header_config.get('footerConfidentialityText', 'Confidential Report - Internal Use Only')
            story.append(Paragraph(confidentiality_text, footer_style))
    
    # Build PDF
    doc.build(story)
    buffer.seek(0)
    return buffer.getvalue()
