"""
PDF generation service
"""
import io
import matplotlib.pyplot as plt
from typing import Dict, Any, List, Optional
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak
from reportlab.platypus.flowables import KeepInFrame
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from datetime import datetime

from shared_pdf_utils import (
    create_standard_document, create_standard_styles, create_header_styles,
    add_standard_logo_and_bank_info, add_standard_title_and_subtitle,
    create_standard_footer_elements, create_standard_watermark_callback,
    ARABIC_FONT_NAME, shape_text_for_arabic
)

class PDFService:
    """Service for PDF generation"""
    
    def __init__(self):
        self.styles = getSampleStyleSheet()
        self.font_name = ARABIC_FONT_NAME if ARABIC_FONT_NAME else 'Helvetica'

    def _para(self, text: str, size: int = 9, leading: int = 12) -> Paragraph:
        raw = str(text) if text is not None else ''
        shaped = shape_text_for_arabic(raw)
        # Detect Arabic codepoints for RTL alignment
        has_arabic = any('\u0600' <= ch <= '\u06FF' for ch in raw)
        style = ParagraphStyle(
            'Base',
            fontSize=size,
            leading=leading,
            fontName=self.font_name,
            alignment=2 if has_arabic else 0  # TA_RIGHT for Arabic, TA_LEFT otherwise
        )
        return Paragraph(shaped, style)
    
    async def generate_risks_pdf(self, risks_data: Dict[str, Any], start_date: Optional[str] = None, 
                                end_date: Optional[str] = None, header_config: Optional[Dict[str, Any]] = None,
                                card_type: Optional[str] = None, only_card: bool = False) -> bytes:
        """Generate risks PDF report"""
        try:
            # Create document
            buffer = io.BytesIO()
            doc = create_standard_document(buffer)
            
            # Create styles
            custom_styles = create_standard_styles()
            header_styles = create_header_styles(header_config)
            
            # Build content
            content = []
            
            # Add header elements
            add_standard_logo_and_bank_info(content, header_config)
            add_standard_title_and_subtitle(content, header_config)
            
            # Add dashboard-specific content
            if only_card and card_type:
                content.extend(await self._generate_risks_card_content(card_type, risks_data, custom_styles))
            else:
                content.extend(await self._generate_risks_full_content(risks_data, custom_styles))
            
            # Add footer
            footer_elements = create_standard_footer_elements(header_config)
            content.extend(footer_elements)
            
            # Create watermark callback
            watermark_callback = create_standard_watermark_callback(header_config)
            
            # Build PDF
            doc.build(content, onFirstPage=watermark_callback, onLaterPages=watermark_callback)
            
            buffer.seek(0)
            return buffer.getvalue()
            
        except Exception as e:
            raise
    
    async def generate_controls_pdf(self, controls_data: Dict[str, Any], start_date: Optional[str] = None,
                                   end_date: Optional[str] = None, header_config: Optional[Dict[str, Any]] = None,
                                   card_type: Optional[str] = None, only_card: bool = False) -> bytes:
        """Generate controls PDF report"""
        try:
            # Create document
            buffer = io.BytesIO()
            doc = create_standard_document(buffer)
            
            # Create styles
            custom_styles = create_standard_styles()
            header_styles = create_header_styles(header_config)
            
            # Build content
            content = []
            
            # Add header elements
            add_standard_logo_and_bank_info(content, header_config)
            add_standard_title_and_subtitle(content, header_config)
            
            # Add dashboard-specific content
            if only_card and card_type:
                content.extend(await self._generate_controls_card_content(card_type, controls_data, custom_styles))
            else:
                content.extend(await self._generate_controls_full_content(controls_data, custom_styles))
            
            # Add footer
            footer_elements = create_standard_footer_elements(header_config)
            content.extend(footer_elements)
            
            # Create watermark callback
            watermark_callback = create_standard_watermark_callback(header_config)
            
            # Build PDF
            doc.build(content, onFirstPage=watermark_callback, onLaterPages=watermark_callback)
            
            buffer.seek(0)
            return buffer.getvalue()
            
        except Exception as e:
            raise

    async def generate_incidents_pdf(self, incidents_data: Dict[str, Any], start_date: Optional[str] = None,
                                     end_date: Optional[str] = None, header_config: Optional[Dict[str, Any]] = None,
                                     card_type: Optional[str] = None, only_card: bool = False) -> bytes:
        """Generate incidents PDF report with chart+table sections and Arabic support"""
        buffer = io.BytesIO()
        doc = create_standard_document(buffer)
        custom_styles = create_standard_styles()
        content: List[Any] = []

        # Header
        add_standard_logo_and_bank_info(content, header_config or {})
        add_standard_title_and_subtitle(content, header_config or {})

        # Body
        print(f"PDF DEBUG: only_card={only_card} (type: {type(only_card)})")
        print(f"PDF DEBUG: card_type={card_type} (type: {type(card_type)})")
        print(f"PDF DEBUG: only_card and card_type = {only_card and card_type}")
        
        if only_card and card_type:
            print("PDF DEBUG: Using single card content")
            # For totalIncidents card, fetch the incidents list
            if card_type == 'totalIncidents':
                from services.api_service import APIService
                api_service = APIService()
                try:
                    incidents_list_data = await api_service.get_incidents_card_data('total', start_date, end_date)
                    incidents_data['incidentsList'] = incidents_list_data
                except Exception as e:
                    print(f"Warning: Could not fetch incidents list: {e}")
                    incidents_data['incidentsList'] = []
            
            content.extend(await self._generate_incidents_card_content(card_type, incidents_data, custom_styles))
        else:
            print("PDF DEBUG: Using full content")
            content.extend(await self._generate_incidents_full_content(incidents_data, custom_styles))

        # Footer + watermark
        content.extend(create_standard_footer_elements(header_config or {}))
        watermark_callback = create_standard_watermark_callback(header_config or {})
        doc.build(content, onFirstPage=watermark_callback, onLaterPages=watermark_callback)
        buffer.seek(0)
        return buffer.getvalue()

    async def _generate_incidents_card_content(self, card_type: str, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        sections: List[Any] = []
        if card_type in ['totalIncidents']:
            sections.extend(await self._generate_incidents_total_content(data, styles_map))
        elif card_type in ['byCategory']:
            sections.extend(await self._generate_incidents_by_category_content(data, styles_map))
        elif card_type in ['byStatus']:
            sections.extend(await self._generate_incidents_by_status_content(data, styles_map))
        elif card_type in ['monthlyTrend']:
            sections.extend(await self._generate_incidents_monthly_trend_content(data, styles_map))
        elif card_type in ['topFinancialImpacts']:
            sections.extend(await self._generate_incidents_top_impacts_content(data, styles_map))
        elif card_type in ['netLossAndRecovery']:
            sections.extend(await self._generate_incidents_net_loss_content(data, styles_map))
        elif card_type in ['overallStatuses']:
            sections.extend(await self._generate_incidents_overall_table_content(data, styles_map))
        return sections

    async def _generate_incidents_full_content(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        sections: List[Any] = []
        sections.extend(await self._generate_incidents_by_category_content(data, styles_map))
        sections.extend(await self._generate_incidents_by_status_content(data, styles_map))
        sections.extend(await self._generate_incidents_monthly_trend_content(data, styles_map))
        sections.extend(await self._generate_incidents_top_impacts_content(data, styles_map))
        sections.extend(await self._generate_incidents_overall_table_content(data, styles_map))
        return sections

    async def _generate_incidents_total_content(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("Total Incidents"), styles_map['CardTitle']))
        content.append(Spacer(1, 8))
        
        # Get total incidents data
        total_incidents = data.get('totalIncidents', 0)
        
        # Add incidents list if data exists
        if total_incidents > 0:
            content.append(Paragraph(shape_text_for_arabic("All Incidents List"), styles_map['CardTitle']))
            content.append(Spacer(1, 8))
            
            # Try to get incidents list from various possible field names
            incidents_list = (data.get('incidentsList') or 
                            data.get('incidents') or 
                            data.get('allIncidents') or 
                            data.get('incidentsData') or 
                            [])
            
            if incidents_list:
                # Create table styles for proper Arabic and multi-line support
                from reportlab.lib.styles import ParagraphStyle
                from reportlab.lib.enums import TA_LEFT, TA_CENTER
                
                table_header_style = ParagraphStyle(
                    'TableHeader',
                    parent=styles_map['Normal'],
                    fontSize=10,
                    fontName=self.font_name,
                    textColor=colors.white,
                    alignment=TA_CENTER,
                    spaceAfter=6,
                    spaceBefore=6
                )
                
                table_data_style = ParagraphStyle(
                    'TableData',
                    parent=styles_map['Normal'],
                    fontSize=9,
                    fontName=self.font_name,
                    textColor=colors.black,
                    alignment=TA_LEFT,
                    spaceAfter=4,
                    spaceBefore=4,
                    leading=12
                )
                
                # Prepare table data with proper Arabic text support using Paragraph objects
                table_data = [
                    [Paragraph(shape_text_for_arabic('Index'), table_header_style), 
                     Paragraph(shape_text_for_arabic('Code'), table_header_style), 
                     Paragraph(shape_text_for_arabic('Title'), table_header_style), 
                     Paragraph(shape_text_for_arabic('Created At'), table_header_style)]
                ]
                
                for i, incident in enumerate(incidents_list):  # Remove 100 limit
                    # Format Created At date to be more readable
                    created_at = incident.get('createdAt', 'N/A')
                    if created_at and created_at != 'N/A':
                        try:
                            from datetime import datetime
                            # Parse ISO format and format to readable date
                            if 'T' in created_at:
                                dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                                created_at = dt.strftime('%Y-%m-%d %H:%M')
                            else:
                                created_at = str(created_at)
                        except:
                            created_at = str(created_at)
                    
                    # Create Paragraph objects for proper Arabic text and multi-line support
                    table_data.append([
                        Paragraph(str(i + 1), table_data_style),
                        Paragraph(shape_text_for_arabic(str(incident.get('code', 'N/A'))), table_data_style),
                        Paragraph(shape_text_for_arabic(str(incident.get('title', 'N/A'))), table_data_style),
                        Paragraph(shape_text_for_arabic(created_at), table_data_style)
                    ])
                
                # Create table with proper column widths for multi-line content
                incidents_table = Table(table_data, colWidths=[0.5*inch, 1*inch, 3*inch, 1.5*inch])
                incidents_table.setStyle(TableStyle([
                    # Header styling
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1F4E79')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('TOPPADDING', (0, 0), (-1, 0), 12),
                    
                    # Body styling
                    ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                    ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
                    ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
                    ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                    ('FONTSIZE', (0, 1), (-1, -1), 9),
                    ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
                    ('TOPPADDING', (0, 1), (-1, -1), 8),
                    
                    # Grid and borders
                    ('GRID', (0, 0), (-1, -1), 1, colors.black),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    
                    # Alternating row colors
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F8F9FA')]),
                    
                    # Word wrapping for multi-line content
                    ('WORDWRAP', (0, 0), (-1, -1), 'CJK')
                ]))
                content.append(incidents_table)
            else:
                content.append(Paragraph(shape_text_for_arabic("Incidents list data not available"), styles_map['Normal']))
        else:
            content.append(Paragraph(shape_text_for_arabic("No incidents found in the system."), styles_map['Normal']))
        
        return content

    async def _generate_incidents_by_category_content(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("Incidents by Category"), styles_map['CardTitle']))
        content.append(Spacer(1, 8))
        # Try both possible field names
        rows = data.get('incidentsByCategory') or data.get('categoryDistribution') or []
        mapped = [
            { 'name': (r.get('name') or r.get('category_name') or 'Unknown'), 'value': r.get('value') or r.get('count') or 0 }
            for r in rows
        ]
        if not mapped:
            content.append(Paragraph(shape_text_for_arabic("No data available."), styles_map['Normal']))
            return content
        # chart
        labels = [m['name'] for m in mapped]
        values = [m['value'] for m in mapped]
        fig, ax = plt.subplots(figsize=(7, 3))
        ax.bar(labels, values)
        ax.set_title('Incidents by Category')
        ax.tick_params(axis='x', labelrotation=45)
        buf = io.BytesIO(); plt.tight_layout(); fig.savefig(buf, format='png', dpi=150, bbox_inches='tight'); plt.close(fig); buf.seek(0)
        content.append(Image(buf, width=5.6*inch, height=2.4*inch))
        content.append(Spacer(1, 8))
        # table
        tbl_data = [[shape_text_for_arabic('Category'), shape_text_for_arabic('Count')]] + [
            [shape_text_for_arabic(m['name']), str(m['value'])] for m in mapped
        ]
        tbl = Table(tbl_data, colWidths=[4.6*inch, 1.2*inch])
        tbl.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E3F2FD')),
            ('FONTNAME', (0, 0), (-1, 0), self.font_name),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.HexColor('#FFFFFF'), colors.HexColor('#F5F5F5')])
        ]))
        content.append(tbl); content.append(Spacer(1, 12))
        return content

    async def _generate_incidents_by_status_content(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("Incidents by Status"), styles_map['CardTitle']))
        content.append(Spacer(1, 8))
        # Try both possible field names
        rows = data.get('incidentsByStatus') or data.get('statusDistribution') or []
        mapped = [
            { 'name': (r.get('name') or r.get('status') or 'Unknown'), 'value': r.get('value') or r.get('count') or 0 }
            for r in rows
        ]
        if not mapped:
            content.append(Paragraph(shape_text_for_arabic("No data available."), styles_map['Normal']))
            return content
        # chart
        labels = [m['name'] for m in mapped]; values = [m['value'] for m in mapped]
        fig, ax = plt.subplots(figsize=(7, 3))
        if any(v > 0 for v in values):
            ax.pie(values, labels=labels, autopct='%1.1f%%', startangle=90)
            ax.set_title('Incidents by Status')
        else:
            ax.text(0.5,0.5,'No data',ha='center',va='center',transform=ax.transAxes)
        buf = io.BytesIO(); plt.tight_layout(); fig.savefig(buf, format='png', dpi=150, bbox_inches='tight'); plt.close(fig); buf.seek(0)
        content.append(Image(buf, width=5.6*inch, height=2.4*inch)); content.append(Spacer(1,8))
        # table
        tbl_data = [[shape_text_for_arabic('Status'), shape_text_for_arabic('Count')]] + [
            [shape_text_for_arabic(m['name']), str(m['value'])] for m in mapped
        ]
        tbl = Table(tbl_data, colWidths=[4.6*inch, 1.2*inch]); tbl.setStyle(TableStyle([
            ('BACKGROUND', (0,0),(-1,0), colors.HexColor('#E3F2FD')),
            ('FONTNAME', (0,0),(-1,0), self.font_name),
            ('ROWBACKGROUNDS', (0,1),(-1,-1), [colors.HexColor('#FFFFFF'), colors.HexColor('#F5F5F5')])
        ])); content.append(tbl); content.append(Spacer(1,12))
        return content

    async def _generate_incidents_monthly_trend_content(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("Incidents Monthly Trend"), styles_map['CardTitle']))
        content.append(Spacer(1,8))
        rows = data.get('monthlyTrend') or []
        if not rows:
            content.append(Paragraph(shape_text_for_arabic("No data available."), styles_map['Normal']))
            return content
        months = [r.get('month') or r.get('month_year') or '' for r in rows]
        counts = [r.get('incident_count') or r.get('value') or 0 for r in rows]
        losses = [r.get('total_loss') or 0 for r in rows]
        fig, ax = plt.subplots(figsize=(7,3))
        if any(counts): ax.plot(months, counts, marker='o', label='Incidents')
        if any(losses): ax.plot(months, losses, marker='s', label='Total Loss')
        ax.tick_params(axis='x', labelrotation=45); ax.grid(True, alpha=0.3); ax.legend(); ax.set_title('Incidents Monthly Trend')
        buf = io.BytesIO(); plt.tight_layout(); fig.savefig(buf, format='png', dpi=150, bbox_inches='tight'); plt.close(fig); buf.seek(0)
        content.append(Image(buf, width=5.6*inch, height=2.4*inch)); content.append(Spacer(1,8))
        tbl_data = [[shape_text_for_arabic('Month'), shape_text_for_arabic('Incidents'), shape_text_for_arabic('Total Loss')]] + [
            [shape_text_for_arabic(months[i]), str(counts[i]), str(losses[i])] for i in range(len(months))
        ]
        tbl = Table(tbl_data, colWidths=[2.4*inch, 1.2*inch, 2.2*inch]); tbl.setStyle(TableStyle([
            ('BACKGROUND', (0,0),(-1,0), colors.HexColor('#E3F2FD')),
            ('FONTNAME', (0,0),(-1,0), self.font_name),
            ('ROWBACKGROUNDS', (0,1),(-1,-1), [colors.HexColor('#FFFFFF'), colors.HexColor('#F5F5F5')])
        ])); content.append(tbl); content.append(Spacer(1,12))
        return content

    async def _generate_incidents_top_impacts_content(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("Top Financial Impacts"), styles_map['CardTitle']))
        content.append(Spacer(1,8))
        rows = data.get('topFinancialImpacts') or []
        mapped = [r for r in rows if (r.get('net_loss') or 0) > 0]
        if not mapped:
            content.append(Paragraph(shape_text_for_arabic("No data available."), styles_map['Normal']))
            return content
        labels = [(r.get('financial_impact_name') or 'Unknown') for r in mapped]
        values = [r.get('net_loss') or 0 for r in mapped]
        fig, ax = plt.subplots(figsize=(7,3))
        ax.pie(values, labels=labels, autopct='%1.1f%%', startangle=90); ax.set_title('Top Financial Impacts')
        buf = io.BytesIO(); plt.tight_layout(); fig.savefig(buf, format='png', dpi=150, bbox_inches='tight'); plt.close(fig); buf.seek(0)
        content.append(Image(buf, width=5.6*inch, height=2.4*inch)); content.append(Spacer(1,8))
        tbl_data = [[shape_text_for_arabic('Impact'), shape_text_for_arabic('Net Loss')]] + [
            [shape_text_for_arabic(lbl), str(val)] for lbl, val in zip(labels, values)
        ]
        tbl = Table(tbl_data, colWidths=[4.6*inch, 1.2*inch]); tbl.setStyle(TableStyle([
            ('BACKGROUND', (0,0),(-1,0), colors.HexColor('#E3F2FD')),
            ('FONTNAME', (0,0),(-1,0), self.font_name),
            ('ROWBACKGROUNDS', (0,1),(-1,-1), [colors.HexColor('#FFFFFF'), colors.HexColor('#F5F5F5')])
        ])); content.append(tbl); content.append(Spacer(1,12))
        return content

    async def _generate_incidents_net_loss_content(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("Net Loss by Incident"), styles_map['CardTitle']))
        content.append(Spacer(1,8))
        rows = data.get('netLossAndRecovery') or []
        mapped = [r for r in rows if (r.get('net_loss') or 0) > 0]
        if not mapped:
            content.append(Paragraph(shape_text_for_arabic("No data available."), styles_map['Normal']))
            return content
        
        # Create chart
        labels = [(r.get('incident_title') or r.get('title') or 'Unknown') for r in mapped]
        values = [r.get('net_loss') or 0 for r in mapped]
        fig, ax = plt.subplots(figsize=(7,3))
        ax.plot(range(len(labels)), values, marker='o', color='#1F4E79', linewidth=2, markersize=4)
        ax.set_title('Net Loss by Incident')
        ax.set_ylabel('Net Loss')
        ax.set_xticks(range(len(labels)))
        ax.set_xticklabels(labels, rotation=45, ha='right')
        ax.grid(True, alpha=0.3)
        buf = io.BytesIO()
        plt.tight_layout()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        content.append(Image(buf, width=5.6*inch, height=2.4*inch))
        content.append(Spacer(1,8))
        
        # Create table
        tbl_data = [[shape_text_for_arabic('Incident Title'), shape_text_for_arabic('Net Loss')]] + [
            [shape_text_for_arabic(lbl), f"${val:,.2f}"] for lbl, val in zip(labels, values)
        ]
        tbl = Table(tbl_data, colWidths=[4.6*inch, 1.2*inch])
        tbl.setStyle(TableStyle([
            ('BACKGROUND', (0,0),(-1,0), colors.HexColor('#E3F2FD')),
            ('FONTNAME', (0,0),(-1,0), self.font_name),
            ('ROWBACKGROUNDS', (0,1),(-1,-1), [colors.HexColor('#FFFFFF'), colors.HexColor('#F5F5F5')])
        ]))
        content.append(tbl)
        content.append(Spacer(1,12))
        return content

    async def _generate_incidents_overall_table_content(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("Overall Incident Statuses"), styles_map['CardTitle']))
        content.append(Spacer(1,8))
        rows = data.get('statusOverview') or data.get('overallStatuses') or []
        if not rows:
            content.append(Paragraph(shape_text_for_arabic("No data available."), styles_map['Normal']))
            return content
        tbl_data: List[List[Any]] = [["#", shape_text_for_arabic('Code'), shape_text_for_arabic('Title'), shape_text_for_arabic('Approval')]]
        for i, r in enumerate(rows, 1):
            tbl_data.append([
                str(i),
                self._para(r.get('code','N/A')), 
                self._para(r.get('title') or r.get('incident_title') or 'N/A'),
                self._para(r.get('status') or r.get('incident_status') or 'N/A')
            ])
        tbl = Table(tbl_data, colWidths=[0.4*inch, 1.1*inch, 3.8*inch, 1.1*inch]); tbl.setStyle(TableStyle([
            ('BACKGROUND', (0,0),(-1,0), colors.HexColor('#E3F2FD')),
            ('FONTNAME', (0,0),(-1,0), self.font_name),
            ('ROWBACKGROUNDS', (0,1),(-1,-1), [colors.HexColor('#FFFFFF'), colors.HexColor('#F5F5F5')])
        ])); content.append(tbl); content.append(Spacer(1,12))
        return content
    
    async def _generate_risks_card_content(self, card_type: str, risks_data: Dict[str, Any], 
                                         custom_styles: Dict[str, Any]) -> List[Any]:
        """Generate content for specific risks card"""
        content = []
        
        if card_type == 'totalRisks':
            content.extend(await self._generate_total_risks_content(risks_data, custom_styles))
        elif card_type == 'newThisMonth':
            content.extend(await self._generate_new_risks_content(risks_data, custom_styles))
        elif card_type == 'highRisk':
            content.extend(await self._generate_high_risk_content(risks_data, custom_styles))
        elif card_type == 'mediumRisk':
            content.extend(await self._generate_medium_risk_content(risks_data, custom_styles))
        elif card_type == 'lowRisk':
            content.extend(await self._generate_low_risk_content(risks_data, custom_styles))
        elif card_type == 'risksByCategory':
            content.extend(await self._generate_risks_by_category_content(risks_data, custom_styles))
        elif card_type == 'risksByEventType':
            content.extend(await self._generate_risks_by_event_type_content(risks_data, custom_styles))
        elif card_type == 'riskTrends':
            content.extend(await self._generate_risk_trends_content(risks_data, custom_styles))
        
        return content
    
    async def _generate_controls_card_content(self, card_type: str, controls_data: Dict[str, Any],
                                            custom_styles: Dict[str, Any]) -> List[Any]:
        """Generate content for specific controls card"""
        content = []
        
        if card_type == 'totalControls':
            content.extend(await self._generate_total_controls_content(controls_data, custom_styles))
        elif card_type == 'unmappedControls':
            content.extend(await self._generate_unmapped_controls_content(controls_data, custom_styles))
        elif card_type == 'pendingPreparer':
            content.extend(await self._generate_pending_preparer_content(controls_data, custom_styles))
        elif card_type == 'pendingChecker':
            content.extend(await self._generate_pending_checker_content(controls_data, custom_styles))
        elif card_type == 'pendingReviewer':
            content.extend(await self._generate_pending_reviewer_content(controls_data, custom_styles))
        elif card_type == 'pendingAcceptance':
            content.extend(await self._generate_pending_acceptance_content(controls_data, custom_styles))
        elif card_type == 'department':
            content.extend(await self._generate_department_chart_content(controls_data, custom_styles))
        elif card_type == 'risk':
            content.extend(await self._generate_risk_response_chart_content(controls_data, custom_styles))
        elif card_type == 'overallStatuses':
            content.extend(await self._generate_overall_statuses_table(controls_data, custom_styles))
        
        return content
    
    async def _generate_total_risks_content(self, risks_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        """Generate total risks content"""
        content = []
        
        # Add title
        content.append(Paragraph(shape_text_for_arabic("Total Risks Details"), custom_styles['CardTitle']))
        content.append(Spacer(1, 12))
        
        # Get all risks data
        all_risks = risks_data.get('allRisks', [])
        if not all_risks:
            content.append(Paragraph(shape_text_for_arabic("No risks data available."), custom_styles['Normal']))
            return content
        
        # Create table
        table_data = [['#', 'Code', 'Risk Name', 'Created At']]
        
        for i, risk in enumerate(all_risks, 1):  # Show all risks
            risk_name = risk.get('title', risk.get('risk_name', 'Unknown'))
            created_at = risk.get('created_at', 'N/A')
            
            # Format date to readable format
            try:
                if created_at and created_at != 'N/A':
                    if 'T' in created_at:
                        date_obj = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        created_at = date_obj.strftime('%B %d, %Y at %I:%M %p')
                    else:
                        created_at = 'N/A'
            except:
                created_at = 'N/A'
            
            # Create multi-line text using Paragraph for better text wrapping
            risk_name_para = self._para(risk_name, 9, 13)
            
            code_para = self._para(risk.get('code', 'N/A'), 9, 13)
            
            date_para = self._para(created_at, 9, 13)
            
            table_data.append([
                str(i),
                code_para,
                risk_name_para,
                date_para
            ])
        
        # Create table
        table = Table(table_data, colWidths=[0.5*inch, 1*inch, 3*inch, 1.5*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor(custom_styles.get('tableHeaderBgColor', '#E3F2FD'))),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor(custom_styles.get('tableBodyBgColor', '#F5F5F5'))),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        content.append(table)
        return content
    
    async def _generate_chart_image(self, chart_data: List[Dict[str, Any]], chart_type: str, title: str) -> Image:
        """Generate chart image"""
        try:
            plt.figure(figsize=(8, 4))
            
            if chart_type == 'pie':
                labels = [item.get('name', 'N/A') for item in chart_data]
                values = [item.get('value', 0) for item in chart_data]
                plt.pie(values, labels=labels, autopct='%1.1f%%')
            elif chart_type == 'bar':
                labels = [item.get('name', 'N/A') for item in chart_data]
                values = [item.get('value', 0) for item in chart_data]
                plt.bar(labels, values)
                plt.xticks(rotation=45)
            
            plt.title(title)
            plt.tight_layout()
            
            # Save to buffer
            chart_buffer = io.BytesIO()
            plt.savefig(chart_buffer, format='png', dpi=150, bbox_inches='tight')
            chart_buffer.seek(0)
            chart_img = Image(chart_buffer)
            chart_img.drawWidth = 6*inch
            chart_img.drawHeight = 3*inch
            plt.close()
            
            return chart_img
        except Exception as e:
            return None
    
    async def _generate_pie_chart(self, data: List[Dict[str, Any]], title: str, name_key: str, value_key: str) -> Any:
        """Generate a pie chart from data"""
        try:
            import matplotlib.pyplot as plt
            import io
            
            if not data:
                return None
            
            # Extract labels and values
            labels = [item.get(name_key, 'Unknown') for item in data]
            values = [item.get(value_key, 0) for item in data]
            
            # Create pie chart
            plt.figure(figsize=(8, 6))
            colors = plt.cm.Set3(range(len(labels)))
            wedges, texts, autotexts = plt.pie(values, labels=labels, autopct='%1.1f%%', 
                                             colors=colors, startangle=90)
            
            # Customize text
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')
                autotext.set_fontsize(8)
            
            plt.title(title, fontsize=14, fontweight='bold', pad=20)
            plt.axis('equal')
            
            # Save to buffer
            chart_buffer = io.BytesIO()
            plt.savefig(chart_buffer, format='png', dpi=150, bbox_inches='tight')
            chart_buffer.seek(0)
            chart_img = Image(chart_buffer)
            chart_img.drawWidth = 6*inch
            chart_img.drawHeight = 4*inch
            plt.close()
            
            return chart_img
        except Exception as e:
            return None
    
    async def _generate_bar_chart(self, data: List[Dict[str, Any]], title: str, name_key: str, value_key: str) -> Any:
        """Generate a bar chart from data"""
        try:
            import matplotlib.pyplot as plt
            import io
            
            if not data:
                return None
            
            # Extract labels and values
            labels = [item.get(name_key, 'Unknown') for item in data]
            values = [item.get(value_key, 0) for item in data]
            
            # Create bar chart
            plt.figure(figsize=(10, 6))
            colors = plt.cm.Set3(range(len(labels)))
            bars = plt.bar(labels, values, color=colors)
            
            # Add value labels on top of bars
            for bar, value in zip(bars, values):
                plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(values)*0.01,
                        str(value), ha='center', va='bottom', fontweight='bold')
            
            plt.title(title, fontsize=14, fontweight='bold', pad=20)
            plt.xlabel('Category', fontsize=12, fontweight='bold')
            plt.ylabel('Count', fontsize=12, fontweight='bold')
            plt.xticks(rotation=45, ha='right')
            plt.grid(axis='y', alpha=0.3)
            plt.tight_layout()
            
            # Save to buffer
            chart_buffer = io.BytesIO()
            plt.savefig(chart_buffer, format='png', dpi=150, bbox_inches='tight')
            chart_buffer.seek(0)
            chart_img = Image(chart_buffer)
            chart_img.drawWidth = 6*inch
            chart_img.drawHeight = 4*inch
            plt.close()
            
            return chart_img
        except Exception as e:
            return None
    
    async def _generate_multi_line_chart(self, data: List[Dict[str, Any]], title: str) -> Any:
        """Generate a multi-line chart from risk trends data"""
        try:
            import matplotlib.pyplot as plt
            import io
            
            if not data:
                return None
            
            # Extract data
            months = [item.get('month', 'Unknown') for item in data]
            total_risks = [item.get('total_risks', 0) for item in data]
            new_risks = [item.get('new_risks', 0) for item in data]
            mitigated_risks = [item.get('mitigated_risks', 0) for item in data]
            
            # Create multi-line chart
            plt.figure(figsize=(10, 6))
            
            # Plot lines
            plt.plot(months, total_risks, marker='o', linewidth=2, label='Total Risks', color='#1f77b4')
            plt.plot(months, new_risks, marker='s', linewidth=2, label='New Risks', color='#ff7f0e')
            plt.plot(months, mitigated_risks, marker='^', linewidth=2, label='Mitigated Risks', color='#2ca02c')
            
            # Customize chart
            plt.title(title, fontsize=14, fontweight='bold', pad=20)
            plt.xlabel('Month', fontsize=12, fontweight='bold')
            plt.ylabel('Number of Risks', fontsize=12, fontweight='bold')
            plt.legend(loc='upper left', frameon=True, fancybox=True, shadow=True)
            plt.grid(True, alpha=0.3)
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            
            # Save to buffer
            chart_buffer = io.BytesIO()
            plt.savefig(chart_buffer, format='png', dpi=150, bbox_inches='tight')
            chart_buffer.seek(0)
            chart_img = Image(chart_buffer)
            chart_img.drawWidth = 6*inch
            chart_img.drawHeight = 4*inch
            plt.close()
            
            return chart_img
        except Exception as e:
            return None
    
    # Add other content generation methods as needed...
    async def _generate_new_risks_content(self, risks_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        """Generate new risks content with actual data table"""
        content = []
        
        # Add title
        content.append(Paragraph(shape_text_for_arabic("New Risks This Month"), custom_styles['CardTitle']))
        content.append(Spacer(1, 12))
        
        # Get new risks data
        new_risks = risks_data.get('newRisks', [])
        
        if not new_risks:
            content.append(Paragraph(shape_text_for_arabic("No new risks data available."), custom_styles['Normal']))
            return content
        
        # Create table
        table_data = [['#', 'Code', 'Risk Name', 'Inherent Value', 'Created At']]
        
        for i, risk in enumerate(new_risks, 1):  # Show all new risks
            risk_name = risk.get('title', risk.get('risk_name', 'Unknown'))
            inherent_value = risk.get('inherent_value', 'N/A')
            created_at = risk.get('created_at', 'N/A')
            
            # Format date to readable format
            try:
                if created_at and created_at != 'N/A':
                    if 'T' in created_at:
                        date_obj = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        created_at = date_obj.strftime('%B %d, %Y at %I:%M %p')
                    else:
                        created_at = 'N/A'
            except:
                created_at = 'N/A'
            
            # Create multi-line text using Paragraph for better text wrapping
            risk_name_para = self._para(risk_name, 9, 13)
            
            code_para = self._para(risk.get('code', 'N/A'), 9, 13)
            
            inherent_para = self._para(inherent_value, 9, 13)
            
            date_para = self._para(created_at, 9, 13)
            
            table_data.append([
                str(i),
                code_para,
                risk_name_para,
                inherent_para,
                date_para
            ])
        
        # Create table
        table = Table(table_data, colWidths=[0.5*inch, 1*inch, 3*inch, 1*inch, 1.5*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor(custom_styles.get('tableHeaderBgColor', '#E3F2FD'))),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor(custom_styles.get('fontColor', '#000000'))),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor(custom_styles.get('tableBodyBgColor', '#FFFFFF'))),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#CCCCCC')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        
        content.append(table)
        content.append(Spacer(1, 12))
        
        return content
    
    async def _generate_high_risk_content(self, risks_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        """Generate high risk content with actual data table"""
        content = []
        
        # Add title
        content.append(Paragraph(shape_text_for_arabic("High Risk Details"), custom_styles['CardTitle']))
        content.append(Spacer(1, 12))
        
        # Get high risk data
        high_risks = risks_data.get('highRisk', [])
        
        if not high_risks:
            content.append(Paragraph(shape_text_for_arabic("No high risk data available."), custom_styles['Normal']))
            return content
        
        # Create table
        table_data = [['#', 'Code', 'Risk Name', 'Created At']]
        
        for i, risk in enumerate(high_risks, 1):  # Show all high risks
            risk_name = risk.get('title', risk.get('risk_name', 'Unknown'))
            created_at = risk.get('created_at', 'N/A')
            
            # Format date to readable format
            try:
                if created_at and created_at != 'N/A':
                    if 'T' in created_at:
                        date_obj = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        created_at = date_obj.strftime('%B %d, %Y at %I:%M %p')
                    else:
                        created_at = 'N/A'
            except:
                created_at = 'N/A'
            
            # Create multi-line text using Paragraph for better text wrapping
            risk_name_para = self._para(risk_name, 9, 13)
            
            code_para = self._para(risk.get('code', 'N/A'), 9, 13)
            
            date_para = self._para(created_at, 9, 13)
            
            table_data.append([
                str(i),
                code_para,
                risk_name_para,
                date_para
            ])
        
        # Create table
        table = Table(table_data, colWidths=[0.5*inch, 1*inch, 3*inch, 1.5*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor(custom_styles.get('tableHeaderBgColor', '#E3F2FD'))),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor(custom_styles.get('tableBodyBgColor', '#F5F5F5'))),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        content.append(table)
        return content
    
    async def _generate_medium_risk_content(self, risks_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        """Generate medium risk content with actual data table"""
        content = []
        
        # Add title
        content.append(Paragraph(shape_text_for_arabic("Medium Risk Details"), custom_styles['CardTitle']))
        content.append(Spacer(1, 12))
        
        # Get medium risk data
        medium_risks = risks_data.get('mediumRisk', [])
        if not medium_risks:
            content.append(Paragraph(shape_text_for_arabic("No medium risk data available."), custom_styles['Normal']))
            return content
        
        # Create table
        table_data = [['#', 'Code', 'Risk Name', 'Created At']]
        
        for i, risk in enumerate(medium_risks, 1):  # Show all medium risks
            risk_name = risk.get('title', risk.get('risk_name', 'Unknown'))
            created_at = risk.get('created_at', 'N/A')
            
            # Format date to readable format
            try:
                if created_at and created_at != 'N/A':
                    if 'T' in created_at:
                        date_obj = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        created_at = date_obj.strftime('%B %d, %Y at %I:%M %p')
                    else:
                        created_at = 'N/A'
            except:
                created_at = 'N/A'
            
            # Create multi-line text using Paragraph for better text wrapping
            risk_name_para = self._para(risk_name, 9, 13)
            
            code_para = self._para(risk.get('code', 'N/A'), 9, 13)
            
            date_para = self._para(created_at, 9, 13)
            
            table_data.append([
                str(i),
                code_para,
                risk_name_para,
                date_para
            ])
        
        # Create table
        table = Table(table_data, colWidths=[0.5*inch, 1*inch, 3*inch, 1.5*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor(custom_styles.get('tableHeaderBgColor', '#E3F2FD'))),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor(custom_styles.get('tableBodyBgColor', '#F5F5F5'))),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        content.append(table)
        return content
    
    async def _generate_low_risk_content(self, risks_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        """Generate low risk content with actual data table"""
        content = []
        
        # Add title
        content.append(Paragraph(shape_text_for_arabic("Low Risk Details"), custom_styles['CardTitle']))
        content.append(Spacer(1, 12))
        
        # Get low risk data
        low_risks = risks_data.get('lowRisk', [])
        if not low_risks:
            content.append(Paragraph(shape_text_for_arabic("No low risk data available."), custom_styles['Normal']))
            return content
        
        # Create table
        table_data = [['#', 'Code', 'Risk Name', 'Created At']]
        
        for i, risk in enumerate(low_risks, 1):  # Show all low risks
            risk_name = risk.get('title', risk.get('risk_name', 'Unknown'))
            created_at = risk.get('created_at', 'N/A')
            
            # Format date to readable format
            try:
                if created_at and created_at != 'N/A':
                    if 'T' in created_at:
                        date_obj = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        created_at = date_obj.strftime('%B %d, %Y at %I:%M %p')
                    else:
                        created_at = 'N/A'
            except:
                created_at = 'N/A'
            
            # Create multi-line text using Paragraph for better text wrapping
            risk_name_para = self._para(risk_name, 9, 13)
            
            code_para = self._para(risk.get('code', 'N/A'), 9, 13)
            
            date_para = self._para(created_at, 9, 13)
            
            table_data.append([
                str(i),
                code_para,
                risk_name_para,
                date_para
            ])
        
        # Create table
        table = Table(table_data, colWidths=[0.5*inch, 1*inch, 3*inch, 1.5*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor(custom_styles.get('tableHeaderBgColor', '#E3F2FD'))),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor(custom_styles.get('tableBodyBgColor', '#F5F5F5'))),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        content.append(table)
        return content
    
    async def _generate_risks_by_category_content(self, risks_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        """Generate risks by category content with chart and data table"""
        content = []
        
        # Add title
        content.append(Paragraph("Risks by Category", custom_styles['CardTitle']))
        content.append(Spacer(1, 12))
        
        # Get risks by category data
        risks_by_category = risks_data.get('risksByCategory', [])
        
        if not risks_by_category:
            content.append(Paragraph("No risks by category data available.", custom_styles['Normal']))
            return content
        
        # Generate chart
        chart_img = await self._generate_bar_chart(
            risks_by_category, 
            "Risks by Category",
            'name', 
            'value'
        )
        
        if chart_img:
            content.append(chart_img)
            content.append(Spacer(1, 12))
        
        # Create data table
        table_data = [['Category', 'Count']]
        
        for category in risks_by_category:
            name = category.get('name', 'Unknown')
            value = category.get('value', 0)
            
            # Create multi-line text using Paragraph for better text wrapping
            name_para = Paragraph(name, ParagraphStyle(
                'CategoryName',
                fontSize=9,
                leading=12,
                fontName='Helvetica'
            ))
            
            value_para = Paragraph(str(value), ParagraphStyle(
                'CategoryValue',
                fontSize=9,
                leading=12,
                fontName='Helvetica'
            ))
            
            table_data.append([name_para, value_para])
        
        # Create table
        table = Table(table_data, colWidths=[4*inch, 1*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor(custom_styles.get('tableHeaderBgColor', '#E3F2FD'))),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor(custom_styles.get('fontColor', '#000000'))),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor(custom_styles.get('tableBodyBgColor', '#FFFFFF'))),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#CCCCCC')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        
        content.append(table)
        content.append(Spacer(1, 12))
        
        return content
    
    async def _generate_risks_by_event_type_content(self, risks_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        """Generate risks by event type content with chart and data table"""
        content = []
        
        # Add title
        content.append(Paragraph("Risks by Event Type", custom_styles['CardTitle']))
        content.append(Spacer(1, 12))
        
        # Get risks by event type data
        risks_by_event_type = risks_data.get('risksByEventType', [])
        
        if not risks_by_event_type:
            content.append(Paragraph("No risks by event type data available.", custom_styles['Normal']))
            return content
        
        # Generate chart
        chart_img = await self._generate_pie_chart(
            risks_by_event_type, 
            "Risks by Event Type",
            'name', 
            'value'
        )
        
        if chart_img:
            content.append(chart_img)
            content.append(Spacer(1, 12))
        
        # Create data table
        table_data = [['Event Type', 'Count']]
        
        for event_type in risks_by_event_type:
            name = event_type.get('name', 'Unknown')
            value = event_type.get('value', 0)
            
            # Create multi-line text using Paragraph for better text wrapping
            name_para = Paragraph(name, ParagraphStyle(
                'EventTypeName',
                fontSize=9,
                leading=12,
                fontName='Helvetica'
            ))
            
            value_para = Paragraph(str(value), ParagraphStyle(
                'EventTypeValue',
                fontSize=9,
                leading=12,
                fontName='Helvetica'
            ))
            
            table_data.append([name_para, value_para])
        
        # Create table
        table = Table(table_data, colWidths=[4*inch, 1*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor(custom_styles.get('tableHeaderBgColor', '#E3F2FD'))),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor(custom_styles.get('fontColor', '#000000'))),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor(custom_styles.get('tableBodyBgColor', '#FFFFFF'))),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#CCCCCC')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        
        content.append(table)
        content.append(Spacer(1, 12))
        
        return content
    
    async def _generate_risk_trends_content(self, risks_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        """Generate risk trends content with chart and data table"""
        content = []
        
        # Add title
        content.append(Paragraph("Risk Trends Over Time", custom_styles['CardTitle']))
        content.append(Spacer(1, 12))
        
        # Get risk trends data
        risk_trends = risks_data.get('riskTrends', [])
        
        if not risk_trends:
            content.append(Paragraph("No risk trends data available.", custom_styles['Normal']))
            return content
        
        # Generate chart
        chart_img = await self._generate_multi_line_chart(
            risk_trends, 
            "Risk Trends Over Time"
        )
        
        if chart_img:
            content.append(chart_img)
            content.append(Spacer(1, 12))
        
        # Create data table
        table_data = [['Month', 'Total Risks', 'New Risks', 'Mitigated Risks']]
        
        for trend in risk_trends:
            month = trend.get('month', 'Unknown')
            total_risks = trend.get('total_risks', 0)
            new_risks = trend.get('new_risks', 0)
            mitigated_risks = trend.get('mitigated_risks', 0)
            
            # Create multi-line text using Paragraph for better text wrapping
            month_para = Paragraph(month, ParagraphStyle(
                'TrendMonth',
                fontSize=9,
                leading=12,
                fontName='Helvetica'
            ))
            
            total_para = Paragraph(str(total_risks), ParagraphStyle(
                'TrendTotal',
                fontSize=9,
                leading=12,
                fontName='Helvetica'
            ))
            
            new_para = Paragraph(str(new_risks), ParagraphStyle(
                'TrendNew',
                fontSize=9,
                leading=12,
                fontName='Helvetica'
            ))
            
            mitigated_para = Paragraph(str(mitigated_risks), ParagraphStyle(
                'TrendMitigated',
                fontSize=9,
                leading=12,
                fontName='Helvetica'
            ))
            
            table_data.append([month_para, total_para, new_para, mitigated_para])
        
        # Create table
        table = Table(table_data, colWidths=[1*inch, 1.5*inch, 1.5*inch, 1.5*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor(custom_styles.get('tableHeaderBgColor', '#E3F2FD'))),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor(custom_styles.get('fontColor', '#000000'))),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor(custom_styles.get('tableBodyBgColor', '#FFFFFF'))),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#CCCCCC')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        
        content.append(table)
        content.append(Spacer(1, 12))
        
        return content
    
    async def _generate_total_controls_content(self, controls_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        """Generate total controls content as a table (#, Code, Control Name)"""
        content: List[Any] = []

        # Title
        content.append(Paragraph("Total Controls Details", custom_styles['CardTitle']))
        content.append(Spacer(1, 12))

        total_controls = controls_data.get('totalControls', []) or []
        if not total_controls:
            content.append(Paragraph("No total controls data available.", custom_styles['Normal']))
            return content

        # Build table data
        table_data: List[List[Any]] = [["#", "Code", "Control Name"]]

        name_style = ParagraphStyle('ControlName', fontSize=9, leading=13, fontName=self.font_name)
        code_style = ParagraphStyle('ControlCode', fontSize=9, leading=13, fontName=self.font_name)

        for i, control in enumerate(total_controls, 1):
            code = str(control.get('control_code', 'N/A') or 'N/A')
            name = str(control.get('control_name', 'N/A') or 'N/A')
            table_data.append([
                str(i),
                self._para(code, 9, 13),
                self._para(name, 9, 13)
            ])

        # Create Table
        tbl = Table(table_data, colWidths=[0.5*inch, 1.4*inch, 4.8*inch])
        tbl.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E3F2FD')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#1F4E79')),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('ALIGN', (0, 0), (-1, 0), 'LEFT'),
            ('LINEBELOW', (0, 0), (-1, 0), 0.5, colors.HexColor('#90CAF9')),

            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.HexColor('#FFFFFF'), colors.HexColor('#F5F5F5')]),
            ('ALIGN', (0, 1), (0, -1), 'LEFT'),
            ('ALIGN', (1, 1), (1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ]))

        content.append(tbl)
        content.append(Spacer(1, 12))
        return content
    
    async def _generate_unmapped_controls_content(self, controls_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        """Generate unmapped controls table (#, Code, Control Name)"""
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("Unmapped Controls"), custom_styles['CardTitle']))
        content.append(Spacer(1, 12))

        rows = controls_data.get('unmappedControls', []) or []
        if not rows:
            content.append(Paragraph(shape_text_for_arabic("No unmapped controls data available."), custom_styles['Normal']))
            return content

        data: List[List[Any]] = [["#", "Code", "Control Name"]]
        code_style = ParagraphStyle('UnmappedCode', fontSize=9, leading=13, fontName=self.font_name)
        name_style = ParagraphStyle('UnmappedName', fontSize=9, leading=13, fontName=self.font_name)
        for i, r in enumerate(rows, 1):
            code = str(r.get('control_code', 'N/A') or 'N/A')
            name = str(r.get('control_name', 'N/A') or 'N/A')
            data.append([str(i), self._para(code, 9, 13), self._para(name, 9, 13)])

        tbl = Table(data, colWidths=[0.5*inch, 1.4*inch, 4.8*inch])
        tbl.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E3F2FD')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#1F4E79')),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('LINEBELOW', (0, 0), (-1, 0), 0.5, colors.HexColor('#90CAF9')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.HexColor('#FFFFFF'), colors.HexColor('#F5F5F5')]),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        content.append(tbl)
        content.append(Spacer(1, 12))
        return content
    
    async def _generate_pending_preparer_content(self, controls_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        return await self._generate_pending_role_table("Pending Preparer Controls", controls_data.get('pendingPreparer', []) or [], 'preparerStatus', custom_styles)
    
    async def _generate_pending_checker_content(self, controls_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        return await self._generate_pending_role_table("Pending Checker Controls", controls_data.get('pendingChecker', []) or [], 'checkerStatus', custom_styles)
    
    async def _generate_pending_reviewer_content(self, controls_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        return await self._generate_pending_role_table("Pending Reviewer Controls", controls_data.get('pendingReviewer', []) or [], 'reviewerStatus', custom_styles)
    
    async def _generate_pending_acceptance_content(self, controls_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        return await self._generate_pending_role_table("Pending Acceptance Controls", controls_data.get('pendingAcceptance', []) or [], 'acceptanceStatus', custom_styles)

    async def _generate_pending_role_table(self, title: str, rows: List[Dict[str, Any]], field: str, custom_styles: Dict[str, Any]) -> List[Any]:
        content: List[Any] = []
        content.append(Paragraph(title, custom_styles['CardTitle']))
        content.append(Spacer(1, 12))
        if not rows:
            content.append(Paragraph("No data available.", custom_styles['Normal']))
            return content
        data: List[List[Any]] = [["#", "Code", "Control Name", "Status"]]
        base_style = ParagraphStyle('Base', fontSize=9, leading=13, fontName=self.font_name)
        for i, r in enumerate(rows, 1):
            status_val = r.get('status')
            if not status_val:
                status_obj = r.get(field) or {}
                status_val = status_obj.get('value') if isinstance(status_obj, dict) else status_obj or 'N/A'
            data.append([
                str(i),
                self._para(str(r.get('control_code', 'N/A') or 'N/A'), 9, 13),
                self._para(str(r.get('control_name', 'N/A') or 'N/A'), 9, 13),
                self._para(str(status_val or 'N/A'), 9, 13)
            ])
        tbl = Table(data, colWidths=[0.5*inch, 1.4*inch, 3.8*inch, 1.4*inch])
        tbl.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E3F2FD')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#1F4E79')),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('LINEBELOW', (0, 0), (-1, 0), 0.5, colors.HexColor('#90CAF9')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.HexColor('#FFFFFF'), colors.HexColor('#F5F5F5')]),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        content.append(tbl)
        content.append(Spacer(1, 12))
        return content
    
    async def _generate_department_chart_content(self, controls_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        """Generate bar chart and table for department distribution"""
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("Controls by Department"), custom_styles['CardTitle']))
        content.append(Spacer(1, 8))

        data = controls_data.get('departmentDistribution', []) or []
        if not data:
            content.append(Paragraph(shape_text_for_arabic("No department distribution data available."), custom_styles['Normal']))
            return content

        # Chart image via matplotlib
        labels = [d.get('name', 'N/A') for d in data]
        values = [d.get('value', 0) for d in data]
        fig, ax = plt.subplots(figsize=(7, 3))
        if any(v > 0 for v in values):
            ax.bar(labels, values)
            ax.set_title('Controls by Department')
            ax.tick_params(axis='x', labelrotation=45)
        else:
            ax.text(0.5, 0.5, 'No data', ha='center', va='center', transform=ax.transAxes)
        buf = io.BytesIO()
        plt.tight_layout()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        content.append(Image(buf, width=5.6*inch, height=2.4*inch))
        content.append(Spacer(1, 8))

        # Table
        table_rows: List[List[Any]] = [[shape_text_for_arabic("Department"), shape_text_for_arabic("Count")]]
        for d in data:
            table_rows.append([shape_text_for_arabic(str(d.get('name', 'N/A') or 'N/A')), shape_text_for_arabic(str(d.get('value', 0)))])
        tbl = Table(table_rows, colWidths=[4.6*inch, 1.2*inch])
        tbl.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E3F2FD')),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.HexColor('#FFFFFF'), colors.HexColor('#F5F5F5')]),
        ]))
        content.append(tbl)
        content.append(Spacer(1, 12))
        return content
    
    async def _generate_risk_response_chart_content(self, controls_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        """Generate pie chart and table for risk response distribution"""
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("Controls by Risk Response Type"), custom_styles['CardTitle']))
        content.append(Spacer(1, 8))

        data = controls_data.get('statusDistribution', []) or []
        if not data:
            content.append(Paragraph(shape_text_for_arabic("No risk response distribution data available."), custom_styles['Normal']))
            return content

        labels = [d.get('name', 'N/A') for d in data]
        values = [d.get('value', 0) for d in data]
        fig, ax = plt.subplots(figsize=(7, 3))
        if any(v > 0 for v in values):
            ax.pie(values, labels=labels, autopct='%1.1f%%', startangle=90)
            ax.set_title('Controls by Risk Response Type')
        else:
            ax.text(0.5, 0.5, 'No data', ha='center', va='center', transform=ax.transAxes)
        buf = io.BytesIO()
        plt.tight_layout()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        buf.seek(0)
        content.append(Image(buf, width=5.6*inch, height=2.4*inch))
        content.append(Spacer(1, 8))

        table_rows: List[List[Any]] = [[shape_text_for_arabic("Risk Response"), shape_text_for_arabic("Count")]]
        for d in data:
            table_rows.append([shape_text_for_arabic(str(d.get('name', 'N/A') or 'N/A')), shape_text_for_arabic(str(d.get('value', 0)))])
        tbl = Table(table_rows, colWidths=[4.6*inch, 1.2*inch])
        tbl.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E3F2FD')),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.HexColor('#FFFFFF'), colors.HexColor('#F5F5F5')]),
        ]))
        content.append(tbl)
        content.append(Spacer(1, 12))
        return content
    
    async def _generate_risks_full_content(self, risks_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        """Generate full risks content - placeholder"""
        return [Paragraph("Full Risks Content", custom_styles['Normal'])]
    
    async def _generate_controls_full_content(self, controls_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        """Generate a concise full controls report: include the two charts with tables"""
        content: List[Any] = []
        # Department chart section
        content.extend(await self._generate_department_chart_content(controls_data, custom_styles))
        # Risk response chart section
        content.extend(await self._generate_risk_response_chart_content(controls_data, custom_styles))
        return content

    async def _generate_overall_statuses_table(self, controls_data: Dict[str, Any], custom_styles: Dict[str, Any]) -> List[Any]:
        """Render Overall Control Statuses table without pagination"""
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("Overall Control Statuses"), custom_styles['CardTitle']))
        content.append(Spacer(1, 12))
        rows = controls_data.get('statusOverview', []) or []
        if not rows:
            content.append(Paragraph(shape_text_for_arabic("No data available."), custom_styles['Normal']))
            return content

        header = ["#", shape_text_for_arabic("Code"), shape_text_for_arabic("Control Name"), shape_text_for_arabic("Preparer"), shape_text_for_arabic("Checker"), shape_text_for_arabic("Reviewer"), shape_text_for_arabic("Acceptance")]
        data: List[List[Any]] = [header]
        base = ParagraphStyle('Base', fontSize=9, leading=13, fontName=self.font_name)
        for i, r in enumerate(rows, 1):
            def norm(v):
                if isinstance(v, dict):
                    return v.get('value') or 'N/A'
                return v or 'N/A'
            data.append([
                str(i),
                self._para(str(r.get('code', 'N/A') or 'N/A'), 9, 13),
                self._para(str(r.get('name', 'N/A') or 'N/A'), 9, 13),
                self._para(str(norm(r.get('preparerStatus'))), 9, 13),
                self._para(str(norm(r.get('checkerStatus'))), 9, 13),
                self._para(str(norm(r.get('reviewerStatus'))), 9, 13),
                self._para(str(norm(r.get('acceptanceStatus'))), 9, 13),
            ])

        tbl = Table(data, colWidths=[0.4*inch, 1.1*inch, 2.8*inch, 0.9*inch, 0.9*inch, 0.9*inch, 1.0*inch])
        tbl.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E3F2FD')),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.HexColor('#FFFFFF'), colors.HexColor('#F5F5F5')]),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ]))
        content.append(tbl)
        content.append(Spacer(1, 12))
        return content

    # KRI PDF Methods
    async def generate_kris_pdf(self, data: Dict[str, Any], start_date: Optional[str] = None,
                               end_date: Optional[str] = None, header_config: Optional[Dict[str, Any]] = None,
                               card_type: Optional[str] = None, only_card: bool = False, 
                               only_chart: bool = False, only_overall_table: bool = False) -> bytes:
        """Generate KRIs PDF report with chart+table sections and Arabic support"""
        buffer = io.BytesIO()
        doc = create_standard_document(buffer)
        styles_map = create_standard_styles()
        
        # Build content
        story = []
        
        # Add header
        add_standard_logo_and_bank_info(story, header_config or {})
        add_standard_title_and_subtitle(story, header_config or {})
        
        # Add content based on parameters
        if only_card and card_type:
            story.extend(await self._generate_kris_card_content(card_type, data, styles_map))
        elif only_chart and card_type:
            story.extend(await self._generate_kris_chart_content(card_type, data, styles_map))
        elif only_overall_table:
            story.extend(await self._generate_kris_overall_table_content(data, styles_map))
        else:
            story.extend(await self._generate_kris_full_content(data, styles_map))
        
        # Add footer
        story.extend(create_standard_footer_elements(header_config or {}))
        
        # Build PDF
        doc.build(story, onFirstPage=create_standard_watermark_callback(header_config or {}))
        buffer.seek(0)
        return buffer.getvalue()

    async def _generate_kris_card_content(self, card_type: str, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        sections: List[Any] = []
        if card_type in ['totalKris']:
            sections.extend(await self._generate_kris_total_content(data, styles_map))
        elif card_type in ['krisByStatus']:
            sections.extend(await self._generate_kris_by_status_content(data, styles_map))
        elif card_type in ['krisByLevel']:
            sections.extend(await self._generate_kris_by_level_content(data, styles_map))
        elif card_type in ['breachedKRIsByDepartment']:
            sections.extend(await self._generate_breached_kris_by_department_content(data, styles_map))
        elif card_type in ['kriAssessmentCount']:
            sections.extend(await self._generate_kri_assessment_count_content(data, styles_map))
        return sections

    async def _generate_kris_chart_content(self, card_type: str, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        sections: List[Any] = []
        if card_type in ['krisByStatus']:
            sections.extend(await self._generate_kris_by_status_chart(data, styles_map))
        elif card_type in ['krisByLevel']:
            sections.extend(await self._generate_kris_by_level_chart(data, styles_map))
        elif card_type in ['breachedKRIsByDepartment']:
            sections.extend(await self._generate_breached_kris_by_department_chart(data, styles_map))
        elif card_type in ['kriAssessmentCount']:
            sections.extend(await self._generate_kri_assessment_count_chart(data, styles_map))
        return sections

    async def _generate_kris_full_content(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        sections: List[Any] = []
        sections.extend(await self._generate_kris_by_status_content(data, styles_map))
        sections.extend(await self._generate_kris_by_level_content(data, styles_map))
        sections.extend(await self._generate_breached_kris_by_department_content(data, styles_map))
        sections.extend(await self._generate_kri_assessment_count_content(data, styles_map))
        return sections

    async def _generate_kris_total_content(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("Total KRIs"), styles_map['CardTitle']))
        
        # Get KRIs list
        kris_list = data.get('krisList', [])
        if not kris_list:
            kris_list = data.get('kriHealth', [])
        
        if kris_list:
            # Create table data
            table_data = [['KRI Code', 'KRI Name', 'Function', 'Level', 'Status', 'Threshold', 'Created At']]
            
            for kri in kris_list[:50]:  # Limit to 50 for PDF
                created_at = kri.get('created_at', kri.get('createdAt', 'N/A'))
                if created_at and created_at != 'N/A':
                    try:
                        # Parse and format date
                        if isinstance(created_at, str):
                            from datetime import datetime
                            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            created_at = dt.strftime('%Y-%m-%d %H:%M')
                    except:
                        pass
                
                table_data.append([
                    kri.get('code', kri.get('kri_code', 'N/A')),
                    kri.get('kri_name', kri.get('kriName', 'N/A')),
                    kri.get('function_name', kri.get('function', 'N/A')),
                    kri.get('kri_level', kri.get('level', 'N/A')),
                    kri.get('status', 'N/A'),
                    str(kri.get('threshold', 'N/A')),
                    created_at
                ])
            
            # Create table
            tbl = Table(table_data, colWidths=[1*inch, 2*inch, 1.5*inch, 1*inch, 1*inch, 1*inch, 1.2*inch])
            tbl.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1F4E79')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ]))
            content.append(tbl)
        else:
            content.append(Paragraph(shape_text_for_arabic("No KRIs data available"), styles_map['Normal']))
        
        content.append(Spacer(1, 12))
        return content

    async def _generate_kris_by_status_content(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("KRIs by Status"), styles_map['CardTitle']))
        
        # Get data
        kris_by_status = data.get('krisByStatus', [])
        if not kris_by_status:
            kris_by_status = data.get('statusDistribution', [])
        
        if kris_by_status:
            # Create chart
            chart_data = [{'name': item.get('status', 'Unknown'), 'value': item.get('count', 0)} for item in kris_by_status]
            chart_img = await self._generate_pie_chart(chart_data, "KRIs by Status", 'name', 'value')
            if chart_img:
                content.append(chart_img)
            content.append(Spacer(1, 12))
            
            # Create table
            table_data = [['Status', 'Count']]
            for item in kris_by_status:
                table_data.append([
                    item.get('status', 'Unknown'),
                    str(item.get('count', 0))
                ])
            
            tbl = Table(table_data, colWidths=[3*inch, 1*inch])
            tbl.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1F4E79')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ]))
            content.append(tbl)
        else:
            content.append(Paragraph(shape_text_for_arabic("No KRIs status data available"), styles_map['Normal']))
        
        content.append(Spacer(1, 12))
        return content

    async def _generate_kris_by_level_content(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("KRIs by Risk Level"), styles_map['CardTitle']))
        
        # Get data
        kris_by_level = data.get('krisByLevel', [])
        if not kris_by_level:
            kris_by_level = data.get('levelDistribution', [])
        
        if kris_by_level:
            # Create chart
            chart_data = [{'name': item.get('level', 'Unknown'), 'value': item.get('count', 0)} for item in kris_by_level]
            chart_img = await self._generate_pie_chart(chart_data, "KRIs by Risk Level", 'name', 'value')
            if chart_img:
                content.append(chart_img)
            content.append(Spacer(1, 12))
            
            # Create table
            table_data = [['Risk Level', 'Count']]
            for item in kris_by_level:
                table_data.append([
                    item.get('level', 'Unknown'),
                    str(item.get('count', 0))
                ])
            
            tbl = Table(table_data, colWidths=[3*inch, 1*inch])
            tbl.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1F4E79')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ]))
            content.append(tbl)
        else:
            content.append(Paragraph(shape_text_for_arabic("No KRIs level data available"), styles_map['Normal']))
        
        content.append(Spacer(1, 12))
        return content

    async def _generate_breached_kris_by_department_content(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("Breached KRIs by Department"), styles_map['CardTitle']))
        
        # Get data
        breached_kris = data.get('breachedKRIsByDepartment', [])
        if not breached_kris:
            breached_kris = data.get('breachedByDepartment', [])
        
        if breached_kris:
            # Create chart
            chart_data = [{'name': item.get('function_name', 'Unknown'), 'value': item.get('breached_count', 0)} for item in breached_kris]
            chart_img = await self._generate_bar_chart(chart_data, "Breached KRIs by Department", 'name', 'value')
            if chart_img:
                content.append(chart_img)
            content.append(Spacer(1, 12))
            
            # Create table
            table_data = [['Department', 'Breached Count']]
            for item in breached_kris:
                table_data.append([
                    item.get('function_name', 'Unknown'),
                    str(item.get('breached_count', 0))
                ])
            
            tbl = Table(table_data, colWidths=[3*inch, 1.5*inch])
            tbl.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1F4E79')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ]))
            content.append(tbl)
        else:
            content.append(Paragraph(shape_text_for_arabic("No breached KRIs data available"), styles_map['Normal']))
        
        content.append(Spacer(1, 12))
        return content

    async def _generate_kri_assessment_count_content(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("KRI Assessment Count by Department"), styles_map['CardTitle']))
        
        # Get data
        assessment_count = data.get('kriAssessmentCount', [])
        if not assessment_count:
            assessment_count = data.get('assessmentByDepartment', [])
        
        if assessment_count:
            # Create chart
            chart_data = [{'name': item.get('function_name', 'Unknown'), 'value': item.get('assessment_count', 0)} for item in assessment_count]
            chart_img = await self._generate_bar_chart(chart_data, "KRI Assessment Count by Department", 'name', 'value')
            if chart_img:
                content.append(chart_img)
            content.append(Spacer(1, 12))
            
            # Create table
            table_data = [['Department', 'Assessment Count']]
            for item in assessment_count:
                table_data.append([
                    item.get('function_name', 'Unknown'),
                    str(item.get('assessment_count', 0))
                ])
            
            tbl = Table(table_data, colWidths=[3*inch, 1.5*inch])
            tbl.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1F4E79')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ]))
            content.append(tbl)
        else:
            content.append(Paragraph(shape_text_for_arabic("No KRI assessment data available"), styles_map['Normal']))
        
        content.append(Spacer(1, 12))
        return content

    async def _generate_kris_overall_table_content(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("Overall KRI Statuses"), styles_map['CardTitle']))
        
        # Get KRIs list
        kris_list = data.get('krisList', [])
        if not kris_list:
            kris_list = data.get('kriHealth', [])
        
        if kris_list:
            # Create table data
            table_data = [['KRI Code', 'KRI Name', 'Function', 'Level', 'Status', 'Threshold', 'Created At']]
            
            for kri in kris_list[:50]:  # Limit to 50 for PDF
                created_at = kri.get('created_at', kri.get('createdAt', 'N/A'))
                if created_at and created_at != 'N/A':
                    try:
                        # Parse and format date
                        if isinstance(created_at, str):
                            from datetime import datetime
                            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            created_at = dt.strftime('%Y-%m-%d %H:%M')
                    except:
                        pass
                
                table_data.append([
                    kri.get('code', kri.get('kri_code', 'N/A')),
                    kri.get('kri_name', kri.get('kriName', 'N/A')),
                    kri.get('function_name', kri.get('function', 'N/A')),
                    kri.get('kri_level', kri.get('level', 'N/A')),
                    kri.get('status', 'N/A'),
                    str(kri.get('threshold', 'N/A')),
                    created_at
                ])
            
            # Create table
            tbl = Table(table_data, colWidths=[1*inch, 2*inch, 1.5*inch, 1*inch, 1*inch, 1*inch, 1.2*inch])
            tbl.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1F4E79')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ]))
            content.append(tbl)
        else:
            content.append(Paragraph(shape_text_for_arabic("No KRIs data available"), styles_map['Normal']))
        
        content.append(Spacer(1, 12))
        return content

    # KRI Chart-only methods
    async def _generate_kris_by_status_chart(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("KRIs by Status"), styles_map['CardTitle']))
        
        kris_by_status = data.get('krisByStatus', [])
        if kris_by_status:
            chart_data = [{'name': item.get('status', 'Unknown'), 'value': item.get('count', 0)} for item in kris_by_status]
            chart_img = await self._generate_pie_chart(chart_data, "KRIs by Status", 'name', 'value')
            if chart_img:
                content.append(chart_img)
        else:
            content.append(Paragraph(shape_text_for_arabic("No KRIs status data available"), styles_map['Normal']))
        
        content.append(Spacer(1, 12))
        return content

    async def _generate_kris_by_level_chart(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("KRIs by Risk Level"), styles_map['CardTitle']))
        
        kris_by_level = data.get('krisByLevel', [])
        if kris_by_level:
            chart_data = [{'name': item.get('level', 'Unknown'), 'value': item.get('count', 0)} for item in kris_by_level]
            chart_img = await self._generate_pie_chart(chart_data, "KRIs by Risk Level", 'name', 'value')
            if chart_img:
                content.append(chart_img)
        else:
            content.append(Paragraph(shape_text_for_arabic("No KRIs level data available"), styles_map['Normal']))
        
        content.append(Spacer(1, 12))
        return content

    async def _generate_breached_kris_by_department_chart(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("Breached KRIs by Department"), styles_map['CardTitle']))
        
        breached_kris = data.get('breachedKRIsByDepartment', [])
        if breached_kris:
            chart_data = [{'name': item.get('function_name', 'Unknown'), 'value': item.get('breached_count', 0)} for item in breached_kris]
            chart_img = await self._generate_bar_chart(chart_data, "Breached KRIs by Department", 'name', 'value')
            if chart_img:
                content.append(chart_img)
        else:
            content.append(Paragraph(shape_text_for_arabic("No breached KRIs data available"), styles_map['Normal']))
        
        content.append(Spacer(1, 12))
        return content

    async def _generate_kri_assessment_count_chart(self, data: Dict[str, Any], styles_map: Dict[str, Any]) -> List[Any]:
        content: List[Any] = []
        content.append(Paragraph(shape_text_for_arabic("KRI Assessment Count by Department"), styles_map['CardTitle']))
        
        assessment_count = data.get('kriAssessmentCount', [])
        if assessment_count:
            chart_data = [{'name': item.get('function_name', 'Unknown'), 'value': item.get('assessment_count', 0)} for item in assessment_count]
            chart_img = await self._generate_bar_chart(chart_data, "KRI Assessment Count by Department", 'name', 'value')
            if chart_img:
                content.append(chart_img)
        else:
            content.append(Paragraph(shape_text_for_arabic("No KRI assessment data available"), styles_map['Normal']))
        
        content.append(Spacer(1, 12))
        return content
