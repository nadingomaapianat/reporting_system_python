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

# Import shared utilities
from shared_pdf_utils import (
    create_standard_document, 
    add_standard_logo_and_bank_info, 
    add_standard_title_and_subtitle,
    create_standard_table_style,
    shape_text_for_arabic,
    generate_standard_pdf_report
)

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

    async def _generate_controls_card_content(self, content, card_type: str, controls_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add content for different control card types"""
        if card_type == 'actionPlansStatus':
            await self._generate_action_plans_status_content(content, controls_data)
        elif card_type == 'numberOfControlsPerComponent':
            await self._generate_number_of_controls_per_component_content(content, controls_data)
        elif card_type == 'controlsNotMappedToAssertions':
            await self._generate_controls_not_mapped_to_assertions_content(content, controls_data)
        elif card_type == 'controlsNotMappedToPrinciples':
            await self._generate_controls_not_mapped_to_principles_content(content, controls_data)

    async def _generate_action_plans_status_content(self, content, controls_data: Dict[str, Any]):
        """Generate PDF content for action plans status"""
        from reportlab.platypus import Table, TableStyle, Paragraph, Spacer
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet
        
        styles = getSampleStyleSheet()
        
        # Get the data
        data = controls_data.get('actionPlansStatus', [])
        if not data:
            content.append(Paragraph("No data available.", styles['Normal']))
            return
        
        # Create table data
        table_data = [['Status', 'Count']]
        
        for item in data:
                    table_data.append([
                item.get('status', 'N/A'),
                str(item.get('value', 0))
            ])
        
        # Create table
        table = Table(table_data, colWidths=[200, 100])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ]))
        
        content.append(table)
        content.append(Spacer(1, 12))
        
        return content
        
    async def _generate_controls_not_mapped_to_assertions_content(self, content, controls_data: Dict[str, Any]):
        """Generate PDF content for controls not mapped to assertions"""
        from reportlab.platypus import Table, TableStyle, Paragraph, Spacer
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet
        
        styles = getSampleStyleSheet()
        
        # Get the data
        data = controls_data.get('controlsNotMappedToAssertions', [])
        if not data:
            content.append(Paragraph("No data available.", styles['Normal']))
            return
        
        # Create table data
        table_data = [['#', 'Control Name', 'Department']]
        
        for i, item in enumerate(data, 1):
            control_name = item.get('Control Name', 'N/A')
            department = item.get('Department', 'N/A')
            
            # Handle multi-line text
            control_name = shape_text_for_arabic(control_name)
            department = shape_text_for_arabic(department)
            
            table_data.append([
                str(i),
                control_name,
                department
            ])
        
        # Create table
        table = Table(table_data, colWidths=[30, 200, 150])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
        ]))
        
        content.append(table)
        content.append(Spacer(1, 12))
        
        return content
    
    async def _generate_controls_not_mapped_to_principles_content(self, content, controls_data: Dict[str, Any]):
        """Generate PDF content for controls not mapped to principles"""
        from reportlab.platypus import Table, TableStyle, Paragraph, Spacer
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet
        
        styles = getSampleStyleSheet()
        
        # Get the data
        data = controls_data.get('controlsNotMappedToPrinciples', [])
        if not data:
            content.append(Paragraph("No data available.", styles['Normal']))
            return
        
        # Create table data
        table_data = [['#', 'Control Name', 'Function Name']]
        
        for i, item in enumerate(data, 1):
            control_name = item.get('Control Name', 'N/A')
            function_name = item.get('Function Name', 'N/A')
            
            # Handle multi-line text
            control_name = shape_text_for_arabic(control_name)
            function_name = shape_text_for_arabic(function_name)
            
            table_data.append([
                str(i),
                control_name,
                function_name
            ])
        
        # Create table
        table = Table(table_data, colWidths=[30, 200, 150])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
        ]))
        
        content.append(table)
        content.append(Spacer(1, 12))
        
        return content
        
    async def _generate_number_of_controls_per_component_content(self, content, controls_data: Dict[str, Any]):
        """Generate PDF content for number of controls per component"""
        from reportlab.platypus import Table, TableStyle, Paragraph, Spacer
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet
        
        styles = getSampleStyleSheet()
        
        # Get the data
        data = controls_data.get('numberOfControlsPerComponent', [])
        if not data:
            content.append(Paragraph("No data available.", styles['Normal']))
            return
        
        # Create table data
        table_data = [['Component', 'Controls Count']]
        for item in data:
            table_data.append([
                item.get('name', 'N/A'),
                str(item.get('value', 0))
            ])
        
        # Create table
        table = Table(table_data, repeatRows=1)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        content.append(Paragraph("Number of Controls per Component", styles['Heading2']))
        content.append(Spacer(1, 12))
        content.append(table)

    async def generate_controls_pdf(self, controls_data: Dict[str, Any], startDate: str, endDate: str, header_config: Dict[str, Any], cardType: str = None, onlyCard: bool = False, onlyOverallTable: bool = False, onlyChart: bool = False) -> bytes:
        """Generate PDF report for controls dashboard using normal export pattern"""
        try:
            from pdf_report_utils import generate_pdf_report
            
            # Debug: Check what we're receiving
            print(f"DEBUG: generate_controls_pdf called with:")
            print(f"  - controls_data type: {type(controls_data)}")
            print(f"  - controls_data keys: {list(controls_data.keys()) if isinstance(controls_data, dict) else 'Not a dict'}")
            print(f"  - cardType: {cardType}")
            print(f"  - onlyOverallTable: {onlyOverallTable}")
            
            # For chart exports, generate chart image + data table
            if onlyChart and cardType:
                # Get the data from controls_data
                data = controls_data.get(cardType, [])
                
                # Define columns based on cardType (as strings for PDF generation)
                if cardType == 'numberOfControlsPerComponent':
                    columns = ['Component', 'Controls Count']
                    # Convert data to rows format
                    data_rows = []
                    chart_data = {'labels': [], 'values': []}
                    if data:  # Only process if data exists
                        for item in data:
                            data_rows.append([
                                item.get('name', 'N/A'),
                                str(item.get('value', 0))
                            ])
                            chart_data['labels'].append(item.get('name', 'N/A'))
                            chart_data['values'].append(item.get('value', 0))
                    else:
                        data_rows.append(['No data available', 'No data available'])
                    
                    # Add chart_data to header_config for chart generation
                    header_config['chart_data'] = chart_data
                    header_config['chart_type'] = 'bar'
                else:
                    # Default columns for other chart types
                    columns = ['Data']
                    data_rows = [['No data available']]
                
                return generate_pdf_report(columns, data_rows, header_config)
            
            # For table exports, use the normal pattern like other tables
            elif onlyOverallTable and cardType:
                # Get the data from controls_data
                data = controls_data.get(cardType, [])
                print(f"DEBUG: Data for {cardType}: {type(data)}")
                if isinstance(data, list) and len(data) > 0:
                    print(f"DEBUG: First item type: {type(data[0])}")
                    print(f"DEBUG: First item: {data[0] if len(data) > 0 else 'Empty'}")
                
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
                    print(f"DEBUG: controlsNotMappedToPrinciples - columns: {columns}")
                    print(f"DEBUG: controlsNotMappedToPrinciples - data_rows count: {len(data_rows)}")
                    if data_rows:
                        print(f"DEBUG: controlsNotMappedToPrinciples - first row: {data_rows[0]}")
                elif cardType == 'controlsTestingApprovalCycle':
                    columns = ['#', 'Code', 'Control Name', 'Business Unit', 'Preparer Status', 'Checker Status', 'Reviewer Status', 'Acceptance Status']
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
                    columns = ['Data']
                    data_rows = [['No data available']]
                
                return generate_pdf_report(columns, data_rows, header_config)
            
            else:
                # Generate basic report for other cases
                data_rows = [['Controls Dashboard Report', 'Generated Successfully']]
                columns = [{'key': 'title', 'label': 'Title'}, {'key': 'status', 'label': 'Status'}]
                return generate_pdf_report(columns, data_rows, header_config)
                
        except Exception as e:
            print(f"Error generating controls PDF: {e}")
            raise e

    async def generate_risks_pdf(self, risks_data: Dict[str, Any], start_date: str, end_date: str, header_config: Dict[str, Any], card_type: str = None, only_card: bool = False) -> bytes:
        """Generate risks PDF report using the same reusable function as Controls dashboard"""
        try:
            from pdf_report_utils import generate_pdf_report
            
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
