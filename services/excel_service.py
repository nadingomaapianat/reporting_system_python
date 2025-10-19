"""
Excel generation service
"""
import io
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
from typing import Dict, Any, List, Optional
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.drawing.image import Image as ExcelImage
from openpyxl.utils import get_column_letter
from datetime import datetime

from export_utils import add_bank_info_to_excel_sheet, add_watermark_to_excel_sheet

class ExcelService:
    """Service for Excel generation"""
    
    def __init__(self):
        self.workbook = None
        self.worksheet = None
    
    async def generate_risks_excel(self, risks_data: Dict[str, Any], start_date: Optional[str] = None,
                                  end_date: Optional[str] = None, header_config: Optional[Dict[str, Any]] = None,
                                  card_type: Optional[str] = None, only_card: bool = False) -> bytes:
        """Generate risks Excel report"""
        try:
            # Create workbook
            wb = Workbook()
            ws = wb.active
            ws.title = "Risks Report"
            
            # Add header information
            await self._add_excel_header(ws, header_config, "Risks Dashboard Report")
            
            # Add content based on card type
            if only_card and card_type:
                await self._add_risks_card_content(ws, card_type, risks_data, header_config)
            else:
                await self._add_risks_full_content(ws, risks_data, header_config)
            
            # Widen common columns for risks sheets
            try:
                for idx, width in enumerate([8, 16, 60, 24, 22, 22, 22], 1):
                    ws.column_dimensions[get_column_letter(idx)].width = width
            except Exception:
                pass

            # Add watermark
            add_watermark_to_excel_sheet(ws, header_config)
            
            # Save to buffer
            buffer = io.BytesIO()
            wb.save(buffer)
            buffer.seek(0)
            return buffer.getvalue()
            
        except Exception as e:
            raise
    
    async def generate_controls_excel(self, controls_data: Dict[str, Any], start_date: Optional[str] = None,
                                     end_date: Optional[str] = None, header_config: Optional[Dict[str, Any]] = None,
                                     card_type: Optional[str] = None, only_card: bool = False) -> bytes:
        """Generate controls Excel report"""
        try:
            # Create workbook
            wb = Workbook()
            ws = wb.active
            ws.title = "Controls Report"
            
            # Add header information
            await self._add_excel_header(ws, header_config, "Controls Dashboard Report")
            
            # Add content based on card type
            if only_card and card_type:
                await self._add_controls_card_content(ws, card_type, controls_data, header_config)
            else:
                await self._add_controls_full_content(ws, controls_data, header_config)
            
            # Widen common columns for controls sheets
            try:
                for idx, width in enumerate([8, 16, 60, 22, 22, 22, 22], 1):
                    ws.column_dimensions[get_column_letter(idx)].width = width
            except Exception:
                pass

            # Add watermark
            add_watermark_to_excel_sheet(ws, header_config)
            
            # Save to buffer
            buffer = io.BytesIO()
            wb.save(buffer)
            buffer.seek(0)
            return buffer.getvalue()
            
        except Exception as e:
            raise

    async def generate_incidents_excel(self, incidents_data: Dict[str, Any], start_date: Optional[str] = None,
                                       end_date: Optional[str] = None, header_config: Optional[Dict[str, Any]] = None,
                                       card_type: Optional[str] = None, only_card: bool = False) -> bytes:
        """Generate incidents Excel report"""
        try:
            # Create workbook
            wb = Workbook()
            ws = wb.active
            ws.title = "Incidents Report"
            
            # Add header information
            await self._add_excel_header(ws, header_config, "Incidents Dashboard Report")
            
            # Add content based on card type
            if only_card and card_type:
                await self._add_incidents_card_content(ws, card_type, incidents_data, header_config)
            else:
                await self._add_incidents_full_content(ws, incidents_data, header_config)
            
            # Widen common columns for incidents sheets
            try:
                for idx, width in enumerate([8, 16, 60, 24, 22, 22, 22], 1):
                    ws.column_dimensions[get_column_letter(idx)].width = width
            except Exception:
                pass

            # Add watermark
            add_watermark_to_excel_sheet(ws, header_config)
            
            # Save to buffer
            buffer = io.BytesIO()
            wb.save(buffer)
            buffer.seek(0)
            return buffer.getvalue()
            
        except Exception as e:
            print(f"Error generating incidents Excel: {str(e)}")
            raise
    
    async def _add_incidents_card_content(self, ws, card_type: str, incidents_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add incidents card-specific content to Excel worksheet"""
        try:
            current_row = 5  # Start after header
            
            if card_type == 'netLossAndRecovery':
                await self._add_incidents_net_loss_content(ws, incidents_data, current_row)
            elif card_type == 'topFinancialImpacts':
                await self._add_incidents_top_impacts_content(ws, incidents_data, current_row)
            elif card_type == 'byCategory':
                await self._add_incidents_by_category_content(ws, incidents_data, current_row)
            elif card_type == 'byStatus':
                await self._add_incidents_by_status_content(ws, incidents_data, current_row)
            elif card_type == 'monthlyTrend':
                await self._add_incidents_monthly_trend_content(ws, incidents_data, current_row)
            elif card_type == 'totalIncidents':
                await self._add_incidents_total_content(ws, incidents_data, current_row)
            elif card_type == 'overallStatuses':
                await self._add_incidents_overall_statuses_content(ws, incidents_data, current_row)
            else:
                ws[f'A{current_row}'] = f"Card type '{card_type}' not supported for Excel export"
                
        except Exception as e:
            print(f"Error adding incidents card content: {str(e)}")
            ws[f'A5'] = f"Error generating content: {str(e)}"
    
    async def _add_incidents_full_content(self, ws, incidents_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add full incidents content to Excel worksheet"""
        try:
            current_row = 5  # Start after header
            
            # Add all incidents data sections
            await self._add_incidents_by_category_content(ws, incidents_data, current_row)
            current_row += 20
            
            await self._add_incidents_by_status_content(ws, incidents_data, current_row)
            current_row += 20
            
            await self._add_incidents_monthly_trend_content(ws, incidents_data, current_row)
            current_row += 20
            
            await self._add_incidents_net_loss_content(ws, incidents_data, current_row)
            current_row += 20
            
            await self._add_incidents_top_impacts_content(ws, incidents_data, current_row)
            
        except Exception as e:
            print(f"Error adding incidents full content: {str(e)}")
            ws[f'A5'] = f"Error generating content: {str(e)}"
    
    async def _add_incidents_net_loss_content(self, ws, incidents_data: Dict[str, Any], start_row: int):
        """Add Net Loss by Incident content"""
        try:
            current_row = start_row
            ws[f'A{current_row}'] = "Net Loss by Incident"
            ws[f'A{current_row}'].font = Font(bold=True, size=14, color="1F4E79")
            current_row += 2
            
            # Get data
            net_loss_data = incidents_data.get('netLossAndRecovery', [])
            if net_loss_data:
                # Create chart
                labels = [item.get('incident_title', 'Unknown')[:30] + '...' if len(item.get('incident_title', '')) > 30 else item.get('incident_title', 'Unknown') for item in net_loss_data]
                values = [item.get('net_loss', 0) for item in net_loss_data]
                
                # Create line chart
                fig, ax = plt.subplots(figsize=(10, 4))
                ax.plot(range(len(labels)), values, marker='o', color='#1F4E79', linewidth=2, markersize=4)
                ax.set_title('Net Loss by Incident', fontsize=12, fontweight='bold')
                ax.set_ylabel('Net Loss', fontsize=10)
                ax.set_xticks(range(len(labels)))
                ax.set_xticklabels(labels, rotation=45, ha='right', fontsize=8)
                ax.grid(True, alpha=0.3)
                plt.tight_layout()
                
                # Save chart to buffer
                chart_buffer = io.BytesIO()
                plt.savefig(chart_buffer, format='png', dpi=150, bbox_inches='tight')
                plt.close(fig)
                chart_buffer.seek(0)
                
                # Add table headers first
                ws[f'A{current_row}'] = "Incident Title"
                ws[f'B{current_row}'] = "Net Loss"
                ws[f'C{current_row}'] = "Recovery Amount"
                
                # Style headers
                for col in ['A', 'B', 'C']:
                    cell = ws[f'{col}{current_row}']
                    cell.font = Font(bold=True, color="FFFFFF")
                    cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
                
                current_row += 1
                
                # Add data
                for item in net_loss_data:
                    ws[f'A{current_row}'] = item.get('incident_title', 'N/A')
                    ws[f'B{current_row}'] = item.get('net_loss', 0)
                    ws[f'C{current_row}'] = item.get('recovery_amount', 0)
                    current_row += 1
                
                # Add chart to the right side
                chart_img = ExcelImage(chart_buffer)
                chart_img.width = 400
                chart_img.height = 200
                ws.add_image(chart_img, f'E{start_row + 2}')  # Position chart to the right
            else:
                ws[f'A{current_row}'] = "No net loss data available"
                
        except Exception as e:
            print(f"Error adding net loss content: {str(e)}")
    
    async def _add_incidents_top_impacts_content(self, ws, incidents_data: Dict[str, Any], start_row: int):
        """Add Top Financial Impacts content"""
        try:
            current_row = start_row
            ws[f'A{current_row}'] = "Top Financial Impacts"
            ws[f'A{current_row}'].font = Font(bold=True, size=14, color="1F4E79")
            current_row += 2
            
            # Get data
            impacts_data = incidents_data.get('topFinancialImpacts', [])
            if impacts_data:
                # Create pie chart
                labels = [item.get('financial_impact_name', 'Unknown') for item in impacts_data]
                values = [item.get('net_loss', 0) for item in impacts_data]
                
                # Create pie chart
                fig, ax = plt.subplots(figsize=(8, 6))
                colors = ['#1F4E79', '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD', '#98D8C8']
                wedges, texts, autotexts = ax.pie(values, labels=labels, autopct='%1.1f%%', colors=colors[:len(values)])
                ax.set_title('Top Financial Impacts', fontsize=12, fontweight='bold')
                
                # Save chart to buffer
                chart_buffer = io.BytesIO()
                plt.savefig(chart_buffer, format='png', dpi=150, bbox_inches='tight')
                plt.close(fig)
                chart_buffer.seek(0)
                
                # Add table headers first
                ws[f'A{current_row}'] = "Incident ID"
                ws[f'B{current_row}'] = "Financial Impact"
                ws[f'C{current_row}'] = "Function"
                ws[f'D{current_row}'] = "Net Loss"
                
                # Style headers
                for col in ['A', 'B', 'C', 'D']:
                    cell = ws[f'{col}{current_row}']
                    cell.font = Font(bold=True, color="FFFFFF")
                    cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
                
                current_row += 1
                
                # Add data
                for item in impacts_data:
                    ws[f'A{current_row}'] = item.get('incident_id', 'N/A')
                    ws[f'B{current_row}'] = item.get('financial_impact_name', 'N/A')
                    ws[f'C{current_row}'] = item.get('function_name', 'N/A')
                    ws[f'D{current_row}'] = item.get('net_loss', 0)
                    current_row += 1
                
                # Add chart to the right side
                chart_img = ExcelImage(chart_buffer)
                chart_img.width = 350
                chart_img.height = 280
                ws.add_image(chart_img, f'F{start_row + 2}')  # Position chart to the right
            else:
                ws[f'A{current_row}'] = "No financial impacts data available"
                
        except Exception as e:
            print(f"Error adding top impacts content: {str(e)}")
    
    async def _add_incidents_by_category_content(self, ws, incidents_data: Dict[str, Any], start_row: int):
        """Add Incidents by Category content"""
        try:
            current_row = start_row
            ws[f'A{current_row}'] = "Incidents by Category"
            ws[f'A{current_row}'].font = Font(bold=True, size=14, color="1F4E79")
            current_row += 2
            
            # Get data
            category_data = incidents_data.get('incidentsByCategory', [])
            if category_data:
                # Create bar chart
                labels = [item.get('category_name', 'Unknown') for item in category_data]
                values = [item.get('count', 0) for item in category_data]
                
                # Create bar chart
                fig, ax = plt.subplots(figsize=(8, 4))
                bars = ax.bar(labels, values, color='#1F4E79')
                ax.set_title('Incidents by Category', fontsize=12, fontweight='bold')
                ax.set_ylabel('Count', fontsize=10)
                ax.set_xlabel('Category', fontsize=10)
                
                # Add value labels on bars
                for bar in bars:
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                           f'{int(height)}', ha='center', va='bottom')
                
                plt.xticks(rotation=45, ha='right')
                plt.tight_layout()
                
                # Save chart to buffer
                chart_buffer = io.BytesIO()
                plt.savefig(chart_buffer, format='png', dpi=150, bbox_inches='tight')
                plt.close(fig)
                chart_buffer.seek(0)
                
                # Add table headers first
                ws[f'A{current_row}'] = "Category"
                ws[f'B{current_row}'] = "Count"
                
                # Style headers
                for col in ['A', 'B']:
                    cell = ws[f'{col}{current_row}']
                    cell.font = Font(bold=True, color="FFFFFF")
                    cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
                
                current_row += 1
                
                # Add data
                for item in category_data:
                    ws[f'A{current_row}'] = item.get('category_name', 'N/A')
                    ws[f'B{current_row}'] = item.get('count', 0)
                    current_row += 1
                
                # Add chart to the right side
                chart_img = ExcelImage(chart_buffer)
                chart_img.width = 400
                chart_img.height = 200
                ws.add_image(chart_img, f'D{start_row + 2}')  # Position chart to the right
            else:
                ws[f'A{current_row}'] = "No category data available"
                
        except Exception as e:
            print(f"Error adding by category content: {str(e)}")
    
    async def _add_incidents_by_status_content(self, ws, incidents_data: Dict[str, Any], start_row: int):
        """Add Incidents by Status content"""
        try:
            current_row = start_row
            ws[f'A{current_row}'] = "Incidents by Status"
            ws[f'A{current_row}'].font = Font(bold=True, size=14, color="1F4E79")
            current_row += 2
            
            # Get data
            status_data = incidents_data.get('incidentsByStatus', [])
            if status_data:
                # Create pie chart
                labels = [item.get('status', 'Unknown') for item in status_data]
                values = [item.get('count', 0) for item in status_data]
                
                # Create pie chart
                fig, ax = plt.subplots(figsize=(8, 6))
                colors = ['#1F4E79', '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD', '#98D8C8']
                wedges, texts, autotexts = ax.pie(values, labels=labels, autopct='%1.1f%%', colors=colors[:len(values)])
                ax.set_title('Incidents by Status', fontsize=12, fontweight='bold')
                
                # Save chart to buffer
                chart_buffer = io.BytesIO()
                plt.savefig(chart_buffer, format='png', dpi=150, bbox_inches='tight')
                plt.close(fig)
                chart_buffer.seek(0)
                
                # Add table headers first
                ws[f'A{current_row}'] = "Status"
                ws[f'B{current_row}'] = "Count"
                
                # Style headers
                for col in ['A', 'B']:
                    cell = ws[f'{col}{current_row}']
                    cell.font = Font(bold=True, color="FFFFFF")
                    cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
                
                current_row += 1
                
                # Add data
                for item in status_data:
                    ws[f'A{current_row}'] = item.get('status', 'N/A')
                    ws[f'B{current_row}'] = item.get('count', 0)
                    current_row += 1
                
                # Add chart to the right side
                chart_img = ExcelImage(chart_buffer)
                chart_img.width = 350
                chart_img.height = 280
                ws.add_image(chart_img, f'D{start_row + 2}')  # Position chart to the right
            else:
                ws[f'A{current_row}'] = "No status data available"
                
        except Exception as e:
            print(f"Error adding by status content: {str(e)}")
    
    async def _add_incidents_monthly_trend_content(self, ws, incidents_data: Dict[str, Any], start_row: int):
        """Add Monthly Trend content"""
        try:
            current_row = start_row
            ws[f'A{current_row}'] = "Incidents Monthly Trend"
            ws[f'A{current_row}'].font = Font(bold=True, size=14, color="1F4E79")
            current_row += 2
            
            # Get data
            trend_data = incidents_data.get('monthlyTrend', [])
            if trend_data:
                # Create line chart (single line to match frontend)
                months = [item.get('month_year', 'N/A') for item in trend_data]
                incident_counts = [item.get('incident_count', 0) for item in trend_data]
                
                # Create single line chart (matching frontend)
                fig, ax = plt.subplots(figsize=(12, 4))
                
                # Plot incident counts only
                ax.plot(range(len(months)), incident_counts, color='#1F4E79', marker='o', linewidth=2, markersize=4)
                ax.set_xlabel('Month/Year', fontsize=10)
                ax.set_ylabel('Incident Count', fontsize=10)
                ax.set_title('Incidents Monthly Trend', fontsize=12, fontweight='bold')
                ax.set_xticks(range(len(months)))
                ax.set_xticklabels(months, rotation=45, ha='right')
                ax.grid(True, alpha=0.3)
                
                plt.tight_layout()
                
                # Save chart to buffer
                chart_buffer = io.BytesIO()
                plt.savefig(chart_buffer, format='png', dpi=150, bbox_inches='tight')
                plt.close(fig)
                chart_buffer.seek(0)
                
                # Add table headers first
                ws[f'A{current_row}'] = "Month/Year"
                ws[f'B{current_row}'] = "Incident Count"
                ws[f'C{current_row}'] = "Total Loss"
                
                # Style headers
                for col in ['A', 'B', 'C']:
                    cell = ws[f'{col}{current_row}']
                    cell.font = Font(bold=True, color="FFFFFF")
                    cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
                
                current_row += 1
                
                # Add data
                for item in trend_data:
                    ws[f'A{current_row}'] = item.get('month_year', 'N/A')
                    ws[f'B{current_row}'] = item.get('incident_count', 0)
                    ws[f'C{current_row}'] = item.get('total_loss', 0)
                    current_row += 1
                
                # Add chart to the right side
                chart_img = ExcelImage(chart_buffer)
                chart_img.width = 500
                chart_img.height = 200
                ws.add_image(chart_img, f'E{start_row + 2}')  # Position chart to the right
            else:
                ws[f'A{current_row}'] = "No monthly trend data available"
                
        except Exception as e:
            print(f"Error adding monthly trend content: {str(e)}")
    
    async def _add_incidents_total_content(self, ws, incidents_data: Dict[str, Any], start_row: int):
        """Add Total Incidents content"""
        try:
            current_row = start_row
            ws[f'A{current_row}'] = "Total Incidents"
            ws[f'A{current_row}'].font = Font(bold=True, size=14, color="1F4E79")
            current_row += 2
            
            # Add summary
            total_incidents = incidents_data.get('totalIncidents', 0)
            ws[f'A{current_row}'] = f"Total Incidents: {total_incidents}"
            ws[f'A{current_row}'].font = Font(bold=True, size=12)
            current_row += 2
            
            # Try to get incidents list from different possible keys
            incidents_list = (incidents_data.get('incidentsList') or 
                            incidents_data.get('incidents') or 
                            incidents_data.get('allIncidents') or 
                            incidents_data.get('incidentsData') or 
                            incidents_data.get('list') or 
                            [])
            
            if incidents_list:
                ws[f'A{current_row}'] = "All Incidents List"
                ws[f'A{current_row}'].font = Font(bold=True, size=12)
                current_row += 2
                
                # Add table headers
                ws[f'A{current_row}'] = "Index"
                ws[f'B{current_row}'] = "Code"
                ws[f'C{current_row}'] = "Title"
                ws[f'D{current_row}'] = "Created At"
                
                # Style headers
                for col in ['A', 'B', 'C', 'D']:
                    cell = ws[f'{col}{current_row}']
                    cell.font = Font(bold=True, color="FFFFFF")
                    cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
                
                current_row += 1
                
                # Add data
                for i, incident in enumerate(incidents_list[:100]):  # Limit to 100 records
                    ws[f'A{current_row}'] = i + 1
                    ws[f'B{current_row}'] = incident.get('code', 'N/A')
                    ws[f'C{current_row}'] = incident.get('title', 'N/A')
                    ws[f'D{current_row}'] = incident.get('createdAt', 'N/A')
                    current_row += 1
            else:
                # If no incidents list available, show a message
                ws[f'A{current_row}'] = "Incidents list not available in the data"
                ws[f'A{current_row}'].font = Font(italic=True, color="666666")
                current_row += 1
                
                # Try to fetch from database if possible
                try:
                    from services.database_service import DatabaseService
                    db_service = DatabaseService()
                    incidents_list = await db_service.get_incidents_list()
                    
                    if incidents_list:
                        ws[f'A{current_row}'] = "All Incidents List (from database)"
                        ws[f'A{current_row}'].font = Font(bold=True, size=12)
                        current_row += 2
                        
                        # Add table headers
                        ws[f'A{current_row}'] = "Index"
                        ws[f'B{current_row}'] = "Code"
                        ws[f'C{current_row}'] = "Title"
                        ws[f'D{current_row}'] = "Created At"
                        
                        # Style headers
                        for col in ['A', 'B', 'C', 'D']:
                            cell = ws[f'{col}{current_row}']
                            cell.font = Font(bold=True, color="FFFFFF")
                            cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
                        
                        current_row += 1
                        
                        # Add data
                        for i, incident in enumerate(incidents_list[:100]):  # Limit to 100 records
                            ws[f'A{current_row}'] = i + 1
                            ws[f'B{current_row}'] = incident.get('code', 'N/A')
                            ws[f'C{current_row}'] = incident.get('title', 'N/A')
                            ws[f'D{current_row}'] = incident.get('createdAt', 'N/A')
                            current_row += 1
                except Exception as db_error:
                    ws[f'A{current_row}'] = f"Could not fetch incidents from database: {str(db_error)}"
                    ws[f'A{current_row}'].font = Font(italic=True, color="666666")
                
        except Exception as e:
            print(f"Error adding total incidents content: {str(e)}")
    
    async def _add_incidents_overall_statuses_content(self, ws, incidents_data: Dict[str, Any], start_row: int):
        """Add Overall Statuses content"""
        try:
            current_row = start_row
            ws[f'A{current_row}'] = "Overall Incident Statuses"
            ws[f'A{current_row}'].font = Font(bold=True, size=14, color="1F4E79")
            current_row += 2
            
            # Add table headers
            ws[f'A{current_row}'] = "Index"
            ws[f'B{current_row}'] = "Code"
            ws[f'C{current_row}'] = "Title"
            ws[f'D{current_row}'] = "Status"
            ws[f'E{current_row}'] = "Created At"
            
            # Style headers
            for col in ['A', 'B', 'C', 'D', 'E']:
                cell = ws[f'{col}{current_row}']
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
            
            current_row += 1
            
            # Add data
            overall_statuses = incidents_data.get('overallStatuses', [])
            if not overall_statuses:
                # Fallback to statusOverview if overallStatuses is not available
                overall_statuses = incidents_data.get('statusOverview', [])
            
            for i, item in enumerate(overall_statuses[:100]):  # Limit to 100 records
                ws[f'A{current_row}'] = i + 1
                ws[f'B{current_row}'] = item.get('code', 'N/A')
                ws[f'C{current_row}'] = item.get('title', 'N/A')
                ws[f'D{current_row}'] = item.get('status', 'N/A')
                ws[f'E{current_row}'] = item.get('createdAt', 'N/A')
                current_row += 1
                
        except Exception as e:
            print(f"Error adding overall statuses content: {str(e)}")
    
    async def _add_excel_header(self, ws, header_config: Optional[Dict[str, Any]], title: str):
        """Add header information to Excel worksheet"""
        try:
            # Add bank information
            add_bank_info_to_excel_sheet(ws, header_config)
            
            # Add title
            ws['A3'] = title
            font_color = header_config.get('fontColor', '#1F4E79')
            # Remove # if present for Excel compatibility
            if font_color.startswith('#'):
                font_color = font_color[1:]
            ws['A3'].font = Font(size=16, bold=True, color=font_color)
            
        except Exception as e:
            pass
    
    async def _add_risks_card_content(self, ws, card_type: str, risks_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add content for specific risks card"""
        if card_type == 'totalRisks':
            await self._add_total_risks_content(ws, risks_data, header_config)
        elif card_type == 'newThisMonth':
            await self._add_new_risks_content(ws, risks_data, header_config)
        elif card_type == 'highRisk':
            await self._add_high_risk_content(ws, risks_data, header_config)
        elif card_type == 'mediumRisk':
            await self._add_medium_risk_content(ws, risks_data, header_config)
        elif card_type == 'lowRisk':
            await self._add_low_risk_content(ws, risks_data, header_config)
        elif card_type == 'risksByCategory':
            await self._add_risks_by_category_content(ws, risks_data, header_config)
        elif card_type == 'risksByEventType':
            await self._add_risks_by_event_type_content(ws, risks_data, header_config)
        elif card_type == 'riskTrends':
            await self._add_risk_trends_content(ws, risks_data, header_config)
    
    async def _add_controls_card_content(self, ws, card_type: str, controls_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add content for specific controls card"""
        if card_type == 'totalControls':
            await self._add_total_controls_content(ws, controls_data, header_config)
        elif card_type == 'unmappedControls':
            await self._add_unmapped_controls_content(ws, controls_data, header_config)
        elif card_type == 'pendingPreparer':
            await self._add_pending_preparer_content(ws, controls_data, header_config)
        elif card_type == 'pendingChecker':
            await self._add_pending_checker_content(ws, controls_data, header_config)
        elif card_type == 'pendingReviewer':
            await self._add_pending_reviewer_content(ws, controls_data, header_config)
        elif card_type == 'pendingAcceptance':
            await self._add_pending_acceptance_content(ws, controls_data, header_config)
        elif card_type == 'department':
            await self._add_department_chart_content(ws, controls_data, header_config)
        elif card_type == 'risk':
            await self._add_risk_response_chart_content(ws, controls_data, header_config)
        elif card_type == 'overallStatuses':
            await self._add_overall_statuses_table(ws, controls_data, header_config)
    
    async def _add_total_risks_content(self, ws, risks_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add total risks content to Excel"""
        ws['A4'] = "Total Risks Details"
        
        # Get all risks data
        all_risks = risks_data.get('allRisks', [])
        if not all_risks:
            ws['A5'] = "No risks data available."
            return
        
        # Add headers
        headers = ['Index', 'Code', 'Risk Name', 'Created At']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=5, column=i, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            # Remove # if present for Excel compatibility
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, 
                                  end_color=header_bg_color, 
                                  fill_type='solid')
        
        # Add data rows
        for i, risk in enumerate(all_risks, 6):
            risk_name = risk.get('title', risk.get('risk_name', 'Unknown'))
            created_at = risk.get('created_at', 'N/A')
            
            # Format date
            try:
                if created_at and created_at != 'N/A':
                    if 'T' in created_at:
                        date_obj = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        created_at = date_obj.strftime('%Y-%m-%d %H:%M')
            except:
                pass
            
            ws.cell(row=i, column=1, value=i-5)  # Index
            ws.cell(row=i, column=2, value=risk.get('code', 'N/A'))
            ws.cell(row=i, column=3, value=risk_name)
            ws.cell(row=i, column=4, value=created_at)
            
            # Apply body background color
            body_bg_color = header_config.get('tableBodyBgColor', '#F5F5F5')
            # Remove # if present for Excel compatibility
            if body_bg_color.startswith('#'):
                body_bg_color = body_bg_color[1:]
            for col in range(1, 5):
                cell = ws.cell(row=i, column=col)
                cell.fill = PatternFill(start_color=body_bg_color, 
                                      end_color=body_bg_color, 
                                      fill_type='solid')
    
    async def _add_chart_to_excel(self, ws, chart_data: List[Dict[str, Any]], chart_type: str, title: str, start_row: int, start_col: int = 1):
        """Add chart image to Excel worksheet - using matplotlib like test script"""
        try:
            # Check if we have data
            if not chart_data:
                ws.cell(row=start_row, column=start_col, value="No chart data available")
                return
            
            # Use matplotlib exactly like the working test script
            fig, ax = plt.subplots(figsize=(8, 4))
            
            if chart_type == 'pie':
                labels = [item.get('name', 'N/A') for item in chart_data]
                values = [item.get('value', 0) for item in chart_data]
                
                # Filter out zero values
                non_zero_data = [(label, value) for label, value in zip(labels, values) if value > 0]
                if non_zero_data:
                    labels, values = zip(*non_zero_data)
                    ax.pie(values, labels=labels, autopct='%1.1f%%', startangle=90)
                    ax.set_title(title, fontsize=12, fontweight='bold')
                else:
                    ax.text(0.5, 0.5, 'No data to display', ha='center', va='center', transform=ax.transAxes)
                    
            elif chart_type == 'bar':
                labels = [item.get('name', 'N/A') for item in chart_data]
                values = [item.get('value', 0) for item in chart_data]
                
                if values and any(v > 0 for v in values):
                    ax.bar(labels, values)
                    ax.set_title(title, fontsize=12, fontweight='bold')
                else:
                    ax.text(0.5, 0.5, 'No data to display', ha='center', va='center', transform=ax.transAxes)
                    
            elif chart_type == 'line':
                months = [item.get('month', 'N/A') for item in chart_data]
                total_risks = [item.get('total_risks', 0) for item in chart_data]
                new_risks = [item.get('new_risks', 0) for item in chart_data]
                mitigated_risks = [item.get('mitigated_risks', 0) for item in chart_data]
                
                if months and (total_risks or new_risks or mitigated_risks):
                    if any(v > 0 for v in total_risks):
                        ax.plot(months, total_risks, marker='o', label='Total Risks', linewidth=2)
                    if any(v > 0 for v in new_risks):
                        ax.plot(months, new_risks, marker='s', label='New Risks', linewidth=2)
                    if any(v > 0 for v in mitigated_risks):
                        ax.plot(months, mitigated_risks, marker='^', label='Mitigated Risks', linewidth=2)
                    ax.set_xticklabels(months, rotation=45, ha='right')
                    ax.legend()
                    ax.grid(True, alpha=0.3)
                    ax.set_title(title, fontsize=12, fontweight='bold')
                else:
                    ax.text(0.5, 0.5, 'No data to display', ha='center', va='center', transform=ax.transAxes)
            
            # Use exact same approach as test script
            plt.tight_layout()
            
            # Save to buffer exactly like test script
            chart_buffer = io.BytesIO()
            plt.savefig(chart_buffer, format='png', dpi=150, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            chart_buffer.seek(0)
            buffer_size = len(chart_buffer.getvalue())
            
            if buffer_size > 0:
                # Add image to Excel exactly like test script
                img = ExcelImage(chart_buffer)
                img.width = 400
                img.height = 200
                # Calculate the column letter for the start position
                from openpyxl.utils import get_column_letter
                col_letter = get_column_letter(start_col)
                ws.add_image(img, f'{col_letter}{start_row}')
            else:
                ws.cell(row=start_row, column=start_col, value="Chart generation failed - empty buffer")
            
            plt.close(fig)
            
        except Exception as e:
            ws.cell(row=start_row, column=1, value=f"Chart generation failed: {str(e)}")
    
    # Add other content methods as placeholders...
    async def _add_new_risks_content(self, ws, risks_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add new risks content to Excel worksheet"""
        ws['A4'] = "New Risks This Month"
        
        # Get new risks data
        new_risks = risks_data.get('newRisks', [])
        
        if not new_risks:
            ws['A5'] = "No new risks data available."
            return
        
        # Add headers
        headers = ['Index', 'Code', 'Risk Name', 'Inherent Value', 'Created At']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=5, column=i, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            # Remove # if present for Excel compatibility
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, 
                                  end_color=header_bg_color, 
                                  fill_type='solid')
        
        # Add data rows
        body_bg_color = header_config.get('tableBodyBgColor', '#FFFFFF')
        if body_bg_color.startswith('#'):
            body_bg_color = body_bg_color[1:]
        
        for i, risk in enumerate(new_risks, 6):  # Start from row 6
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
            
            # Add data to cells
            ws.cell(row=i, column=1, value=i-5)  # Index
            ws.cell(row=i, column=2, value=risk.get('code', 'N/A'))  # Code
            ws.cell(row=i, column=3, value=risk_name)  # Risk Name
            ws.cell(row=i, column=4, value=inherent_value)  # Inherent Value
            ws.cell(row=i, column=5, value=created_at)  # Created At
            
            # Apply body background color
            for col in range(1, 6):
                cell = ws.cell(row=i, column=col)
                cell.fill = PatternFill(start_color=body_bg_color, 
                                      end_color=body_bg_color, 
                                      fill_type='solid')
                cell.alignment = Alignment(horizontal='left', wrap_text=True, vertical='top')
    
    async def _add_high_risk_content(self, ws, risks_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add high risk content to Excel worksheet"""
        ws['A4'] = "High Risk Details"
        
        # Get high risk data
        high_risks = risks_data.get('highRisk', [])
        
        if not high_risks:
            ws['A5'] = "No high risk data available."
            return
        
        # Add headers
        headers = ['Index', 'Code', 'Risk Name', 'Created At']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=5, column=i, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            # Remove # if present for Excel compatibility
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, 
                                  end_color=header_bg_color, 
                                  fill_type='solid')
        
        # Add data rows
        for i, risk in enumerate(high_risks, 6):
            risk_name = risk.get('title', risk.get('risk_name', 'Unknown'))
            created_at = risk.get('created_at', 'N/A')
            
            # Format date
            try:
                if created_at and created_at != 'N/A':
                    if 'T' in created_at:
                        from datetime import datetime
                        date_obj = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        created_at = date_obj.strftime('%B %d, %Y at %I:%M %p')
            except:
                pass
            
            ws.cell(row=i, column=1, value=i-5)  # Index
            ws.cell(row=i, column=2, value=risk.get('code', 'N/A'))
            ws.cell(row=i, column=3, value=risk_name)
            ws.cell(row=i, column=4, value=created_at)
            
            # Apply body background color
            body_bg_color = header_config.get('tableBodyBgColor', '#F5F5F5')
            # Remove # if present for Excel compatibility
            if body_bg_color.startswith('#'):
                body_bg_color = body_bg_color[1:]
            for col in range(1, 5):
                cell = ws.cell(row=i, column=col)
                cell.fill = PatternFill(start_color=body_bg_color, 
                                      end_color=body_bg_color, 
                                      fill_type='solid')
    
    async def _add_medium_risk_content(self, ws, risks_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add medium risk content to Excel worksheet"""
        ws['A4'] = "Medium Risk Details"
        
        # Get medium risk data
        medium_risks = risks_data.get('mediumRisk', [])
        
        if not medium_risks:
            ws['A5'] = "No medium risk data available."
            return
        
        # Add headers
        headers = ['Index', 'Code', 'Risk Name', 'Created At']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=5, column=i, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, 
                                  end_color=header_bg_color, 
                                  fill_type='solid')
        
        # Add data rows
        for i, risk in enumerate(medium_risks, 6):
            risk_name = risk.get('title', risk.get('risk_name', 'Unknown'))
            created_at = risk.get('created_at', 'N/A')
            
            # Format date
            try:
                if created_at and created_at != 'N/A':
                    if 'T' in created_at:
                        from datetime import datetime
                        date_obj = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        created_at = date_obj.strftime('%B %d, %Y at %I:%M %p')
            except:
                pass
            
            ws.cell(row=i, column=1, value=i-5)  # Index
            ws.cell(row=i, column=2, value=risk.get('code', 'N/A'))
            ws.cell(row=i, column=3, value=risk_name)
            ws.cell(row=i, column=4, value=created_at)
            
            # Apply body background color
            body_bg_color = header_config.get('tableBodyBgColor', '#F5F5F5')
            if body_bg_color.startswith('#'):
                body_bg_color = body_bg_color[1:]
            for col in range(1, 5):
                cell = ws.cell(row=i, column=col)
                cell.fill = PatternFill(start_color=body_bg_color, 
                                      end_color=body_bg_color, 
                                      fill_type='solid')
    
    async def _add_low_risk_content(self, ws, risks_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add low risk content to Excel worksheet"""
        ws['A4'] = "Low Risk Details"
        
        # Get low risk data
        low_risks = risks_data.get('lowRisk', [])
        
        if not low_risks:
            ws['A5'] = "No low risk data available."
            return
        
        # Add headers
        headers = ['Index', 'Code', 'Risk Name', 'Created At']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=5, column=i, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, 
                                  end_color=header_bg_color, 
                                  fill_type='solid')
        
        # Add data rows
        for i, risk in enumerate(low_risks, 6):
            risk_name = risk.get('title', risk.get('risk_name', 'Unknown'))
            created_at = risk.get('created_at', 'N/A')
            
            # Format date
            try:
                if created_at and created_at != 'N/A':
                    if 'T' in created_at:
                        from datetime import datetime
                        date_obj = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        created_at = date_obj.strftime('%B %d, %Y at %I:%M %p')
            except:
                pass
            
            ws.cell(row=i, column=1, value=i-5)  # Index
            ws.cell(row=i, column=2, value=risk.get('code', 'N/A'))
            ws.cell(row=i, column=3, value=risk_name)
            ws.cell(row=i, column=4, value=created_at)
            
            # Apply body background color
            body_bg_color = header_config.get('tableBodyBgColor', '#F5F5F5')
            if body_bg_color.startswith('#'):
                body_bg_color = body_bg_color[1:]
            for col in range(1, 5):
                cell = ws.cell(row=i, column=col)
                cell.fill = PatternFill(start_color=body_bg_color, 
                                      end_color=body_bg_color, 
                                      fill_type='solid')
    
    async def _add_risks_by_category_content(self, ws, risks_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add risks by category content to Excel worksheet with chart and data table"""
        ws['A4'] = "Risks by Category"
        
        # Get risks by category data
        risks_by_category = risks_data.get('risksByCategory', [])
        
        if not risks_by_category:
            ws['A5'] = "No risks by category data available."
            return
        
        # Add table and chart side by side, 2 rows after header
        start_row = 6  # 2 rows after header (row 4)
        table_start_col = 1  # Start table in column A (left side)
        chart_start_col = 8  # Start chart in column H (right side)
        
        # Add data table on the left first
        headers = ['Category', 'Count']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=start_row, column=table_start_col + i - 1, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, 
                                  end_color=header_bg_color, 
                                  fill_type='solid')
        
        # Add data rows
        body_bg_color = header_config.get('tableBodyBgColor', '#FFFFFF')
        if body_bg_color.startswith('#'):
            body_bg_color = body_bg_color[1:]
        
        for i, category in enumerate(risks_by_category, start_row + 1):
            name = category.get('name', 'Unknown')
            value = category.get('value', 0)
            
            ws.cell(row=i, column=table_start_col, value=name)
            ws.cell(row=i, column=table_start_col + 1, value=value)
            
            # Apply body background color
            for col in range(table_start_col, table_start_col + 2):
                cell = ws.cell(row=i, column=col)
                cell.fill = PatternFill(start_color=body_bg_color, 
                                      end_color=body_bg_color, 
                                      fill_type='solid')
                cell.alignment = Alignment(horizontal='left', wrap_text=True, vertical='top')
        
        # Generate and add chart on the right (starting from column H)
        await self._add_chart_to_excel(ws, risks_by_category, 'bar', "Risks by Category", start_row, chart_start_col)
    
    async def _add_risks_by_event_type_content(self, ws, risks_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add risks by event type content to Excel worksheet with chart and data table"""
        ws['A4'] = "Risks by Event Type"
        
        # Get risks by event type data
        risks_by_event_type = risks_data.get('risksByEventType', [])
        
        if not risks_by_event_type:
            ws['A5'] = "No risks by event type data available."
            return
        
        # Add table and chart side by side, 2 rows after header
        start_row = 6  # 2 rows after header (row 4)
        table_start_col = 1  # Start table in column A (left side)
        chart_start_col = 8  # Start chart in column H (right side)
        
        # Add data table on the left first
        headers = ['Event Type', 'Count']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=start_row, column=table_start_col + i - 1, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, 
                                  end_color=header_bg_color, 
                                  fill_type='solid')
        
        # Add data rows
        body_bg_color = header_config.get('tableBodyBgColor', '#FFFFFF')
        if body_bg_color.startswith('#'):
            body_bg_color = body_bg_color[1:]
        
        for i, event_type in enumerate(risks_by_event_type, start_row + 1):
            name = event_type.get('name', 'Unknown')
            value = event_type.get('value', 0)
            
            ws.cell(row=i, column=table_start_col, value=name)
            ws.cell(row=i, column=table_start_col + 1, value=value)
            
            # Apply body background color
            for col in range(table_start_col, table_start_col + 2):
                cell = ws.cell(row=i, column=col)
                cell.fill = PatternFill(start_color=body_bg_color, 
                                      end_color=body_bg_color, 
                                      fill_type='solid')
                cell.alignment = Alignment(horizontal='left', wrap_text=True, vertical='top')
        
        # Generate and add chart on the right (starting from column H)
        await self._add_chart_to_excel(ws, risks_by_event_type, 'pie', "Risks by Event Type", start_row, chart_start_col)
    
    async def _add_risk_trends_content(self, ws, risks_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add risk trends content to Excel worksheet with chart and data table"""
        ws['A4'] = "Risk Trends Over Time"
        
        # Get risk trends data
        risk_trends = risks_data.get('riskTrends', [])
        
        if not risk_trends:
            ws['A5'] = "No risk trends data available."
            return
        
        # Add table and chart side by side, 2 rows after header
        start_row = 6  # 2 rows after header (row 4)
        table_start_col = 1  # Start table in column A (left side)
        chart_start_col = 8  # Start chart in column H (right side)
        
        # Add data table on the left first
        headers = ['Month', 'Total Risks', 'New Risks', 'Mitigated Risks']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=start_row, column=table_start_col + i - 1, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, 
                                  end_color=header_bg_color, 
                                  fill_type='solid')
        
        # Add data rows
        body_bg_color = header_config.get('tableBodyBgColor', '#FFFFFF')
        if body_bg_color.startswith('#'):
            body_bg_color = body_bg_color[1:]
        
        for i, trend in enumerate(risk_trends, start_row + 1):
            month = trend.get('month', 'Unknown')
            total_risks = trend.get('total_risks', 0)
            new_risks = trend.get('new_risks', 0)
            mitigated_risks = trend.get('mitigated_risks', 0)
            
            ws.cell(row=i, column=table_start_col, value=month)
            ws.cell(row=i, column=table_start_col + 1, value=total_risks)
            ws.cell(row=i, column=table_start_col + 2, value=new_risks)
            ws.cell(row=i, column=table_start_col + 3, value=mitigated_risks)
            
            # Apply body background color
            for col in range(table_start_col, table_start_col + 4):
                cell = ws.cell(row=i, column=col)
                cell.fill = PatternFill(start_color=body_bg_color, 
                                      end_color=body_bg_color, 
                                      fill_type='solid')
                cell.alignment = Alignment(horizontal='left', wrap_text=True, vertical='top')
        
        # Generate and add chart on the right (starting from column H)
        await self._add_chart_to_excel(ws, risk_trends, 'line', "Risk Trends Over Time", start_row, chart_start_col)
    
    async def _add_total_controls_content(self, ws, controls_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add total controls content to Excel worksheet"""
        ws['A4'] = "Total Controls Details"
        
        # Get total controls data
        total_controls = controls_data.get('totalControls', [])
        
        if not total_controls:
            ws['A5'] = "Total controls data not available. Please check if the Node.js API is running and accessible."
            return
        
        # Add table headers
        start_row = 6
        headers = ['Index', 'Code', 'Control Name']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=start_row, column=i, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, 
                                  end_color=header_bg_color, 
                                  fill_type='solid')
        
        # Add data rows
        body_bg_color = header_config.get('tableBodyBgColor', '#FFFFFF')
        if body_bg_color.startswith('#'):
            body_bg_color = body_bg_color[1:]
        
        for i, control in enumerate(total_controls, start_row + 1):
            ws.cell(row=i, column=1, value=i - start_row)  # Index
            ws.cell(row=i, column=2, value=control.get('control_code', 'N/A'))  # Code
            ws.cell(row=i, column=3, value=control.get('control_name', 'N/A'))  # Control Name
            
            # Apply body background color
            for col in range(1, 4):
                cell = ws.cell(row=i, column=col)
                cell.fill = PatternFill(start_color=body_bg_color, 
                                      end_color=body_bg_color, 
                                      fill_type='solid')
                cell.alignment = Alignment(horizontal='left', wrap_text=True, vertical='top')
    
    async def _add_unmapped_controls_content(self, ws, controls_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add unmapped controls content to Excel worksheet"""
        ws['A4'] = "Unmapped Controls"
        
        # Get unmapped controls data
        unmapped_controls = controls_data.get('unmappedControls', [])
        
        if not unmapped_controls:
            ws['A5'] = "Unmapped controls data not available. Please check if the Node.js API is running and accessible."
            return
        
        # Add table headers
        start_row = 6
        headers = ['Index', 'Code', 'Control Name']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=start_row, column=i, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, 
                                  end_color=header_bg_color, 
                                  fill_type='solid')
        
        # Add data rows
        body_bg_color = header_config.get('tableBodyBgColor', '#FFFFFF')
        if body_bg_color.startswith('#'):
            body_bg_color = body_bg_color[1:]
        
        for i, control in enumerate(unmapped_controls, start_row + 1):
            ws.cell(row=i, column=1, value=i - start_row)  # Index
            ws.cell(row=i, column=2, value=control.get('control_code', 'N/A'))  # Code
            ws.cell(row=i, column=3, value=control.get('control_name', 'N/A'))  # Control Name
            
            # Apply body background color
            for col in range(1, 4):
                cell = ws.cell(row=i, column=col)
                cell.fill = PatternFill(start_color=body_bg_color, 
                                      end_color=body_bg_color, 
                                      fill_type='solid')
                cell.alignment = Alignment(horizontal='left', wrap_text=True, vertical='top')
    
    async def _add_pending_preparer_content(self, ws, controls_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add pending preparer controls content to Excel worksheet"""
        ws['A4'] = "Pending Preparer Controls"
        
        # Get pending preparer controls data
        pending_preparer = controls_data.get('pendingPreparer', [])
        
        if not pending_preparer:
            ws['A5'] = "Pending preparer controls data not available. Please check if the Node.js API is running and accessible."
            return
        
        # Add table headers
        start_row = 6
        headers = ['Index', 'Code', 'Control Name', 'Preparer Status']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=start_row, column=i, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, 
                                  end_color=header_bg_color, 
                                  fill_type='solid')
        
        # Add data rows
        body_bg_color = header_config.get('tableBodyBgColor', '#FFFFFF')
        if body_bg_color.startswith('#'):
            body_bg_color = body_bg_color[1:]
        
        for i, control in enumerate(pending_preparer, start_row + 1):
            ws.cell(row=i, column=1, value=i - start_row)  # Index
            ws.cell(row=i, column=2, value=control.get('control_code', 'N/A'))  # Code
            ws.cell(row=i, column=3, value=control.get('control_name', 'N/A'))  # Control Name
            ws.cell(row=i, column=4, value=control.get('preparerStatus', {}).get('value', 'N/A'))  # Preparer Status
            
            # Apply body background color
            for col in range(1, 5):
                cell = ws.cell(row=i, column=col)
                cell.fill = PatternFill(start_color=body_bg_color, 
                                      end_color=body_bg_color, 
                                      fill_type='solid')
                cell.alignment = Alignment(horizontal='left', wrap_text=True, vertical='top')
    
    async def _add_pending_checker_content(self, ws, controls_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add pending checker controls content to Excel worksheet"""
        ws['A4'] = "Pending Checker Controls"
        
        # Get pending checker controls data
        pending_checker = controls_data.get('pendingChecker', [])
        
        if not pending_checker:
            ws['A5'] = "Pending checker controls data not available. Please check if the Node.js API is running and accessible."
            return
        
        # Add table headers
        start_row = 6
        headers = ['Index', 'Code', 'Control Name', 'Checker Status']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=start_row, column=i, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, 
                                  end_color=header_bg_color, 
                                  fill_type='solid')
        
        # Add data rows
        body_bg_color = header_config.get('tableBodyBgColor', '#FFFFFF')
        if body_bg_color.startswith('#'):
            body_bg_color = body_bg_color[1:]
        
        for i, control in enumerate(pending_checker, start_row + 1):
            ws.cell(row=i, column=1, value=i - start_row)  # Index
            ws.cell(row=i, column=2, value=control.get('control_code', 'N/A'))  # Code
            ws.cell(row=i, column=3, value=control.get('control_name', 'N/A'))  # Control Name
            ws.cell(row=i, column=4, value=control.get('checkerStatus', {}).get('value', 'N/A'))  # Checker Status
            
            # Apply body background color
            for col in range(1, 5):
                cell = ws.cell(row=i, column=col)
                cell.fill = PatternFill(start_color=body_bg_color, 
                                      end_color=body_bg_color, 
                                      fill_type='solid')
                cell.alignment = Alignment(horizontal='left', wrap_text=True, vertical='top')
    
    async def _add_pending_reviewer_content(self, ws, controls_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add pending reviewer controls content to Excel worksheet"""
        ws['A4'] = "Pending Reviewer Controls"
        
        # Get pending reviewer controls data
        pending_reviewer = controls_data.get('pendingReviewer', [])
        
        if not pending_reviewer:
            ws['A5'] = "Pending reviewer controls data not available. Please check if the Node.js API is running and accessible."
            return
        
        # Add table headers
        start_row = 6
        headers = ['Index', 'Code', 'Control Name', 'Reviewer Status']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=start_row, column=i, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, 
                                  end_color=header_bg_color, 
                                  fill_type='solid')
        
        # Add data rows
        body_bg_color = header_config.get('tableBodyBgColor', '#FFFFFF')
        if body_bg_color.startswith('#'):
            body_bg_color = body_bg_color[1:]
        
        for i, control in enumerate(pending_reviewer, start_row + 1):
            ws.cell(row=i, column=1, value=i - start_row)  # Index
            ws.cell(row=i, column=2, value=control.get('control_code', 'N/A'))  # Code
            ws.cell(row=i, column=3, value=control.get('control_name', 'N/A'))  # Control Name
            ws.cell(row=i, column=4, value=control.get('reviewerStatus', {}).get('value', 'N/A'))  # Reviewer Status
            
            # Apply body background color
            for col in range(1, 5):
                cell = ws.cell(row=i, column=col)
                cell.fill = PatternFill(start_color=body_bg_color, 
                                      end_color=body_bg_color, 
                                      fill_type='solid')
                cell.alignment = Alignment(horizontal='left', wrap_text=True, vertical='top')
    
    async def _add_pending_acceptance_content(self, ws, controls_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add pending acceptance controls content to Excel worksheet"""
        ws['A4'] = "Pending Acceptance Controls"
        
        # Get pending acceptance controls data
        pending_acceptance = controls_data.get('pendingAcceptance', [])
        
        if not pending_acceptance:
            ws['A5'] = "Pending acceptance controls data not available. Please check if the Node.js API is running and accessible."
            return
        
        # Add table headers
        start_row = 6
        headers = ['Index', 'Code', 'Control Name', 'Acceptance Status']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=start_row, column=i, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, 
                                  end_color=header_bg_color, 
                                  fill_type='solid')
        
        # Add data rows
        body_bg_color = header_config.get('tableBodyBgColor', '#FFFFFF')
        if body_bg_color.startswith('#'):
            body_bg_color = body_bg_color[1:]
        
        for i, control in enumerate(pending_acceptance, start_row + 1):
            ws.cell(row=i, column=1, value=i - start_row)  # Index
            ws.cell(row=i, column=2, value=control.get('control_code', 'N/A'))  # Code
            ws.cell(row=i, column=3, value=control.get('control_name', 'N/A'))  # Control Name
            ws.cell(row=i, column=4, value=control.get('acceptanceStatus', {}).get('value', 'N/A'))  # Acceptance Status
            
            # Apply body background color
            for col in range(1, 5):
                cell = ws.cell(row=i, column=col)
                cell.fill = PatternFill(start_color=body_bg_color, 
                                      end_color=body_bg_color, 
                                      fill_type='solid')
                cell.alignment = Alignment(horizontal='left', wrap_text=True, vertical='top')
    
    async def _add_department_chart_content(self, ws, controls_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add controls by department content to Excel worksheet with chart and data table"""
        ws['A4'] = "Controls by Department"
        
        # Get department distribution data
        department_data = controls_data.get('departmentDistribution', [])
        
        if not department_data:
            ws['A5'] = "No department data available."
            return
        
        # Add table and chart side by side, 2 rows after header
        start_row = 6  # 2 rows after header (row 4)
        table_start_col = 1  # Start table in column A (left side)
        chart_start_col = 8  # Start chart in column H (right side)
        
        # Add data table on the left first
        headers = ['Department', 'Controls Count']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=start_row, column=table_start_col + i - 1, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, 
                                  end_color=header_bg_color, 
                                  fill_type='solid')
        
        # Add data rows
        body_bg_color = header_config.get('tableBodyBgColor', '#FFFFFF')
        if body_bg_color.startswith('#'):
            body_bg_color = body_bg_color[1:]
        
        for i, department in enumerate(department_data, start_row + 1):
            name = department.get('name', 'Unknown')
            value = department.get('value', 0)
            
            ws.cell(row=i, column=table_start_col, value=name)
            ws.cell(row=i, column=table_start_col + 1, value=value)
            
            # Apply body background color
            for col in range(table_start_col, table_start_col + 2):
                cell = ws.cell(row=i, column=col)
                cell.fill = PatternFill(start_color=body_bg_color, 
                                      end_color=body_bg_color, 
                                      fill_type='solid')
                cell.alignment = Alignment(horizontal='left', wrap_text=True, vertical='top')
        
        # Generate and add chart on the right (starting from column H)
        await self._add_chart_to_excel(ws, department_data, 'bar', "Controls by Department", start_row, chart_start_col)
    
    async def _add_risk_response_chart_content(self, ws, controls_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add controls by risk response type content to Excel worksheet with chart and data table"""
        ws['A4'] = "Controls by Risk Response Type"
        
        # Get status distribution data
        status_data = controls_data.get('statusDistribution', [])
        
        if not status_data:
            ws['A5'] = "No risk response data available."
            return
        
        # Add table and chart side by side, 2 rows after header
        start_row = 6  # 2 rows after header (row 4)
        table_start_col = 1  # Start table in column A (left side)
        chart_start_col = 8  # Start chart in column H (right side)
        
        # Add data table on the left first
        headers = ['Risk Response', 'Count']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=start_row, column=table_start_col + i - 1, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, 
                                  end_color=header_bg_color, 
                                  fill_type='solid')
        
        # Add data rows
        body_bg_color = header_config.get('tableBodyBgColor', '#FFFFFF')
        if body_bg_color.startswith('#'):
            body_bg_color = body_bg_color[1:]
        
        for i, status in enumerate(status_data, start_row + 1):
            name = status.get('name', 'Unknown')
            value = status.get('value', 0)
            
            ws.cell(row=i, column=table_start_col, value=name)
            ws.cell(row=i, column=table_start_col + 1, value=value)
            
            # Apply body background color
            for col in range(table_start_col, table_start_col + 2):
                cell = ws.cell(row=i, column=col)
                cell.fill = PatternFill(start_color=body_bg_color, 
                                      end_color=body_bg_color, 
                                      fill_type='solid')
                cell.alignment = Alignment(wrap_text=True, vertical='top')
        
        # Generate and add chart on the right (starting from column H)
        await self._add_chart_to_excel(ws, status_data, 'pie', "Controls by Risk Response Type", start_row, chart_start_col)
    
    async def _add_risks_full_content(self, ws, risks_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        ws['A4'] = "Full Risks Report"
        ws['A5'] = "Content placeholder"
    
    async def _add_controls_full_content(self, ws, controls_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        ws['A4'] = "Full Controls Report"
        ws['A5'] = "Content placeholder"

    async def _add_overall_statuses_table(self, ws, controls_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Render Overall Control Statuses table without pagination"""
        ws['A4'] = "Overall Control Statuses"
        rows = controls_data.get('statusOverview', []) or []
        if not rows:
            ws['A5'] = "No data available."
            return

        start_row = 6
        headers = ['Index', 'Code', 'Control Name', 'Preparer', 'Checker', 'Reviewer', 'Acceptance']
        for i, header in enumerate(headers, 1):
            cell = ws.cell(row=start_row, column=i, value=header)
            cell.font = Font(bold=True)
            header_bg_color = header_config.get('tableHeaderBgColor', '#E3F2FD')
            if header_bg_color.startswith('#'):
                header_bg_color = header_bg_color[1:]
            cell.fill = PatternFill(start_color=header_bg_color, end_color=header_bg_color, fill_type='solid')

        body_bg_color = header_config.get('tableBodyBgColor', '#FFFFFF')
        if body_bg_color.startswith('#'):
            body_bg_color = body_bg_color[1:]

        def norm(v):
            if isinstance(v, dict):
                return v.get('value') or 'N/A'
            return v or 'N/A'

        for i, r in enumerate(rows, start_row + 1):
            ws.cell(row=i, column=1, value=i - start_row)
            ws.cell(row=i, column=2, value=r.get('code', 'N/A'))
            ws.cell(row=i, column=3, value=r.get('name', 'N/A'))
            ws.cell(row=i, column=4, value=norm(r.get('preparerStatus')))
            ws.cell(row=i, column=5, value=norm(r.get('checkerStatus')))
            ws.cell(row=i, column=6, value=norm(r.get('reviewerStatus')))
            ws.cell(row=i, column=7, value=norm(r.get('acceptanceStatus')))

            for col in range(1, 8):
                cell = ws.cell(row=i, column=col)
                cell.fill = PatternFill(start_color=body_bg_color, end_color=body_bg_color, fill_type='solid')
                cell.alignment = Alignment(horizontal='left', wrap_text=True, vertical='top')

        # Widen columns for readability
        widths = [6, 14, 48, 14, 14, 14, 16]
        from openpyxl.utils import get_column_letter
        for idx, width in enumerate(widths, 1):
            ws.column_dimensions[get_column_letter(idx)].width = width

    # KRI Excel Methods
    async def generate_kris_excel(self, kris_data: Dict[str, Any], start_date: Optional[str] = None,
                                 end_date: Optional[str] = None, header_config: Optional[Dict[str, Any]] = None,
                                 card_type: Optional[str] = None, only_card: bool = False,
                                 only_chart: bool = False, only_overall_table: bool = False) -> bytes:
        """Generate KRIs Excel report"""
        try:
            # Create workbook
            wb = Workbook()
            ws = wb.active
            ws.title = "KRIs Report"
            
            # Add header information
            await self._add_excel_header(ws, header_config, "KRIs Dashboard Report")
            
            # Add content based on parameters
            if only_card and card_type:
                await self._add_kris_card_content(ws, card_type, kris_data, header_config)
            elif only_chart and card_type:
                await self._add_kris_chart_content(ws, card_type, kris_data, header_config)
            elif only_overall_table:
                await self._add_kris_overall_table_content(ws, kris_data, header_config)
            else:
                await self._add_kris_full_content(ws, kris_data, header_config)
            
            # Add watermark
            add_watermark_to_excel_sheet(ws, header_config or {})
            
            # Save to buffer
            buffer = io.BytesIO()
            wb.save(buffer)
            buffer.seek(0)
            return buffer.getvalue()
        except Exception as e:
            print(f"Error generating KRIs Excel: {e}")
            return b""

    async def _add_kris_card_content(self, ws, card_type: str, kris_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add KRIs card-specific content to Excel worksheet"""
        try:
            start_row = 8  # Start after header
            
            if card_type in ['totalKris']:
                await self._add_kris_total_content(ws, kris_data, start_row)
            elif card_type in ['krisByStatus']:
                await self._add_kris_by_status_content(ws, kris_data, start_row)
            elif card_type in ['krisByLevel']:
                await self._add_kris_by_level_content(ws, kris_data, start_row)
            elif card_type in ['breachedKRIsByDepartment']:
                await self._add_breached_kris_by_department_content(ws, kris_data, start_row)
            elif card_type in ['kriAssessmentCount']:
                await self._add_kri_assessment_count_content(ws, kris_data, start_row)
        except Exception as e:
            print(f"Error adding KRIs card content: {e}")

    async def _add_kris_chart_content(self, ws, card_type: str, kris_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add KRIs chart-only content to Excel worksheet"""
        try:
            start_row = 8  # Start after header
            
            if card_type in ['krisByStatus']:
                await self._add_kris_by_status_chart(ws, kris_data, start_row)
            elif card_type in ['krisByLevel']:
                await self._add_kris_by_level_chart(ws, kris_data, start_row)
            elif card_type in ['breachedKRIsByDepartment']:
                await self._add_breached_kris_by_department_chart(ws, kris_data, start_row)
            elif card_type in ['kriAssessmentCount']:
                await self._add_kri_assessment_count_chart(ws, kris_data, start_row)
        except Exception as e:
            print(f"Error adding KRIs chart content: {e}")

    async def _add_kris_full_content(self, ws, kris_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add full KRIs content to Excel worksheet"""
        try:
            start_row = 8  # Start after header
            
            # Add all KRI sections
            await self._add_kris_by_status_content(ws, kris_data, start_row)
            start_row += 20  # Space for next section
            
            await self._add_kris_by_level_content(ws, kris_data, start_row)
            start_row += 20  # Space for next section
            
            await self._add_breached_kris_by_department_content(ws, kris_data, start_row)
            start_row += 20  # Space for next section
            
            await self._add_kri_assessment_count_content(ws, kris_data, start_row)
        except Exception as e:
            print(f"Error adding full KRIs content: {e}")

    async def _add_kris_total_content(self, ws, kris_data: Dict[str, Any], start_row: int):
        """Add Total KRIs content"""
        try:
            # Get KRIs list
            kris_list = kris_data.get('krisList', [])
            if not kris_list:
                kris_list = kris_data.get('kriHealth', [])
            
            if not kris_list:
                ws.cell(row=start_row, column=1, value="No KRIs data available")
                return
            
            # Add title
            ws.cell(row=start_row, column=1, value="Total KRIs").font = Font(bold=True, size=14)
            start_row += 2
            
            # Add table headers
            headers = ['KRI Code', 'KRI Name', 'Function', 'Level', 'Status', 'Threshold', 'Created At']
            for i, header in enumerate(headers, 1):
                cell = ws.cell(row=start_row, column=i, value=header)
                cell.font = Font(bold=True, color='FFFFFF')
                cell.fill = PatternFill(start_color='1F4E79', end_color='1F4E79', fill_type='solid')
                cell.alignment = Alignment(horizontal='center')
            
            start_row += 1
            
            # Add data rows
            for kri in kris_list[:100]:  # Limit to 100 for Excel
                created_at = kri.get('created_at', kri.get('createdAt', 'N/A'))
                if created_at and created_at != 'N/A':
                    try:
                        if isinstance(created_at, str):
                            from datetime import datetime
                            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            created_at = dt.strftime('%Y-%m-%d %H:%M')
                    except:
                        pass
                
                ws.cell(row=start_row, column=1, value=kri.get('code', kri.get('kri_code', 'N/A')))
                ws.cell(row=start_row, column=2, value=kri.get('kri_name', kri.get('kriName', 'N/A')))
                ws.cell(row=start_row, column=3, value=kri.get('function_name', kri.get('function', 'N/A')))
                ws.cell(row=start_row, column=4, value=kri.get('kri_level', kri.get('level', 'N/A')))
                ws.cell(row=start_row, column=5, value=kri.get('status', 'N/A'))
                ws.cell(row=start_row, column=6, value=str(kri.get('threshold', 'N/A')))
                ws.cell(row=start_row, column=7, value=created_at)
                
                # Style data rows
                for col in range(1, 8):
                    cell = ws.cell(row=start_row, column=col)
                    cell.alignment = Alignment(horizontal='left', wrap_text=True, vertical='top')
                
                start_row += 1
            
            # Widen columns
            widths = [12, 25, 20, 10, 12, 12, 18]
            for idx, width in enumerate(widths, 1):
                ws.column_dimensions[get_column_letter(idx)].width = width
                
        except Exception as e:
            print(f"Error adding KRIs total content: {e}")

    async def _add_kris_by_status_content(self, ws, kris_data: Dict[str, Any], start_row: int):
        """Add KRIs by Status content"""
        try:
            # Get data
            kris_by_status = kris_data.get('krisByStatus', [])
            if not kris_by_status:
                kris_by_status = kris_data.get('statusDistribution', [])
            
            if not kris_by_status:
                ws.cell(row=start_row, column=1, value="No KRIs status data available")
                return
            
            # Add title
            ws.cell(row=start_row, column=1, value="KRIs by Status").font = Font(bold=True, size=14)
            start_row += 2
            
            # Create chart
            chart_buffer = self._create_pie_chart(
                [item.get('status', 'Unknown') for item in kris_by_status],
                [item.get('count', 0) for item in kris_by_status],
                "KRIs by Status"
            )
            
            # Add chart to Excel
            chart_img = ExcelImage(chart_buffer)
            chart_img.width = 300
            chart_img.height = 200
            ws.add_image(chart_img, f'E{start_row + 2}')
            
            # Add table
            ws.cell(row=start_row, column=1, value="Status").font = Font(bold=True)
            ws.cell(row=start_row, column=2, value="Count").font = Font(bold=True)
            
            start_row += 1
            for item in kris_by_status:
                ws.cell(row=start_row, column=1, value=item.get('status', 'Unknown'))
                ws.cell(row=start_row, column=2, value=item.get('count', 0))
                start_row += 1
                
        except Exception as e:
            print(f"Error adding KRIs by status content: {e}")

    async def _add_kris_by_level_content(self, ws, kris_data: Dict[str, Any], start_row: int):
        """Add KRIs by Risk Level content"""
        try:
            # Get data
            kris_by_level = kris_data.get('krisByLevel', [])
            if not kris_by_level:
                kris_by_level = kris_data.get('levelDistribution', [])
            
            if not kris_by_level:
                ws.cell(row=start_row, column=1, value="No KRIs level data available")
                return
            
            # Add title
            ws.cell(row=start_row, column=1, value="KRIs by Risk Level").font = Font(bold=True, size=14)
            start_row += 2
            
            # Create chart
            chart_buffer = self._create_pie_chart(
                [item.get('level', 'Unknown') for item in kris_by_level],
                [item.get('count', 0) for item in kris_by_level],
                "KRIs by Risk Level"
            )
            
            # Add chart to Excel
            chart_img = ExcelImage(chart_buffer)
            chart_img.width = 300
            chart_img.height = 200
            ws.add_image(chart_img, f'E{start_row + 2}')
            
            # Add table
            ws.cell(row=start_row, column=1, value="Risk Level").font = Font(bold=True)
            ws.cell(row=start_row, column=2, value="Count").font = Font(bold=True)
            
            start_row += 1
            for item in kris_by_level:
                ws.cell(row=start_row, column=1, value=item.get('level', 'Unknown'))
                ws.cell(row=start_row, column=2, value=item.get('count', 0))
                start_row += 1
                
        except Exception as e:
            print(f"Error adding KRIs by level content: {e}")

    async def _add_breached_kris_by_department_content(self, ws, kris_data: Dict[str, Any], start_row: int):
        """Add Breached KRIs by Department content"""
        try:
            # Get data
            breached_kris = kris_data.get('breachedKRIsByDepartment', [])
            if not breached_kris:
                breached_kris = kris_data.get('breachedByDepartment', [])
            
            if not breached_kris:
                ws.cell(row=start_row, column=1, value="No breached KRIs data available")
                return
            
            # Add title
            ws.cell(row=start_row, column=1, value="Breached KRIs by Department").font = Font(bold=True, size=14)
            start_row += 2
            
            # Create chart
            chart_buffer = self._create_bar_chart(
                [item.get('function_name', 'Unknown') for item in breached_kris],
                [item.get('breached_count', 0) for item in breached_kris],
                "Breached KRIs by Department"
            )
            
            # Add chart to Excel
            chart_img = ExcelImage(chart_buffer)
            chart_img.width = 300
            chart_img.height = 200
            ws.add_image(chart_img, f'E{start_row + 2}')
            
            # Add table
            ws.cell(row=start_row, column=1, value="Department").font = Font(bold=True)
            ws.cell(row=start_row, column=2, value="Breached Count").font = Font(bold=True)
            
            start_row += 1
            for item in breached_kris:
                ws.cell(row=start_row, column=1, value=item.get('function_name', 'Unknown'))
                ws.cell(row=start_row, column=2, value=item.get('breached_count', 0))
                start_row += 1
                
        except Exception as e:
            print(f"Error adding breached KRIs by department content: {e}")

    async def _add_kri_assessment_count_content(self, ws, kris_data: Dict[str, Any], start_row: int):
        """Add KRI Assessment Count by Department content"""
        try:
            # Get data
            assessment_count = kris_data.get('kriAssessmentCount', [])
            if not assessment_count:
                assessment_count = kris_data.get('assessmentByDepartment', [])
            
            if not assessment_count:
                ws.cell(row=start_row, column=1, value="No KRI assessment data available")
                return
            
            # Add title
            ws.cell(row=start_row, column=1, value="KRI Assessment Count by Department").font = Font(bold=True, size=14)
            start_row += 2
            
            # Create chart
            chart_buffer = self._create_bar_chart(
                [item.get('function_name', 'Unknown') for item in assessment_count],
                [item.get('assessment_count', 0) for item in assessment_count],
                "KRI Assessment Count by Department"
            )
            
            # Add chart to Excel
            chart_img = ExcelImage(chart_buffer)
            chart_img.width = 300
            chart_img.height = 200
            ws.add_image(chart_img, f'E{start_row + 2}')
            
            # Add table
            ws.cell(row=start_row, column=1, value="Department").font = Font(bold=True)
            ws.cell(row=start_row, column=2, value="Assessment Count").font = Font(bold=True)
            
            start_row += 1
            for item in assessment_count:
                ws.cell(row=start_row, column=1, value=item.get('function_name', 'Unknown'))
                ws.cell(row=start_row, column=2, value=item.get('assessment_count', 0))
                start_row += 1
                
        except Exception as e:
            print(f"Error adding KRI assessment count content: {e}")

    async def _add_kris_overall_table_content(self, ws, kris_data: Dict[str, Any], header_config: Optional[Dict[str, Any]]):
        """Add Overall KRI Statuses content"""
        try:
            start_row = 8  # Start after header
            
            # Get KRIs list
            kris_list = kris_data.get('krisList', [])
            if not kris_list:
                kris_list = kris_data.get('kriHealth', [])
            
            if not kris_list:
                ws.cell(row=start_row, column=1, value="No KRIs data available")
                return
            
            # Add title
            ws.cell(row=start_row, column=1, value="Overall KRI Statuses").font = Font(bold=True, size=14)
            start_row += 2
            
            # Add table headers
            headers = ['KRI Code', 'KRI Name', 'Function', 'Level', 'Status', 'Threshold', 'Created At']
            for i, header in enumerate(headers, 1):
                cell = ws.cell(row=start_row, column=i, value=header)
                cell.font = Font(bold=True, color='FFFFFF')
                cell.fill = PatternFill(start_color='1F4E79', end_color='1F4E79', fill_type='solid')
                cell.alignment = Alignment(horizontal='center')
            
            start_row += 1
            
            # Add data rows
            for kri in kris_list[:100]:  # Limit to 100 for Excel
                created_at = kri.get('created_at', kri.get('createdAt', 'N/A'))
                if created_at and created_at != 'N/A':
                    try:
                        if isinstance(created_at, str):
                            from datetime import datetime
                            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            created_at = dt.strftime('%Y-%m-%d %H:%M')
                    except:
                        pass
                
                ws.cell(row=start_row, column=1, value=kri.get('code', kri.get('kri_code', 'N/A')))
                ws.cell(row=start_row, column=2, value=kri.get('kri_name', kri.get('kriName', 'N/A')))
                ws.cell(row=start_row, column=3, value=kri.get('function_name', kri.get('function', 'N/A')))
                ws.cell(row=start_row, column=4, value=kri.get('kri_level', kri.get('level', 'N/A')))
                ws.cell(row=start_row, column=5, value=kri.get('status', 'N/A'))
                ws.cell(row=start_row, column=6, value=str(kri.get('threshold', 'N/A')))
                ws.cell(row=start_row, column=7, value=created_at)
                
                # Style data rows
                for col in range(1, 8):
                    cell = ws.cell(row=start_row, column=col)
                    cell.alignment = Alignment(horizontal='left', wrap_text=True, vertical='top')
                
                start_row += 1
            
            # Widen columns
            widths = [12, 25, 20, 10, 12, 12, 18]
            for idx, width in enumerate(widths, 1):
                ws.column_dimensions[get_column_letter(idx)].width = width
                
        except Exception as e:
            print(f"Error adding KRIs overall table content: {e}")

    # KRI Chart-only methods
    async def _add_kris_by_status_chart(self, ws, kris_data: Dict[str, Any], start_row: int):
        """Add KRIs by Status chart only"""
        try:
            kris_by_status = kris_data.get('krisByStatus', [])
            if kris_by_status:
                chart_buffer = self._create_pie_chart(
                    [item.get('status', 'Unknown') for item in kris_by_status],
                    [item.get('count', 0) for item in kris_by_status],
                    "KRIs by Status"
                )
                
                chart_img = ExcelImage(chart_buffer)
                chart_img.width = 400
                chart_img.height = 300
                ws.add_image(chart_img, f'A{start_row}')
        except Exception as e:
            print(f"Error adding KRIs by status chart: {e}")

    async def _add_kris_by_level_chart(self, ws, kris_data: Dict[str, Any], start_row: int):
        """Add KRIs by Risk Level chart only"""
        try:
            kris_by_level = kris_data.get('krisByLevel', [])
            if kris_by_level:
                chart_buffer = self._create_pie_chart(
                    [item.get('level', 'Unknown') for item in kris_by_level],
                    [item.get('count', 0) for item in kris_by_level],
                    "KRIs by Risk Level"
                )
                
                chart_img = ExcelImage(chart_buffer)
                chart_img.width = 400
                chart_img.height = 300
                ws.add_image(chart_img, f'A{start_row}')
        except Exception as e:
            print(f"Error adding KRIs by level chart: {e}")

    async def _add_breached_kris_by_department_chart(self, ws, kris_data: Dict[str, Any], start_row: int):
        """Add Breached KRIs by Department chart only"""
        try:
            breached_kris = kris_data.get('breachedKRIsByDepartment', [])
            if breached_kris:
                chart_buffer = self._create_bar_chart(
                    [item.get('function_name', 'Unknown') for item in breached_kris],
                    [item.get('breached_count', 0) for item in breached_kris],
                    "Breached KRIs by Department"
                )
                
                chart_img = ExcelImage(chart_buffer)
                chart_img.width = 400
                chart_img.height = 300
                ws.add_image(chart_img, f'A{start_row}')
        except Exception as e:
            print(f"Error adding breached KRIs by department chart: {e}")

    async def _add_kri_assessment_count_chart(self, ws, kris_data: Dict[str, Any], start_row: int):
        """Add KRI Assessment Count by Department chart only"""
        try:
            assessment_count = kris_data.get('kriAssessmentCount', [])
            if assessment_count:
                chart_buffer = self._create_bar_chart(
                    [item.get('function_name', 'Unknown') for item in assessment_count],
                    [item.get('assessment_count', 0) for item in assessment_count],
                    "KRI Assessment Count by Department"
                )
                
                chart_img = ExcelImage(chart_buffer)
                chart_img.width = 400
                chart_img.height = 300
                ws.add_image(chart_img, f'A{start_row}')
        except Exception as e:
            print(f"Error adding KRI assessment count chart: {e}")
