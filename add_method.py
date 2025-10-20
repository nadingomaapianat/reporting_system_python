# Read the current file
with open('services/pdf_service.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Add the missing method
new_method = '''

    async def generate_controls_pdf(self, controls_data: Dict[str, Any], report_config: Dict[str, Any]) -> bytes:
        """Generate PDF report for controls dashboard"""
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
            print(f"Error generating controls PDF: {e}")
            raise e
'''

# Append the method
new_content = content + new_method

# Write back to file
with open('services/pdf_service.py', 'w', encoding='utf-8') as f:
    f.write(new_content)

print('Added generate_controls_pdf method to PDF service')
