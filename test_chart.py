#!/usr/bin/env python3
"""
Test script to verify matplotlib chart generation
"""
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import io
from openpyxl import Workbook
from openpyxl.drawing.image import Image as ExcelImage

def test_chart_generation():
    """Test basic chart generation"""
    print("Testing matplotlib chart generation...")
    
    # Test data
    test_data = [
        {'name': 'Event', 'value': 741},
        {'name': 'Consequence', 'value': 223},
        {'name': 'Unknown', 'value': 22}
    ]
    
    try:
        # Create figure
        fig, ax = plt.subplots(figsize=(8, 4))
        
        # Create pie chart
        labels = [item['name'] for item in test_data]
        values = [item['value'] for item in test_data]
        
        print(f"Labels: {labels}")
        print(f"Values: {values}")
        
        ax.pie(values, labels=labels, autopct='%1.1f%%', startangle=90)
        ax.set_title('Test Pie Chart', fontsize=12, fontweight='bold')
        plt.tight_layout()
        
        # Save to buffer
        chart_buffer = io.BytesIO()
        plt.savefig(chart_buffer, format='png', dpi=150, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        chart_buffer.seek(0)
        buffer_size = len(chart_buffer.getvalue())
        
        print(f"Chart buffer size: {buffer_size} bytes")
        
        if buffer_size > 0:
            # Create Excel workbook and add chart
            wb = Workbook()
            ws = wb.active
            ws.title = "Test Chart"
            
            # Add image to Excel
            img = ExcelImage(chart_buffer)
            img.width = 400
            img.height = 200
            ws.add_image(img, 'A1')
            
            # Save Excel file
            wb.save('test_chart_output.xlsx')
            print("Chart successfully added to Excel file: test_chart_output.xlsx")
        else:
            print("ERROR: Chart buffer is empty!")
        
        plt.close(fig)
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_chart_generation()
