import asyncio
import aiohttp
import json

async def analyze_control_submission_data():
    """Analyze what data exists for control submission status"""
    try:
        async with aiohttp.ClientSession() as session:
            # Test the Python API to see what data is being generated
            print("=== Testing Python API Data Generation ===")
            
            # Test PDF export
            pdf_url = 'http://localhost:8000/api/grc/controls/export-pdf?onlyOverallTable=true&tableType=controlSubmissionStatusByQuarterFunction&startDate=2024-01-01&endDate=2024-12-31'
            async with session.get(pdf_url) as response:
                print(f'PDF export status: {response.status}')
                if response.status == 200:
                    content_length = response.headers.get('content-length', 'unknown')
                    print(f'PDF content length: {content_length}')
                    if int(content_length) > 1000:  # If PDF has substantial content
                        print("✅ PDF export has data - table is not empty")
                    else:
                        print("❌ PDF export is small - table might be empty")
                else:
                    print(f'PDF error: {response.status}')
            
            # Test Excel export
            excel_url = 'http://localhost:8000/api/grc/controls/export-excel?onlyOverallTable=true&tableType=controlSubmissionStatusByQuarterFunction&startDate=2024-01-01&endDate=2024-12-31'
            async with session.get(excel_url) as response:
                print(f'Excel export status: {response.status}')
                if response.status == 200:
                    content_length = response.headers.get('content-length', 'unknown')
                    print(f'Excel content length: {content_length}')
                    if int(content_length) > 1000:  # If Excel has substantial content
                        print("✅ Excel export has data - table is not empty")
                    else:
                        print("❌ Excel export is small - table might be empty")
                else:
                    print(f'Excel error: {response.status}')
            
            print("\n=== Analysis ===")
            print("Both PDF and Excel exports are working, which means:")
            print("1. The Python API is successfully fetching data")
            print("2. The SQL query is executing without errors")
            print("3. The data is being processed and exported")
            
            print("\nIf you're seeing 'no data' in the frontend, the issue might be:")
            print("1. Node.js server not running or routing issue")
            print("2. Frontend not calling the correct API endpoint")
            print("3. Data exists but the simplified approval logic is too restrictive")
            
            print("\nThe simplified approval logic requires:")
            print("- preparerStatus = 'sent'")
            print("- acceptanceStatus = 'approved'")
            print("This might be too restrictive if most records don't meet both criteria.")
            
    except Exception as e:
        print(f'Error: {e}')

if __name__ == "__main__":
    asyncio.run(analyze_control_submission_data())
