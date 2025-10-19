#!/usr/bin/env python3
"""
Test the Net Loss by Incident data fetching
"""
import asyncio
import sys
import os

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.database_service import DatabaseService
from services.api_service import APIService

async def test_net_loss_data():
    """Test the Net Loss by Incident data fetching"""
    print("Testing Net Loss by Incident data fetching...")
    
    # Test database service
    db_service = DatabaseService()
    try:
        net_loss_data = await db_service.get_incidents_net_loss_recovery()
        print(f"✅ Database service returned: {len(net_loss_data)} records")
        if net_loss_data:
            print(f"   Sample record: {net_loss_data[0]}")
        else:
            print("   No data returned")
    except Exception as e:
        print(f"❌ Database service error: {str(e)}")
    
    # Test API service
    api_service = APIService()
    try:
        incidents_data = await api_service.get_incidents_data()
        print(f"✅ API service returned incidents data")
        print(f"   Keys: {list(incidents_data.keys())}")
        
        net_loss = incidents_data.get('netLossAndRecovery', [])
        print(f"   netLossAndRecovery: {len(net_loss)} records")
        if net_loss:
            print(f"   Sample record: {net_loss[0]}")
        else:
            print("   No netLossAndRecovery data")
            
    except Exception as e:
        print(f"❌ API service error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(test_net_loss_data())
