"""
API service for communicating with Node.js backend
"""
import aiohttp
import asyncio
import json
from typing import Dict, Any, Optional, List
from config import API_CONFIG

class APIService:
    """Service for API communications"""
    
    def __init__(self):
        self.node_api_url = API_CONFIG['node_api_url']
        self.timeout = API_CONFIG['timeout']
    
    async def get_risks_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """Get risks data from Node.js API"""
        try:
            url = f"{self.node_api_url}/api/grc/risks"
            params = {}
            if start_date:
                params['startDate'] = start_date
            if end_date:
                params['endDate'] = end_date
            
            # Add reasonable connect/read timeouts to avoid long hangs
            timeout = aiohttp.ClientTimeout(total=self.timeout, connect=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data
                    else:
                        print(f"DEBUG: Node API returned status {response.status}")
                        return {}
        except asyncio.TimeoutError:
            print("DEBUG: Node API timeout - returning empty risks data")
            return {}
        except aiohttp.ClientError as e:
            print(f"DEBUG: Node API client error: {e}")
            return {}
        except Exception as e:
            print(f"DEBUG: Node API error: {e}")
            return {}
    
    async def get_controls_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """Get controls data from Node.js API with fallback"""
        try:
            url = f"{self.node_api_url}/api/grc/controls"
            params = {}
            if start_date:
                params['startDate'] = start_date
            if end_date:
                params['endDate'] = end_date
            
            # Increase timeout for complex queries
            timeout = aiohttp.ClientTimeout(total=self.timeout, connect=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data
                    else:
                        print(f"DEBUG: Node API returned status {response.status}")
                        return {}
        except asyncio.TimeoutError:
            print("DEBUG: Node API timeout - returning empty data")
            return {}
        except aiohttp.ClientError as e:
            print(f"DEBUG: Node API client error: {e}")
            return {}
        except Exception as e:
            print(f"DEBUG: Node API error: {e}")
            return {}
    
    async def get_risks_card_data(self, card_type: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get specific risks card data from Node.js API"""
        try:
            url = f"{self.node_api_url}/api/grc/risks/{card_type}"
            params = {}
            if start_date:
                params['startDate'] = start_date
            if end_date:
                params['endDate'] = end_date
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        # Handle paginated response
                        if isinstance(data, dict) and 'data' in data:
                            return data['data']
                        elif isinstance(data, list):
                            return data
                        else:
                            return []
                    else:
                        return []
        except Exception as e:
            return []
    
    async def get_controls_chart_data(self, chart_type: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]] | Dict[str, Any]:
        """Get specific controls chart data from Node.js API; returns list or dict depending on endpoint."""
        try:
            # Convert camelCase to kebab-case and map to charts endpoint
            charts_map = {
                'department': 'charts/department',
                'risk': 'charts/risk',
                'quarterlyControlCreationTrend': 'charts/quarterly-control-creation-trend',
                'controlsByType': 'charts/controls-by-type',
                'antiFraudDistribution': 'charts/anti-fraud-distribution',
                'controlsPerLevel': 'charts/controls-per-level',
                'controlExecutionFrequency': 'charts/control-execution-frequency',
                'numberOfControlsByIcofrStatus': 'charts/number-of-controls-by-icofr-status',
                'numberOfFocusPointsPerPrinciple': 'charts/number-of-focus-points-per-principle',
                'numberOfFocusPointsPerComponent': 'charts/number-of-focus-points-per-component',
                'numberOfControlsPerComponent': 'charts/number-of-controls-per-component',
                'actionPlansStatus': 'charts/action-plans-status',
            }
            endpoint = charts_map.get(chart_type, f"charts/{chart_type}")
            url = f"{self.node_api_url}/api/grc/controls/{endpoint}"
            params = {}
            if start_date:
                params['startDate'] = start_date
            if end_date:
                params['endDate'] = end_date

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout, connect=10)) as session:
                async with session.get(url, params=params) as response:
                    if response.status != 200:
                        return []
                    data = await response.json()
                    if isinstance(data, dict):
                        # common wrappers
                        for key in ['data', 'items', 'results']:
                            if key in data and isinstance(data[key], list):
                                return data[key]
                        return data
                    return data if isinstance(data, list) else []
        except Exception:
            return []
    
    async def get_controls_card_data(self, card_type: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get specific controls card data from Node.js API"""
        try:
            # Convert camelCase to kebab-case for endpoints
            endpoint_map = {
                'totalControls': 'total',
                'unmappedControls': 'unmapped',
                'pendingPreparer': 'pending-preparer',
                'pendingChecker': 'pending-checker',
                'pendingReviewer': 'pending-reviewer',
                'pendingAcceptance': 'pending-acceptance',
                'testsPendingPreparer': 'tests/pending-preparer',
                'testsPendingChecker': 'tests/pending-checker',
                'testsPendingReviewer': 'tests/pending-reviewer',
                'testsPendingAcceptance': 'tests/pending-acceptance'
            }
            
            endpoint = endpoint_map.get(card_type, card_type)
            url = f"{self.node_api_url}/api/grc/controls/{endpoint}"
            # Request a large page size to retrieve all rows for export
            params = {'page': 1, 'limit': 10000}
            if start_date:
                params['startDate'] = start_date
            if end_date:
                params['endDate'] = end_date
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        # Handle paginated or array response
                        if isinstance(data, dict):
                            if 'data' in data and isinstance(data['data'], list):
                                return data['data']
                            # Some endpoints may nest results under result/items
                            if 'items' in data and isinstance(data['items'], list):
                                return data['items']
                            if 'results' in data and isinstance(data['results'], list):
                                return data['results']
                            return []
                        elif isinstance(data, list):
                            return data
                        else:
                            return []
                    else:
                        return []
        except Exception as e:
            return []

    async def get_incidents_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """Get incidents dashboard data from Node.js API"""
        try:
            url = f"{self.node_api_url}/api/grc/incidents"
            params = {}
            if start_date:
                params['startDate'] = start_date
            if end_date:
                params['endDate'] = end_date
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    return {}
        except Exception:
            return {}

    async def get_incidents_card_data(self, card_type: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get specific incidents card data from Node.js API (paginated-safe)"""
        try:
            url = f"{self.node_api_url}/api/grc/incidents/{card_type}"
            params = {'page': 1, 'limit': 10000}
            if start_date:
                params['startDate'] = start_date
            if end_date:
                params['endDate'] = end_date
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if isinstance(data, dict):
                            for key in ['data', 'items', 'results']:
                                if key in data and isinstance(data[key], list):
                                    return data[key]
                            return []
                        if isinstance(data, list):
                            return data
                        return []
                    return []
        except Exception:
            return []

    async def get_kris_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """Get KRIs dashboard data from Node.js API"""
        try:
            url = f"{self.node_api_url}/api/grc/kris"
            params = {}
            if start_date:
                params['startDate'] = start_date
            if end_date:
                params['endDate'] = end_date
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    return {}
        except Exception:
            return {}

    async def get_kris_card_data(self, card_type: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get specific KRIs card data from Node.js API"""
        try:
            url = f"{self.node_api_url}/api/grc/kris/{card_type}"
            params = {'page': 1, 'limit': 10000}
            if start_date:
                params['startDate'] = start_date
            if end_date:
                params['endDate'] = end_date
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout)) as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if isinstance(data, dict):
                            for key in ['data', 'items', 'results']:
                                if key in data and isinstance(data[key], list):
                                    return data[key]
                            return []
                        if isinstance(data, list):
                            return data
                        return []
                    return []
        except Exception:
            return []
