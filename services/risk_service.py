"""
Risk service for risk operations
"""
import pyodbc
import asyncio
from typing import List, Dict, Any, Optional
from config import get_database_connection_string

def write_debug(msg):
    """Write debug message to file with timestamp"""
    from datetime import datetime
    timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    msg_with_time = f"[{timestamp}] {msg}"
    with open('debug_log.txt', 'a', encoding='utf-8') as f:
        f.write(f"{msg_with_time}\n")
        f.flush()
    import sys
    sys.stderr.write(f"{msg_with_time}\n")
    sys.stderr.flush()

class RiskService:

    """Service for risk operations"""
    
    def __init__(self):
        self.connection_string = get_database_connection_string()

    def get_fully_qualified_table_name(self, table_name: str) -> str:
        """Get fully qualified table name using configuration"""
        from config import DATABASE_CONFIG
        database_name = DATABASE_CONFIG.get('database', 'NEWDCC-V4-UAT')
        return f"[{database_name}].dbo.[{table_name}]"
    
    async def execute_query(self, query: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results"""
        try:
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._execute_sync_query, query, params)
            return result
        except Exception as e:
            return []
    
    def _execute_sync_query(self, query: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
        """Execute synchronous database query"""
        try:
            with pyodbc.connect(self.connection_string) as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # Get column names
                columns = [column[0] for column in cursor.description]
                
                # Fetch all results
                rows = cursor.fetchall()
                
                # Convert to list of dictionaries
                result = []
                for row in rows:
                    row_dict = {}
                    for i, value in enumerate(row):
                        row_dict[columns[i]] = value
                    result.append(row_dict)
                
                return result
        except Exception as e:
            return []

    async def get_risks_by_category(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get risks grouped by category"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.created_at BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.created_at >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.created_at <= '{end_date}'"
        
        query = f"""
        SELECT 
            c.name as category_name,
            COUNT(*) as risk_count
        FROM dbo.[Risks] r
        INNER JOIN dbo.[RiskCategories] rc ON r.id = rc.risk_id
        INNER JOIN dbo.[Categories] c ON rc.category_id = c.id
        WHERE r.isDeleted = 0 
        {date_filter}
        GROUP BY c.name
        ORDER BY risk_count DESC
        """
        
        return await self.execute_query(query)
    
    async def get_risks_by_event_type(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get risks grouped by event type"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.created_at BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.created_at >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.created_at <= '{end_date}'"
        
        query = f"""
        SELECT 
            r.event_type,
            COUNT(*) as risk_count
        FROM dbo.[Risks] r
        WHERE r.isDeleted = 0 
        {date_filter}
        GROUP BY r.event_type
        ORDER BY risk_count DESC
        """
        
        return await self.execute_query(query)
