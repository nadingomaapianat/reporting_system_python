"""
KRI service for KRI operations
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

class KriService:
    """Service for kri operations"""

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
    
   
 
    # KRI Database Methods
    async def get_kris_by_status(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return KRIs grouped by status"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            CASE 
                WHEN k.preparerStatus = 'sent' THEN 'Pending Preparer'
                WHEN k.checkerStatus = 'approved' AND k.reviewerStatus = 'pending' THEN 'Pending Reviewer'
                WHEN k.checkerStatus = 'approved' AND k.reviewerStatus = 'sent' AND k.acceptanceStatus = 'pending' THEN 'Pending Acceptance'
                WHEN k.checkerStatus = 'approved' AND k.reviewerStatus = 'sent' AND k.acceptanceStatus = 'approved' THEN 'Approved'
                ELSE 'Other'
            END as status,
            COUNT(*) as count
        FROM Kris k
        WHERE k.isDeleted = 0 {date_filter}
        GROUP BY 
            CASE 
                WHEN k.preparerStatus = 'sent' THEN 'Pending Preparer'
                WHEN k.checkerStatus = 'approved' AND k.reviewerStatus = 'pending' THEN 'Pending Reviewer'
                WHEN k.checkerStatus = 'approved' AND k.reviewerStatus = 'sent' AND k.acceptanceStatus = 'pending' THEN 'Pending Acceptance'
                WHEN k.checkerStatus = 'approved' AND k.reviewerStatus = 'sent' AND k.acceptanceStatus = 'approved' THEN 'Approved'
                ELSE 'Other'
            END
        ORDER BY count DESC
        """
        return await self.execute_query(query)

    async def get_kris_by_level(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return KRIs grouped by risk level"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            k.kri_level as level,
            COUNT(*) as count
        FROM Kris k
        WHERE k.isDeleted = 0 {date_filter}
        AND k.kri_level IS NOT NULL
        GROUP BY k.kri_level
        ORDER BY count DESC
        """
        return await self.execute_query(query)

    async def get_breached_kris_by_department(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return breached KRIs by department"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            f.name as function_name,
            COUNT(k.id) as breached_count
        FROM Kris k
        LEFT JOIN Functions f ON k.related_function_id = f.id
        WHERE k.isDeleted = 0 {date_filter}
        AND k.status = 'Breached'
        GROUP BY f.name
        ORDER BY breached_count DESC
        """
        return await self.execute_query(query)

    async def get_kri_assessment_count(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return KRI assessment count by department"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            f.name as function_name,
            COUNT(k.id) as assessment_count
        FROM Kris k
        LEFT JOIN Functions f ON k.related_function_id = f.id
        WHERE k.isDeleted = 0 {date_filter}
        GROUP BY f.name
        ORDER BY assessment_count DESC
        """
        return await self.execute_query(query)

    async def get_kris_list(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return list of all KRIs"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            k.id,
            k.code,
            k.kriName as kri_name,
            k.threshold,
            k.isAscending as is_ascending,
            k.kri_level,
            k.status,
            k.createdAt as created_at,
            k.updatedAt as updated_at,
            f.name as function_name
        FROM Kris k
        LEFT JOIN Functions f ON k.related_function_id = f.id
        WHERE k.isDeleted = 0 {date_filter}
        ORDER BY k.createdAt DESC
        """
        return await self.execute_query(query)
    
   
   