"""
Incident service for incident operations
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

class IncidentService:

    """Service for incident operations"""
    
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

       # ===== Incidents fallbacks =====
    async def get_incidents_list(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return incidents detailed list (basic fields)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
            code,
            title,
            CASE 
                WHEN acceptanceStatus = 'approved' THEN 'approved'
                WHEN reviewerStatus = 'approved' THEN 'approved'
                WHEN checkerStatus = 'approved' THEN 'approved'
                ELSE ISNULL(preparerStatus, acceptanceStatus)
            END as status,
            'N/A' as createdAt
        FROM Incidents
        WHERE isDeleted = 0 {date_filter}
        ORDER BY id DESC
        """
        return await self.execute_query(query)

    async def get_incidents_by_category(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return incidents count by category"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
            ic.name as category_name,
            COUNT(i.id) as count
        FROM Incidents i
        LEFT JOIN IncidentCategories ic ON i.category_id = ic.id
        WHERE i.isDeleted = 0 {date_filter}
        GROUP BY ic.name
        ORDER BY COUNT(i.id) DESC
        """
        return await self.execute_query(query)

    async def get_incidents_by_status(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return incidents count by status"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
            CASE 
                WHEN i.preparerStatus = 'sent' THEN 'Pending Preparer'
                WHEN i.checkerStatus = 'approved' AND i.reviewerStatus = 'pending' THEN 'Pending Reviewer'
                WHEN i.checkerStatus = 'approved' AND i.reviewerStatus = 'sent' AND i.acceptanceStatus = 'pending' THEN 'Pending Acceptance'
                WHEN i.checkerStatus = 'approved' AND i.reviewerStatus = 'sent' AND i.acceptanceStatus = 'approved' THEN 'Approved'
                ELSE 'Other'
            END as status,
            COUNT(*) as count
        FROM Incidents i
        WHERE i.isDeleted = 0 {date_filter}
        GROUP BY 
            CASE 
                WHEN i.preparerStatus = 'sent' THEN 'Pending Preparer'
                WHEN i.checkerStatus = 'approved' AND i.reviewerStatus = 'pending' THEN 'Pending Reviewer'
                WHEN i.checkerStatus = 'approved' AND i.reviewerStatus = 'sent' AND i.acceptanceStatus = 'pending' THEN 'Pending Acceptance'
                WHEN i.checkerStatus = 'approved' AND i.reviewerStatus = 'sent' AND i.acceptanceStatus = 'approved' THEN 'Approved'
                ELSE 'Other'
            END
        ORDER BY COUNT(*) DESC
        """
        return await self.execute_query(query)

    async def get_incidents_monthly_trend(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return incidents monthly trend counts"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
            FORMAT(i.occurrence_date, 'MMM yyyy') as month_year,
            COUNT(i.id) as incident_count,
            SUM(ISNULL(i.net_loss, 0)) as total_loss
        FROM Incidents i
        WHERE i.isDeleted = 0 {date_filter}
        AND i.occurrence_date IS NOT NULL
        GROUP BY FORMAT(i.occurrence_date, 'MMM yyyy')
        ORDER BY MIN(i.occurrence_date)
        """
        return await self.execute_query(query)
    
    async def get_incidents_top_financial_impacts(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return top financial impacts for incidents"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            'Financial Impact' as financial_impact_name,
            ISNULL(SUM(i.net_loss), 0) as net_loss
        FROM Incidents i
        WHERE i.isDeleted = 0 {date_filter}
        AND i.net_loss > 0
        GROUP BY 'Financial Impact'
        ORDER BY net_loss DESC
        """
        return await self.execute_query(query)
    
    async def get_incidents_net_loss_recovery(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return net loss and recovery data for incidents"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT 
            i.title as incident_title,
            ISNULL(i.net_loss, 0) as net_loss,
            ISNULL(i.recovery_amount, 0) as recovery_amount
        FROM Incidents i
        WHERE i.isDeleted = 0 {date_filter}
        AND i.net_loss > 0
        ORDER BY i.net_loss DESC
        """
        return await self.execute_query(query)
