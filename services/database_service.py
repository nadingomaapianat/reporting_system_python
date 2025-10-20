"""
Database service for data operations
"""
import pyodbc
import asyncio
from typing import List, Dict, Any, Optional
from config import get_database_connection_string
from models import RiskData, ControlData

class DatabaseService:
    """Service for database operations"""
    
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
    
    async def get_risks_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[RiskData]:
        """Get risks data from database"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND r.created_at BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND r.created_at >= '{start_date}'"
        elif end_date:
            date_filter = f"AND r.created_at <= '{end_date}'"
        
        query = f"""
        SELECT 
            r.code,
            r.name as risk_name,
            r.title,
            r.inherent_value,
            r.created_at
        FROM dbo.[Risks] r 
        WHERE r.isDeleted = 0 
        {date_filter}
        ORDER BY r.created_at DESC
        """
        
        results = await self.execute_query(query)
        return [RiskData(**row) for row in results]
    
    async def get_controls_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[ControlData]:
        """Get controls data from database"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.created_at BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.created_at >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.created_at <= '{end_date}'"
        
        query = f"""
        SELECT 
            c.control_code,
            c.control_name,
            d.department_name,
            c.status,
            c.created_at
        FROM dbo.[Controls] c
        LEFT JOIN dbo.[Departments] d ON c.department_id = d.id
        WHERE c.isDeleted = 0 
        {date_filter}
        ORDER BY c.createdAt DESC
        """
        
        results = await self.execute_query(query)
        return [ControlData(**row) for row in results]

    async def get_unmapped_controls(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get unmapped controls based on Node.js config logic"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"

        # Mirror Node query: controls with no ControlCosos mapping
        query = f"""
        SELECT 
            c.code as control_code,
            c.name as control_name
        FROM {self.get_fully_qualified_table_name('Controls')} c
        WHERE c.isDeleted = 0 {date_filter}
          AND NOT EXISTS (
            SELECT 1 FROM {self.get_fully_qualified_table_name('ControlCosos')} ccx 
            WHERE ccx.control_id = c.id AND ccx.deletedAt IS NULL
          )
        ORDER BY c.createdAt DESC, c.name
        """
        return await self.execute_query(query)

    async def get_pending_controls(self, role: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get pending controls for a given role: preparer/checker/reviewer/acceptance"""
        status_field_map = {
            'preparer': 'preparerStatus',
            'checker': 'checkerStatus',
            'reviewer': 'reviewerStatus',
            'acceptance': 'acceptanceStatus',
        }
        field = status_field_map.get(role)
        if not field:
            return []

        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"

        query = f"""
        SELECT 
            c.code as control_code,
            c.name as control_name,
            c.{field} as status
        FROM dbo.[Controls] c
        WHERE c.isDeleted = 0 {date_filter}
          AND (c.{field} IS NULL OR c.{field} <> 'approved')
        ORDER BY c.createdAt DESC, c.name
        """
        return await self.execute_query(query)
    
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

    async def get_unmapped_icofr_controls(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get unmapped ICOFR controls data for export"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT c.id, c.name as control_name, c.code as control_code, a.name as assertion_name, a.account_type as assertion_type,
          'Not Mapped' as coso_component,
          'Not Mapped' as coso_point
        FROM {self.get_fully_qualified_table_name('Controls')} c 
        JOIN {self.get_fully_qualified_table_name('Assertions')} a ON c.icof_id = a.id 
        WHERE c.isDeleted = 0 AND c.icof_id IS NOT NULL 
        AND NOT EXISTS (SELECT 1 FROM {self.get_fully_qualified_table_name('ControlCosos')} ccx WHERE ccx.control_id = c.id AND ccx.deletedAt IS NULL) 
        AND ((a.C = 1 OR a.E = 1 OR a.A = 1 OR a.V = 1 OR a.O = 1 OR a.P = 1) 
             AND a.account_type IN ('Balance Sheet', 'Income Statement')) 
        AND a.isDeleted = 0 {date_filter}
        ORDER BY c.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_unmapped_non_icofr_controls(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get unmapped Non-ICOFR controls data for export"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.createdAt <= '{end_date}'"
        
        query = f"""
        SELECT c.id, c.name as control_name, c.code as control_code, a.name as assertion_name, a.account_type as assertion_type,
          'Not Mapped' as coso_component,
          'Not Mapped' as coso_point
        FROM {self.get_fully_qualified_table_name('Controls')} c 
        LEFT JOIN {self.get_fully_qualified_table_name('Assertions')} a ON c.icof_id = a.id 
        WHERE c.isDeleted = 0 
        AND NOT EXISTS (SELECT 1 FROM {self.get_fully_qualified_table_name('ControlCosos')} ccx WHERE ccx.control_id = c.id AND ccx.deletedAt IS NULL) 
        AND (c.icof_id IS NULL OR ((a.C IS NULL OR a.C = 0) AND (a.E IS NULL OR a.E = 0) AND (a.A IS NULL OR a.A = 0) 
             AND (a.V IS NULL OR a.V = 0) AND (a.O IS NULL OR a.O = 0) AND (a.P IS NULL OR a.P = 0) 
             OR a.account_type NOT IN ('Balance Sheet', 'Income Statement'))) 
        AND (a.isDeleted = 0 OR a.id IS NULL) {date_filter}
        ORDER BY c.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_controls_by_department(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get controls grouped by department"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.created_at BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.created_at >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.created_at <= '{end_date}'"
        
        query = f"""
        SELECT 
            d.department_name,
            COUNT(*) as control_count
        FROM dbo.[Controls] c
        LEFT JOIN dbo.[Departments] d ON c.department_id = d.id
        WHERE c.isDeleted = 0 
        {date_filter}
        GROUP BY d.department_name
        ORDER BY control_count DESC
        """
        
        return await self.execute_query(query)
    
    async def get_controls_by_risk_response(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get controls grouped by risk response type"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND c.created_at BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND c.created_at >= '{start_date}'"
        elif end_date:
            date_filter = f"AND c.created_at <= '{end_date}'"
        
        query = f"""
        SELECT 
            c.risk_response_type,
            COUNT(*) as control_count
        FROM dbo.[Controls] c
        WHERE c.isDeleted = 0 
        {date_filter}
        GROUP BY c.risk_response_type
        ORDER BY control_count DESC
        """
        
        return await self.execute_query(query)

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
