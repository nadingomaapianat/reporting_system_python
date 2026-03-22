"""
Incident service for incident operations
"""
import asyncio
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from config import get_db_connection
from utils.jwt_context import get_request_jwt_claims
from utils.reporting_access import is_reporting_admin

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
        pass  # connection via get_db_connection() when needed

    def get_fully_qualified_table_name(self, table_name: str) -> str:
        """Get fully qualified table name using configuration"""
        from config import DATABASE_CONFIG
        database_name = DATABASE_CONFIG.get('database', 'NEWDCC-V4-UAT')
        return f"[{database_name}].dbo.[{table_name}]"
    
    async def _get_user_function_access(self, user_id: Optional[str], group_name: Optional[str]):
        """
        Mirror Node UserFunctionAccessService.getUserFunctionAccess for Incidents.
        - Admin: super_admin_, REPORTING_SUPER_ADMIN_USER_IDS, JWT role=admin, isAdmin=true.
        - If user_id is None: treat as unrestricted (backward compatibility).
        - Otherwise: fetch functionIds from UserFunction + Functions.
        """
        claims = get_request_jwt_claims()
        if is_reporting_admin(user_id, group_name, claims):
            return {"is_super_admin": True, "function_ids": []}

        if not user_id:
            return {"is_super_admin": True, "function_ids": []}

        query = f"""
        SELECT LTRIM(RTRIM(uf.functionId)) AS id
        FROM {self.get_fully_qualified_table_name('UserFunction')} uf
        JOIN {self.get_fully_qualified_table_name('Functions')} f
          ON LTRIM(RTRIM(f.id)) = LTRIM(RTRIM(uf.functionId))
        WHERE uf.userId = %s
          AND uf.deletedAt IS NULL
          AND f.isDeleted = 0
          AND f.deletedAt IS NULL
        """
        rows = await self.execute_query(query, [user_id])
        # Trim function IDs to handle spaces
        function_ids = [str(r.get('id')).strip() if r.get('id') else None for r in rows]
        function_ids = [fid for fid in function_ids if fid]  # Remove None values
        if (
            not function_ids
            and os.getenv("REPORTING_ALLOW_ALL_WHEN_NO_USER_FUNCTIONS", "").lower() in ("1", "true", "yes")
        ):
            return {"is_super_admin": True, "function_ids": []}
        return {"is_super_admin": False, "function_ids": function_ids}

    def _build_incident_function_filter(
        self,
        table_alias: str,
        access: dict,
        selected_function_id: Optional[str] = None,
    ) -> str:
        """
        Mirror Node buildDirectFunctionFilter for Incidents:
        - Uses function_id column directly (not a join table).
        - If selected_function_id: filter by that only (verify access for non-admins).
        - If no selected_function_id: super admin sees all, normal user sees only their functions.
        """
        if selected_function_id is not None:
            selected_function_id = selected_function_id.strip() or None

        if selected_function_id:
            if (not access.get("is_super_admin")) and (selected_function_id not in access.get("function_ids", [])):
                return " AND 1 = 0"
            # Use LTRIM(RTRIM()) to handle spaces in function_id column
            return f" AND LTRIM(RTRIM({table_alias}.function_id)) = '{selected_function_id}'"

        if access.get("is_super_admin"):
            return ""

        function_ids = access.get("function_ids") or []
        if not function_ids:
            return " AND 1 = 0"

        ids = ",".join(f"'{fid}'" for fid in function_ids)
        # Use LTRIM(RTRIM()) to handle spaces in function_id column
        return f" AND LTRIM(RTRIM({table_alias}.function_id)) IN ({ids})"

    def _build_incident_date_filter(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> str:
        """
        Build date filter for incidents to match Node.js buildDateFilter:
        - start: i.createdAt >= startDate
        - end: i.createdAt < endDate+1day (exclusive of next day) so full end date is included.
        """
        if not start_date and not end_date:
            return ""
        parts = []
        if start_date:
            parts.append(f"AND i.createdAt >= '{start_date}'")
        if end_date:
            try:
                s = end_date.replace("Z", "+00:00").strip()
                if "T" not in s:
                    s = s + "T00:00:00"
                dt = datetime.fromisoformat(s)
                next_day = dt + timedelta(days=1)
                end_exclusive = next_day.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            except Exception:
                end_exclusive = end_date
            parts.append(f"AND i.createdAt < '{end_exclusive}'")
        return (" " + " ".join(parts)) if parts else ""
    
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
            conn = get_db_connection()
            try:
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
            finally:
                conn.close()
        except Exception as e:
            return []

       # ===== Incidents fallbacks =====
  
    async def get_incidents_list(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return incidents list with all columns for Excel export. Uses only tables/columns used elsewhere in this service."""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        # Same query shape as Node (grc-incidents.service getTotalIncidents): RootCauses, Currencies on i.currency, Users for owner, i.status as recoveryStatus
        query = f"""
        SELECT 
            ISNULL(i.code, '') AS code,
            ISNULL(i.title, '') AS title,
            ISNULL(f.name, '') AS function_name,
            CASE 
                WHEN ISNULL(i.preparerStatus, '') <> 'sent' THEN 'Pending Preparer'
                WHEN ISNULL(i.preparerStatus, '') = 'sent' AND ISNULL(i.checkerStatus, '') <> 'approved' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Checker'
                WHEN ISNULL(i.checkerStatus, '') = 'approved' AND ISNULL(i.reviewerStatus, '') <> 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Reviewer'
                WHEN ISNULL(i.reviewerStatus, '') = 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Acceptance'
                WHEN ISNULL(i.acceptanceStatus, '') = 'approved' THEN 'Approved'
                ELSE 'Other'
            END AS status,
            ISNULL(c.name, '') AS categoryName,
            ISNULL(sc.name, '') AS subCategoryName,
            ISNULL(u.name, '') AS owner,
            ISNULL(i.importance, '') AS importance,
            ISNULL(i.timeFrame, '') AS timeFrame,
            ISNULL(CASE WHEN i.occurrence_date IS NOT NULL THEN FORMAT(i.occurrence_date, 'yyyy-MM-dd') END, '') AS occurrenceDate,
            ISNULL(CASE WHEN i.reported_date IS NOT NULL THEN FORMAT(i.reported_date, 'yyyy-MM-dd') END, '') AS reportedDate,
            ISNULL(CAST(i.description AS NVARCHAR(MAX)), '') AS description,
            ISNULL(i.rootCause, '') AS rootCause,
            ISNULL(rc.name, '') AS causeName,
            ISNULL(i.total_loss, 0) AS totalLoss,
            ISNULL(i.recovery_amount, 0) AS recoveryAmount,
            ISNULL(i.net_loss, 0) AS netLoss,
            ISNULL(fi.name, '') AS financialImpactName,
            ISNULL(cu.name, '') AS currencyName,
            ISNULL(i.exchange_rate, 0) AS exchangeRate,
            ISNULL(i.status, '') AS recoveryStatus,
            ISNULL(ie.name, '') AS eventType,
            ISNULL(i.preparerStatus, '') AS preparerStatus,
            ISNULL(i.reviewerStatus, '') AS reviewerStatus,
            ISNULL(i.checkerStatus, '') AS checkerStatus,
            ISNULL(i.acceptanceStatus, '') AS acceptanceStatus,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') AS createdAt
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id AND f.isDeleted = 0 AND f.deletedAt IS NULL
        LEFT JOIN Categories c ON i.category_id = c.id AND c.isDeleted = 0 AND c.deletedAt IS NULL
        LEFT JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id AND sc.isDeleted = 0 AND sc.deletedAt IS NULL
        LEFT JOIN FinancialImpacts fi ON i.financial_impact_id = fi.id AND fi.isDeleted = 0 AND fi.deletedAt IS NULL
        LEFT JOIN IncidentEvents ie ON i.event_type_id = ie.id AND ie.isDeleted = 0 AND ie.deletedAt IS NULL
        LEFT JOIN RootCauses rc ON i.cause_id = rc.id AND rc.isDeleted = 0 AND rc.deletedAt IS NULL
        LEFT JOIN Currencies cu ON i.currency = cu.id AND cu.isDeleted = 0 AND cu.deletedAt IS NULL
        LEFT JOIN Users u ON i.created_by = u.id AND u.deletedAt IS NULL
        WHERE i.isDeleted = 0 AND i.deletedAt IS NULL {date_filter}
        {function_filter}
        ORDER BY i.createdAt DESC
        """
        write_debug(f"[INCIDENTS LIST] query: {query}")
        rows = await self.execute_query(query)
        if rows:
            return rows
        # Full query failed (e.g. missing Causes/Currencies). Try same columns but without Causes/Currencies JOINs.
        write_debug("[INCIDENTS LIST] full query returned empty, trying without Causes/Currencies JOINs")
        extended_query = f"""
        SELECT 
            ISNULL(i.code, '') AS code,
            ISNULL(i.title, '') AS title,
            ISNULL(f.name, '') AS function_name,
            CASE WHEN ISNULL(i.preparerStatus, '') <> 'sent' THEN 'Pending Preparer'
                WHEN ISNULL(i.preparerStatus, '') = 'sent' AND ISNULL(i.checkerStatus, '') <> 'approved' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Checker'
                WHEN ISNULL(i.checkerStatus, '') = 'approved' AND ISNULL(i.reviewerStatus, '') <> 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Reviewer'
                WHEN ISNULL(i.reviewerStatus, '') = 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Acceptance'
                WHEN ISNULL(i.acceptanceStatus, '') = 'approved' THEN 'Approved' ELSE 'Other' END AS status,
            ISNULL(c.name, '') AS categoryName,
            ISNULL(sc.name, '') AS subCategoryName,
            ISNULL(CAST(i.owner AS NVARCHAR(255)), '') AS owner,
            ISNULL(CAST(i.importance AS NVARCHAR(255)), '') AS importance,
            ISNULL(i.timeFrame, '') AS timeFrame,
            ISNULL(CASE WHEN i.occurrence_date IS NOT NULL THEN FORMAT(i.occurrence_date, 'yyyy-MM-dd') END, '') AS occurrenceDate,
            ISNULL(CASE WHEN i.reported_date IS NOT NULL THEN FORMAT(i.reported_date, 'yyyy-MM-dd') END, '') AS reportedDate,
            ISNULL(CAST(i.description AS NVARCHAR(MAX)), '') AS description,
            ISNULL(i.rootCause, '') AS rootCause,
            '' AS causeName,
            ISNULL(i.total_loss, 0) AS totalLoss,
            ISNULL(i.recovery_amount, 0) AS recoveryAmount,
            ISNULL(i.net_loss, 0) AS netLoss,
            ISNULL(fi.name, '') AS financialImpactName,
            '' AS currencyName,
            ISNULL(i.exchange_rate, 0) AS exchangeRate,
            ISNULL(CAST(i.recovery_status AS NVARCHAR(255)), '') AS recoveryStatus,
            ISNULL(ie.name, '') AS eventType,
            ISNULL(i.preparerStatus, '') AS preparerStatus,
            ISNULL(i.reviewerStatus, '') AS reviewerStatus,
            ISNULL(i.checkerStatus, '') AS checkerStatus,
            ISNULL(i.acceptanceStatus, '') AS acceptanceStatus,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') AS createdAt
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id AND f.isDeleted = 0 AND f.deletedAt IS NULL
        LEFT JOIN Categories c ON i.category_id = c.id AND c.isDeleted = 0
        LEFT JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id AND sc.deletedAt IS NULL
        LEFT JOIN FinancialImpacts fi ON i.financial_impact_id = fi.id AND fi.isDeleted = 0 AND fi.deletedAt IS NULL
        LEFT JOIN IncidentEvents ie ON i.event_type_id = ie.id AND ie.isDeleted = 0 AND ie.deletedAt IS NULL
        WHERE i.isDeleted = 0 AND i.deletedAt IS NULL {date_filter}
        {function_filter}
        ORDER BY i.createdAt DESC
        """
        rows = await self.execute_query(extended_query)
        if rows:
            return rows
        # Extended failed (e.g. owner/importance columns missing). Try without optional Incidents columns.
        write_debug("[INCIDENTS LIST] extended query returned empty, trying medium (no owner/importance from DB)")
        medium_query = f"""
        SELECT 
            ISNULL(i.code, '') AS code,
            ISNULL(i.title, '') AS title,
            ISNULL(f.name, '') AS function_name,
            CASE WHEN ISNULL(i.preparerStatus, '') <> 'sent' THEN 'Pending Preparer'
                WHEN ISNULL(i.preparerStatus, '') = 'sent' AND ISNULL(i.checkerStatus, '') <> 'approved' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Checker'
                WHEN ISNULL(i.checkerStatus, '') = 'approved' AND ISNULL(i.reviewerStatus, '') <> 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Reviewer'
                WHEN ISNULL(i.reviewerStatus, '') = 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Acceptance'
                WHEN ISNULL(i.acceptanceStatus, '') = 'approved' THEN 'Approved' ELSE 'Other' END AS status,
            ISNULL(c.name, '') AS categoryName,
            ISNULL(sc.name, '') AS subCategoryName,
            '' AS owner,
            '' AS importance,
            ISNULL(i.timeFrame, '') AS timeFrame,
            ISNULL(CASE WHEN i.occurrence_date IS NOT NULL THEN FORMAT(i.occurrence_date, 'yyyy-MM-dd') END, '') AS occurrenceDate,
            ISNULL(CASE WHEN i.reported_date IS NOT NULL THEN FORMAT(i.reported_date, 'yyyy-MM-dd') END, '') AS reportedDate,
            ISNULL(i.description, '') AS description,
            ISNULL(i.rootCause, '') AS rootCause,
            '' AS causeName,
            ISNULL(i.total_loss, 0) AS totalLoss,
            ISNULL(i.recovery_amount, 0) AS recoveryAmount,
            ISNULL(i.net_loss, 0) AS netLoss,
            ISNULL(fi.name, '') AS financialImpactName,
            '' AS currencyName,
            0 AS exchangeRate,
            '' AS recoveryStatus,
            ISNULL(ie.name, '') AS eventType,
            ISNULL(i.preparerStatus, '') AS preparerStatus,
            ISNULL(i.reviewerStatus, '') AS reviewerStatus,
            ISNULL(i.checkerStatus, '') AS checkerStatus,
            ISNULL(i.acceptanceStatus, '') AS acceptanceStatus,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') AS createdAt
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id AND f.isDeleted = 0 AND f.deletedAt IS NULL
        LEFT JOIN Categories c ON i.category_id = c.id AND c.isDeleted = 0
        LEFT JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id AND sc.deletedAt IS NULL
        LEFT JOIN FinancialImpacts fi ON i.financial_impact_id = fi.id AND fi.isDeleted = 0 AND fi.deletedAt IS NULL
        LEFT JOIN IncidentEvents ie ON i.event_type_id = ie.id AND ie.isDeleted = 0 AND ie.deletedAt IS NULL
        WHERE i.isDeleted = 0 AND i.deletedAt IS NULL {date_filter}
        {function_filter}
        ORDER BY i.createdAt DESC
        """
        rows = await self.execute_query(medium_query)
        if rows:
            return rows
        write_debug("[INCIDENTS LIST] medium query returned empty, trying basic query")
        fallback = f"""
        SELECT 
            i.code,
            i.title,
            ISNULL(f.name, '') AS function_name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') AS createdAt
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id AND f.isDeleted = 0 AND f.deletedAt IS NULL
        WHERE i.isDeleted = 0 AND i.deletedAt IS NULL {date_filter}
        {function_filter}
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(fallback)

    async def get_incidents_status_overview(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return incidents status overview list with computed status (matches Node.js statusOverview)"""
        date_filter = self._build_incident_date_filter(start_date, end_date)

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
          i.code,
          i.title,
          f.name AS function_name,
          CASE 
            WHEN ISNULL(i.preparerStatus, '') <> 'sent' THEN 'Pending Preparer'
            WHEN ISNULL(i.preparerStatus, '') = 'sent' AND ISNULL(i.checkerStatus, '') <> 'approved' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Checker'
            WHEN ISNULL(i.checkerStatus, '') = 'approved' AND ISNULL(i.reviewerStatus, '') <> 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Reviewer'
            WHEN ISNULL(i.reviewerStatus, '') = 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Acceptance'
            WHEN ISNULL(i.acceptanceStatus, '') = 'approved' THEN 'Approved'
            ELSE 'Other'
          END as status,
          FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id
          AND f.isDeleted = 0
          AND f.deletedAt IS NULL
        WHERE i.isDeleted = 0 
          AND i.deletedAt IS NULL
          {date_filter}
          {function_filter}
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_incidents_by_status(
        self,
        status: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return incidents rows filtered by computed status label (not counts)."""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        # Build query that computes the label and filters to requested status
        query = f"""
        WITH IncidentStatus AS (
            SELECT 
                i.code,
                i.title,
                ISNULL(f.name, 'Unknown') AS function_name,
                CASE 
                    -- 1) Pending preparer: preparerStatus is anything other than 'sent'
                    WHEN ISNULL(i.preparerStatus, '') <> 'sent' THEN 'pendingPreparer'
                    -- 2) Pending checker: preparer sent AND checker not approved AND acceptance not approved
                    WHEN ISNULL(i.preparerStatus, '') = 'sent' AND ISNULL(i.checkerStatus, '') <> 'approved' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'pendingChecker'
                    -- 3) Pending reviewer: checker approved AND reviewer not approved AND acceptance not approved
                    WHEN ISNULL(i.checkerStatus, '') = 'approved' AND ISNULL(i.reviewerStatus, '') <> 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'pendingReviewer'
                    -- 4) Pending acceptance: reviewer approved AND acceptance not approved
                    WHEN ISNULL(i.reviewerStatus, '') = 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'pendingAcceptance'
                    -- 5) Fully approved
                    WHEN ISNULL(i.acceptanceStatus, '') = 'approved' THEN 'Approved'
                    ELSE 'Other'
                END AS status,
                FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt
            FROM Incidents i
            LEFT JOIN Functions f ON i.function_id = f.id AND f.isDeleted = 0 AND f.deletedAt IS NULL
            WHERE i.isDeleted = 0 AND i.deletedAt IS NULL {date_filter}
            {function_filter}
        )
        SELECT *
        FROM IncidentStatus
        WHERE status = '{status}'
        ORDER BY createdAt DESC;
        """

        write_debug(f"[INCIDENTS BY STATUS] query: {query}")
        return await self.execute_query(query)

   
    async def get_incidents_by_category(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
       
        write_debug(f"[INCIDENTS BY CATEGORY] fetching incidents by category for {start_date} to {end_date}")
        """Return incidents count by category (excludes NULL and deleted categories)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            ISNULL(c.name, 'Unknown') as category_name,
            COUNT(i.id) as count
        FROM Incidents i
        LEFT JOIN Categories c ON i.category_id = c.id
            AND c.isDeleted = 0
            AND c.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
        GROUP BY ISNULL(c.name, 'Unknown')
        ORDER BY COUNT(i.id) DESC
        """
        write_debug(f"[INCIDENTS BY CATEGORY] query: {query}")
        return await self.execute_query(query)

    async def get_incidents_by_status_distribution(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return incidents count by status (distribution for charts)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)
        
        query = f"""
        WITH IncidentStatus AS (
            SELECT 
                i.id,
                CASE 
                    WHEN ISNULL(i.preparerStatus, '') <> 'sent' THEN 'Pending Preparer'
                    WHEN ISNULL(i.preparerStatus, '') = 'sent' AND ISNULL(i.checkerStatus, '') <> 'approved' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Checker'
                    WHEN ISNULL(i.checkerStatus, '') = 'approved' AND ISNULL(i.reviewerStatus, '') <> 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Reviewer'
                    WHEN ISNULL(i.reviewerStatus, '') = 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Acceptance'
                    WHEN ISNULL(i.acceptanceStatus, '') = 'approved' THEN 'Approved'
                    ELSE 'Other'
                END AS status
            FROM Incidents i
            WHERE i.isDeleted = 0 AND i.deletedAt IS NULL {date_filter}
            {function_filter}
        ),
        StatusCounts AS (
            SELECT 
                status as status_name,
                COUNT(*) as count
            FROM IncidentStatus
            GROUP BY status
        ),
        AllStatuses AS (
            SELECT 'Pending Preparer' AS status_name
            UNION ALL SELECT 'Pending Checker'
            UNION ALL SELECT 'Pending Reviewer'
            UNION ALL SELECT 'Pending Acceptance'
            UNION ALL SELECT 'Approved'
            UNION ALL SELECT 'Other'
        )
        SELECT 
            a.status_name,
            ISNULL(s.count, 0) as count
        FROM AllStatuses a
        LEFT JOIN StatusCounts s ON a.status_name = s.status_name
        ORDER BY s.count DESC, a.status_name
        """
        return await self.execute_query(query)
 
    async def get_incidents_monthly_trend(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return incidents monthly trend counts grouped by occurrence_date"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            FORMAT(i.createdAt, 'MMM yyyy') as month_year,
            COUNT(i.id) as incident_count
        FROM Incidents i
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND i.createdAt IS NOT NULL
        GROUP BY FORMAT(i.createdAt, 'MMM yyyy')
        ORDER BY MIN(i.createdAt)
        """
        return await self.execute_query(query)
    
    async def get_incidents_time_series(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return incidents time series by month (matches grc-incidents.service.ts)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        WITH month_series AS (
          SELECT  
            DATEFROMPARTS(YEAR(MIN(i.createdAt)), MONTH(MIN(i.createdAt)), 1) AS start_month, 
            DATEFROMPARTS(YEAR(MAX(i.createdAt)), MONTH(MAX(i.createdAt)), 1) AS end_month 
          FROM Incidents i
          WHERE i.isDeleted = 0 AND i.deletedAt IS NULL {date_filter}
          {function_filter}
        ), 
        months AS ( 
          SELECT start_month AS month_date 
          FROM month_series 
          UNION ALL 
          SELECT DATEADD(MONTH, 1, month_date) 
          FROM months, month_series 
          WHERE DATEADD(MONTH, 1, month_date) <= (SELECT end_month FROM month_series) 
        ) 
        SELECT 
          m.month_date AS month, 
          COUNT(i.id) AS total_incidents 
        FROM months AS m 
        LEFT JOIN Incidents AS i 
          ON YEAR(i.createdAt) = YEAR(m.month_date) 
          AND MONTH(i.createdAt) = MONTH(m.month_date)
          AND i.isDeleted = 0
          AND i.deletedAt IS NULL
          {function_filter}
        GROUP BY  
          m.month_date 
        ORDER BY  
          m.month_date 
        OPTION (MAXRECURSION 0)
        """
        return await self.execute_query(query)

   
   
   
   
   
    async def get_incidents_top_financial_impacts(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return top financial impacts grouped by category with total net loss"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)
        
        query = f"""
        SELECT 
            ISNULL(fi.name, 'Unknown') as financial_impact_name,
            ISNULL(SUM(i.net_loss), 0) as net_loss
        FROM Incidents i
        LEFT JOIN FinancialImpacts fi ON i.financial_impact_id = fi.id
            AND fi.isDeleted = 0
            AND fi.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND i.net_loss IS NOT NULL
            AND i.net_loss > 0
        GROUP BY fi.name
        ORDER BY net_loss DESC
        """
        return await self.execute_query(query)
    
    async def get_incidents_net_loss_recovery(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return net loss and recovery data for incidents"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)
        
        query = f"""
        SELECT 
            i.title as incident_title,
            f.name AS function_name,
            ISNULL(i.net_loss, 0) as net_loss,
            ISNULL(i.recovery_amount, 0) as recovery_amount
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id
          AND f.isDeleted = 0
          AND f.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND i.net_loss > 0
        ORDER BY i.net_loss DESC
        """
        return await self.execute_query(query)

   
    async def get_incidents_by_event_type(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return incidents count by event type (excludes NULL and deleted event types)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
          ISNULL(ie.name, 'Unknown') AS event_type, 
          COUNT(i.id) AS incident_count 
        FROM Incidents i 
        LEFT JOIN IncidentEvents ie ON i.event_type_id = ie.id
          AND ie.isDeleted = 0
          AND ie.deletedAt IS NULL
        WHERE i.isDeleted = 0 
          AND i.deletedAt IS NULL
          {date_filter}
          {function_filter}
        GROUP BY ISNULL(ie.name, 'Unknown')
        ORDER BY COUNT(i.id) DESC
        """
        return await self.execute_query(query)

    async def get_incidents_by_financial_impact(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return incidents count by financial impact (excludes NULL and deleted financial impacts)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
          ISNULL(fi.name, 'Unknown') AS financial_impact_name, 
          COUNT(i.id) AS incident_count 
        FROM Incidents i
        LEFT JOIN FinancialImpacts fi ON i.financial_impact_id = fi.id
          AND fi.isDeleted = 0
          AND fi.deletedAt IS NULL
        WHERE i.isDeleted = 0 
          AND i.deletedAt IS NULL
          {date_filter}
          {function_filter}
        GROUP BY ISNULL(fi.name, 'Unknown')
        ORDER BY COUNT(i.id) DESC
        """
        return await self.execute_query(query)

    
    async def get_new_incidents_by_month(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return new incidents count per month (matches grc-incidents.service.ts)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
          CAST(
            DATEFROMPARTS(
              YEAR(i.createdAt), 
              MONTH(i.createdAt), 
              1 
            ) AS datetime2
          ) AS month, 
          COUNT(*) AS new_incidents 
        FROM Incidents i
        WHERE i.isDeleted = 0 {date_filter}
          AND i.deletedAt IS NULL
          {function_filter}
        GROUP BY 
          CAST(
            DATEFROMPARTS(
              YEAR(i.createdAt), 
              MONTH(i.createdAt), 
              1 
            ) AS datetime2
          )
        ORDER BY month ASC
        """
        return await self.execute_query(query)

    async def get_incidents_with_timeframe(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return incidents with timeframe values (matches grc-incidents.service.ts)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
          i.title AS incident_name, 
          i.timeFrame AS time_frame,
          f.name AS function_name
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id
          AND f.isDeleted = 0
          AND f.deletedAt IS NULL
        WHERE i.isDeleted = 0 
          AND i.deletedAt IS NULL
          {date_filter}
          {function_filter}
        ORDER BY i.timeFrame DESC
        """
        rows = await self.execute_query(query)
        return [
            {
                "incident_name": r.get("incident_name") or "Unknown",
                "time_frame": r.get("time_frame") or "",
                "function_name": r.get("function_name") or "Unknown",
            }
            for r in rows
        ]

    async def get_incidents_financial_details(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return incidents financial details with net loss, recovery, and gross amount (matches Node.js)"""
        date_filter = self._build_incident_date_filter(start_date, end_date)

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
          i.title AS incident_name, 
          i.rootCause AS rootCause, 
          f.name AS function_name, 
          i.net_loss AS netLoss, 
          i.total_loss AS totalLoss, 
          i.recovery_amount AS recoveryAmount, 
          (ISNULL(i.total_loss, 0) + ISNULL(i.recovery_amount, 0)) AS grossAmount, 
          CASE 
            WHEN ISNULL(i.preparerStatus, '') <> 'sent' THEN 'Pending Preparer'
            WHEN ISNULL(i.preparerStatus, '') = 'sent' AND ISNULL(i.checkerStatus, '') <> 'approved' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Checker'
            WHEN ISNULL(i.checkerStatus, '') = 'approved' AND ISNULL(i.reviewerStatus, '') <> 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Reviewer'
            WHEN ISNULL(i.reviewerStatus, '') = 'sent' AND ISNULL(i.acceptanceStatus, '') <> 'approved' THEN 'Pending Acceptance'
            WHEN ISNULL(i.acceptanceStatus, '') = 'approved' THEN 'Approved'
            ELSE 'Other'
          END AS status 
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id
          AND f.isDeleted = 0
          AND f.deletedAt IS NULL
        WHERE i.isDeleted = 0 
          AND i.deletedAt IS NULL
          {date_filter}
          {function_filter}
        """
        return await self.execute_query(query)

    # ===== Operational Loss Metrics =====
    
    async def get_atm_theft_incidents(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return ATM theft incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount,
            ISNULL(f.name, 'Unknown') AS function_name
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id AND f.isDeleted = 0 AND f.deletedAt IS NULL
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND sc.name = N'ATM issue'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_internal_fraud_incidents(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return internal fraud incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount,
            ISNULL(f.name, 'Unknown') AS function_name
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id AND f.isDeleted = 0 AND f.deletedAt IS NULL
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND ie.name = N'Internal Fraud'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_external_fraud_incidents(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return external fraud incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount,
            ISNULL(f.name, 'Unknown') AS function_name
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id AND f.isDeleted = 0 AND f.deletedAt IS NULL
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND ie.name = N'External Fraud'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_physical_asset_damage_incidents(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return physical asset damage incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount,
            ISNULL(f.name, 'Unknown') AS function_name
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id AND f.isDeleted = 0 AND f.deletedAt IS NULL
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND ie.name = N'Damage to Physical Assets'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_people_error_incidents(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return people error incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount,
            ISNULL(f.name, 'Unknown') AS function_name
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id AND f.isDeleted = 0 AND f.deletedAt IS NULL
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND sc.name = N'Human Mistake'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_incidents_with_recognition_time(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return incidents with recognition time calculation (last 12 months)"""
        date_filter = "AND i.occurrence_date >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND i.occurrence_date BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.occurrence_date >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.occurrence_date <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(i.occurrence_date, 'yyyy-MM-dd') as occurrence_date,
            FORMAT(i.reported_date, 'yyyy-MM-dd') as reported_date,
            DATEDIFF(DAY, i.occurrence_date, i.reported_date) AS recognition_days,
            CAST(DATEDIFF(DAY, i.occurrence_date, i.reported_date) AS FLOAT) / 30.44 AS recognition_months
        FROM Incidents i
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND i.occurrence_date IS NOT NULL
            AND i.reported_date IS NOT NULL
            AND i.reported_date >= i.occurrence_date
        ORDER BY recognition_months DESC
        """
        return await self.execute_query(query)

    async def get_operational_loss_value_monthly(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return operational loss value by month (last 12 months)"""
        date_filter = "AND i.occurrence_date >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND i.occurrence_date BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.occurrence_date >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.occurrence_date <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            YEAR(i.occurrence_date) AS year,
            MONTH(i.occurrence_date) AS month,
            CAST(SUM(i.net_loss) AS DECIMAL(18,2)) AS totalLossValue,
            COUNT(*) AS incidentCount
        FROM Incidents i
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND i.net_loss IS NOT NULL
        GROUP BY YEAR(i.occurrence_date), MONTH(i.occurrence_date)
        ORDER BY year, month
        """
        return await self.execute_query(query)

    async def get_monthly_trend_by_incident_type(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return monthly trend analysis by incident type (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            FORMAT(COALESCE(i.occurrence_date, i.createdAt), 'yyyy-MM') AS Period,
            SUM(CASE WHEN ie.name = N'Internal Fraud' THEN 1 ELSE 0 END) AS InternalFrauds,
            SUM(CASE WHEN ie.name = N'External Fraud' THEN 1 ELSE 0 END) AS ExternalFrauds,
            SUM(CASE WHEN ie.name = N'Damage to Physical Assets' THEN 1 ELSE 0 END) AS PhysicalAssetDamages,
            SUM(CASE WHEN sc.name = N'Human Mistake' THEN 1 ELSE 0 END) AS HumanErrors,
            SUM(CASE WHEN sc.name = N'ATM issue' THEN 1 ELSE 0 END) AS ATMIssues,
            SUM(CASE WHEN sc.name IN (N'System Error', N'Prime System Issue', N'Transaction system error (TRX BUG)') THEN 1 ELSE 0 END) AS SystemErrors
        FROM Incidents i
        LEFT JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        LEFT JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
        GROUP BY FORMAT(COALESCE(i.occurrence_date, i.createdAt), 'yyyy-MM')
        ORDER BY Period
        """
        result = await self.execute_query(query)
        # Transform to match Node.js format (lowercase/camelCase)
        return [
            {
                'period': row.get('Period', ''),
                'internalFrauds': row.get('InternalFrauds', 0) or 0,
                'externalFrauds': row.get('ExternalFrauds', 0) or 0,
                'physicalAssetDamages': row.get('PhysicalAssetDamages', 0) or 0,
                'humanErrors': row.get('HumanErrors', 0) or 0,
                'atmIssues': row.get('ATMIssues', 0) or 0,
                'systemErrors': row.get('SystemErrors', 0) or 0
            }
            for row in result
        ]

    async def get_loss_by_risk_category(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return loss analysis by risk category (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            c.name AS riskCategory,
            COUNT(*) AS incidentCount,
            CAST(SUM(ISNULL(i.net_loss, 0)) AS DECIMAL(18,2)) AS totalLoss,
            CAST(AVG(NULLIF(i.net_loss, 0)) AS DECIMAL(18,2)) AS averageLoss
        FROM Incidents i
        INNER JOIN Categories c ON i.category_id = c.id
            AND c.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND i.net_loss IS NOT NULL
        GROUP BY c.name
        ORDER BY totalLoss DESC
        """
        return await self.execute_query(query)

    async def get_comprehensive_operational_loss(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return comprehensive operational loss dashboard metrics (last 12 months)"""
        date_filter = "COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            'Total Operational Loss Incidents' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            {function_filter}

        UNION ALL

        SELECT 
            'ATM Issues' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            {function_filter}
            AND sc.name = N'ATM issue'

        UNION ALL

        SELECT 
            'Internal Fraud' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            {function_filter}
            AND ie.name = N'Internal Fraud'

        UNION ALL

        SELECT 
            'External Fraud' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            {function_filter}
            AND ie.name = N'External Fraud'

        UNION ALL

        SELECT 
            'Human Mistakes' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            {function_filter}
            AND sc.name = N'Human Mistake'

        UNION ALL

        SELECT 
            'System Errors' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            {function_filter}
            AND sc.name IN (N'System Error', N'Prime System Issue', N'Transaction system error (TRX BUG)')
        """
        return await self.execute_query(query)

    async def get_incidents_with_financial_and_function(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return incidents with financial impact and function details (matches grc-incidents.service.ts)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND i.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
          i.title AS title, 
          ISNULL(fi.name, 'Unknown') AS financial_impact_name, 
          ISNULL(f.name, 'Unknown') AS function_name 
        FROM Incidents i
        LEFT JOIN FinancialImpacts fi ON i.financial_impact_id = fi.id
          AND fi.isDeleted = 0
          AND fi.deletedAt IS NULL
        LEFT JOIN Functions f ON i.function_id = f.id
          AND f.isDeleted = 0
          AND f.deletedAt IS NULL
        WHERE i.isDeleted = 0 
          AND i.deletedAt IS NULL
          {date_filter}
          {function_filter}
        """
        return await self.execute_query(query)

    # ===== Operational Loss Metrics =====
    
    async def get_atm_theft_incidents(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return ATM theft incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount,
            ISNULL(f.name, 'Unknown') AS function_name
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id AND f.isDeleted = 0 AND f.deletedAt IS NULL
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND sc.name = N'ATM issue'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_internal_fraud_incidents(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return internal fraud incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount,
            ISNULL(f.name, 'Unknown') AS function_name
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id AND f.isDeleted = 0 AND f.deletedAt IS NULL
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND ie.name = N'Internal Fraud'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_external_fraud_incidents(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return external fraud incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount,
            ISNULL(f.name, 'Unknown') AS function_name
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id AND f.isDeleted = 0 AND f.deletedAt IS NULL
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND ie.name = N'External Fraud'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_physical_asset_damage_incidents(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return physical asset damage incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount,
            ISNULL(f.name, 'Unknown') AS function_name
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id AND f.isDeleted = 0 AND f.deletedAt IS NULL
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND ie.name = N'Damage to Physical Assets'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_people_error_incidents(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return people error incidents (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(CONVERT(datetime, i.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt,
            i.net_loss,
            i.recovery_amount,
            ISNULL(f.name, 'Unknown') AS function_name
        FROM Incidents i
        LEFT JOIN Functions f ON i.function_id = f.id AND f.isDeleted = 0 AND f.deletedAt IS NULL
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND sc.name = N'Human Mistake'
        ORDER BY i.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_incidents_with_recognition_time(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return incidents with recognition time calculation (last 12 months)"""
        date_filter = "AND i.occurrence_date >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND i.occurrence_date BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.occurrence_date >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.occurrence_date <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            i.code,
            i.title AS name,
            FORMAT(i.occurrence_date, 'yyyy-MM-dd') as occurrence_date,
            FORMAT(i.reported_date, 'yyyy-MM-dd') as reported_date,
            DATEDIFF(DAY, i.occurrence_date, i.reported_date) AS recognition_days,
            CAST(DATEDIFF(DAY, i.occurrence_date, i.reported_date) AS FLOAT) / 30.44 AS recognition_months
        FROM Incidents i
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND i.occurrence_date IS NOT NULL
            AND i.reported_date IS NOT NULL
            AND i.reported_date >= i.occurrence_date
        ORDER BY recognition_months DESC
        """
        return await self.execute_query(query)

    async def get_operational_loss_value_monthly(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return operational loss value by month (last 12 months)"""
        date_filter = "AND i.occurrence_date >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND i.occurrence_date BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND i.occurrence_date >= '{start_date}'"
        elif end_date:
            date_filter = f"AND i.occurrence_date <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            YEAR(i.occurrence_date) AS year,
            MONTH(i.occurrence_date) AS month,
            CAST(SUM(i.net_loss) AS DECIMAL(18,2)) AS totalLossValue,
            COUNT(*) AS incidentCount
        FROM Incidents i
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND i.net_loss IS NOT NULL
        GROUP BY YEAR(i.occurrence_date), MONTH(i.occurrence_date)
        ORDER BY year, month
        """
        return await self.execute_query(query)

    async def get_monthly_trend_by_incident_type(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return monthly trend analysis by incident type (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            FORMAT(COALESCE(i.occurrence_date, i.createdAt), 'yyyy-MM') AS Period,
            SUM(CASE WHEN ie.name = N'Internal Fraud' THEN 1 ELSE 0 END) AS InternalFrauds,
            SUM(CASE WHEN ie.name = N'External Fraud' THEN 1 ELSE 0 END) AS ExternalFrauds,
            SUM(CASE WHEN ie.name = N'Damage to Physical Assets' THEN 1 ELSE 0 END) AS PhysicalAssetDamages,
            SUM(CASE WHEN sc.name = N'Human Mistake' THEN 1 ELSE 0 END) AS HumanErrors,
            SUM(CASE WHEN sc.name = N'ATM issue' THEN 1 ELSE 0 END) AS ATMIssues,
            SUM(CASE WHEN sc.name IN (N'System Error', N'Prime System Issue', N'Transaction system error (TRX BUG)') THEN 1 ELSE 0 END) AS SystemErrors
        FROM Incidents i
        LEFT JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        LEFT JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
        GROUP BY FORMAT(COALESCE(i.occurrence_date, i.createdAt), 'yyyy-MM')
        ORDER BY Period
        """
        result = await self.execute_query(query)
        # Transform to match Node.js format (lowercase/camelCase)
        return [
            {
                'period': row.get('Period', ''),
                'internalFrauds': row.get('InternalFrauds', 0) or 0,
                'externalFrauds': row.get('ExternalFrauds', 0) or 0,
                'physicalAssetDamages': row.get('PhysicalAssetDamages', 0) or 0,
                'humanErrors': row.get('HumanErrors', 0) or 0,
                'atmIssues': row.get('ATMIssues', 0) or 0,
                'systemErrors': row.get('SystemErrors', 0) or 0
            }
            for row in result
        ]

    async def get_loss_by_risk_category(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return loss analysis by risk category (last 12 months)"""
        date_filter = "AND COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"AND COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            c.name AS riskCategory,
            COUNT(*) AS incidentCount,
            CAST(SUM(ISNULL(i.net_loss, 0)) AS DECIMAL(18,2)) AS totalLoss,
            CAST(AVG(NULLIF(i.net_loss, 0)) AS DECIMAL(18,2)) AS averageLoss
        FROM Incidents i
        INNER JOIN Categories c ON i.category_id = c.id
            AND c.deletedAt IS NULL
        WHERE i.isDeleted = 0 
            AND i.deletedAt IS NULL
            {date_filter}
            {function_filter}
            AND i.net_loss IS NOT NULL
        GROUP BY c.name
        ORDER BY totalLoss DESC
        """
        return await self.execute_query(query)

    async def get_comprehensive_operational_loss(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return comprehensive operational loss dashboard metrics (last 12 months)"""
        date_filter = "COALESCE(i.occurrence_date, i.createdAt) >= DATEADD(MONTH, -12, GETDATE())"
        if start_date and end_date:
            date_filter = f"COALESCE(i.occurrence_date, i.createdAt) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"COALESCE(i.occurrence_date, i.createdAt) >= '{start_date}'"
        elif end_date:
            date_filter = f"COALESCE(i.occurrence_date, i.createdAt) <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_incident_function_filter("i", access, function_id)

        query = f"""
        SELECT 
            'Total Operational Loss Incidents' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            {function_filter}

        UNION ALL

        SELECT 
            'ATM Issues' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            {function_filter}
            AND sc.name = N'ATM issue'

        UNION ALL

        SELECT 
            'Internal Fraud' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            {function_filter}
            AND ie.name = N'Internal Fraud'

        UNION ALL

        SELECT 
            'External Fraud' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentEvents ie ON i.event_type_id = ie.id
            AND ie.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            {function_filter}
            AND ie.name = N'External Fraud'

        UNION ALL

        SELECT 
            'Human Mistakes' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            {function_filter}
            AND sc.name = N'Human Mistake'

        UNION ALL

        SELECT 
            'System Errors' as metric,
            COUNT(*) as count,
            CAST(SUM(COALESCE(i.net_loss, 0)) AS DECIMAL(18,2)) as totalValue
        FROM Incidents i
        INNER JOIN IncidentSubCategories sc ON i.sub_category_id = sc.id
            AND sc.deletedAt IS NULL
        WHERE {date_filter}
            AND i.isDeleted = 0
            AND i.deletedAt IS NULL
            {function_filter}
            AND sc.name IN (N'System Error', N'Prime System Issue', N'Transaction system error (TRX BUG)')
        """
        return await self.execute_query(query)