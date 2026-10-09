"""
KRI service for KRI operations
"""
import asyncio
import os
from typing import List, Dict, Any, Optional
from config import get_db_connection
from utils.pymssql_params import normalize_params
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

class KriService:
    """Service for kri operations"""

    def __init__(self):
        pass  # connection via get_db_connection() when needed
    
    def get_fully_qualified_table_name(self, table_name: str) -> str:
        """Get fully qualified table name using configuration"""
        from config import DATABASE_CONFIG
        database_name = DATABASE_CONFIG.get('database', 'NEWDCC-V4-UAT')
        return f"[{database_name}].dbo.[{table_name}]"
    
    async def _get_user_function_access(self, user_id: Optional[str], group_name: Optional[str]):
        """
        Mirror Node UserFunctionAccessService.getUserFunctionAccess for KRIs.
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

    def _sql_escape_function_id(self, fid: str) -> str:
        return str(fid).replace("'", "''")

    def _selected_function_ids(self, function_id: Optional[str], function_ids_csv: Optional[str]) -> Optional[List[str]]:
        from utils.node_grc_query import grc_parse_selected_function_ids_list

        return grc_parse_selected_function_ids_list(function_id, function_ids_csv)

    def _build_submission_filter(
        self,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> str:
        """Mirror Node buildKriValueSubmissionFilter: filter KriValues by the SUBMISSION period
        -- the reporting month/year the value is FOR (kv.[year]/kv.[month]), NOT when the row was
        entered (kv.createdAt). A value FOR March entered in April must match a "March" window.
        Takes the month & year from the from/to dates and compares against the value's reporting
        month & year as a single YYYYMM integer, so the day is ignored and it works no matter how
        year/month are stored (strings, spaces, no leading zero). The referenced table MUST be
        aliased `kv`. Returns '' when neither bound is set."""
        if not submission_start_date and not submission_end_date:
            return ""
        from datetime import datetime
        ym = "(TRY_CONVERT(int, kv.[year]) * 100 + TRY_CONVERT(int, kv.[month]))"
        parts = ""
        if submission_start_date:
            try:
                s = datetime.fromisoformat(str(submission_start_date)[:10])
                parts += f" AND {ym} >= {s.year * 100 + s.month}"
            except Exception:
                pass
        if submission_end_date:
            try:
                e = datetime.fromisoformat(str(submission_end_date)[:10])
                parts += f" AND {ym} <= {e.year * 100 + e.month}"
            except Exception:
                pass
        return parts

    def _build_month_cell_expr(self, month_num: int) -> str:
        """SQL expression for one month column of "KRIs Submission Status by Function". Mirrors
        Node's buildMonthCellExpr: a Quarterly KRI can only ever have a value in Mar/Jun/Sep/Dec,
        an Annually KRI only in Dec -- any other frequency reports every month. Returns, per row:
        'grey' (not a valid reporting slot for this KRI's frequency -- any value entered there
        anyway is bad data and is ignored, never selected by the MAX(CASE...) below), 'pending'
        (a valid slot with no value yet), or the value cast to text. Assumes the query groups by
        (at least) k.id so MAX(k.frequency) is just that KRI's frequency."""
        is_quarterly_due_month = month_num in (3, 6, 9, 12)
        is_annually_due_month = month_num == 12
        raw_value = f"MAX(CASE WHEN TRY_CONVERT(int, kv.[month]) = {month_num} THEN kv.value END)"
        pending_or_value = f"CASE WHEN {raw_value} IS NULL THEN 'pending' ELSE CAST({raw_value} AS NVARCHAR(50)) END"
        quarterly_branch = pending_or_value if is_quarterly_due_month else "'grey'"
        annually_branch = pending_or_value if is_annually_due_month else "'grey'"
        return f"""
          CASE
            WHEN LOWER(ISNULL(MAX(k.frequency), '')) = 'quarterly' THEN {quarterly_branch}
            WHEN LOWER(ISNULL(MAX(k.frequency), '')) = 'annually' THEN {annually_branch}
            ELSE {pending_or_value}
          END
        """

    def _build_kri_function_filter(
        self,
        table_alias: str,
        access: dict,
        selected_function_ids: Optional[List[str]] = None,
    ) -> str:
        """
        Mirror Node buildKriFunctionFilter (including multi-select IN (...)):
        - Uses related_function_id column directly (not KriFunctions join).
        """
        sel = [str(x).strip() for x in (selected_function_ids or []) if x and str(x).strip()]
        sel = list(dict.fromkeys(sel))

        if sel:
            if not access.get("is_super_admin"):
                allowed = set(access.get("function_ids") or [])
                if not all(s in allowed for s in sel):
                    return " AND 1 = 0"
            in_sql = ",".join(f"'{self._sql_escape_function_id(fid)}'" for fid in sel)
            return f" AND LTRIM(RTRIM({table_alias}.related_function_id)) IN ({in_sql})"

        if access.get("is_super_admin"):
            return ""

        function_ids = access.get("function_ids") or []
        if not function_ids:
            return " AND 1 = 0"

        ids = ",".join(f"'{self._sql_escape_function_id(fid)}'" for fid in function_ids)
        return f" AND LTRIM(RTRIM({table_alias}.related_function_id)) IN ({ids})"
    
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
                    cursor.execute(query, normalize_params(params))
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
    
   
 
    # KRI Database Methods
    async def get_kris_by_status(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return KRIs grouped by status"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))
        
        query = f"""
        SELECT 
            CASE 
                WHEN ISNULL(k.preparerStatus, '') <> 'sent' THEN 'Pending Preparer'
                WHEN ISNULL(k.preparerStatus, '') = 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' AND ISNULL(k.checkerStatus, '') <> 'approved' THEN 'Pending Checker'
                WHEN ISNULL(k.checkerStatus, '') = 'approved' AND ISNULL(k.acceptanceStatus, '') <> 'approved' AND ISNULL(k.reviewerStatus, '') <> 'sent' THEN 'Pending Reviewer'
                WHEN ISNULL(k.reviewerStatus, '') = 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'Pending Acceptance'
                WHEN ISNULL(k.acceptanceStatus, '') = 'approved' THEN 'Approved'
                ELSE 'Other'
            END as status,
            COUNT(*) as count
        FROM Kris k
        WHERE k.isDeleted = 0 
          AND k.deletedAt IS NULL {date_filter}
          {function_filter}
        GROUP BY 
            CASE 
                WHEN ISNULL(k.preparerStatus, '') <> 'sent' THEN 'Pending Preparer'
                WHEN ISNULL(k.preparerStatus, '') = 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' AND ISNULL(k.checkerStatus, '') <> 'approved' THEN 'Pending Checker'
                WHEN ISNULL(k.checkerStatus, '') = 'approved' AND ISNULL(k.acceptanceStatus, '') <> 'approved' AND ISNULL(k.reviewerStatus, '') <> 'sent' THEN 'Pending Reviewer'
                WHEN ISNULL(k.reviewerStatus, '') = 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'Pending Acceptance'
                WHEN ISNULL(k.acceptanceStatus, '') = 'approved' THEN 'Approved'
                ELSE 'Other'
            END
        ORDER BY count DESC
        """
        return await self.execute_query(query)

    async def get_kris_by_level(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return KRIs grouped by risk level"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))
        
        query = f"""
        SELECT 
            k.kri_level as level,
            COUNT(*) as count
        FROM Kris k
        WHERE k.isDeleted = 0 
          AND k.deletedAt IS NULL {date_filter}
          {function_filter}
        AND k.kri_level IS NOT NULL
        GROUP BY k.kri_level
        ORDER BY count DESC
        """
        return await self.execute_query(query)

    async def get_breached_kris_by_department(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return breached KRIs by department"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))
        
        query = f"""
        SELECT 
            ISNULL(f.name, 'Unknown') as function_name,
            COUNT(k.id) as breached_count
        FROM Kris k
        LEFT JOIN Functions f ON k.related_function_id = f.id
          AND f.isDeleted = 0
          AND f.deletedAt IS NULL
        WHERE k.isDeleted = 0 
          AND k.deletedAt IS NULL {date_filter}
          {function_filter}
        AND k.status = 'Breached'
        GROUP BY f.name
        ORDER BY breached_count DESC
        """
        return await self.execute_query(query)

    async def get_kri_assessment_count(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return KRI assessment count by department"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))
        
        query = f"""
        SELECT 
            ISNULL(f.name, 'Unknown') as function_name,
            COUNT(k.id) as assessment_count
        FROM Kris k
        LEFT JOIN Functions f ON k.related_function_id = f.id
          AND f.isDeleted = 0
          AND f.deletedAt IS NULL
        WHERE k.isDeleted = 0 
          AND k.deletedAt IS NULL {date_filter}
          {function_filter}
        GROUP BY f.name
        ORDER BY assessment_count DESC
        """
        return await self.execute_query(query)

    async def get_kris_list(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return list of all KRIs with same columns as UI Total KRIs modal (code, function_name, kri_name, frequency, threshold, added_by_name, assigned_person_name, type, type_percentage_or_figure, rcm_functions, risk_mapping, status, created_by_name, kri_status, first_approval, review, second_approval, createdAt)."""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))

        # Match Node getTotalKris catalog columns for Excel/UI parity
        query = f"""
        SELECT
            k.code,
            ISNULL(f.name, '') AS function_name,
            k.kriName AS kri_name,
            ISNULL(k.frequency, '') AS frequency,
            ISNULL(k.threshold, '') AS threshold,
            k.low_from AS low_risk,
            k.medium_from AS medium_risk,
            k.high_from AS high_risk,
            ISNULL(added_by_u.name, '') AS added_by_name,
            ISNULL(assigned_u.name, '') AS assigned_person_name,
            ISNULL(k.type, '') AS type,
            ISNULL(k.typePercentageOrFigure, '') AS type_percentage_or_figure,
            (SELECT STRING_AGG(f2.name, ', ') WITHIN GROUP (ORDER BY f2.name)
             FROM KriFunctions kf
             INNER JOIN Functions f2 ON f2.id = kf.function_id AND f2.deletedAt IS NULL AND f2.isDeleted = 0
             WHERE kf.kri_id = k.id AND kf.deletedAt IS NULL) AS rcm_functions,
            (SELECT STRING_AGG(r.name, ', ') WITHIN GROUP (ORDER BY r.name)
             FROM KriRisks kr
             INNER JOIN Risks r ON r.id = kr.risk_id AND r.deletedAt IS NULL
             WHERE kr.kri_id = k.id AND kr.deletedAt IS NULL) AS risk_mapping,
            ISNULL(k.status, '') AS status,
            ISNULL(created_by_u.name, '') AS created_by_name,
            CASE
                WHEN ISNULL(k.preparerStatus, '') <> 'sent' THEN 'Draft'
                WHEN ISNULL(k.reviewerStatus, '') = 'sent' THEN 'Review Sent'
                WHEN ISNULL(k.acceptanceStatus, '') = 'approved' THEN 'Approved'
                ELSE 'In Progress'
            END AS kri_status,
            CASE WHEN k.checkerStatus = 'approved' THEN 'Approved' WHEN k.checkerStatus = 'refused' THEN 'Refused' WHEN k.checkerStatus IS NULL THEN (CASE WHEN LOWER(k.preparerStart) LIKE '%orm%' THEN 'N/A' ELSE 'Pending' END) ELSE 'Pending' END AS first_approval,
            CASE WHEN k.reviewerStatus = 'sent' THEN 'Sent' WHEN k.reviewerStatus IS NULL THEN (CASE WHEN LOWER(k.preparerStart) LIKE '%orm%' THEN 'N/A' ELSE 'Pending' END) ELSE 'Pending' END AS review,
            CASE WHEN ISNULL(k.acceptanceStatus, '') = 'approved' THEN 'Approved' WHEN ISNULL(k.acceptanceStatus, '') = 'refused' THEN 'Refused' ELSE 'Pending' END AS second_approval,
            FORMAT(CONVERT(datetime, k.createdAt), 'yyyy-MM-dd HH:mm:ss') AS createdAt
        FROM Kris k
        LEFT JOIN Functions f ON k.related_function_id = f.id AND f.isDeleted = 0 AND f.deletedAt IS NULL
        LEFT JOIN users added_by_u ON k.addedBy = added_by_u.id AND added_by_u.deletedAt IS NULL
        LEFT JOIN users assigned_u ON k.assignedPersonId = assigned_u.id AND assigned_u.deletedAt IS NULL
        LEFT JOIN users created_by_u ON k.created_by = created_by_u.id AND created_by_u.deletedAt IS NULL
        WHERE k.isDeleted = 0 AND k.deletedAt IS NULL
          {date_filter}
          {function_filter}
        ORDER BY k.createdAt DESC
        """
        write_debug(f"get_kris_list query (truncated): SELECT ... FROM Kris k ...")
        return await self.execute_query(query)

    async def get_kri_values_list(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return list of individual KRI VALUE assessments (one row per periodic value, not
        distinct KRIs) with the same columns as the UI Total KRI Assessments modal. Mirrors
        get_kris_list, but joins KriValues and accepts the independent Submission Date Filter."""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        submission_filter = self._build_submission_filter(submission_start_date, submission_end_date)
        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))

        query = f"""
        SELECT
            k.code,
            ISNULL(COALESCE(frel.name, fkf.name), 'Unknown') AS function_name,
            k.kriName AS kri_name,
            ISNULL(k.frequency, '') AS frequency,
            ISNULL(k.threshold, '') AS threshold,
            kv.[month] AS month,
            kv.[year] AS year,
            kv.value AS value,
            kv.assessment AS assessment,
            FORMAT(CONVERT(datetime, kv.createdAt), 'yyyy-MM-dd HH:mm:ss') AS createdAt
        FROM Kris k
        INNER JOIN KriValues kv ON kv.kriId = k.id AND kv.deletedAt IS NULL {submission_filter}
        LEFT JOIN Functions frel ON frel.id = k.related_function_id AND frel.isDeleted = 0 AND frel.deletedAt IS NULL
        OUTER APPLY (
          SELECT TOP 1 f2.name
          FROM KriFunctions kf2
          INNER JOIN Functions f2 ON f2.id = kf2.function_id AND f2.isDeleted = 0 AND f2.deletedAt IS NULL
          WHERE kf2.kri_id = k.id AND kf2.deletedAt IS NULL
          ORDER BY kf2.function_id
        ) fkf(name)
        WHERE k.isDeleted = 0 AND k.deletedAt IS NULL
          {date_filter}
          {function_filter}
        ORDER BY kv.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_kris_by_status_detail(
        self,
        status: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return KRIs rows filtered by computed status label (not counts, matches incidents pattern)"""
        write_debug(f"Getting KRIS by status detail: {status}")
       
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))

        # Build query that computes the label and filters to requested status
        query = f"""
        WITH KrisStatus AS (
            SELECT
                k.code,
                ISNULL(f.name, 'Unknown') AS function_name,
                k.kriName as kri_name,
                CASE
                    -- 1) Pending preparer: preparerStatus is anything other than 'sent'
                    WHEN ISNULL(k.preparerStatus, '') <> 'sent' THEN 'pendingPreparer'
                    -- 2) Pending checker: preparer sent AND checker not approved AND acceptance not approved
                    WHEN ISNULL(k.preparerStatus, '') = 'sent' AND ISNULL(k.checkerStatus, '') <> 'approved' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'pendingChecker'
                    -- 3) Pending reviewer: checker approved AND reviewer not approved AND acceptance not approved
                    WHEN ISNULL(k.checkerStatus, '') = 'approved' AND ISNULL(k.reviewerStatus, '') <> 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'pendingReviewer'
                    -- 4) Pending acceptance: reviewer approved AND acceptance not approved
                    WHEN ISNULL(k.reviewerStatus, '') = 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'pendingAcceptance'
                    -- 5) Fully approved
                    WHEN ISNULL(k.acceptanceStatus, '') = 'approved' THEN 'Approved'
                    ELSE 'Other'
                END AS status,
                FORMAT(CONVERT(datetime, k.createdAt), 'yyyy-MM-dd HH:mm:ss') as createdAt
            FROM {self.get_fully_qualified_table_name('Kris')} k
            LEFT JOIN {self.get_fully_qualified_table_name('Functions')} f ON k.related_function_id = f.id AND f.isDeleted = 0 AND f.deletedAt IS NULL
            WHERE k.isDeleted = 0 AND k.deletedAt IS NULL {date_filter}
            {function_filter}
        )
        SELECT *
        FROM KrisStatus
        WHERE status = '{status}'
        ORDER BY createdAt DESC;
        """
        write_debug(f"Query: {query}")
        return await self.execute_query(query)

    async def get_kris_status_counts(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> Dict[str, int]:
        """Return KRIs status counts (independent counts, matches Node.js logic)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))
        
        query = f"""
        WITH KrisStatus AS (
          SELECT 
            CASE 
              WHEN ISNULL(k.preparerStatus, '') <> 'sent' THEN 'pendingPreparer'
              WHEN ISNULL(k.preparerStatus, '') = 'sent' AND ISNULL(k.checkerStatus, '') <> 'approved' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'pendingChecker'
              WHEN ISNULL(k.checkerStatus, '') = 'approved' AND ISNULL(k.reviewerStatus, '') <> 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'pendingReviewer'
              WHEN ISNULL(k.reviewerStatus, '') = 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'pendingAcceptance'
              WHEN ISNULL(k.acceptanceStatus, '') = 'approved' THEN 'approved'
              ELSE 'Other'
            END AS status
          FROM Kris k
          WHERE k.isDeleted = 0 AND k.deletedAt IS NULL {date_filter}
          {function_filter}
        )
        SELECT 
          CAST(SUM(CASE WHEN status = 'pendingPreparer' THEN 1 ELSE 0 END) AS INT) AS pendingPreparer,
          CAST(SUM(CASE WHEN status = 'pendingChecker' THEN 1 ELSE 0 END) AS INT) AS pendingChecker,
          CAST(SUM(CASE WHEN status = 'pendingReviewer' THEN 1 ELSE 0 END) AS INT) AS pendingReviewer,
          CAST(SUM(CASE WHEN status = 'pendingAcceptance' THEN 1 ELSE 0 END) AS INT) AS pendingAcceptance,
          CAST(SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS INT) AS approved
        FROM KrisStatus
        """
        result = await self.execute_query(query)
        return result[0] if result else {}

    async def get_overall_kri_statuses(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return all KRIs with their combined status (for Overall KRI Statuses table)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))
        
        query = f"""
        SELECT
          k.code             AS code,
          ISNULL(COALESCE(frel.name, fkf.name), 'Unknown') AS function_name,
          k.kriName          AS kri_name,
          CASE
            WHEN ISNULL(k.preparerStatus, '') <> 'sent' THEN 'Pending Preparer'
            WHEN ISNULL(k.checkerStatus, '') = 'refused' THEN 'Checker Refused'
            WHEN ISNULL(k.preparerStatus, '') = 'sent' AND ISNULL(k.checkerStatus, '') <> 'approved' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'Pending Checker'
            WHEN ISNULL(k.acceptanceStatus, '') = 'refused' THEN 'Acceptance Refused'
            WHEN ISNULL(k.checkerStatus, '') = 'approved' AND ISNULL(k.reviewerStatus, '') <> 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'Pending Reviewer'
            WHEN ISNULL(k.reviewerStatus, '') = 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'Pending Acceptance'
            WHEN ISNULL(k.acceptanceStatus, '') = 'approved' THEN 'Approved'
            ELSE 'Unknown'
          END AS status
        FROM Kris k
        LEFT JOIN KriFunctions kf ON k.id = kf.kri_id
          AND kf.deletedAt IS NULL
        LEFT JOIN Functions fkf ON fkf.id = kf.function_id
          AND fkf.isDeleted = 0
          AND fkf.deletedAt IS NULL
        LEFT JOIN Functions frel ON frel.id = k.related_function_id
          AND frel.isDeleted = 0
          AND frel.deletedAt IS NULL
        WHERE
          k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
          {function_filter}
        ORDER BY k.kriName
        """
        return await self.execute_query(query)

    async def get_kris_by_level_detailed(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return KRIs by level with derived logic from latest values (matches Node.js)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        submission_filter = self._build_submission_filter(submission_start_date, submission_end_date)
        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))

        query = f"""
        WITH LatestKV AS (
          SELECT kv.kriId,
                 kv.value,
                 ROW_NUMBER() OVER (PARTITION BY kv.kriId ORDER BY COALESCE(CONVERT(datetime, CONCAT(kv.[year], '-', kv.[month], '-01')), kv.createdAt) DESC) rn
          FROM KriValues kv
          WHERE kv.deletedAt IS NULL {submission_filter}
        ),
        K AS (
          SELECT k.id,
                 k.kri_level,
                 CAST(k.isAscending AS int) AS isAscending,
                 TRY_CONVERT(float, k.medium_from) AS med_thr,
                 TRY_CONVERT(float, k.high_from) AS high_thr
          FROM Kris k
          WHERE k.isDeleted = 0 AND k.deletedAt IS NULL {date_filter}
          {function_filter}
        ),
        KL AS (
          SELECT K.id, K.kri_level, K.isAscending, K.med_thr, K.high_thr,
                 TRY_CONVERT(float, kv.value) AS val
          FROM K
          LEFT JOIN LatestKV kv ON kv.kriId = K.id AND kv.rn = 1
        ),
        Derived AS (
          SELECT CASE
                   WHEN kri_level IS NOT NULL AND LTRIM(RTRIM(kri_level)) <> '' THEN kri_level
                   WHEN val IS NULL OR med_thr IS NULL OR high_thr IS NULL THEN 'Unknown'
                   WHEN isAscending = 1 AND val >= high_thr THEN 'High'
                   WHEN isAscending = 1 AND val >= med_thr THEN 'Medium'
                   WHEN isAscending = 1 THEN 'Low'
                   WHEN isAscending = 0 AND val <= high_thr THEN 'High'
                   WHEN isAscending = 0 AND val <= med_thr THEN 'Medium'
                   ELSE 'Low'
                 END AS level_bucket
          FROM KL
        )
        SELECT level_bucket AS level, COUNT(*) AS count
        FROM Derived
        GROUP BY level_bucket
        ORDER BY count DESC
        """
        return await self.execute_query(query)

    async def get_assessment_history_by_level(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Assessment History by Risk Level (the "KRIs by Risk Level" chart): count EVERY
        assessment record (all periods, not just the latest per KRI) grouped by its recorded
        risk level, using KriValues.assessment. Mirrors Node assessmentHistoryByLevelQuery."""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        submission_filter = self._build_submission_filter(submission_start_date, submission_end_date)
        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))

        query = f"""
        SELECT
          CASE UPPER(LTRIM(RTRIM(kv.assessment)))
            WHEN 'HIGH'   THEN 'High'
            WHEN 'MEDIUM' THEN 'Medium'
            WHEN 'LOW'    THEN 'Low'
          END AS level,
          COUNT(kv.id) AS count
        FROM Kris k
        INNER JOIN KriValues kv ON kv.kriId = k.id AND kv.deletedAt IS NULL
        WHERE k.isDeleted = 0 AND k.deletedAt IS NULL {date_filter}
          AND UPPER(LTRIM(RTRIM(kv.assessment))) IN ('HIGH', 'MEDIUM', 'LOW')
          {function_filter} {submission_filter}
        GROUP BY
          CASE UPPER(LTRIM(RTRIM(kv.assessment)))
            WHEN 'HIGH'   THEN 'High'
            WHEN 'MEDIUM' THEN 'Medium'
            WHEN 'LOW'    THEN 'Low'
          END
        ORDER BY count DESC
        """
        return await self.execute_query(query)

    async def get_kris_by_level_records(
        self,
        level: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Detail rows backing the Low/Medium/High KRI Value cards: one row per
        KriValues assessment record classified as `level`. Mirrors Node's
        getKrisByLevel exactly (same INNER JOIN KriValues, same CASE classification,
        same date/function/submission filters) so the exported row count always
        matches the on-screen card/chart count for that level."""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        submission_filter = self._build_submission_filter(submission_start_date, submission_end_date)
        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))
        level_bucket = level.replace("'", "''")

        query = f"""
        WITH K AS (
          SELECT k.id, k.code, k.kriName, k.createdAt, k.related_function_id
          FROM Kris k
          WHERE k.isDeleted = 0 AND k.deletedAt IS NULL {date_filter}
          {function_filter}
        ),
        Derived AS (
          SELECT
            K.code,
            K.kriName AS name,
            K.createdAt,
            kv.id AS kriValueId,
            kv.value AS value,
            ISNULL(COALESCE(frel.name, fkf.name), 'Unknown') AS function_name,
            CASE UPPER(LTRIM(RTRIM(kv.assessment)))
              WHEN 'HIGH'   THEN 'High'
              WHEN 'MEDIUM' THEN 'Medium'
              WHEN 'LOW'    THEN 'Low'
              ELSE 'Unknown'
            END AS level_bucket
          FROM K
          INNER JOIN KriValues kv ON kv.kriId = K.id AND kv.deletedAt IS NULL
          LEFT JOIN Functions frel ON frel.id = K.related_function_id AND frel.isDeleted = 0 AND frel.deletedAt IS NULL
          OUTER APPLY (
            SELECT TOP 1 f2.name
            FROM KriFunctions kf2
            INNER JOIN Functions f2 ON f2.id = kf2.function_id AND f2.isDeleted = 0 AND f2.deletedAt IS NULL
            WHERE kf2.kri_id = K.id AND kf2.deletedAt IS NULL
            ORDER BY kf2.function_id
          ) fkf(name)
          WHERE 1 = 1 {submission_filter}
        )
        SELECT code, function_name, name, value, createdAt
        FROM Derived
        WHERE level_bucket = '{level_bucket}'
        ORDER BY createdAt DESC, kriValueId DESC
        """
        return await self.execute_query(query)

    async def _get_kri_values_by_status_bucket(
        self,
        bucket: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Shared query builder for the 5 "KRI Values Pending .../Approved" export detail
        methods below. Mirrors get_kris_by_level_records's structure exactly (same K CTE with
        date_filter+function_filter, Derived CTE INNER JOINing KriValues with submission_filter
        applied inside it, function name resolved via LEFT JOIN Functions frel + OUTER APPLY
        ... KriFunctions kf2 to avoid many-to-many fan-out), but classifies each KriValues row
        via the EXACT SAME 5-state waterfall CASE as Node's kriValueStatusCountsQuery / this
        file's get_kris_status_counts, applied to kv.preparerStatus/checkerStatus/reviewerStatus/
        acceptanceStatus instead of the KRI's own k.* status columns."""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        submission_filter = self._build_submission_filter(submission_start_date, submission_end_date)
        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))

        bucket_case = """
          CASE
            WHEN ISNULL(kv.preparerStatus, '') <> 'sent' THEN 'pendingPreparer'
            WHEN ISNULL(kv.preparerStatus, '') = 'sent' AND ISNULL(kv.checkerStatus, '') <> 'approved' AND ISNULL(kv.acceptanceStatus, '') <> 'approved' THEN 'pendingChecker'
            WHEN ISNULL(kv.checkerStatus, '') = 'approved' AND ISNULL(kv.reviewerStatus, '') <> 'sent' AND ISNULL(kv.acceptanceStatus, '') <> 'approved' THEN 'pendingReviewer'
            WHEN ISNULL(kv.reviewerStatus, '') = 'sent' AND ISNULL(kv.acceptanceStatus, '') <> 'approved' THEN 'pendingAcceptance'
            WHEN ISNULL(kv.acceptanceStatus, '') = 'approved' THEN 'approved'
            ELSE 'Other'
          END
        """

        query = f"""
        WITH K AS (
          SELECT k.id, k.code, k.kriName, k.createdAt, k.related_function_id
          FROM Kris k
          WHERE k.isDeleted = 0 AND k.deletedAt IS NULL {date_filter}
          {function_filter}
        ),
        Derived AS (
          SELECT
            K.code,
            K.kriName AS name,
            K.createdAt,
            kv.id AS kriValueId,
            kv.value AS value,
            kv.createdAt AS submittedAt,
            kv.preparerStatus AS preparerStatus,
            kv.checkerStatus AS checkerStatus,
            kv.reviewerStatus AS reviewerStatus,
            kv.acceptanceStatus AS acceptanceStatus,
            ISNULL(COALESCE(frel.name, fkf.name), 'Unknown') AS function_name,
            {bucket_case} AS status_bucket
          FROM K
          INNER JOIN KriValues kv ON kv.kriId = K.id AND kv.deletedAt IS NULL
          LEFT JOIN Functions frel ON frel.id = K.related_function_id AND frel.isDeleted = 0 AND frel.deletedAt IS NULL
          OUTER APPLY (
            SELECT TOP 1 f2.name
            FROM KriFunctions kf2
            INNER JOIN Functions f2 ON f2.id = kf2.function_id AND f2.isDeleted = 0 AND f2.deletedAt IS NULL
            WHERE kf2.kri_id = K.id AND kf2.deletedAt IS NULL
            ORDER BY kf2.function_id
          ) fkf(name)
          WHERE 1 = 1 {submission_filter}
        )
        SELECT
          code, function_name, name, value,
          preparerStatus, checkerStatus, reviewerStatus, acceptanceStatus,
          submittedAt, createdAt
        FROM Derived
        WHERE status_bucket = '{bucket}'
        ORDER BY submittedAt DESC, kriValueId DESC
        """
        return await self.execute_query(query)

    async def get_kri_values_pending_preparer(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Detail rows backing the "KRI Values Pending Preparer" card/export."""
        return await self._get_kri_values_by_status_bucket(
            'pendingPreparer', start_date, end_date, user_id, group_name, function_id, function_ids,
            submission_start_date, submission_end_date,
        )

    async def get_kri_values_pending_checker(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Detail rows backing the "KRI Values Pending Checker" card/export."""
        return await self._get_kri_values_by_status_bucket(
            'pendingChecker', start_date, end_date, user_id, group_name, function_id, function_ids,
            submission_start_date, submission_end_date,
        )

    async def get_kri_values_pending_reviewer(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Detail rows backing the "KRI Values Pending Reviewer" card/export."""
        return await self._get_kri_values_by_status_bucket(
            'pendingReviewer', start_date, end_date, user_id, group_name, function_id, function_ids,
            submission_start_date, submission_end_date,
        )

    async def get_kri_values_pending_acceptance(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Detail rows backing the "KRI Values Pending Acceptance" card/export."""
        return await self._get_kri_values_by_status_bucket(
            'pendingAcceptance', start_date, end_date, user_id, group_name, function_id, function_ids,
            submission_start_date, submission_end_date,
        )

    async def get_kri_values_approved(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Detail rows backing the "KRI Values Approved" card/export."""
        return await self._get_kri_values_by_status_bucket(
            'approved', start_date, end_date, user_id, group_name, function_id, function_ids,
            submission_start_date, submission_end_date,
        )

    async def _get_kris_refused_detail(
        self,
        field: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Shared query builder for the KRI-level "Checker Refused" / "Acceptance Refused" export
        detail methods. A refused status is a direct equality on its own field (k.checkerStatus or
        k.acceptanceStatus), not part of the pendingPreparer/.../approved waterfall, so this is a
        plain filter rather than a CASE bucket."""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))

        query = f"""
        SELECT
          k.code,
          k.kriName AS title,
          ISNULL(COALESCE(frel.name, fkf.name), 'Unknown') AS function_name,
          'Refused' AS status,
          FORMAT(CONVERT(datetime, k.createdAt), 'yyyy-MM-dd HH:mm:ss') AS createdAt
        FROM Kris k
        LEFT JOIN Functions frel ON frel.id = k.related_function_id AND frel.isDeleted = 0 AND frel.deletedAt IS NULL
        OUTER APPLY (
          SELECT TOP 1 f2.name
          FROM KriFunctions kf2
          INNER JOIN Functions f2 ON f2.id = kf2.function_id AND f2.isDeleted = 0 AND f2.deletedAt IS NULL
          WHERE kf2.kri_id = k.id AND kf2.deletedAt IS NULL
          ORDER BY kf2.function_id
        ) fkf(name)
        WHERE k.isDeleted = 0 AND k.deletedAt IS NULL
          AND ISNULL(k.{field}, '') = 'refused'
          {date_filter}
          {function_filter}
        ORDER BY k.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_checker_refused_kris(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Detail rows backing the KRI-level "Checker Refused" card/export."""
        return await self._get_kris_refused_detail(
            'checkerStatus', start_date, end_date, user_id, group_name, function_id, function_ids,
        )

    async def get_acceptance_refused_kris(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Detail rows backing the KRI-level "Acceptance Refused" card/export."""
        return await self._get_kris_refused_detail(
            'acceptanceStatus', start_date, end_date, user_id, group_name, function_id, function_ids,
        )

    async def _get_kri_values_refused_detail(
        self,
        field: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Shared query builder for the KRI VALUE "Checker Refused" / "Acceptance Refused" export
        detail methods. Mirrors _get_kri_values_by_status_bucket's structure (same K CTE, Derived
        CTE INNER JOINing KriValues with submission_filter, function name resolved the same way),
        but a refused status is a direct equality on its own field (kv.checkerStatus or
        kv.acceptanceStatus), not part of the pending/approved waterfall."""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        submission_filter = self._build_submission_filter(submission_start_date, submission_end_date)
        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))

        query = f"""
        WITH K AS (
          SELECT k.id, k.code, k.kriName, k.createdAt, k.related_function_id
          FROM Kris k
          WHERE k.isDeleted = 0 AND k.deletedAt IS NULL {date_filter}
          {function_filter}
        )
        SELECT
          K.code,
          K.kriName AS name,
          ISNULL(COALESCE(frel.name, fkf.name), 'Unknown') AS function_name,
          kv.value AS value,
          kv.preparerStatus AS preparerStatus,
          kv.checkerStatus AS checkerStatus,
          kv.reviewerStatus AS reviewerStatus,
          kv.acceptanceStatus AS acceptanceStatus,
          FORMAT(CONVERT(datetime, kv.createdAt), 'yyyy-MM-dd HH:mm:ss') AS submittedAt,
          K.createdAt
        FROM K
        INNER JOIN KriValues kv ON kv.kriId = K.id AND kv.deletedAt IS NULL AND ISNULL(kv.{field}, '') = 'refused'
          {submission_filter}
        LEFT JOIN Functions frel ON frel.id = K.related_function_id AND frel.isDeleted = 0 AND frel.deletedAt IS NULL
        OUTER APPLY (
          SELECT TOP 1 f2.name
          FROM KriFunctions kf2
          INNER JOIN Functions f2 ON f2.id = kf2.function_id AND f2.isDeleted = 0 AND f2.deletedAt IS NULL
          WHERE kf2.kri_id = K.id AND kf2.deletedAt IS NULL
          ORDER BY kf2.function_id
        ) fkf(name)
        ORDER BY kv.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_kri_values_checker_refused(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Detail rows backing the "KRI Assessment Checker Refused" card/export."""
        return await self._get_kri_values_refused_detail(
            'checkerStatus', start_date, end_date, user_id, group_name, function_id, function_ids,
            submission_start_date, submission_end_date,
        )

    async def get_kri_values_acceptance_refused(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Detail rows backing the "KRI Assessment Acceptance Refused" card/export."""
        return await self._get_kri_values_refused_detail(
            'acceptanceStatus', start_date, end_date, user_id, group_name, function_id, function_ids,
            submission_start_date, submission_end_date,
        )

    async def get_breached_kris_by_department_detailed(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return breached KRIs by function: counts individual KRI VALUE assessments
        (not distinct KRIs) whose recorded kv.assessment is High — the same stored field
        the "KRIs by Risk Level" chart reads, not a threshold recomputation."""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        submission_filter = self._build_submission_filter(submission_start_date, submission_end_date)
        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))

        query = f"""
        SELECT
          ISNULL(COALESCE(frel.name, fkf.name), 'Unknown') AS function_name,
          COUNT(kv.id) AS breached_count
        FROM Kris k
        INNER JOIN KriValues kv ON kv.kriId = k.id AND kv.deletedAt IS NULL {submission_filter}
        LEFT JOIN KriFunctions kf ON kf.kri_id = k.id AND kf.deletedAt IS NULL
        LEFT JOIN Functions fkf ON fkf.id = kf.function_id AND fkf.isDeleted = 0 AND fkf.deletedAt IS NULL
        LEFT JOIN Functions frel ON frel.id = k.related_function_id AND frel.isDeleted = 0 AND frel.deletedAt IS NULL
        WHERE k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
          {function_filter}
          AND UPPER(LTRIM(RTRIM(kv.assessment))) = 'HIGH'
        GROUP BY ISNULL(COALESCE(frel.name, fkf.name), 'Unknown')
        ORDER BY breached_count DESC
        """
        return await self.execute_query(query)

    async def get_kri_health(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return KRI health status list"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))
        
        query = f"""
        SELECT
          k.code AS code,
          k.kriName,
          COALESCE(frel.name, fkf.name, 'Unknown') AS function_name,
          k.status,
          COALESCE(k.kri_level, 'Unknown') AS kri_level,
          k.threshold,
          k.frequency
        FROM Kris k
        LEFT JOIN KriFunctions kf ON k.id = kf.kri_id
          AND kf.deletedAt IS NULL
        LEFT JOIN Functions fkf ON fkf.id = kf.function_id
          AND fkf.isDeleted = 0
          AND fkf.deletedAt IS NULL
        LEFT JOIN Functions frel ON frel.id = k.related_function_id
          AND frel.isDeleted = 0
          AND frel.deletedAt IS NULL
        WHERE k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
          {function_filter}
        ORDER BY k.createdAt DESC
        """
        return await self.execute_query(query)

    async def get_kri_assessment_count_detailed(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return KRI assessment count by function (count assessments from KriValues table)"""
        # Date range scopes on the KRI's own creation date (k.createdAt), matching the
        # Node dashboard summary for this chart — not kv.createdAt (the assessment date),
        # which would select a different set of assessments once a date range is applied.
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        submission_filter = self._build_submission_filter(submission_start_date, submission_end_date)
        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))

        query = f"""
        SELECT
          ISNULL(COALESCE(frel.name, fkf.name), 'Unknown') AS function_name,
          COUNT(kv.id) AS assessment_count
        FROM KriValues kv
        INNER JOIN Kris k ON kv.kriId = k.id
          AND k.isDeleted = 0
          AND k.deletedAt IS NULL
        LEFT JOIN KriFunctions kf ON k.id = kf.kri_id
          AND kf.deletedAt IS NULL
        LEFT JOIN Functions fkf ON fkf.id = kf.function_id
          AND fkf.isDeleted = 0
          AND fkf.deletedAt IS NULL
        LEFT JOIN Functions frel ON frel.id = k.related_function_id
          AND frel.isDeleted = 0
          AND frel.deletedAt IS NULL
        WHERE kv.deletedAt IS NULL {date_filter} {submission_filter}
          {function_filter}
        GROUP BY ISNULL(COALESCE(frel.name, fkf.name), 'Unknown')
        ORDER BY assessment_count DESC
        """
        return await self.execute_query(query)

    async def get_kri_monthly_assessment(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return monthly KRI counts grouped by assessment"""
        # Date range scopes on k.createdAt (matching the Node dashboard summary for this
        # chart); only the month bucketing/grouping below uses kv.createdAt (the
        # assessment date), which is the intended x-axis field.
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        submission_filter = self._build_submission_filter(submission_start_date, submission_end_date)
        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))

        query = f"""
        SELECT
          CAST(DATEADD(month, DATEPART(month, kv.createdAt) - 1, DATEFROMPARTS(YEAR(kv.createdAt), 1, 1)) AS datetime2) AS createdAt,
          kv.assessment AS assessment,
          COUNT(kv.id) AS count
        FROM Kris AS k
        INNER JOIN KriValues AS kv ON kv.kriId = k.id AND kv.deletedAt IS NULL
        WHERE k.isDeleted = 0
          AND k.deletedAt IS NULL
          {function_filter}
          AND kv.assessment IS NOT NULL {date_filter} {submission_filter}
        GROUP BY
          CAST(DATEADD(month, DATEPART(month, kv.createdAt) - 1, DATEFROMPARTS(YEAR(kv.createdAt), 1, 1)) AS datetime2),
          kv.assessment
        ORDER BY createdAt ASC, assessment ASC
        """
        return await self.execute_query(query)

    async def get_newly_created_kris_per_month(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return number of newly created KRIs per month"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))
        
        query = f"""
        SELECT 
          CAST(DATEFROMPARTS(YEAR(k.createdAt), MONTH(k.createdAt), 1) AS datetime2) AS createdAt,
          COUNT(*) AS count
        FROM Kris k
        WHERE k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
          {function_filter}
        GROUP BY CAST(DATEFROMPARTS(YEAR(k.createdAt), MONTH(k.createdAt), 1) AS datetime2)
        ORDER BY createdAt ASC
        """
        return await self.execute_query(query)

    async def get_deleted_kris_per_month(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return number of deleted KRIs by month"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))
        
        # Bucket by the deletion date (falling back to createdAt for KRIs with no
        # deletedAt timestamp), matching the Node dashboard summary — bucketing by
        # k.createdAt instead would answer "created in month X and later deleted"
        # rather than "deleted in month X", misaligning the export from the chart.
        # The range filter above stays on k.createdAt, same as the Node summary.
        query = f"""
        SELECT
          CAST(DATEFROMPARTS(YEAR(COALESCE(k.deletedAt, k.createdAt)), MONTH(COALESCE(k.deletedAt, k.createdAt)), 1) AS datetime2) AS createdAt,
          COUNT(*) AS count
        FROM Kris k
        WHERE (k.isDeleted = 1 OR k.deletedAt IS NOT NULL)
          AND COALESCE(k.deletedAt, k.createdAt) IS NOT NULL {date_filter}
          {function_filter}
        GROUP BY YEAR(COALESCE(k.deletedAt, k.createdAt)), MONTH(COALESCE(k.deletedAt, k.createdAt))
        ORDER BY YEAR(COALESCE(k.deletedAt, k.createdAt)) ASC, MONTH(COALESCE(k.deletedAt, k.createdAt)) ASC
        """
        return await self.execute_query(query)

    async def get_kri_overdue_status_counts(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return KRIs overdue vs not overdue based on related Action Plans"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))
        
        query = f"""
        WITH classified AS (
          SELECT
            k.id,
            CASE
              WHEN EXISTS (
                SELECT 1
                FROM Actionplans ap
                WHERE ap.kri_id = k.id
                  AND ap.deletedAt IS NULL
                  AND ap.implementation_date < GETDATE()
                  AND (ap.done = 0 OR ap.done IS NULL)
              ) THEN 'Overdue'
              ELSE 'Not Overdue'
            END AS KRIStatus
          FROM Kris AS k
          WHERE k.isDeleted = 0
            AND k.deletedAt IS NULL {date_filter}
            {function_filter}
        )
        SELECT
          KRIStatus AS status,
          COUNT(*) AS count
        FROM classified
        GROUP BY KRIStatus
        """
        return await self.execute_query(query)

    async def get_kris_submission_by_month_detailed(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return one row per KRI per month: whether the KRI was Submitted (a value was
        recorded that month) or Not Submitted. Months run continuously from the earliest KRI's
        creation month through the later of "now" or the latest month that actually has data,
        so zero-submission months still appear and no real submission is ever dropped."""
        # Date range scopes on the KRI's own creation date (k.createdAt), matching the Node
        # dashboard summary for this chart (krisSubmittedMonthlyQuery).
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        submission_filter = self._build_submission_filter(submission_start_date, submission_end_date)
        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))

        query = f"""
        WITH MonthsBase AS (
          SELECT
            (SELECT MIN(createdAt) FROM Kris WHERE isDeleted = 0 AND deletedAt IS NULL) AS start_date,
            (SELECT MAX(DATEFROMPARTS(TRY_CONVERT(int, [year]), TRY_CONVERT(int, [month]), 1))
             FROM KriValues WHERE deletedAt IS NULL AND [year] IS NOT NULL AND [month] IS NOT NULL) AS max_data_period
        ),
        Months AS (
          SELECT
            YEAR(DATEFROMPARTS(YEAR(start_date), MONTH(start_date), 1)) AS yr,
            MONTH(DATEFROMPARTS(YEAR(start_date), MONTH(start_date), 1)) AS mo,
            DATEFROMPARTS(YEAR(start_date), MONTH(start_date), 1) AS period,
            CASE WHEN max_data_period > DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)
                 THEN max_data_period ELSE DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) END AS end_period
          FROM MonthsBase
          UNION ALL
          SELECT YEAR(DATEADD(MONTH, 1, period)), MONTH(DATEADD(MONTH, 1, period)), DATEADD(MONTH, 1, period), end_period
          FROM Months
          WHERE period < end_period
        ),
        Expected AS (
          -- Only months the KRI is actually due in (Quarterly -> every 3rd month, Annually ->
          -- December; anything else, incl. Monthly/Daily/Event Base/NULL, is due every month —
          -- mirrors isKriMonthDue in v2_backend/src/kri/kri-frequency.util.ts), and excludes
          -- months at/after the KRI went inactive (mirrors isKriMonthPaused there).
          SELECT m.yr, m.mo, k.id AS kri_id,
                 k.code AS kri_code, k.kriName AS kri_name,
                 ISNULL(COALESCE(frel.name, fkf.name), 'Unknown') AS function_name
          FROM Months m
          INNER JOIN Kris k
            ON k.isDeleted = 0 AND k.deletedAt IS NULL
            AND k.createdAt < DATEADD(MONTH, 1, DATEFROMPARTS(m.yr, m.mo, 1))
            AND (
              (k.frequency = 'Quarterly' AND m.mo % 3 = 0)
              OR (k.frequency = 'Annually' AND m.mo % 12 = 0)
              OR (ISNULL(k.frequency, '') NOT IN ('Quarterly', 'Annually'))
            )
            AND NOT (
              LOWER(ISNULL(k.status, '')) = 'inactive'
              AND k.inactiveYearMonth IS NOT NULL
              AND TRY_CONVERT(date, k.inactiveYearMonth + '-01') IS NOT NULL
              AND DATEFROMPARTS(m.yr, m.mo, 1) >= TRY_CONVERT(date, k.inactiveYearMonth + '-01')
            )
            {date_filter}
            {function_filter}
          LEFT JOIN KriFunctions kf ON kf.kri_id = k.id AND kf.deletedAt IS NULL
          LEFT JOIN Functions fkf ON fkf.id = kf.function_id AND fkf.isDeleted = 0 AND fkf.deletedAt IS NULL
          LEFT JOIN Functions frel ON frel.id = k.related_function_id AND frel.isDeleted = 0 AND frel.deletedAt IS NULL
        ),
        Sub AS (
          SELECT DISTINCT kv.kriId, TRY_CONVERT(int, kv.[year]) AS yr, TRY_CONVERT(int, kv.[month]) AS mo
          FROM KriValues kv WHERE kv.deletedAt IS NULL {submission_filter}
        )
        SELECT
          e.kri_code AS kri_code,
          e.function_name AS function_name,
          e.kri_name AS kri_name,
          CASE WHEN s.kriId IS NOT NULL THEN 'Submitted' ELSE 'Not Submitted' END AS status,
          FORMAT(DATEFROMPARTS(e.yr, e.mo, 1), 'MMM yyyy') AS month
        FROM Expected e
        LEFT JOIN Sub s ON s.kriId = e.kri_id AND s.yr = e.yr AND s.mo = e.mo
        ORDER BY e.yr, e.mo, e.kri_name
        OPTION (MAXRECURSION 1000)
        """
        return await self.execute_query(query)

    async def get_monthly_kri_submission_by_function(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Monthly KRI submission by function: one row per KRI per month (all months),
        ordered by function, with month name, year, Submitted? (Yes/No) and Approved (Yes/No)."""
        # Date range scopes on the KRI's own creation date (k.createdAt), matching the Node
        # dashboard summary for this table (getMonthlyKriSubmissionByFunctionTablePage).
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        submission_filter = self._build_submission_filter(submission_start_date, submission_end_date)
        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))

        query = f"""
        WITH MonthsBase AS (
          SELECT
            (SELECT MIN(createdAt) FROM Kris WHERE isDeleted = 0 AND deletedAt IS NULL) AS start_date,
            (SELECT MAX(DATEFROMPARTS(TRY_CONVERT(int, [year]), TRY_CONVERT(int, [month]), 1))
             FROM KriValues WHERE deletedAt IS NULL AND [year] IS NOT NULL AND [month] IS NOT NULL) AS max_data_period
        ),
        Months AS (
          -- Continuous calendar months from the earliest KRI's creation month through the later
          -- of "now" or the latest month that actually has data, so zero-submission months still
          -- appear (as Not Submitted) and no real (even future-dated) submission is ever dropped.
          SELECT
            YEAR(DATEFROMPARTS(YEAR(start_date), MONTH(start_date), 1)) AS yr,
            MONTH(DATEFROMPARTS(YEAR(start_date), MONTH(start_date), 1)) AS mo,
            DATEFROMPARTS(YEAR(start_date), MONTH(start_date), 1) AS period,
            CASE WHEN max_data_period > DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)
                 THEN max_data_period ELSE DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) END AS end_period
          FROM MonthsBase
          UNION ALL
          SELECT YEAR(DATEADD(MONTH, 1, period)), MONTH(DATEADD(MONTH, 1, period)), DATEADD(MONTH, 1, period), end_period
          FROM Months
          WHERE period < end_period
        ),
        Expected AS (
          -- Only months the KRI is actually due in (Quarterly -> every 3rd month, Annually ->
          -- December; anything else, incl. Monthly/Daily/Event Base/NULL, is due every month —
          -- mirrors isKriMonthDue in v2_backend/src/kri/kri-frequency.util.ts), and excludes
          -- months at/after the KRI went inactive (mirrors isKriMonthPaused there).
          SELECT m.yr, m.mo, k.id AS kri_id, k.code AS kri_code, k.kriName AS kri_name,
                 ISNULL(COALESCE(frel.name, fkf.name), 'Unknown') AS function_name
          FROM Months m
          INNER JOIN Kris k
            ON k.isDeleted = 0 AND k.deletedAt IS NULL
            AND k.createdAt < DATEADD(MONTH, 1, DATEFROMPARTS(m.yr, m.mo, 1))
            AND (
              (k.frequency = 'Quarterly' AND m.mo % 3 = 0)
              OR (k.frequency = 'Annually' AND m.mo % 12 = 0)
              OR (ISNULL(k.frequency, '') NOT IN ('Quarterly', 'Annually'))
            )
            AND NOT (
              LOWER(ISNULL(k.status, '')) = 'inactive'
              AND k.inactiveYearMonth IS NOT NULL
              AND TRY_CONVERT(date, k.inactiveYearMonth + '-01') IS NOT NULL
              AND DATEFROMPARTS(m.yr, m.mo, 1) >= TRY_CONVERT(date, k.inactiveYearMonth + '-01')
            )
            {date_filter}
            {function_filter}
          LEFT JOIN KriFunctions kf ON kf.kri_id = k.id AND kf.deletedAt IS NULL
          LEFT JOIN Functions fkf ON fkf.id = kf.function_id AND fkf.isDeleted = 0 AND fkf.deletedAt IS NULL
          LEFT JOIN Functions frel ON frel.id = k.related_function_id AND frel.isDeleted = 0 AND frel.deletedAt IS NULL
        ),
        Sub AS (
          SELECT kv.kriId, TRY_CONVERT(int, kv.[year]) AS yr, TRY_CONVERT(int, kv.[month]) AS mo,
                 MAX(CASE WHEN kv.acceptanceStatus = 'approved' THEN 1 ELSE 0 END) AS is_approved
          FROM KriValues kv WHERE kv.deletedAt IS NULL {submission_filter}
          GROUP BY kv.kriId, TRY_CONVERT(int, kv.[year]), TRY_CONVERT(int, kv.[month])
        )
        SELECT
          e.kri_code AS kri_code,
          e.function_name AS function_name,
          e.kri_name AS kri_name,
          DATENAME(MONTH, DATEFROMPARTS(e.yr, e.mo, 1)) AS month,
          e.yr AS year,
          CASE WHEN s.kriId IS NOT NULL THEN 'Yes' ELSE 'No' END AS submitted,
          CASE WHEN s.is_approved = 1 THEN 'Yes' ELSE 'No' END AS approved
        FROM Expected e
        LEFT JOIN Sub s ON s.kriId = e.kri_id AND s.yr = e.yr AND s.mo = e.mo
        ORDER BY e.function_name, e.kri_code, e.yr, e.mo
        OPTION (MAXRECURSION 1000)
        """
        return await self.execute_query(query)

    async def get_overdue_kris_by_department(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return overdue KRIs with department from Actionplans or linked Function"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        submission_filter = self._build_submission_filter(submission_start_date, submission_end_date)
        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))
        
        query = f"""
        SELECT
          k.code AS code,
          ISNULL(COALESCE(frel.name, fkf.name), 'Unknown') AS function_name,
          k.kriName AS kriName,
          ISNULL(k.threshold, '') AS threshold,
          k.low_from AS low_from,
          k.medium_from AS medium_from,
          k.high_from AS high_from,
          CASE
            -- Actionplans.year/month are 0 (not NULL) as a sentinel on many rows,
            -- and DATEFROMPARTS errors on an out-of-range month/year, so check ranges
            -- explicitly rather than just IS NOT NULL. When the action plan itself has no
            -- valid period, fall back to the KRI's latest recorded value's period instead of
            -- leaving this blank.
            WHEN ap.[month] BETWEEN 1 AND 12 AND ap.[year] BETWEEN 1 AND 9999
            THEN DATENAME(MONTH, DATEFROMPARTS(ap.[year], ap.[month], 1))
            WHEN kv.kv_month BETWEEN 1 AND 12 AND kv.kv_year BETWEEN 1 AND 9999
            THEN DATENAME(MONTH, DATEFROMPARTS(kv.kv_year, kv.kv_month, 1))
            ELSE ''
          END AS month,
          CASE
            WHEN ap.[year] BETWEEN 1 AND 9999 THEN CAST(ap.[year] AS VARCHAR(10))
            WHEN kv.kv_year BETWEEN 1 AND 9999 THEN CAST(kv.kv_year AS VARCHAR(10))
            ELSE ''
          END AS year,
          CASE
            WHEN kv.value IS NULL THEN ''
            ELSE
              -- Route the float through DECIMAL before stringifying: casting a float
              -- straight to NVARCHAR caps at 6 significant digits and can silently
              -- switch to scientific notation (e.g. 2300517 -> '2.30052e+006', which
              -- rounds to the wrong number). DECIMAL -> VARCHAR never does that.
              CASE
                WHEN kv.value = ROUND(kv.value, 0) THEN CAST(CAST(kv.value AS DECIMAL(38, 0)) AS VARCHAR(50))
                ELSE CAST(CAST(kv.value AS DECIMAL(38, 4)) AS VARCHAR(50))
              END
              + CASE WHEN k.typePercentageOrFigure = '%' THEN '%' ELSE '' END
          END AS value,
          ISNULL(ap.control_procedure, '') AS action_plan,
          FORMAT(CONVERT(datetime, ap.implementation_date), 'yyyy-MM-dd') AS target_date,
          CASE
            WHEN ISNULL(ap.business_unit, '') = '' THEN 'Pending'
            ELSE ap.business_unit
          END AS status
        FROM Kris AS k
        INNER JOIN Actionplans AS ap ON ap.kri_id = k.id
          AND ap.deletedAt IS NULL
        LEFT JOIN Functions AS frel ON frel.id = k.related_function_id
          AND frel.isDeleted = 0
          AND frel.deletedAt IS NULL
        OUTER APPLY (
          SELECT TOP 1 f2.name
          FROM KriFunctions kf2
          INNER JOIN Functions f2 ON f2.id = kf2.function_id AND f2.isDeleted = 0 AND f2.deletedAt IS NULL
          WHERE kf2.kri_id = k.id AND kf2.deletedAt IS NULL
          ORDER BY kf2.function_id
        ) fkf(name)
        -- Prefer the KriValues row matching the action plan's own period exactly; when the
        -- action plan has no valid period (or no exact-period value exists), fall back to the
        -- KRI's most recently submitted value instead of leaving Month/Year/Value blank.
        OUTER APPLY (
          SELECT TOP 1
            kv2.value AS value,
            TRY_CONVERT(int, kv2.[year]) AS kv_year,
            TRY_CONVERT(int, kv2.[month]) AS kv_month
          FROM KriValues kv2
          WHERE kv2.kriId = k.id
            AND kv2.deletedAt IS NULL {submission_filter}
          ORDER BY
            CASE
              WHEN ap.[month] BETWEEN 1 AND 12 AND ap.[year] BETWEEN 1 AND 9999
                AND TRY_CONVERT(int, kv2.[year]) = ap.[year] AND TRY_CONVERT(int, kv2.[month]) = ap.[month]
              THEN 0 ELSE 1
            END,
            kv2.createdAt DESC
        ) kv(value, kv_year, kv_month)
        WHERE k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
          {function_filter}
        ORDER BY CASE WHEN ap.implementation_date IS NULL THEN 1 ELSE 0 END,
                 ap.implementation_date ASC, function_name, kriName
        """
        return await self.execute_query(query)

    async def get_all_kris_submitted_by_function(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
        submission_start_date: Optional[str] = None,
        submission_end_date: Optional[str] = None,
        order_by_function_asc: bool = False,
    ) -> List[Dict[str, Any]]:
        """Return KRIs Submission Status by Function: one row per KRI per year it has at least
        one recorded value, with a Jan..Dec column showing that month's submitted value (or NULL
        if nothing was recorded that month). Function attribution prioritizes related_function_id
        over KriFunctions, matching the main app/heatmap's authoritative logic (adib_backend
        kri.service.ts). Function name resolved via OUTER APPLY ... TOP 1 (not a plain
        LEFT JOIN KriFunctions) so a KRI linked to several functions is never fanned out into
        duplicate rows per (KRI, year).

        Row order mirrors Node's getAllKrisSubmittedByFunctionTablePage EXACTLY (the method the
        live view actually calls, since widgetMode fetches this table on its own, not through the
        combined dashboard payload): by default, most-recently-submitted-value first
        (MAX(kv.createdAt) DESC, then function name); with "Order by Function" toggled on,
        function name A->Z then KRI code then year DESC. Without this, the export used a third,
        unrelated order (function name/code/year only, no recency) that didn't match either live
        view mode — on a near-year-end dataset that put the single most-recently-touched row
        (page 1 in the view) on the very last export page instead."""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        submission_filter = self._build_submission_filter(submission_start_date, submission_end_date)
        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))

        query = f"""
        SELECT
          k.code,
          ISNULL(COALESCE(frel.name, fkf.name), 'Unknown') AS function_name,
          k.kriName AS kri_name,
          TRY_CONVERT(int, kv.[year]) AS year,
          COUNT(kv.id) AS assessments_count,
          CASE WHEN COUNT(kv.id) > 0 THEN 'Yes' ELSE 'No' END AS submission,
          {self._build_month_cell_expr(1)} AS jan,
          {self._build_month_cell_expr(2)} AS feb,
          {self._build_month_cell_expr(3)} AS mar,
          {self._build_month_cell_expr(4)} AS apr,
          {self._build_month_cell_expr(5)} AS may,
          {self._build_month_cell_expr(6)} AS jun,
          {self._build_month_cell_expr(7)} AS jul,
          {self._build_month_cell_expr(8)} AS aug,
          {self._build_month_cell_expr(9)} AS sep,
          {self._build_month_cell_expr(10)} AS oct,
          {self._build_month_cell_expr(11)} AS nov,
          {self._build_month_cell_expr(12)} AS [dec]
        FROM Kris AS k
        INNER JOIN KriValues kv ON kv.kriId = k.id AND kv.deletedAt IS NULL {submission_filter}
        LEFT JOIN Functions AS frel ON frel.id = k.related_function_id
          AND frel.isDeleted = 0
          AND frel.deletedAt IS NULL
        OUTER APPLY (
          SELECT TOP 1 f2.name
          FROM KriFunctions kf2
          INNER JOIN Functions f2 ON f2.id = kf2.function_id AND f2.isDeleted = 0 AND f2.deletedAt IS NULL
          WHERE kf2.kri_id = k.id AND kf2.deletedAt IS NULL
          ORDER BY kf2.function_id
        ) fkf(name)
        WHERE k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
          {function_filter}
        GROUP BY k.code, k.kriName, ISNULL(COALESCE(frel.name, fkf.name), 'Unknown'), TRY_CONVERT(int, kv.[year])
        ORDER BY {"ISNULL(COALESCE(frel.name, fkf.name), 'Unknown'), k.code, year DESC" if order_by_function_asc else "MAX(kv.createdAt) DESC, ISNULL(COALESCE(frel.name, fkf.name), 'Unknown')"}
        """
        return await self.execute_query(query)

    async def get_kri_counts_by_month_year(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return KRI counts by month and year"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))
        
        query = f"""
        SELECT  
          FORMAT(k.createdAt, 'MMM yyyy') AS month_year,
          DATENAME(month, k.createdAt) AS month_name, 
          YEAR(k.createdAt) AS year, 
          COUNT(*) AS kri_count 
        FROM Kris k 
        WHERE k.isDeleted = 0 
          AND k.deletedAt IS NULL {date_filter}
          {function_filter}
        GROUP BY FORMAT(k.createdAt, 'MMM yyyy'), YEAR(k.createdAt), DATENAME(month, k.createdAt), MONTH(k.createdAt) 
        ORDER BY YEAR(k.createdAt), MONTH(k.createdAt)
        """
        return await self.execute_query(query)

    async def get_kri_counts_by_frequency(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return KRI counts by frequency"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))
        
        query = f"""
        SELECT 
          ISNULL(k.frequency, 'Unknown') AS frequency, 
          COUNT(*) AS count 
        FROM Kris k
        WHERE k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
          {function_filter}
        GROUP BY ISNULL(k.frequency, 'Unknown')
        ORDER BY frequency ASC
        """
        return await self.execute_query(query)

    async def get_kri_risks_by_kri_name(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return risks linked to KRIs (count per KRI name)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))
        
        query = f"""
        SELECT
          k.kriName AS kriName,
          COUNT(*) AS count
        FROM Risks r
        INNER JOIN KriRisks kr ON r.id = kr.risk_id
          AND kr.deletedAt IS NULL
        INNER JOIN Kris k ON kr.kri_id = k.id
          AND k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
          {function_filter}
        WHERE r.isDeleted = 0
          AND r.deletedAt IS NULL
          AND k.kriName IS NOT NULL
        GROUP BY k.kriName
        ORDER BY k.kriName ASC
        """
        return await self.execute_query(query)

    async def get_kri_risk_relationships(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return KRI and Risk relationships (detailed list)"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))
        
        query = f"""
        SELECT
          k.code AS kri_code,
          ISNULL(COALESCE(frel.name, fkf.name), 'Unknown') AS function_name,
          k.kriName AS kri_name,
          r.code AS risk_code,
          r.name AS risk_name
        FROM Kris k
        LEFT JOIN KriFunctions kf ON k.id = kf.kri_id AND kf.deletedAt IS NULL
        LEFT JOIN Functions fkf ON fkf.id = kf.function_id AND fkf.isDeleted = 0 AND fkf.deletedAt IS NULL
        LEFT JOIN Functions frel ON frel.id = k.related_function_id AND frel.isDeleted = 0 AND frel.deletedAt IS NULL
        INNER JOIN KriRisks kr ON kr.kri_id = k.id
          AND kr.deletedAt IS NULL
        INNER JOIN Risks r ON r.id = kr.risk_id
          AND r.isDeleted = 0
          AND r.deletedAt IS NULL
        WHERE k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
          {function_filter}
        ORDER BY k.kriName, r.name
        """
        return await self.execute_query(query)

    async def get_kris_without_linked_risks(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return KRIs without linked risks"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))
        
        query = f"""
        SELECT
        k.code AS kriCode,
        ISNULL(COALESCE(frel.name, fkf.name), 'Unknown') AS function_name,
        k.kriName AS kriName
        FROM Kris AS k
        LEFT JOIN KriFunctions kf ON k.id = kf.kri_id AND kf.deletedAt IS NULL
        LEFT JOIN Functions fkf ON fkf.id = kf.function_id AND fkf.isDeleted = 0 AND fkf.deletedAt IS NULL
        LEFT JOIN Functions frel ON frel.id = k.related_function_id AND frel.isDeleted = 0 AND frel.deletedAt IS NULL
        WHERE k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
          {function_filter}
          AND NOT EXISTS (
            SELECT 1
            FROM KriRisks AS kr
            WHERE kr.kri_id = k.id
              AND kr.deletedAt IS NULL
          )
        ORDER BY k.kriName
        """
        return await self.execute_query(query)

    async def get_active_kris_details(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[str] = None,
        group_name: Optional[str] = None,
        function_id: Optional[str] = None,
        function_ids: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return active KRIs details"""
        date_filter = ""
        if start_date and end_date:
            date_filter = f"AND k.createdAt BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"AND k.createdAt >= '{start_date}'"
        elif end_date:
            date_filter = f"AND k.createdAt <= '{end_date}'"

        access = await self._get_user_function_access(user_id, group_name)
        function_filter = self._build_kri_function_filter("k", access, self._selected_function_ids(function_id, function_ids))
        
        query = f"""
        SELECT
          k.code AS code,
          ISNULL(COALESCE(frel.name, f.name), NULL) AS function_name,
          k.kriName AS kriName,
          CASE
            WHEN ISNULL(k.preparerStatus, '') <> 'sent' THEN 'Pending Preparer'
            WHEN ISNULL(k.preparerStatus, '') = 'sent' AND ISNULL(k.checkerStatus, '') <> 'approved' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'Pending Checker'
            WHEN ISNULL(k.checkerStatus, '') = 'approved' AND ISNULL(k.reviewerStatus, '') <> 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'Pending Reviewer'
            WHEN ISNULL(k.reviewerStatus, '') = 'sent' AND ISNULL(k.acceptanceStatus, '') <> 'approved' THEN 'Pending Acceptance'
            WHEN ISNULL(k.acceptanceStatus, '') = 'approved' THEN 'Approved'
            ELSE 'Unknown'
          END AS approved_status,
          u.name AS assignedPersonId,
          u2.name AS addedBy,
          k.status AS status,
          k.frequency AS frequency,
          k.threshold AS threshold,
          k.low_from AS low_from,
          k.medium_from AS medium_from,
          k.high_from AS high_from
        FROM Kris k
        LEFT JOIN KriFunctions kf ON k.id = kf.kri_id
          AND kf.deletedAt IS NULL
        LEFT JOIN Functions f ON f.id = kf.function_id
          AND f.isDeleted = 0
          AND f.deletedAt IS NULL
        LEFT JOIN Functions frel ON frel.id = k.related_function_id
          AND frel.isDeleted = 0
          AND frel.deletedAt IS NULL
        LEFT JOIN users u ON k.assignedPersonId = u.id
          AND u.deletedAt IS NULL
        LEFT JOIN users u2 ON k.addedBy = u2.id
          AND u2.deletedAt IS NULL
        WHERE k.isDeleted = 0
          AND k.deletedAt IS NULL {date_filter}
          {function_filter}
          AND k.status = 'active'
        """
        return await self.execute_query(query)