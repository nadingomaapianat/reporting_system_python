from fastapi import APIRouter, HTTPException, Request
import json
import logging
import traceback
from config import get_db_connection
from utils.dynamic_query import (
    QueryBuildError,
    SchemaResolver,
    condition_sql,
    join_type,
    params_or_none,
    preview_row_limit,
)
from utils.export_utils import hide_id_fields

logger = logging.getLogger(__name__)
router = APIRouter()




@router.post("/api/reports/dynamic-dashboard/save-chart")
async def save_dynamic_dashboard_chart(request: Request):
    """
    Save a chart configuration for the dynamic dashboard.
    Persists configuration in dynamic_dashboard_charts table.
    Expects JSON body: { title?, chartType?, tables?, joins?, columns?, ... }
    """
    logger.info("save-chart: request received")
    try:
        try:
            body = await request.json()
            logger.info("save-chart: body parsed ok")
        except Exception as e:
            logger.exception("save-chart: invalid or missing JSON body: %s", e)
            raise HTTPException(
                status_code=400,
                detail="Request body must be valid JSON (e.g. { \"title\": \"...\", \"chartType\": \"bar\", ... }).",
            )
        if body is None or not isinstance(body, dict):
            raise HTTPException(status_code=400, detail="Request body must be a JSON object.")

        title = (body.get("title") or "Transaction Chart").strip()
        chart_type = (body.get("chartType") or "bar").strip()
        config = {
            "tables": body.get("tables") or [],
            "joins": body.get("joins") or [],
            "columns": body.get("columns") or [],
            "whereConditions": body.get("whereConditions") or [],
            "timeFilter": body.get("timeFilter"),
            "xKey": body.get("xKey"),
            "yKey": body.get("yKey"),
            "visibleColumns": body.get("visibleColumns") or [],
        }

        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            # Create table if not exists (column is chart_config to match existing DB schema)
            cursor.execute(
                """
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dynamic_dashboard_charts' AND xtype='U')
                CREATE TABLE dynamic_dashboard_charts (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    title NVARCHAR(255) NOT NULL,
                    chart_type NVARCHAR(20) NOT NULL,
                    chart_config NVARCHAR(MAX) NOT NULL,
                    created_at DATETIME2 DEFAULT GETDATE()
                );
                """
            )
            conn.commit()

            # Ensure chart_config column exists (migrate table created by older schema with 'config')
            cursor.execute(
                """
                IF EXISTS (SELECT * FROM sysobjects WHERE name='dynamic_dashboard_charts' AND xtype='U')
                AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('dynamic_dashboard_charts') AND name = 'chart_config')
                ALTER TABLE dynamic_dashboard_charts ADD chart_config NVARCHAR(MAX) NOT NULL DEFAULT '{}';
                """
            )
            conn.commit()

            # Use whichever config column exists (chart_config preferred; fallback to config)
            cursor.execute(
                """
                SELECT name FROM sys.columns
                WHERE object_id = OBJECT_ID('dynamic_dashboard_charts') AND name IN ('chart_config', 'config')
                """
            )
            config_columns = [r[0] for r in cursor.fetchall() or []]
            config_col = "chart_config" if "chart_config" in config_columns else ("config" if "config" in config_columns else "chart_config")

            config_json = json.dumps(config)
            cursor.execute(
                f"""
                INSERT INTO dynamic_dashboard_charts (title, chart_type, {config_col})
                VALUES (%s, %s, %s)
                """,
                (title, chart_type, config_json),
            )
            # Get identity in same scope as INSERT, before commit (SCOPE_IDENTITY() can be NULL after commit)
            cursor.execute("SELECT SCOPE_IDENTITY() AS id")
            row = cursor.fetchone()
            conn.commit()

            new_id = row[0] if row and row[0] is not None else None
            if new_id is not None:
                try:
                    new_id = int(new_id) if isinstance(new_id, int) else int(float(new_id))
                except (TypeError, ValueError):
                    new_id = None
            # Always return 200 when insert succeeded; id may be None if SCOPE_IDENTITY() was unavailable
            return {"success": True, "id": new_id}
        finally:
            cursor.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        import sys
        print(f"[save-chart] ERROR: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
        traceback.print_exc()
        logger.exception("save-chart: %s", e)
        raise HTTPException(status_code=500, detail="Failed to save chart")


@router.post("/api/reports/dynamic/preview")
async def preview_dynamic_report(request: Request):
    """
    Preview dynamic report data (transactions) without generating a file.
    Returns JSON: { success, columns, rows } where rows is a list of dicts.
    """
    try:
        body = await request.json()
        tables = body.get('tables', [])
        joins = body.get('joins', [])
        columns = body.get('columns', [])
        where_conditions = body.get('whereConditions', [])
        time_filter = body.get('timeFilter')
        preview_limit = preview_row_limit(body.get('previewLimit'))

        if not tables or not columns:
            raise HTTPException(status_code=400, detail="Tables and columns are required")

        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            try:
                sql_query, sql_params = build_dynamic_sql_query(cursor, tables, joins, columns, where_conditions, time_filter)
            except QueryBuildError as e:
                raise HTTPException(status_code=400, detail=str(e))
            cursor.execute(sql_query, sql_params)
            # Fetch limited number of rows for preview
            rows = cursor.fetchmany(preview_limit)

            json_rows = []
            for row in rows:
                rec = {}
                for idx, col_name in enumerate(columns):
                    rec[col_name] = str(row[idx]) if idx < len(row) and row[idx] is not None else ''
                json_rows.append(rec)

            columns, json_rows = hide_id_fields(columns, json_rows)  # no ids on screen
            return {
                "success": True,
                "columns": columns,
                "rows": json_rows,
            }
        finally:
            cursor.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to preview dynamic report")


def build_dynamic_sql_query(cursor, tables, joins, columns, where_conditions, time_filter):
    """
    Build the dynamic report query. Returns (sql, params) for cursor.execute.
    Table/column names must exist in the database schema; values are bound parameters.
    Raises QueryBuildError for anything else.
    """
    db = SchemaResolver(cursor)
    tables = list(tables or [])
    first = tables[0]
    params = []

    # Start with SELECT clause (bare columns belong to the first table)
    select_columns = [db.column_ref(col, tables, first, qualify_bare=True) for col in columns]
    query = f"SELECT {', '.join(select_columns)}"

    # Add FROM clause
    query += f" FROM {db.from_ref(first)}"

    # Add JOINs
    for join in joins or []:
        if join.get('leftTable') and join.get('rightTable') and join.get('leftColumn') and join.get('rightColumn'):
            query += (
                f" {join_type(join.get('type', 'INNER'))} JOIN {db.from_ref(join['rightTable'])}"
                f" ON {db.column(join['leftTable'], join['leftColumn'])}"
                f" = {db.column(join['rightTable'], join['rightColumn'])}"
            )

    # Add WHERE conditions (bare columns stay unqualified, as before)
    where_clauses = []
    for condition in where_conditions or []:
        if condition.get('column') and condition.get('operator') and condition.get('value'):
            col_sql = db.column_ref(condition['column'], tables, first, qualify_bare=False)
            clause, values = condition_sql(col_sql, condition['operator'], condition['value'])
            where_clauses.append(clause)
            params.extend(values)

    # Add time filter
    if time_filter and time_filter.get('column') and time_filter.get('startDate') and time_filter.get('endDate'):
        col_sql = db.column_ref(time_filter['column'], tables, first, qualify_bare=False)
        where_clauses.append(f"{col_sql} BETWEEN %s AND %s")
        params.extend([str(time_filter['startDate']), str(time_filter['endDate'])])

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    return query, params_or_none(params)
