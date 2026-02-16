"""
API routes for the reporting system
"""
import asyncio
import json
from datetime import datetime
import os
import httpx
from fastapi import APIRouter, Query, HTTPException, Request, UploadFile, File, Form
from fastapi.responses import Response, FileResponse, StreamingResponse
from typing import Optional

from services import APIService, PDFService, ExcelService, ControlService, IncidentService
from services.bank_check_service import BankCheckService
from services.enhanced_bank_check_service import EnhancedBankCheckService
# Dashboard activity service moved to Node.js backend (NestJS)
DashboardActivityService = None  # type: ignore
from utils.export_utils import get_default_header_config
from models import ExportRequest, ExportResponse
from routes.route_utils import (
    write_debug, 
    parse_header_config, 
    merge_header_config, 
    convert_to_boolean, 
    save_and_log_export,
    build_dynamic_sql_query,
    generate_excel_report,
    generate_word_report,
    generate_pdf_report,
    extract_user_and_function_params
)

# Initialize services
api_service = APIService()
pdf_service = PDFService()
excel_service = ExcelService()
control_service = ControlService()
incident_service = IncidentService() if IncidentService else None
dashboard_activity_service = DashboardActivityService() if DashboardActivityService else None
bank_check_service = BankCheckService()
enhanced_bank_check_service = EnhancedBankCheckService()

# db_service points to control_service for backward compatibility
db_service = control_service

# Create router
router = APIRouter()





# -------------------------------
# Export logging (Excel/PDF) APIs
# -------------------------------
@router.post("/api/exports/log")
async def log_report_export(request: Request):
    """Log an export (excel/pdf/word/zip) with title and src for later download listing."""
    try:
        import pyodbc
        from config import get_database_connection_string

        body = await request.json()
        title = (body.get("title") or "").strip() or "Untitled Report"
        src = (body.get("src") or "").strip()
        fmt = (body.get("format") or "").strip().lower() or "unknown"
        dashboard = (body.get("dashboard") or "").strip() or "general"

        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        try:
            # Ensure table exists and has created_by column
            cursor.execute(
                """
                IF NOT EXISTS (
                  SELECT * FROM INFORMATION_SCHEMA.TABLES 
                  WHERE TABLE_NAME = 'report_exports' AND TABLE_SCHEMA='dbo'
                )
                BEGIN
                  CREATE TABLE dbo.report_exports (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    title NVARCHAR(255) NOT NULL,
                    src NVARCHAR(1024) NULL,
                    format NVARCHAR(20) NOT NULL,
                    dashboard NVARCHAR(100) NULL,
                    created_by NVARCHAR(255) NULL,
                    created_at DATETIME2 DEFAULT GETDATE()
                  )
                END
                """
            )
            conn.commit()

            # Add created_by column if it doesn't exist (for existing tables)
            try:
                cursor.execute(
                    """
                    IF NOT EXISTS (
                      SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'report_exports' AND COLUMN_NAME = 'created_by'
                    )
                    BEGIN
                      ALTER TABLE dbo.report_exports ADD created_by NVARCHAR(255) NULL
                    END
                    """
                )
                conn.commit()
            except Exception:
                pass  # Column might already exist

            created_by = (body.get("created_by") or "").strip() or "System"
            # Determine type based on dashboard
            export_type = "transaction"  # Default
            if dashboard and dashboard.lower() in ['incidents', 'kris', 'risks', 'controls']:
                export_type = "dashboard"
            
            cursor.execute(
                """
                INSERT INTO dbo.report_exports (title, src, format, dashboard, type, created_by)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (title, src, fmt, dashboard, export_type, created_by)
            )
            conn.commit()

            new_id = cursor.execute("SELECT @@IDENTITY").fetchone()[0]
            return {"success": True, "id": int(new_id)}
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        return {"success": False, "error": str(e)}

@router.get("/api/exports/recent")
async def list_recent_exports(request: Request, limit: int = Query(50), page: int = Query(1), search: str = Query("")):
    """Return recent report exports (newest first) with simple pagination and dashboard filtering.
    Filters by user_id or users in the same group."""
    try:
        import pyodbc
        from config import get_database_connection_string

        # Extract user_id and group_name from token
        user_id, group_name, _ = extract_user_and_function_params(request)

        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                IF NOT EXISTS (
                  SELECT * FROM INFORMATION_SCHEMA.TABLES 
                  WHERE TABLE_NAME = 'report_exports' AND TABLE_SCHEMA='dbo'
                )
                BEGIN
                  CREATE TABLE dbo.report_exports (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    title NVARCHAR(255) NOT NULL,
                    src NVARCHAR(1024) NULL,
                    format NVARCHAR(20) NOT NULL,
                    dashboard NVARCHAR(100) NULL,
                    type NVARCHAR(50) NULL,
                    created_by NVARCHAR(255) NULL,
                    created_at DATETIME2 DEFAULT GETDATE()
                  )
                END
                """
            )
            conn.commit()
            
            # Add type column if it doesn't exist
            try:
                cursor.execute(
                    """
                    IF NOT EXISTS (
                      SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'report_exports' AND COLUMN_NAME = 'type'
                    )
                    BEGIN
                      ALTER TABLE dbo.report_exports ADD type NVARCHAR(50) NULL
                    END
                    """
                )
                conn.commit()
            except Exception:
                pass
            
            # Add created_by column if it doesn't exist
            try:
                cursor.execute(
                    """
                    IF NOT EXISTS (
                      SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'report_exports' AND COLUMN_NAME = 'created_by'
                    )
                    BEGIN
                      ALTER TABLE dbo.report_exports ADD created_by NVARCHAR(255) NULL
                    END
                    """
                )
                conn.commit()
            except Exception:
                pass
            
            # Add created_by_user_id column if it doesn't exist
            try:
                cursor.execute(
                    """
                    IF NOT EXISTS (
                      SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                      WHERE TABLE_NAME = 'report_exports' AND COLUMN_NAME = 'created_by_user_id'
                    )
                    BEGIN
                      ALTER TABLE dbo.report_exports ADD created_by_user_id NVARCHAR(255) NULL
                    END
                    """
                )
                conn.commit()
            except Exception:
                pass

            # Build search condition
            search_condition = ""
            search_params = []
            conditions = []
            
            # Filter by user_id or users with the same functions
            if user_id:
                # Trim user_id to handle spaces
                user_id = str(user_id).strip() if user_id else None
                
                # If user is super admin, show all reports
                if group_name == 'super_admin_':
                    # Super admin sees all reports - no filter needed
                    pass
                else:
                    # Get current user's functions
                    try:
                        cursor.execute(
                            """
                            SELECT DISTINCT LTRIM(RTRIM(uf.functionId))
                            FROM dbo.UserFunction uf
                            JOIN dbo.Functions f ON LTRIM(RTRIM(f.id)) = LTRIM(RTRIM(uf.functionId))
                            WHERE uf.userId = ? 
                              AND uf.deletedAt IS NULL
                              AND f.isDeleted = 0
                              AND f.deletedAt IS NULL
                            """,
                            (user_id,)
                        )
                        user_functions = [str(row[0]).strip() for row in cursor.fetchall() if row[0] and str(row[0]).strip()]
                        write_debug(f"[List Exports] User {user_id} has functions: {user_functions}")
                    except Exception as e:
                        write_debug(f"[List Exports] Error fetching user functions: {e}")
                        import traceback
                        write_debug(f"[List Exports] Traceback: {traceback.format_exc()}")
                        user_functions = []
                    
                    # Get all user_ids that share at least one function with the current user
                    shared_user_ids = []
                    if user_functions:
                        try:
                            # Build placeholders for function IDs (safe parameterized query)
                            func_placeholders = ','.join(['?' for _ in user_functions])
                            query = f"""
                                SELECT DISTINCT CAST(uf.userId AS NVARCHAR(255))
                                FROM dbo.UserFunction uf
                                JOIN dbo.Functions f ON LTRIM(RTRIM(f.id)) = LTRIM(RTRIM(uf.functionId))
                                WHERE LTRIM(RTRIM(uf.functionId)) IN ({func_placeholders})
                                  AND uf.deletedAt IS NULL
                                  AND f.isDeleted = 0
                                  AND f.deletedAt IS NULL
                                  AND uf.userId IS NOT NULL
                            """
                            cursor.execute(query, user_functions)
                            shared_user_ids = [str(row[0]).strip() for row in cursor.fetchall() if row[0] and str(row[0]).strip()]
                            write_debug(f"[List Exports] Users sharing functions with {user_id}: {shared_user_ids} (total: {len(shared_user_ids)})")
                        except Exception as e:
                            write_debug(f"[List Exports] Error fetching shared users: {e}")
                            import traceback
                            write_debug(f"[List Exports] Traceback: {traceback.format_exc()}")
                            shared_user_ids = []
                    
                    # Always include the current user_id (even if they have no functions or no shared users)
                    if user_id:
                        user_id_trimmed = str(user_id).strip()
                        if user_id_trimmed and user_id_trimmed not in shared_user_ids:
                            shared_user_ids.append(user_id_trimmed)
                            write_debug(f"[List Exports] Added current user_id to shared list: {user_id_trimmed}")
                    
                    # Filter by user_id or users with the same functions
                    if shared_user_ids:
                        placeholders = ','.join(['?' for _ in shared_user_ids])
                        conditions.append(f"created_by_user_id IN ({placeholders})")
                        search_params.extend(shared_user_ids)
                        write_debug(f"[List Exports] Filtering by user_ids: {shared_user_ids}")
                    else:
                        # If no shared users found, only show current user's reports
                        conditions.append("created_by_user_id = ?")
                        search_params.append(user_id)
                        write_debug(f"[List Exports] No shared users found, filtering by current user only: {user_id}")
            else:
                # If no user_id, show only reports with no user_id (legacy reports)
                conditions.append("(created_by_user_id IS NULL OR created_by_user_id = '')")
            
            # Handle title search
            if search and search.strip():
                conditions.append("title LIKE ?")
                search_params.append(f"%{search.strip()}%")
            
            # Handle type filter (prefer type over dashboard filter)
            type_filter = request.query_params.get('type', None)
            if type_filter:
                if type_filter.lower() == 'dashboard':
                    conditions.append("type = 'dashboard'")
                elif type_filter.lower() == 'transaction':
                    conditions.append("(type = 'transaction' OR type IS NULL)")
            
            # Fallback to dashboard filter if type is not provided (for backward compatibility)
            elif request.query_params.get('dashboard', None):
                dashboard_filter = request.query_params.get('dashboard')
                dashboard_list = [d.strip() for d in dashboard_filter.split(',')]
                if len(dashboard_list) == 1 and dashboard_list[0] == 'transaction':
                    # Transaction reports: exclude dashboard reports or explicitly transaction
                    conditions.append("(type = 'transaction' OR type IS NULL OR type != 'dashboard')")
                elif len(dashboard_list) > 0:
                    # Dashboard reports: filter by specific dashboard types
                    conditions.append("type = 'dashboard'")
            
            # Combine all conditions
            if conditions:
                search_condition = "WHERE " + " AND ".join(conditions)

            # Total count with search and filters
            count_query = f"SELECT COUNT(*) FROM dbo.report_exports {search_condition}"
            cursor.execute(count_query, search_params)
            total_count = int(cursor.fetchone()[0])

            # Pagination via OFFSET/FETCH
            safe_limit = max(1, min(200, int(limit)))
            safe_page = max(1, int(page))
            offset = (safe_page - 1) * safe_limit
            
            select_query = f"""
                SELECT id, title, src, format, dashboard, type, created_by, created_by_user_id, created_at
                FROM dbo.report_exports
                {search_condition}
                ORDER BY created_at DESC, id DESC
                OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """
            cursor.execute(select_query, search_params + [offset, safe_limit])
            rows = cursor.fetchall()
            exports = [
                {
                    "id": int(r[0]),
                    "title": r[1],
                    "src": r[2],
                    "format": r[3],
                    "dashboard": r[4],
                    "type": r[5] if len(r) > 5 else None,
                    "created_by": r[6] if len(r) > 6 else "System",
                    "created_by_user_id": r[7] if len(r) > 7 else None,
                    "created_at": r[8].isoformat() if len(r) > 8 and hasattr(r[8], 'isoformat') else (str(r[8]) if len(r) > 8 else "")
                }
                for r in rows
            ]
            return {
                "success": True,
                "exports": exports,
                "pagination": {
                    "page": safe_page,
                    "limit": safe_limit,
                    "total": total_count,
                    "totalPages": (total_count + safe_limit - 1) // safe_limit,
                    "hasNext": offset + safe_limit < total_count,
                    "hasPrev": safe_page > 1
                }
            }
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        return {"success": False, "error": str(e), "exports": [], "pagination": {}}

@router.delete("/api/exports/{export_id}")
async def delete_export(export_id: int):
    """Delete an export row and its file if present"""
    try:
        import pyodbc
        from config import get_database_connection_string
        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT src FROM dbo.report_exports WHERE id = ?", export_id)
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Export not found")
            src = row[0]

            # Delete DB row
            cursor.execute("DELETE FROM dbo.report_exports WHERE id = ?", export_id)
            conn.commit()

            # Delete file if exists
            if src:
                try:
                    import os
                    file_path = src if os.path.isabs(src) else os.path.join(os.getcwd(), src)
                    if os.path.exists(file_path):
                        os.remove(file_path)
                except Exception as fe:
                    return {"success": True, "deleted": True, "fileDeleted": False, "warning": str(fe)}

            return {"success": True, "deleted": True, "fileDeleted": True}
        finally:
            cursor.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        return {"success": False, "error": str(e)}

@router.post("/api/reports/dynamic")
async def generate_dynamic_report(request: Request):
    """Generate dynamic report based on table selection, joins, columns, and conditions"""
    try:
        body = await request.json()
        tables = body.get('tables', [])
        joins = body.get('joins', [])
        columns = body.get('columns', [])
        where_conditions = body.get('whereConditions', [])
        time_filter = body.get('timeFilter')
        format_type = body.get('format', 'excel')
        
        write_debug(f"[Dynamic Report] Request received: tables={tables}, columns={columns}, format={format_type}")
        
        if not tables or not columns:
            raise HTTPException(status_code=400, detail="Tables and columns are required")
        
        # Build SQL query
        try:
            sql_query = build_dynamic_sql_query(tables, joins, columns, where_conditions, time_filter)
            write_debug(f"[Dynamic Report] SQL query built: {sql_query[:200]}...")
        except Exception as sql_err:
            write_debug(f"[Dynamic Report] SQL query build failed: {str(sql_err)}")
            raise HTTPException(status_code=400, detail=f"Failed to build SQL query: {str(sql_err)}")
        
        # Execute query and get data
        import pyodbc
        from config import get_database_connection_string
        
        try:
            connection_string = get_database_connection_string()
            conn = pyodbc.connect(connection_string)
            cursor = conn.cursor()
        except Exception as db_err:
            write_debug(f"[Dynamic Report] Database connection failed: {str(db_err)}")
            raise HTTPException(status_code=500, detail=f"Database connection failed: {str(db_err)}")
        
        try:
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            write_debug(f"[Dynamic Report] Query executed, fetched {len(rows)} rows")
            
            # Convert to list of dicts keyed by column name
            data_rows: list[dict] = []
            for row in rows:
                rec = {}
                for idx, col_name in enumerate(columns):
                    rec[col_name] = str(row[idx]) if idx < len(row) and row[idx] is not None else ''
                data_rows.append(rec)
            
            # Add index column at the beginning for all dynamic reports
            index_column_name = "#"
            columns_with_index = [index_column_name] + columns
            
            # Prepend index to each row
            indexed_rows: list[dict] = []
            for idx, rec in enumerate(data_rows, start=1):
                new_rec = {index_column_name: str(idx)}
                new_rec.update(rec)
                indexed_rows.append(new_rec)
            
            # Use the modified columns and data
            columns = columns_with_index
            data_rows = indexed_rows
            
            write_debug(f"[Dynamic Report] Added index column, total columns: {len(columns)}, total rows: {len(data_rows)}")
            
            # Get header configuration from request body
            header_config = body.get('headerConfig', {}) or {}
            from utils.export_utils import get_default_header_config
            default_config = get_default_header_config("dynamic")
            merged_config = {**default_config, **header_config}

            # If frontend sent chartConfig, convert it to chart_data for Excel/PDF
            chart_cfg = header_config.get('chartConfig') if isinstance(header_config, dict) else None
            if chart_cfg:
                x_key = chart_cfg.get('xKey')
                y_key = chart_cfg.get('yKey')
                chart_type = chart_cfg.get('type') or 'bar'
                if x_key and y_key:
                    from collections import defaultdict

                    def to_float_safe(val):
                        try:
                            # Remove commas commonly found in formatted numbers
                            return float(str(val).replace(',', ''))
                        except Exception:
                            return None

                    numeric_samples = [
                        to_float_safe(row.get(y_key))
                        for row in data_rows
                        if row.get(y_key) not in (None, '', ' ')
                    ]
                    y_is_numeric = any(v is not None for v in numeric_samples)

                    labels: list[str] = []
                    values: list[float] = []

                    if chart_type == 'pie' and not y_is_numeric:
                        # For pie with non-numeric Y, count occurrences of Y values
                        counts: dict[str, int] = defaultdict(int)
                        for row in data_rows:
                            y_val = str(row.get(y_key) or '').strip()
                            if y_val:
                                counts[y_val] += 1
                        labels = list(counts.keys())
                        values = [counts[l] for l in labels]
                    else:
                        # Aggregate by X; sum numeric Y or count rows if non-numeric
                        agg: dict[str, float] = defaultdict(float)
                        for row in data_rows:
                            x_val = str(row.get(x_key) or '').strip()
                            y_raw = row.get(y_key)
                            if not x_val or y_raw in (None, '', ' '):
                                continue
                            if y_is_numeric:
                                y_val = to_float_safe(y_raw)
                                if y_val is None:
                                    continue
                            else:
                                y_val = 1.0
                            agg[x_val] += y_val
                        labels = list(agg.keys())
                        values = [agg[l] for l in labels]

                    if labels and values:
                        merged_config['chart_type'] = chart_type
                        merged_config['chart_data'] = {
                            'labels': labels,
                            'values': values,
                        }
                        write_debug(f"[Dynamic Report] chart_data prepared with {len(labels)} labels for chart export")
            
            # Get export type from request (transaction or dashboard)
            export_type = body.get('type')
            
            # Generate report based on format
            report_content = None
            file_extension = format_type
            try:
                if format_type == 'excel':
                    write_debug(f"[Dynamic Report] Generating Excel report...")
                    report_content = generate_excel_report(columns, data_rows, merged_config)
                    file_extension = 'xlsx'
                elif format_type == 'word':
                    write_debug(f"[Dynamic Report] Generating Word report...")
                    report_content = generate_word_report(columns, data_rows, merged_config)
                    file_extension = 'docx'
                elif format_type == 'pdf':
                    write_debug(f"[Dynamic Report] Generating PDF report...")
                    report_content = generate_pdf_report(columns, data_rows, merged_config)
                    file_extension = 'pdf'
                else:
                    raise HTTPException(status_code=400, detail="Unsupported format")
                write_debug(f"[Dynamic Report] Report generated successfully, size: {len(report_content)} bytes")
            except Exception as gen_err:
                write_debug(f"[Dynamic Report] Report generation failed: {str(gen_err)}")
                import traceback
                write_debug(f"[Dynamic Report] Traceback: {traceback.format_exc()}")
                raise HTTPException(status_code=500, detail=f"Failed to generate report: {str(gen_err)}")
            
            # Save file and log to database
            try:
                created_by = request.headers.get('X-User-Name') or request.headers.get('Authorization') or "System"
                write_debug(f"[Dynamic Report] Saving export, type: {export_type}")
                
                # Create report name from table names
                # Join table names with underscore for filename (e.g., "Users_Orders_Products")
                # This will be formatted by save_and_log_export to create readable names
                table_names = '_'.join(tables) if tables else 'Dynamic_Report'
                write_debug(f"[Dynamic Report] Using table names for report: {table_names}")
                
                export_info = await save_and_log_export(
                    content=report_content,
                    file_extension=file_extension,
                    dashboard='transactions',  # Use 'transactions' instead of 'dynamic' for filename
                    card_type=table_names,  # Use table names as card_type for naming
                    header_config=merged_config,
                    created_by=created_by,
                    export_type=export_type,
                    request=request
                )
                write_debug(f"[Dynamic Report] Export saved: {export_info.get('relative_path')}")
            except Exception as save_err:
                write_debug(f"[Dynamic Report] Save failed: {str(save_err)}")
                import traceback
                write_debug(f"[Dynamic Report] Save traceback: {traceback.format_exc()}")
                # Continue even if save fails - still return the file
                export_info = {
                    'filename': f'dynamic_report.{file_extension}',
                    'relative_path': '',
                    'export_id': None
                }
            
            # Determine media type
            media_types = {
                'pdf': 'application/pdf',
                'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
            }
            media_type = media_types.get(file_extension, 'application/octet-stream')
            
            # Return file with headers
            return Response(
                content=report_content,
                media_type=media_type,
                headers={
                    'Content-Disposition': f'attachment; filename="{export_info["filename"]}"',
                    'X-Export-Src': export_info['relative_path'],
                    'X-Export-Id': str(export_info.get('export_id', ''))
                }
            )
                
        finally:
            cursor.close()
            conn.close()
            
    except HTTPException:
        raise
    except Exception as e:
        write_debug(f"[Dynamic Report] Unexpected error: {str(e)}")
        import traceback
        write_debug(f"[Dynamic Report] Full traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Failed to generate dynamic report: {str(e)}")


@router.get("/api/reports/dynamic-dashboard/charts")
async def get_dynamic_dashboard_charts():
    """
    List saved dynamic dashboard charts created from Transaction Reports.
    """
    try:
        import pyodbc
        import json
        from config import get_database_connection_string

        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        try:
            # Ensure table exists (column chart_config to match save-chart and existing DB)
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

            cursor.execute(
                """
                SELECT id, title, chart_type, chart_config, created_at
                FROM dynamic_dashboard_charts
                ORDER BY created_at DESC, id DESC
                """
            )
            rows = cursor.fetchall()
            charts = []
            for r in rows:
                cfg = {}
                try:
                    cfg = json.loads(r[3]) if r[3] else {}
                except Exception:
                    cfg = {}
                charts.append(
                    {
                        "id": int(r[0]),
                        "title": r[1],
                        "chartType": r[2],
                        "config": cfg,
                        "createdAt": r[4].isoformat() if len(r) > 4 and hasattr(r[4], "isoformat") else None,
                    }
                )
            return {"success": True, "charts": charts}
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        return {"success": False, "error": str(e), "charts": []}


@router.delete("/api/reports/dynamic-dashboard/charts/{chart_id}")
async def delete_dynamic_dashboard_chart(chart_id: int):
    """
    Delete a saved dynamic dashboard chart.
    """
    try:
        import pyodbc
        from config import get_database_connection_string

        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM dynamic_dashboard_charts WHERE id = ?", chart_id)
            conn.commit()
            deleted = cursor.rowcount > 0
            return {"success": True, "deleted": deleted}
        finally:
            cursor.close()
            conn.close()
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/api/exports/{export_id}/download")
async def download_export(export_id: int):
    """Download a saved export file by ID"""
    try:
        import pyodbc
        from config import get_database_connection_string
        import os
        
        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT src, format FROM dbo.report_exports WHERE id = ?", export_id)
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Export not found")
            
            src = row[0]
            fmt = row[1] or 'pdf'
            
            if not src:
                raise HTTPException(status_code=404, detail="Export file not found")
            
            # Build file path
            base_dir = os.path.dirname(os.path.dirname(__file__))
            file_path = os.path.join(base_dir, src)
            
            if not os.path.exists(file_path):
                raise HTTPException(status_code=404, detail="Export file not found on disk")
            
            # Determine media type
            media_types = {
                'pdf': 'application/pdf',
                'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'xls': 'application/vnd.ms-excel',
                'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                'doc': 'application/msword'
            }
            media_type = media_types.get(fmt.lower(), 'application/octet-stream')
            
            return FileResponse(
                file_path,
                media_type=media_type,
                filename=os.path.basename(file_path)
            )
        finally:
            cursor.close()
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to download export: {str(e)}")


@router.post("/api/reports/schedule")
async def save_report_schedule(request: Request):
    """Save scheduled report configuration"""
    try:
        body = await request.json()
        report_config = body.get('reportConfig', {})
        schedule = body.get('schedule', {})
        
        # Save to database (you can create a scheduled_reports table)
        import pyodbc
        from config import get_database_connection_string
        
        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        try:
            # Create table if not exists
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='scheduled_reports' and xtype='U')
                CREATE TABLE scheduled_reports (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    report_config NVARCHAR(MAX) NOT NULL,
                    schedule_config NVARCHAR(MAX) NOT NULL,
                    is_active BIT DEFAULT 1,
                    created_at DATETIME2 DEFAULT GETDATE()
                );
            """)
            conn.commit()
            
            # Insert schedule
            import json
            cursor.execute("""
                INSERT INTO scheduled_reports (report_config, schedule_config)
                VALUES (?, ?)
            """, json.dumps(report_config), json.dumps(schedule))
            conn.commit()
            
            return {"success": True, "message": "Schedule saved successfully"}
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save schedule: {str(e)}")






