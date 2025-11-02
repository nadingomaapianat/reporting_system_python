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
from routes.route_utils import write_debug, parse_header_config, merge_header_config, convert_to_boolean

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
    """Return recent report exports (newest first) with simple pagination and dashboard filtering."""
    try:
        import pyodbc
        from config import get_database_connection_string

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

            # Build search condition
            search_condition = ""
            search_params = []
            conditions = []
            
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
                SELECT id, title, src, format, dashboard, type, created_by, created_at
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
                    "created_at": r[7].isoformat() if len(r) > 7 and hasattr(r[7], 'isoformat') else (str(r[7]) if len(r) > 7 else "")
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
        
        if not tables or not columns:
            raise HTTPException(status_code=400, detail="Tables and columns are required")
        
        # Build SQL query
        sql_query = build_dynamic_sql_query(tables, joins, columns, where_conditions, time_filter)
        
        # Execute query and get data
        import pyodbc
        from config import get_database_connection_string
        
        connection_string = get_database_connection_string()
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        try:
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries
            data_rows = []
            for row in rows:
                data_rows.append([str(cell) if cell is not None else '' for cell in row])
            
            # Get header configuration from request body
            header_config = body.get('headerConfig', {})
            if header_config:
                from export_utils import get_default_header_config
                default_config = get_default_header_config("dynamic")
                merged_config = {**default_config, **header_config}
            else:
                from export_utils import get_default_header_config
                merged_config = get_default_header_config("dynamic")
            
            # Generate report based on format
            if format_type == 'excel':
                return generate_excel_report(columns, data_rows, merged_config)
            elif format_type == 'word':
                return generate_word_report(columns, data_rows, merged_config)
            elif format_type == 'pdf':
                return generate_pdf_report(columns, data_rows, merged_config)
            else:
                raise HTTPException(status_code=400, detail="Unsupported format")
                
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate dynamic report: {str(e)}")


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






