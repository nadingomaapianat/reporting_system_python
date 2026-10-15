"""
Table list for the Transaction Report Builder (ReportGenerator loads it via
getEnhancedBankCheckTables). The other bank-check endpoints were unused and removed.
"""
from fastapi import APIRouter

# Create router
router = APIRouter()


@router.get("/api/reports/enhanced-bank-check/tables")
async def get_database_tables():
    """Get all tables from SQL Server database"""
    try:
        from config import get_db_connection
        
        print(f"[EnhancedBankCheck] Connecting to SQL Server...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Get all tables from the database
            tables_query = """
                SELECT 
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    TABLE_TYPE
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_SCHEMA, TABLE_NAME;
            """
            
            cursor.execute(tables_query)
            tables_result = cursor.fetchall()
            
            tables = []
            for table in tables_result:  # Get ALL tables from database
                table_name = table[1]  # TABLE_NAME
                schema_name = table[0]  # TABLE_SCHEMA
                full_table_name = f"{schema_name}.{table_name}" if schema_name != 'dbo' else table_name
                
                # Get columns for each table
                columns_query = """
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        IS_NULLABLE,
                        COLUMN_DEFAULT
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION;
                """
                
                cursor.execute(columns_query, (schema_name, table_name))
                columns_result = cursor.fetchall()
                column_names = [col[0] for col in columns_result]  # COLUMN_NAME
                
                tables.append({
                    "id": f"{schema_name}_{table_name}",
                    "name": table_name,
                    "schema": schema_name,
                    "full_name": full_table_name,
                    "fields": column_names,
                    "columns": column_names,  # Add columns for frontend compatibility
                    "field_count": len(column_names)
                })
            
            print(f"[EnhancedBankCheck] Found {len(tables)} tables in SQL Server database")
            
            return {
                "success": True,
                "tables": tables,
                "count": len(tables)
            }
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"[EnhancedBankCheck] SQL Server connection error: {e}")
        # Fallback to sample data if database connection fails
        sample_tables = [
            {
                "id": "dbo_bank_checks",
                "name": "bank_checks",
                "schema": "dbo",
                "full_name": "bank_checks",
                "fields": ["id", "bank_name", "date", "payee_name", "amount_value", "amount_text", "currency", "status_note", "issuer_signature", "created_at"],
                "columns": ["id", "bank_name", "date", "payee_name", "amount_value", "amount_text", "currency", "status_note", "issuer_signature", "created_at"],
                "field_count": 10
            },
            {
                "id": "dbo_customer_records", 
                "name": "customer_records",
                "schema": "dbo",
                "full_name": "customer_records",
                "fields": ["id", "customer_name", "project_name", "building_number", "apartment_number", "check_number", "due_date", "collection_date", "remaining_days", "collection_status", "total_receivables", "created_at"],
                "columns": ["id", "customer_name", "project_name", "building_number", "apartment_number", "check_number", "due_date", "collection_date", "remaining_days", "collection_status", "total_receivables", "created_at"],
                "field_count": 12
            }
        ]
        
        return {
            "success": True,
            "tables": sample_tables,
            "count": len(sample_tables),
            "note": "Using fallback data - SQL Server connection failed",
            "error": str(e)
        }

