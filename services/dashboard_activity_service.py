"""
Dashboard Activity Service for tracking user dashboard visits
"""
import pyodbc
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
from config import get_database_connection_string

class DashboardActivityService:
    """Service for managing dashboard activity tracking"""
    
    def __init__(self):
        self.connection_string = get_database_connection_string()
    
    async def execute_query(self, query: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results"""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, self._execute_sync_query, query, params)
            return result
        except Exception as e:
            print(f"Database query error: {str(e)}")
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
                
                # For CREATE TABLE, UPDATE, INSERT queries, there are no results to fetch
                query_upper = query.strip().upper()
                if (query_upper.startswith('CREATE') or 
                    query_upper.startswith('IF NOT EXISTS') or
                    query_upper.startswith('UPDATE') or
                    query_upper.startswith('INSERT')):
                    return []
                
                # Get column names
                columns = [column[0] for column in cursor.description] if cursor.description else []
                
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
            print(f"Database connection error: {str(e)}")
            return []
    
    async def create_activity_table(self):
        """Create dashboard_activity table if it doesn't exist"""
        query = """
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dashboard_activity' AND xtype='U')
        CREATE TABLE dashboard_activity (
            id INT IDENTITY(1,1) PRIMARY KEY,
            dashboard_id NVARCHAR(50) NOT NULL,
            user_id NVARCHAR(100) DEFAULT 'default_user',
            last_seen DATETIME2 NOT NULL DEFAULT GETDATE(),
            card_count INT DEFAULT 0,
            created_at DATETIME2 DEFAULT GETDATE(),
            updated_at DATETIME2 DEFAULT GETDATE()
        )
        """
        try:
            await self.execute_query(query)
            print("Dashboard activity table created or already exists")
        except Exception as e:
            print(f"Error creating dashboard_activity table: {str(e)}")
    
    async def get_dashboard_activities(self, user_id: str = 'default_user') -> List[Dict[str, Any]]:
        """Get all dashboard activities for a user"""
        query = """
        SELECT 
            dashboard_id,
            last_seen,
            card_count,
            updated_at
        FROM dashboard_activity 
        WHERE user_id = ?
        ORDER BY last_seen DESC
        """
        return await self.execute_query(query, [user_id])
    
    async def update_dashboard_activity(self, dashboard_id: str, user_id: str = 'default_user', card_count: int = 0) -> Dict[str, Any]:
        """Update or create dashboard activity record"""
        try:
            # First check if record exists
            check_query = """
            SELECT id FROM dashboard_activity 
            WHERE dashboard_id = ? AND user_id = ?
            """
            existing = await self.execute_query(check_query, [dashboard_id, user_id])
            
            if existing:
                # Update existing record
                update_query = """
                UPDATE dashboard_activity 
                SET last_seen = GETUTCDATE(), 
                    card_count = ?, 
                    updated_at = GETUTCDATE()
                WHERE dashboard_id = ? AND user_id = ?
                """
                await self.execute_query(update_query, [card_count, dashboard_id, user_id])
            else:
                # Insert new record
                insert_query = """
                INSERT INTO dashboard_activity (dashboard_id, user_id, last_seen, card_count)
                VALUES (?, ?, GETUTCDATE(), ?)
                """
                await self.execute_query(insert_query, [dashboard_id, user_id, card_count])
            
            # Return the updated record
            get_query = """
            SELECT 
                dashboard_id,
                last_seen,
                card_count,
                updated_at
            FROM dashboard_activity 
            WHERE dashboard_id = ? AND user_id = ?
            """
            result = await self.execute_query(get_query, [dashboard_id, user_id])
            return result[0] if result else {}
        except Exception as e:
            print(f"Error updating dashboard activity: {str(e)}")
            return {}
    
    async def get_dashboard_activity(self, dashboard_id: str, user_id: str = 'default_user') -> Optional[Dict[str, Any]]:
        """Get specific dashboard activity"""
        query = """
        SELECT 
            dashboard_id,
            last_seen,
            card_count,
            updated_at
        FROM dashboard_activity 
        WHERE dashboard_id = ? AND user_id = ?
        """
        result = await self.execute_query(query, [dashboard_id, user_id])
        return result[0] if result else None
    
    async def initialize_default_activities(self):
        """Initialize default dashboard activities if they don't exist"""
        default_dashboards = [
            {'dashboard_id': 'controls', 'card_count': 6},
            {'dashboard_id': 'incidents', 'card_count': 7},
            {'dashboard_id': 'kris', 'card_count': 5},
            {'dashboard_id': 'risks', 'card_count': 8}
        ]
        
        for dashboard in default_dashboards:
            existing = await self.get_dashboard_activity(dashboard['dashboard_id'])
            if not existing:
                await self.update_dashboard_activity(
                    dashboard['dashboard_id'], 
                    'default_user', 
                    dashboard['card_count']
                )
                print(f"Initialized activity for {dashboard['dashboard_id']}")
