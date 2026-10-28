#!/usr/bin/env python3
"""
Database Setup Script for Enhanced Bank Check
This script helps you configure the database connection
"""

import os
import asyncio
import asyncpg
from config.env import require_env

def create_env_file():
    """Create a blank .env skeleton (never overwrites an existing one; values come from you, not the source)."""
    if os.path.exists('.env'):
        print("⚠️  .env already exists; not overwriting it")
        return
    env_content = """# Database Configuration
DB_HOST=
DB_PORT=
DB_NAME=
DB_USER=
DB_PASSWORD=

# API Configuration
PYTHON_API_BASE=

# OCR Configuration
OCR_WEBHOOK_URL=
"""

    with open('.env', 'w') as f:
        f.write(env_content)
    
    print("✅ Created .env file")
    print("📝 Please edit .env file with your actual database credentials")

async def test_connection():
    """Test database connection"""
    try:
        # Load environment variables
        from dotenv import load_dotenv
        load_dotenv()
        
        # From env only (was localhost / postgres / 'password' in the source).
        db_host = require_env('DB_HOST')
        db_port = int(require_env('DB_PORT'))
        db_name = require_env('DB_NAME')
        db_user = require_env('DB_USER')
        db_password = require_env('DB_PASSWORD')
        
        print(f"🔌 Testing connection to {db_host}:{db_port}/{db_name} as {db_user}")
        
        conn = await asyncpg.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        
        # Test query
        version = await conn.fetchval("SELECT version()")
        tables_count = await conn.fetchval("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'")
        
        await conn.close()
        
        print("✅ Database connection successful!")
        print(f"📊 PostgreSQL version: {version}")
        print(f"📋 Found {tables_count} tables in public schema")
        
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("\n🔧 Troubleshooting:")
        print("1. Make sure PostgreSQL is running")
        print("2. Check your database credentials in .env file")
        print("3. Ensure the database 'reporting_system' exists")
        print("4. Verify your user has proper permissions")
        return False

async def list_tables():
    """List all tables in the database"""
    try:
        from dotenv import load_dotenv
        load_dotenv()
        
        # From env only (was localhost / postgres / 'password' in the source).
        db_host = require_env('DB_HOST')
        db_port = int(require_env('DB_PORT'))
        db_name = require_env('DB_NAME')
        db_user = require_env('DB_USER')
        db_password = require_env('DB_PASSWORD')
        
        conn = await asyncpg.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        
        # Get all tables
        tables = await conn.fetch("""
            SELECT 
                schemaname,
                tablename,
                tableowner
            FROM pg_tables 
            WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
            ORDER BY schemaname, tablename;
        """)
        
        print(f"\n📋 Found {len(tables)} tables in your database:")
        for table in tables:
            print(f"  • {table['schemaname']}.{table['tablename']} (owner: {table['tableowner']})")
        
        await conn.close()
        
    except Exception as e:
        print(f"❌ Failed to list tables: {e}")

def main():
    print("🚀 Enhanced Bank Check - Database Setup")
    print("=" * 50)
    
    # Check if .env exists
    if not os.path.exists('.env'):
        print("📝 Creating .env file...")
        create_env_file()
        print("\n⚠️  Please edit .env file with your database credentials and run this script again")
        return
    
    print("🔍 Testing database connection...")
    
    # Test connection
    success = asyncio.run(test_connection())
    
    if success:
        print("\n📋 Listing database tables...")
        asyncio.run(list_tables())
        print("\n✅ Setup complete! You can now use the enhanced bank check feature.")
    else:
        print("\n❌ Setup failed. Please fix the database connection issues.")

if __name__ == "__main__":
    main()
