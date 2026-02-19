import os
# Load .env / environment.env so all links and config come from env
try:
    from dotenv import load_dotenv
    load_dotenv()
    load_dotenv("environment.env")
except ImportError:
    pass
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Import API routes
print("DEBUG: main.py - About to import routers...")
# Import directly from api_routes to avoid circular dependency via utils/__init__.py
from utils import api_routes
from utils.csrf import CSRFMiddleware, create_csrf_token, set_csrf_cookie
from utils.auth import JWTAuthMiddleware
api_router = api_routes.router
print("DEBUG: main.py - All routers imported (consolidated in api_router)")


def create_app() -> FastAPI:
    print("DEBUG: create_app() called")
    
    # Setup logging
    import logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
        ]
    )
    
    app = FastAPI(
        title="Reporting System Python API",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )
    
    print("DEBUG: FastAPI app created, adding middleware...")
    
    # Add request logging middleware and exception handler
    logger = logging.getLogger(__name__)
    
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.error(f"GLOBAL EXCEPTION: {type(exc).__name__}: {exc}")
        import traceback
        traceback.print_exc()
        return {"detail": f"Internal error: {str(exc)}"}, 500
    
    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        logger.error(f"REQUEST: {request.method} {request.url.path}")
        logger.error(f"PARAMS: {request.query_params}")
        try:
            response = await call_next(request)
            logger.error(f"RESPONSE: {response.status_code}")
            return response
        except Exception as e:
            logger.error(f"ERROR in middleware: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    print("DEBUG: Middleware added")

    csrf_cookie_secure = os.getenv("CSRF_COOKIE_SECURE", "true").lower() == "true"

    # Add JWT authentication middleware (before CSRF)
    app.add_middleware(JWTAuthMiddleware)

    app.add_middleware(
        CSRFMiddleware,
        exempt_paths=(
            "/csrf/token",
            "/docs",
            "/redoc",
            "/openapi.json",
            "/exports",
            # Dynamic report export / preview / execute-sql are called via trusted backends (Node/Next),
            # which already enforce auth and CSRF. Skip Python-side CSRF here to avoid 403s.
            "/api/reports/dynamic",
            "/api/reports/dynamic/preview",
            "/api/reports/execute-sql",
        ),
    )

    # CORS MUST be the outermost middleware so it can handle preflight (OPTIONS) before auth/CSRF
    # Build from .env: CORS_ORIGINS (comma-separated) or FRONTEND_ORIGIN; fallback to localhost
    _cors_env = os.getenv("CORS_ORIGINS", "").strip()
    if _cors_env:
        allowed_origins = [o.strip() for o in _cors_env.split(",") if o.strip()]
    else:
        allowed_origins = [
            os.getenv("FRONTEND_ORIGIN", "https://reporting-system-frontend.pianat.ai").strip(),
            "http://127.0.0.1:3000",
            "https://reporting-system-frontend.pianat.ai",
        ]
    _extra = os.getenv("FRONTEND_ORIGIN")
    if _extra and _extra not in allowed_origins and _extra != "*":
        allowed_origins.append(_extra)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Export-Src"],
    )

    # Include consolidated API router (contains all sub-routers)
    app.include_router(api_router)

    @app.get("/csrf/token")
    async def get_csrf_token(response: Response):
        token = create_csrf_token()
        set_csrf_cookie(response, token, secure=csrf_cookie_secure)
        return {"csrfToken": token}

    # Serve exported files statically under /exports
    app.mount("/exports", StaticFiles(directory="exports"), name="exports")

    @app.on_event("startup")
    async def startup_event():
        """Verify fonts and test database connection at startup"""
        print("\n" + "="*70)
        print("🚀 APPLICATION STARTUP")
        print("="*70)
        
        # Test database connection
        print("\n📊 Testing Database Connection...")
        try:
            from config.settings import test_database_connection, DATABASE_CONFIG
            
            success, message, details = test_database_connection()
            
            if success:
                print("✅ DATABASE CONNECTION: SUCCESS")
                print(f"   Server: {details.get('server', 'N/A')}")
                print(f"   Database: {details.get('database', 'N/A')}")
                print(f"   Authentication: {details.get('auth_type', 'N/A')}")
                print(f"   Username: {details.get('username', 'N/A')}")
                # Show the actual Windows user that connected (VERIFY Windows Auth)
                if 'connected_windows_user' in details:
                    print(f"   ✅ Connected Windows User: {details.get('connected_windows_user', 'N/A')}")
                    print(f"   ✅ System User: {details.get('system_user', 'N/A')}")
                    print(f"   ✅ Database User: {details.get('database_user', 'N/A')}")
                print(f"   Tables Found: {details.get('table_count', 0)}")
                print(f"   SQL Version: {details.get('sql_version', 'N/A')[:50]}...")
                logger.info("✅ Database connection successful")
            else:
                print("❌ DATABASE CONNECTION: FAILED")
                print(f"   Server: {details.get('server', 'N/A')}")
                print(f"   Database: {details.get('database', 'N/A')}")
                print(f"   Authentication: {details.get('auth_type', 'N/A')}")
                print(f"   Error: {message}")
                if 'error' in details:
                    error_detail = details['error']
                    if 'Kerberos' in error_detail or 'SSPI' in error_detail:
                        print("\n   ⚠️  TROUBLESHOOTING:")
                        print("   - Windows Authentication (Kerberos/NTLM via SSPI) doesn't work in Docker containers")
                        print("   - Set DB_USE_WINDOWS_AUTH=no in environment.env to use SQL Server Authentication with NTLM")
                        print("   - Make sure DB_DOMAIN, DB_USERNAME and DB_PASSWORD are set correctly")
                        print("   - Using domain\\username format enables NTLM authentication in Docker")
                    elif 'timeout' in error_detail.lower():
                        print("\n   ⚠️  TROUBLESHOOTING:")
                        print("   - Check if SQL Server is running and accessible")
                        print("   - Verify network connectivity to the database server")
                        print("   - Check firewall settings")
                    elif '18456' in error_detail or 'Login failed' in error_detail:
                        print("\n   ⚠️  TROUBLESHOOTING (Login Failed - Error 18456):")
                        print("   - Account ADIBEG\\GRCSVC is Windows Authentication ONLY")
                        print("   - SQL Server Authentication is NOT enabled for this account")
                        print("   - Docker (Linux) cannot use Windows Authentication")
                        print("\n   💡 SOLUTIONS:")
                        print("   1. Ask bank to enable SQL Server Authentication for ADIBEG\\GRCSVC")
                        print("      - Enable SQL Server Authentication mode on SQL Server")
                        print("      - Create SQL Server login for ADIBEG\\GRCSVC")
                        print("   2. Get a different account with SQL Server Authentication enabled")
                        print("   3. Run on Windows host (not Docker):")
                        print("      - Set DB_USE_WINDOWS_AUTH=yes")
                        print("      - Run Python as ADIBEG\\GRCSVC user")
                        print("      - This will use Windows Authentication (works on Windows only)")
                    else:
                        print("\n   ⚠️  TROUBLESHOOTING:")
                        print("   - Verify database credentials in environment.env")
                        print("   - Check if SQL Server is running")
                        print("   - Ensure ODBC Driver 18 for SQL Server is installed")
                logger.error(f"❌ Database connection failed: {message}")
        except Exception as e:
            print(f"❌ DATABASE CONNECTION: ERROR - {str(e)}")
            logger.error(f"Database connection test error: {e}")
        
        # Verify fonts
        print("\n🔤 Verifying PDF Fonts...")
        try:
            from reportlab.pdfbase import pdfmetrics
            from utils.pdf_utils import ARABIC_FONT_NAME, DEFAULT_FONT_NAME
            
            registered_fonts = pdfmetrics.getRegisteredFontNames()
            logger.info(f"PDF Fonts: {len(registered_fonts)} fonts registered")
            logger.info(f"PDF Fonts: ARABIC_FONT_NAME={ARABIC_FONT_NAME}, DEFAULT_FONT_NAME={DEFAULT_FONT_NAME}")
            
            # Verify key fonts are available
            if ARABIC_FONT_NAME:
                if ARABIC_FONT_NAME in registered_fonts:
                    logger.info(f"PDF Fonts: ✓ {ARABIC_FONT_NAME} is registered and available")
                else:
                    logger.warning(f"PDF Fonts: ✗ {ARABIC_FONT_NAME} is not in registered fonts list")
            
            if DEFAULT_FONT_NAME in registered_fonts or DEFAULT_FONT_NAME == 'Helvetica':
                logger.info(f"PDF Fonts: ✓ {DEFAULT_FONT_NAME} is available")
            else:
                logger.warning(f"PDF Fonts: ✗ {DEFAULT_FONT_NAME} may not be available")
        except Exception as e:
            logger.warning(f"PDF Fonts: Could not verify fonts at startup: {e}")

    @app.get("/")
    async def health_root():
        return {"status": "ok", "service": "python-api"}

    return app


# Create app instance - this will be imported by uvicorn
app = create_app()

if __name__ == "__main__":
    import uvicorn
    print("DEBUG: About to start uvicorn server...")
    port = int(os.getenv("PORT", "8000"))
    use_reload = os.getenv("RELOAD", "1").strip().lower() in ("1", "true", "yes")
    print(f"DEBUG: Starting server on port {port}... (reload={use_reload})")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=use_reload)


