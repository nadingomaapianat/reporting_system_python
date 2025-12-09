import os
from dotenv import load_dotenv

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Load .env for local/dev; in Docker/offline, env comes from docker run
load_dotenv()

print("DEBUG: main.py - About to import routers...")
from utils import api_routes
from utils.csrf import CSRFMiddleware, create_csrf_token, set_csrf_cookie
from utils.auth import JWTAuthMiddleware
from utils.db import get_connection
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

    # JWT authentication middleware
    app.add_middleware(JWTAuthMiddleware)

    # CSRF middleware
    app.add_middleware(
        CSRFMiddleware,
        exempt_paths=(
            "/csrf/token",
            "/docs",
            "/redoc",
            "/openapi.json",
            "/exports",
        ),
    )

    # CORS configuration
    allowed_origins = [
        "https://reporting-system-frontend.pianat.ai",
        "https://reporting-system-frontend.pianat.ai",
        "http://127.0.0.1:3000",
    ]
    extra_origin = os.getenv("FRONTEND_ORIGIN")
    if extra_origin and extra_origin not in allowed_origins and extra_origin != "*":
        allowed_origins.append(extra_origin)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Export-Src"],
    )

    # Include consolidated API router
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
        print("\n" + "=" * 70)
        print("🚀 APPLICATION STARTUP")
        print("=" * 70)

        # Test database connection using AD auth via FreeTDS + pyodbc
        print("\n📊 Testing Database Connection (AD / domain auth, FreeTDS + pyodbc)...")
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version_row = cursor.fetchone()
            sql_version = version_row[0] if version_row else "Unknown"
            print("✅ DATABASE CONNECTION: SUCCESS")
            print(f"   SQL Version: {sql_version[:80]}...")
            cursor.close()
            conn.close()
            logger.info("✅ Database connection successful at startup")
        except Exception as e:
            print("❌ DATABASE CONNECTION: FAILED")
            print(f"   Error: {e}")
            logger.error(f"❌ Database connection failed at startup: {e}")

        # Verify fonts (unchanged)
        print("\n🔤 Verifying PDF Fonts...")
        try:
            from reportlab.pdfbase import pdfmetrics
            from utils.pdf_utils import ARABIC_FONT_NAME, DEFAULT_FONT_NAME

            registered_fonts = pdfmetrics.getRegisteredFontNames()
            logger.info(f"PDF Fonts: {len(registered_fonts)} fonts registered")
            logger.info(f"PDF Fonts: ARABIC_FONT_NAME={ARABIC_FONT_NAME}, DEFAULT_FONT_NAME={DEFAULT_FONT_NAME}")

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


app = create_app()

if __name__ == "__main__":
    import uvicorn
    print("DEBUG: About to start uvicorn server...")
    port = int(os.getenv("PORT", "8000"))
    print(f"DEBUG: Starting server on port {port}...")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
