import os
import logging
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from utils import api_routes
from utils.db import get_connection

print("DEBUG: Starting application initialization...")

def create_app() -> FastAPI:
    # Setup logging
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    logger = logging.getLogger("main")

    logger.debug("Creating FastAPI app...")

    app = FastAPI(
        title="Reporting System Python API",
        version="2.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    # Middleware
    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        logger.info(f"REQUEST: {request.method} {request.url.path}")
        try:
            response = await call_next(request)
            logger.info(f"RESPONSE: {response.status_code}")
            return response
        except Exception as e:
            logger.error(f"Middleware error: {e}")
            raise

    # CORS
    origins = [
        "https://reporting-system-frontend.pianat.ai",
        "http://127.0.0.1:3000",
        os.getenv("FRONTEND_ORIGIN", "*"),
    ]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Export-Src"],
    )

    # Static files
    app.mount("/exports", StaticFiles(directory="exports"), name="exports")

    # Routers
    app.include_router(api_routes.router)

    # Startup events
    @app.on_event("startup")
    async def startup_tasks():
        logger.info("?? Application startup — testing database NTLM connection...")

        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()
            logger.info(f"SQL Server version: {version[0]}")
            conn.close()
        except Exception as e:
            logger.error(f"Database startup check failed: {e}")

    @app.get("/")
    async def health_check():
        return {"status": "ok", "service": "python-api"}

    return app


app = create_app()
