import os
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Import API routes
print("DEBUG: main.py - About to import routers...")
# Import directly from api_routes to avoid circular dependency via utils/__init__.py
from utils import api_routes
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

    # CORS
    allowed_origins = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        os.getenv("FRONTEND_ORIGIN", "*")
    ]
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

    # Serve exported files statically under /exports
    app.mount("/exports", StaticFiles(directory="exports"), name="exports")

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
    print(f"DEBUG: Starting server on port {port}...")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)


