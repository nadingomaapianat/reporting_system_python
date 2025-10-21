import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Import API routes
from utils.api_routes import router as api_router
from dynamic_report_endpoints import router as dynamic_router
from excel_to_word_endpoints import router as excel_word_router


def create_app() -> FastAPI:
    app = FastAPI(
        title="Reporting System Python API",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

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

    # Routers
    app.include_router(api_router)
    app.include_router(dynamic_router)
    app.include_router(excel_word_router)

    # Serve exported files statically under /exports
    app.mount("/exports", StaticFiles(directory="exports"), name="exports")

    @app.get("/")
    async def health_root():
        return {"status": "ok", "service": "python-api"}

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main_working:app", host="0.0.0.0", port=port, reload=True)


