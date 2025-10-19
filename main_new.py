"""
Main application entry point for the reporting system
"""
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import redis
from contextlib import asynccontextmanager

from utils import router
from config import API_CONFIG

# Initialize Redis (optional)
redis_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    global redis_client
    try:
        redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        redis_client.ping()
    except Exception as e:
        redis_client = None
    
    yield
    
    # Shutdown
    if redis_client:
        redis_client.close()

# Create FastAPI application
app = FastAPI(
    title="Reporting System API",
    description="Professional reporting system for GRC dashboards",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001", "http://localhost:3002"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(router)

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Reporting System API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "risks_pdf": "/api/grc/risks/export-pdf",
            "risks_excel": "/api/grc/risks/export-excel",
            "controls_pdf": "/api/grc/controls/export-pdf",
            "controls_excel": "/api/grc/controls/export-excel",
            "health": "/health"
        }
    }

if __name__ == "__main__":
    uvicorn.run(
        "main_new:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
