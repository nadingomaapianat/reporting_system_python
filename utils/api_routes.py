"""
API routes module that combines all routers
"""
from fastapi import APIRouter
from routes.control_api_routes import router as control_router
from routes.risk_api_routes import router as risk_router
from routes.incident_api_routes import router as incident_router
from routes.kri_api_routes import router as kri_router
from routes.report_api_routes import router as report_router
from routes.bank_check_api_routes import router as bank_check_router
from routes.dynamic_report_endpoints import router as dynamic_router
from routes.excel_to_word_endpoints import router as excel_word_router

# Create main router
router = APIRouter()

# Include all sub-routers
router.include_router(control_router)
router.include_router(risk_router)
router.include_router(incident_router)
router.include_router(kri_router)
router.include_router(report_router)
router.include_router(bank_check_router)
router.include_router(dynamic_router)
router.include_router(excel_word_router)

