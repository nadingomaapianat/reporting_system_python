"""
Utilities package for the reporting system
"""
# Lazy import to avoid circular dependencies
def get_router():
    """Get the API router (lazy import)"""
    from .api_routes import router
    return router

# Export the lazy import function instead of the router directly
__all__ = ['get_router']
