"""
Export utilities - re-exports for backward compatibility.
Header config is defined in config.settings.
merge_header_config lives in routes.route_utils; re-export here for services that expect it from export_utils.
"""
from config import get_default_header_config

try:
    from routes.route_utils import merge_header_config
except ImportError:
    # Avoid circular import when route_utils not yet loaded; define a minimal fallback
    def merge_header_config(module_name: str, header_config: dict):
        from config import get_default_header_config
        default_config = get_default_header_config(module_name)
        return {**default_config, **header_config}

__all__ = ['get_default_header_config', 'merge_header_config']
