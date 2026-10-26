from fastapi import APIRouter, Depends
from utils.auth import get_current_user

router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.get("/profile")
async def get_profile(current_user: dict = Depends(get_current_user)):
    """Current user from the verified JWT. Only claims the token really has (no "admin" default)."""
    return {
        "id": current_user.get("id"),
        "email": current_user.get("email"),
        "name": current_user.get("name"),
        "role": current_user.get("role"),
    }


# POST /validate-token was removed: it was public, nothing called it, and it told anyone whether a
# token was valid and returned its claims (same endpoint already removed from both Node backends).


@router.post("/logout")
async def logout(current_user: dict = Depends(get_current_user)):
    """
    Logout endpoint - matching v2_backend format.
    Note: Session management is handled by v2_backend.
    This endpoint validates the token and returns success.
    """
    try:
        # Note: Session management is handled by v2_backend
        # This endpoint validates the token and returns success
        # The actual logout (isSessionActive update) should be done via v2_backend
        
        return {
            "isSuccess": True,
            "message": "Logged out successfully",
        }
    except Exception as e:
        return {
            "isSuccess": False,
            "message": "An error occurred during logout",
        }

