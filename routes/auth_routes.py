from fastapi import APIRouter, Depends
from pydantic import BaseModel
from utils.auth import validate_token, get_current_user

router = APIRouter(prefix="/api/auth", tags=["auth"])


class TokenValidationRequest(BaseModel):
    token: str


@router.get("/profile")
async def get_profile(current_user: dict = Depends(get_current_user)):
    """Get current user profile from JWT token (token comes from v2_backend)."""
    return {
        "id": current_user.get("id", "1"),
        "email": current_user.get("email", "demo@example.com"),
        "name": current_user.get("name", "Demo User"),
        "role": current_user.get("role", "admin"),
    }


@router.post("/validate-token")
async def validate_token_endpoint(request: TokenValidationRequest):
    """Validate a JWT token from v2_backend and return user information."""
    return validate_token(request.token)


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

