from fastapi import APIRouter
from app.api import auth
api_router =  APIRouter()

api_router.include_router(auth.auth_router,prefix="/auth",tags=["auth"])
__all__ = ["api_router"]