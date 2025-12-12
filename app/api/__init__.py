from fastapi import APIRouter
from app.api import auth, query

api_router = APIRouter()

api_router.include_router(auth.auth_router, prefix="/auth", tags=["auth"])
api_router.include_router(query.query_router, prefix="/query", tags=["query"])

__all__ = ["api_router"]