from fastapi import APIRouter

from app.api import api_messages
from app.api.endpoints import auth, users, analysis, sse

api_router = APIRouter()
api_router.include_router(analysis.router, prefix="/analysis", tags=["analysis"])
api_router.include_router(sse.router, prefix="/status", tags=["status"])
