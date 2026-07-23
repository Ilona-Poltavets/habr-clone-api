from fastapi import APIRouter

from ion_pulse.api.routes import (
    auth,
    author_applications,
    health,
    publications,
    ratings,
    subscriptions,
)

api_router = APIRouter()
api_router.include_router(auth.router, tags=["auth"])
api_router.include_router(author_applications.router, tags=["author applications"])
api_router.include_router(publications.router, tags=["publications"])
api_router.include_router(ratings.router, tags=["ratings"])
api_router.include_router(subscriptions.router, tags=["subscriptions"])
api_router.include_router(health.router, tags=["system"])
