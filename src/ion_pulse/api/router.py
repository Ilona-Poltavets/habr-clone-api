from fastapi import APIRouter

from ion_pulse.api.routes import (
    admin,
    auth,
    author_applications,
    categories,
    comments,
    health,
    journal,
    moderation,
    publications,
    ratings,
    reports,
    subscriptions,
)

api_router = APIRouter()
api_router.include_router(auth.router, tags=["auth"])
api_router.include_router(admin.router, tags=["admin"])
api_router.include_router(author_applications.router, tags=["author applications"])
api_router.include_router(categories.router, tags=["categories"])
api_router.include_router(journal.router, tags=["journal"])
api_router.include_router(publications.router, tags=["publications"])
api_router.include_router(comments.router, tags=["comments"])
api_router.include_router(moderation.router, tags=["moderation"])
api_router.include_router(ratings.router, tags=["ratings"])
api_router.include_router(reports.router, tags=["reports"])
api_router.include_router(subscriptions.router, tags=["subscriptions"])
api_router.include_router(health.router, tags=["system"])
