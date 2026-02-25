from fastapi import FastAPI

from src.api.v1.jobs import router as jobs_router
from src.core.config import settings


app = FastAPI(
    debug=settings.app.DEBUG,
    title=settings.app.TITLE,
    summary=settings.app.SUMMARY,
    description=settings.app.DESCRIPTION,
    version=settings.app.VERSION,
    docs_url=settings.app.DOCS_URL,
    redoc_url=settings.app.REDOC_URL
)


app.include_router(jobs_router, prefix="/api/v1/jobs")
