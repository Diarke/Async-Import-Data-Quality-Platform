from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class JobResponse(BaseModel):
    """Ответ при создании job"""
    job_id: str
    status_url: str


class JobStatusResponse(BaseModel):
    """Статус job"""
    job_id: str
    filename: str
    status: str  # queued, processing, completed, failed
    total_chunks: int
    processed_chunks: int
    failed_chunks: int
    created_at: datetime
    updated_at: datetime
    result_url: Optional[str] = None
    error: Optional[str] = None


class JobDownloadResponse(BaseModel):
    """Для скачивания файла"""
    filename: str
    download_url: str
