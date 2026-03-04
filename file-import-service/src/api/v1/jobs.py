import uuid
import os
from typing import Annotated, Optional
from datetime import datetime

from fastapi import APIRouter, File, UploadFile, status, Request, Depends, HTTPException
from src.schemas.jobs import JobResponse, JobStatusResponse
from src.messaging.producer import RabbitProducer
from src.core.config import settings
from src.services.redis_store import RedisJobStore


router = APIRouter(tags=["Jobs"])


async def get_rabbit(request: Request):
    return request.app.state.rabbit


async def get_redis(request: Request) -> RedisJobStore:
    return request.app.state.redis_store


@router.post(
    "/process-file",
    summary="Загрузка и обработка файла",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=JobResponse
)
async def process_file(
    file: Annotated[UploadFile, File(...)],
    client_id: Optional[str] = None,
    rabbit=Depends(get_rabbit),
    redis_store: RedisJobStore = Depends(get_redis),
):
    """
    Upload and process file in chunks.
    
    - Reads file as stream (no full load in memory)
    - Splits into chunks (configurable size)
    - Publishes each chunk to RabbitMQ
    - Returns 202 Accepted with job_id
    """
    job_id = str(uuid.uuid4())
    chunk_size = settings.chunk.CHUNK_SIZE
    
    # Initialize producer
    producer = RabbitProducer(rabbit)
    
    # Store initial job metadata
    job_metadata = {
        "job_id": job_id,
        "filename": file.filename,
        "status": "queued",
        "total_chunks": 0,  # Will be updated after reading
        "processed_chunks": 0,
        "failed_chunks": 0,
        "client_id": client_id,
        "content_type": file.content_type or "application/octet-stream",
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
    }
    
    await redis_store.save_job(job_id, job_metadata)
    
    # Read file in chunks and publish to RabbitMQ
    chunk_index = 0
    chunks_published = 0
    
    try:
        while True:
            chunk = await file.read(chunk_size)
            if not chunk:
                break
            
            # Publish chunk
            success = await producer.publish_chunk(
                exchange_name="file_exchange",
                routing_key="file.chunk",
                chunk_data=chunk,
                job_id=job_id,
                chunk_index=chunk_index,
                total_chunks=0,  # Will be known after reading all chunks
                filename=file.filename or "unknown",
                content_type=file.content_type or "application/octet-stream",
                client_id=client_id,
            )
            
            if success:
                chunks_published += 1
            
            chunk_index += 1
        
        # Update job with total_chunks count
        job_metadata["total_chunks"] = chunk_index
        job_metadata["status"] = "processing"
        await redis_store.save_job(job_id, job_metadata)
        
        return JobResponse(
            job_id=job_id,
            status_url=f"/api/v1/jobs/{job_id}"
        )
        
    except Exception as e:
        # Mark job as failed
        job_metadata["status"] = "failed"
        job_metadata["error"] = str(e)
        await redis_store.save_job(job_id, job_metadata)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing file: {str(e)}"
        )


@router.get(
    "/{job_id}",
    summary="Получить статус обработки",
    response_model=JobStatusResponse
)
async def get_job_status(
    job_id: str,
    redis_store: RedisJobStore = Depends(get_redis),
):
    """
    Get job processing status and progress.
    """
    job = await redis_store.get_job(job_id)
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    
    return JobStatusResponse(
        job_id=job["job_id"],
        filename=job["filename"],
        status=job["status"],
        total_chunks=job.get("total_chunks", 0),
        processed_chunks=job.get("processed_chunks", 0),
        failed_chunks=job.get("failed_chunks", 0),
        created_at=job["created_at"],
        updated_at=job["updated_at"],
        result_url=job.get("result_url"),
        error=job.get("error"),
    )


@router.get(
    "/{job_id}/download",
    summary="Скачать обработанный файл"
)
async def download_job_result(
    job_id: str,
    redis_store: RedisJobStore = Depends(get_redis),
):
    """
    Download processed file if job is completed.
    """
    job = await redis_store.get_job(job_id)
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found"
        )
    
    if job["status"] != "completed":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Job status is {job['status']}, not completed"
        )
    
    result_file = job.get("result_url")
    if not result_file or not os.path.exists(result_file):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Result file not found"
        )
    
    # Return file as FileResponse
    from fastapi.responses import FileResponse
    return FileResponse(
        result_file,
        filename=f"{job_id}-result.csv",
        media_type="text/csv"
    )
