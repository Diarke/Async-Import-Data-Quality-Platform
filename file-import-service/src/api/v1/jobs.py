from typing import Annotated

from fastapi import APIRouter, File, UploadFile, status

from src.schemas.jobs import JobResponse
from src.services.jobs import JobService


router = APIRouter(tags=["Jobs"])

service = JobService()


@router.post(
    "/upload",
    summary="Загрузка и получение содержимого файла",
    status_code=status.HTTP_200_OK,
    response_model=JobResponse
)
async def create_upload_file(
    file: Annotated[UploadFile, File(...)]
):
    return await service.is_file_supported(file)


@router.post(
    "/check",
    summary="Проверка файла на ошибки",
    status_code=status.HTTP_200_OK
)
async def import_job(
    file: Annotated[UploadFile, File(...)]
):
    contents = await file.read()
    return {
        "filename": file.filename,
        "contents": contents.decode("utf-8")
    }
