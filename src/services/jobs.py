from fastapi import UploadFile

from src.core.dependencies.exceptions import file_isnt_suported
from src.schemas.jobs import JobResponse


class JobService:
    def __init__(self):
        pass


    async def is_file_supported(
        self,
        file: UploadFile
    ) -> JobResponse:
        try:
            contents = await file.read()
            content_type = file.content_type
            return {
                "filename": file.filename,
                "contents": contents.decode("utf-8"),
                "content_type": content_type
            }
        except UnicodeDecodeError:
            raise file_isnt_suported


    async def check_file(
        self
    ):
        pass
