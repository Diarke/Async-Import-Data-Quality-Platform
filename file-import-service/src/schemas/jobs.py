from pydantic import BaseModel


class JobResponse(BaseModel):
    filename: str
    contents: str
    content_type: str
