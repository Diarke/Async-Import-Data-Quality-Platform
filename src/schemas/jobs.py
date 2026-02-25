from pydantic import BaseModel, model_validator


class JobResponse(BaseModel):
    filename: str
    contents: str
    content_type: str
