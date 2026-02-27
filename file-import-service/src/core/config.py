from dotenv import find_dotenv
from typing import Annotated

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class ModelConfig(BaseSettings):
    model_config=SettingsConfigDict(
        env_file=find_dotenv(".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )


class AppSettings(ModelConfig):
    DEBUG: Annotated[bool, Field(alias="DEBUG")]
    TITLE: Annotated[str, Field(alias="TITLE")]
    SUMMARY: Annotated[str, Field(alias="SUMMARY")]
    DESCRIPTION: Annotated[str, Field(alias="DESCRIPTION")]
    VERSION: Annotated[str, Field(alias="VERSION")]
    DOCS_URL: Annotated[str, Field(alias="DOCS_URL")]
    REDOC_URL: Annotated[str, Field(alias="REDOC_URL")]


class Settings(ModelConfig):
    app: AppSettings = AppSettings()


settings = Settings()
