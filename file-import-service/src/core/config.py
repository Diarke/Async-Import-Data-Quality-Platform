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


class DocUrlSettings(ModelConfig):
    DOCS_URL: Annotated[str, Field(alias="DOCS_URL")]
    REDOC_URL: Annotated[str, Field(alias="REDOC_URL")]


class RMQSettings(ModelConfig):
    RMQ_HOST: Annotated[str, Field(alias="RMQ_HOST")]
    RMQ_PORT: Annotated[int, Field(alias="RMQ_PORT")]
    RMQ_USER: Annotated[str, Field(alias="RMQ_USER")]
    RMQ_PASS: Annotated[str, Field(alias="RMQ_PASS")]

    def rmq_url(self) -> str:
        # return f"amqp://{self.RMQ_USER}:{self.RMQ_PASS}@{self.RMQ_HOST}:{self.RMQ_PORT}/"
        return (
            f"amqp://{self.RMQ_USER}:"
            f"{self.RMQ_PASS}@"
            f"{self.RMQ_HOST}:"
            f"{self.RMQ_PORT}/"
        )


class RedisSettings(ModelConfig):
    REDIS_HOST: Annotated[str, Field(alias="REDIS_HOST", default="localhost")]
    REDIS_PORT: Annotated[int, Field(alias="REDIS_PORT", default=6379)]
    REDIS_DB: Annotated[int, Field(alias="REDIS_DB", default=0)]
    REDIS_PASSWORD: Annotated[str | None, Field(alias="REDIS_PASSWORD", default=None)]

    def redis_url(self) -> str:
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"


class ChunkSettings(ModelConfig):
    CHUNK_SIZE: Annotated[int, Field(alias="CHUNK_SIZE", default=512*1024)]  # 512 KB
    MAX_MESSAGE_SIZE: Annotated[int, Field(alias="MAX_MESSAGE_SIZE", default=1024*1024)]  # 1 MB
    DEFAULT_COUNTRY_CODE: Annotated[str, Field(alias="DEFAULT_COUNTRY_CODE", default="KZ")]


class StorageSettings(ModelConfig):
    STORAGE_PATH: Annotated[str, Field(alias="STORAGE_PATH", default="./storage/results")]
    TEMP_PATH: Annotated[str, Field(alias="TEMP_PATH", default="./storage/temp")]


class Settings(ModelConfig):
    app: AppSettings = AppSettings()
    rmq: RMQSettings = RMQSettings()
    doc_url: DocUrlSettings = DocUrlSettings()
    redis: RedisSettings = RedisSettings()
    chunk: ChunkSettings = ChunkSettings()
    storage: StorageSettings = StorageSettings()


settings = Settings()
