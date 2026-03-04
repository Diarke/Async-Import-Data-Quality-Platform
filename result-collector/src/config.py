import os
from typing import Annotated

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class ModelConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


class RMQSettings(ModelConfig):
    RMQ_HOST: Annotated[str, Field(alias="RMQ_HOST", default="localhost")]
    RMQ_PORT: Annotated[int, Field(alias="RMQ_PORT", default=5672)]
    RMQ_USER: Annotated[str, Field(alias="RMQ_USER", default="guest")]
    RMQ_PASS: Annotated[str, Field(alias="RMQ_PASS", default="guest")]

    def rmq_url(self) -> str:
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


class StorageSettings(ModelConfig):
    STORAGE_PATH: Annotated[str, Field(alias="STORAGE_PATH", default="./storage/results")]


class Settings(ModelConfig):
    rmq: RMQSettings = RMQSettings()
    redis: RedisSettings = RedisSettings()
    storage: StorageSettings = StorageSettings()


settings = Settings()
