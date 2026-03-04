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


class ConsumerSettings(ModelConfig):
    MAX_RETRIES: Annotated[int, Field(alias="MAX_RETRIES", default=3)]
    PREFETCH_COUNT: Annotated[int, Field(alias="PREFETCH_COUNT", default=1)]
    DEFAULT_COUNTRY_CODE: Annotated[str, Field(alias="DEFAULT_COUNTRY_CODE", default="KZ")]


class Settings(ModelConfig):
    rmq: RMQSettings = RMQSettings()
    consumer: ConsumerSettings = ConsumerSettings()


settings = Settings()
