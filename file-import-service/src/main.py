from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.api.v1.jobs import router as jobs_router
from src.core.config import settings
from src.messaging.broker import RabbitConnection
from src.services.redis_store import RedisJobStore


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize RabbitMQ connection
    connection = RabbitConnection(settings.rmq.rmq_url())
    channel = await connection.get_channel()
    callback_queue = await channel.declare_queue(exclusive=True)
    
    app.state.rabbit = connection
    app.state.callback_queue = callback_queue
    print("RabbitMQ connected")
    
    # Initialize Redis store
    redis_store = RedisJobStore(settings.redis.redis_url())
    await redis_store.connect()
    app.state.redis_store = redis_store
    print("Redis connected")

    yield

    # Cleanup
    await connection.close()
    await redis_store.disconnect()
    print("RabbitMQ and Redis closed")


app = FastAPI(
    lifespan=lifespan,
    debug=settings.app.DEBUG,
    title=settings.app.TITLE,
    summary=settings.app.SUMMARY,
    description=settings.app.DESCRIPTION,
    version=settings.app.VERSION,
    docs_url=settings.doc_url.DOCS_URL,
    redoc_url=settings.doc_url.REDOC_URL
)


app.include_router(jobs_router, prefix="/api/v1/jobs")

