import aio_pika
from aio_pika.abc import AbstractRobustConnection


class RabbitConnection:
    def __init__(self, url: str):
        self.url = url
        self._connection: AbstractRobustConnection | None = None

    async def connect(self):
        if not self._connection:
            self._connection = await aio_pika.connect_robust(self.url)
        return self._connection

    async def get_channel(self):
        connection = await self.connect()
        return await connection.channel()

    async def close(self):
        if self._connection:
            await self._connection.close()
