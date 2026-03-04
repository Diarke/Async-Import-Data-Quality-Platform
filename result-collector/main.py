import asyncio
import json
import logging
import sys
from typing import Optional

import aio_pika
from aio_pika import IncomingMessage, DeliveryMode

from src.config import settings
from src.redis_store import RedisResultStore
from src.storage import write_result_file


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ResultCollector:
    """Collects and assembles processed file chunks"""
    
    def __init__(self, rmq_url: str, redis_url: str, storage_path: str):
        self.rmq_url = rmq_url
        self.redis_url = redis_url
        self.storage_path = storage_path
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.exchange: Optional[aio_pika.Exchange] = None
        self.queue: Optional[aio_pika.Queue] = None
        self.redis_store: Optional[RedisResultStore] = None
    
    async def connect(self):
        """Connect to RabbitMQ"""
        logger.info(f"Connecting to RabbitMQ: {self.rmq_url}")
        self.connection = await aio_pika.connect_robust(self.rmq_url)
        self.channel = await self.connection.channel()
        
        # Declare exchange and queue for processed chunks
        self.exchange = await self.channel.declare_exchange(
            "file_exchange",
            "direct",
            durable=True,
            auto_delete=False
        )
        
        self.queue = await self.channel.declare_queue(
            "file_chunks_processed",
            durable=True
        )
        await self.queue.bind(self.exchange, "file.chunk.processed")
        
        logger.info("RabbitMQ connected")
        
        # Connect to Redis
        self.redis_store = RedisResultStore(self.redis_url)
        await self.redis_store.connect()
        logger.info("Redis connected")
    
    async def disconnect(self):
        """Disconnect from services"""
        if self.connection:
            await self.connection.close()
            logger.info("RabbitMQ disconnected")
        
        if self.redis_store:
            await self.redis_store.disconnect()
            logger.info("Redis disconnected")
    
    async def process_message(self, message: IncomingMessage):
        """Process processed chunk message"""
        try:
            headers = message.headers or {}
            job_id = headers.get("job_id")
            chunk_index = headers.get("chunk_index")
            total_chunks = headers.get("total_chunks")
            filename = headers.get("filename")
            status = headers.get("status")
            
            logger.info(
                f"Received processed chunk {chunk_index}/{total_chunks} "
                f"for job {job_id} with status {status}"
            )
            
            if not all([job_id, chunk_index is not None, total_chunks]):
                logger.error("Missing required headers")
                await message.ack()
                return
            
            # Store processed chunk
            await self.redis_store.save_processed_chunk(
                job_id=job_id,
                chunk_index=chunk_index,
                chunk_data=message.body
            )
            
            # Increment processed count
            processed_count = await self.redis_store.incr_processed_count(job_id)
            
            logger.info(
                f"Stored chunk {chunk_index}/{total_chunks} for job {job_id} "
                f"(total received: {processed_count})"
            )
            
            # Check if all chunks received
            if processed_count == total_chunks:
                logger.info(f"All chunks received for job {job_id}, assembling result...")
                await self.assemble_result(job_id, total_chunks, filename)
            
            # Acknowledge message
            await message.ack()
            
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            # Nack to retry
            await message.nack(requeue=True)
    
    async def assemble_result(self, job_id: str, total_chunks: int, filename: str):
        """Assemble all chunks into result file and update job status"""
        try:
            # Get all chunks from Redis
            chunks = await self.redis_store.get_all_processed_chunks(job_id, total_chunks)
            
            if chunks is None:
                logger.error(f"Could not retrieve all chunks for job {job_id}")
                return
            
            # Write to file
            result_path = write_result_file(job_id, chunks, self.storage_path)
            
            # Update job status in Redis (using simple store)
            # In production, this would update the main job database
            logger.info(f"Result file created: {result_path}")
            
            # Clean up processed chunks from Redis
            await self.redis_store.cleanup_job(job_id, total_chunks)
            
            # Optionally publish to results queue
            await self.publish_job_completed(job_id, result_path, filename)
            
        except Exception as e:
            logger.error(f"Error assembling result for job {job_id}: {e}", exc_info=True)
    
    async def publish_job_completed(self, job_id: str, result_path: str, filename: str):
        """Publish job completed message"""
        try:
            completion_message = aio_pika.Message(
                body=json.dumps({
                    "job_id": job_id,
                    "status": "completed",
                    "result_path": result_path,
                    "filename": filename
                }).encode(),
                delivery_mode=DeliveryMode.PERSISTENT,
                correlation_id=job_id
            )
            
            await self.exchange.publish(
                completion_message,
                routing_key="file.job.completed"
            )
            
            logger.info(f"Published job completed message for {job_id}")
            
        except Exception as e:
            logger.error(f"Error publishing completion message: {e}")
    
    async def start(self):
        """Start consuming processed chunks"""
        await self.connect()
        
        try:
            logger.info("Starting result collector...")
            async with self.queue.iterator() as queue_iter:
                async for message in queue_iter:
                    await self.process_message(message)
                    
        except Exception as e:
            logger.error(f"Error in collector loop: {e}", exc_info=True)
        finally:
            await self.disconnect()


async def main():
    """Main entry point"""
    collector = ResultCollector(
        rmq_url=settings.rmq.rmq_url(),
        redis_url=settings.redis.redis_url(),
        storage_path=settings.storage.STORAGE_PATH
    )
    
    try:
        await collector.start()
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
