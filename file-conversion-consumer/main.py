import asyncio
import json
import sys
import logging
from typing import Optional

import aio_pika
from aio_pika import IncomingMessage, DeliveryMode, Message

from src.config import settings
from src.normalizers import (
    detect_format,
    process_chunk,
)


# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FileConversionConsumer:
    """Consumer for processing file chunks"""
    
    def __init__(self, rmq_url: str, prefetch_count: int = 1, max_retries: int = 3):
        self.rmq_url = rmq_url
        self.prefetch_count = prefetch_count
        self.max_retries = max_retries
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.Channel] = None
        self.exchange: Optional[aio_pika.Exchange] = None
        self.queue: Optional[aio_pika.Queue] = None
        self.dlx: Optional[aio_pika.Exchange] = None
        self.dlq: Optional[aio_pika.Queue] = None
    
    async def connect(self):
        """Connect to RabbitMQ and declare resources"""
        logger.info(f"Connecting to RabbitMQ: {self.rmq_url}")
        self.connection = await aio_pika.connect_robust(self.rmq_url)
        self.channel = await self.connection.channel()
        
        # Set prefetch
        await self.channel.set_qos(prefetch_count=self.prefetch_count)
        
        # Declare DLX and DLQ
        self.dlx = await self.channel.declare_exchange(
            "file_dlx",
            "direct",
            durable=True,
            auto_delete=False
        )
        
        self.dlq = await self.channel.declare_queue(
            "file_chunks_dlq",
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-message-ttl": 86400000  # 24 hours in ms
            }
        )
        await self.dlq.bind(self.dlx, "file.chunk.dlq")
        
        # Declare main exchange and queue
        self.exchange = await self.channel.declare_exchange(
            "file_exchange",
            "direct",
            durable=True,
            auto_delete=False
        )
        
        self.queue = await self.channel.declare_queue(
            "file_chunks",
            durable=True,
            arguments={
                "x-dead-letter-exchange": "file_dlx",
                "x-max-length": 100000
            }
        )
        await self.queue.bind(self.exchange, "file.chunk")
        
        logger.info("RabbitMQ connected and resources declared")
    
    async def disconnect(self):
        """Disconnect from RabbitMQ"""
        if self.connection:
            await self.connection.close()
            logger.info("Disconnected from RabbitMQ")
    
    async def process_message(self, message: IncomingMessage):
        """Process incoming chunk message"""
        attempt = 0
        max_attempts = self.max_retries + 1
        
        while attempt < max_attempts:
            attempt += 1
            
            try:
                # Extract headers
                headers = message.headers or {}
                job_id = headers.get("job_id")
                chunk_index = headers.get("chunk_index")
                total_chunks = headers.get("total_chunks")
                filename = headers.get("filename")
                original_format = headers.get("original_format", "csv")
                
                logger.info(
                    f"Processing chunk {chunk_index}/{total_chunks} for job {job_id} "
                    f"(attempt {attempt}/{max_attempts})"
                )
                
                # Validate required fields
                if not all([job_id, chunk_index is not None, total_chunks, filename]):
                    raise ValueError("Missing required message headers")
                
                # Detect format and process
                file_format = detect_format(filename)
                processed_chunk, metadata = process_chunk(
                    chunk_data=message.body,
                    format=file_format,
                    default_country_code=settings.consumer.DEFAULT_COUNTRY_CODE
                )
                
                # Publish processed chunk
                success = await self.publish_processed_chunk(
                    job_id=job_id,
                    chunk_index=chunk_index,
                    total_chunks=total_chunks,
                    filename=filename,
                    processed_data=processed_chunk,
                    metadata=metadata,
                    headers=headers
                )
                
                if success:
                    # Acknowledge original message
                    await message.ack()
                    logger.info(f"Chunk {chunk_index} processed and published successfully")
                    return
                else:
                    raise Exception("Failed to publish processed chunk")
                    
            except Exception as e:
                logger.error(
                    f"Error processing chunk {chunk_index} (attempt {attempt}/{max_attempts}): {e}",
                    exc_info=True
                )
                
                if attempt >= max_attempts:
                    # Send to DLQ
                    logger.warning(f"Max retries reached for chunk {chunk_index}, sending to DLQ")
                    await self.send_to_dlq(message, str(e))
                    await message.ack()  # Ack original to avoid infinite retry
                    return
                else:
                    # Nack with requeue for retry
                    if attempt < max_attempts:
                        await message.nack(requeue=True)
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
    
    async def publish_processed_chunk(
        self,
        job_id: str,
        chunk_index: int,
        total_chunks: int,
        filename: str,
        processed_data: bytes,
        metadata: dict,
        headers: dict
    ) -> bool:
        """Publish processed chunk to result queue"""
        try:
            # Prepare headers for processed message
            processed_headers = headers.copy()
            processed_headers["status"] = "processed"
            processed_headers["processed_rows"] = metadata.get("rows_processed", 0)
            processed_headers["processing_errors"] = len(metadata.get("errors", []))
            
            # Create message
            processed_message = Message(
                body=processed_data,
                delivery_mode=DeliveryMode.PERSISTENT,
                correlation_id=job_id,
                message_id=f"{job_id}-{chunk_index}-processed",
                headers=processed_headers,
            )
            
            # Publish to processed queue
            await self.exchange.publish(
                processed_message,
                routing_key="file.chunk.processed"
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error publishing processed chunk: {e}")
            return False
    
    async def send_to_dlq(self, original_message: IncomingMessage, error: str):
        """Send message to DLQ"""
        try:
            dlq_message = Message(
                body=original_message.body,
                headers={
                    **(original_message.headers or {}),
                    "error": error,
                    "dlq_timestamp": str(asyncio.get_event_loop().time())
                },
                delivery_mode=DeliveryMode.PERSISTENT
            )
            
            await self.dlx.publish(dlq_message, routing_key="file.chunk.dlq")
            logger.info("Message sent to DLQ")
            
        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e}")
    
    async def start(self):
        """Start consuming messages"""
        await self.connect()
        
        try:
            logger.info("Starting message consumption...")
            async with self.queue.iterator() as queue_iter:
                async for message in queue_iter:
                    await self.process_message(message)
                    
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}", exc_info=True)
        finally:
            await self.disconnect()


async def main():
    """Main entry point"""
    consumer = FileConversionConsumer(
        rmq_url=settings.rmq.rmq_url(),
        prefetch_count=settings.consumer.PREFETCH_COUNT,
        max_retries=settings.consumer.MAX_RETRIES
    )
    
    try:
        await consumer.start()
    except KeyboardInterrupt:
        logger.info("Shut down requested")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

