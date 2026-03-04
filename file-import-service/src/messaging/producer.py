import json
from typing import Dict, Any, Optional
from aio_pika import Message, DeliveryMode


class RabbitProducer:
    def __init__(self, connection):
        self.connection = connection

    async def publish(self, routing_key: str, payload: dict):
        """Publish JSON message"""
        channel = await self.connection.get_channel()

        await channel.default_exchange.publish(
            Message(
                body=json.dumps(payload).encode(),
                content_type="application/json",
            ),
            routing_key=routing_key,
        )

    async def publish_chunk(
        self,
        exchange_name: str,
        routing_key: str,
        chunk_data: bytes,
        job_id: str,
        chunk_index: int,
        total_chunks: int,
        filename: str,
        content_type: str = "application/octet-stream",
        headers: Optional[Dict[str, Any]] = None,
        client_id: Optional[str] = None,
    ) -> bool:
        """
        Publish chunk to RabbitMQ with metadata headers
        
        Args:
            exchange_name: Exchange name (e.g., "file_exchange")
            routing_key: Routing key (e.g., "file.chunk")
            chunk_data: Raw chunk bytes
            job_id: Job identifier (UUID)
            chunk_index: Index of chunk (0-based)
            total_chunks: Total number of chunks
            filename: Original filename
            content_type: MIME type of content
            headers: Additional headers dict
            client_id: Optional client identifier
        
        Returns:
            True if successful, False otherwise
        """
        try:
            channel = await self.connection.get_channel()
            
            # Declare exchange if not exists
            exchange = await channel.declare_exchange(
                exchange_name,
                "direct",
                durable=True,
                auto_delete=False
            )
            
            # Prepare headers
            msg_headers = {
                "job_id": job_id,
                "chunk_index": chunk_index,
                "total_chunks": total_chunks,
                "filename": filename,
                "original_format": content_type,
            }
            
            if client_id:
                msg_headers["client_id"] = client_id
            
            if headers:
                msg_headers.update(headers)
            
            # Create message
            message = Message(
                body=chunk_data,
                delivery_mode=DeliveryMode.PERSISTENT,
                correlation_id=job_id,
                message_id=f"{job_id}-{chunk_index}",
                content_type=content_type,
                headers=msg_headers,
            )
            
            # Publish
            await exchange.publish(message, routing_key=routing_key)
            return True
            
        except Exception as e:
            print(f"Error publishing chunk: {e}")
            return False

