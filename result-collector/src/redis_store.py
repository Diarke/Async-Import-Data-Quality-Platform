import json
import redis.asyncio as redis
from typing import Optional, Dict, Any, List


class RedisResultStore:
    """Store for buffering processed chunks in Redis"""
    
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.client: Optional[redis.Redis] = None
    
    async def connect(self):
        """Connect to Redis"""
        self.client = await redis.from_url(self.redis_url, decode_responses=True)
    
    async def disconnect(self):
        """Disconnect from Redis"""
        if self.client:
            await self.client.close()
    
    async def save_processed_chunk(
        self,
        job_id: str,
        chunk_index: int,
        chunk_data: bytes,
        ttl: int = 86400 * 7
    ) -> bool:
        """
        Save processed chunk to Redis
        
        Args:
            job_id: Job ID
            chunk_index: Chunk index
            chunk_data: Chunk bytes
            ttl: Time to live in seconds
        """
        if not self.client:
            return False
        
        key = f"job:{job_id}:processed_chunk:{chunk_index}"
        await self.client.setex(key, ttl, chunk_data)
        return True
    
    async def get_processed_chunk(self, job_id: str, chunk_index: int) -> Optional[bytes]:
        """Get processed chunk from Redis"""
        if not self.client:
            return None
        
        key = f"job:{job_id}:processed_chunk:{chunk_index}"
        return await self.client.get(key)
    
    async def get_all_processed_chunks(
        self,
        job_id: str,
        total_chunks: int
    ) -> Optional[List[bytes]]:
        """
        Get all processed chunks in order.
        
        Returns:
            List of chunks if all present, None if any missing
        """
        if not self.client:
            return None
        
        chunks = []
        for i in range(total_chunks):
            chunk = await self.get_processed_chunk(job_id, i)
            if chunk is None:
                return None  # Not all chunks ready
            chunks.append(chunk)
        
        return chunks
    
    async def mark_job_ready(self, job_id: str) -> bool:
        """Mark job as ready for assembly (all chunks received)"""
        if not self.client:
            return False
        
        key = f"job:{job_id}:ready"
        await self.client.set(key, "true")
        return True
    
    async def is_job_ready(self, job_id: str) -> bool:
        """Check if all chunks for job have been received"""
        if not self.client:
            return False
        
        key = f"job:{job_id}:ready"
        result = await self.client.get(key)
        return bool(result)
    
    async def incr_processed_count(self, job_id: str) -> int:
        """Increment processed chunk count"""
        if not self.client:
            return -1
        
        key = f"job:{job_id}:processed_count"
        return await self.client.incr(key)
    
    async def get_processed_count(self, job_id: str) -> int:
        """Get count of processed chunks received"""
        if not self.client:
            return 0
        
        key = f"job:{job_id}:processed_count"
        count = await self.client.get(key)
        return int(count) if count else 0
    
    async def cleanup_job(self, job_id: str, total_chunks: int):
        """Clean up Redis keys for completed job"""
        if not self.client:
            return
        
        keys_to_delete = [
            f"job:{job_id}:ready",
            f"job:{job_id}:processed_count",
        ]
        
        for i in range(total_chunks):
            keys_to_delete.append(f"job:{job_id}:processed_chunk:{i}")
        
        if keys_to_delete:
            await self.client.delete(*keys_to_delete)
