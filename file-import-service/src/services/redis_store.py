import json
import redis.asyncio as redis
from typing import Optional, Dict, Any
from datetime import datetime


class RedisJobStore:
    """Асинхронное хранилище для job метаданных в Redis"""
    
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.client: Optional[redis.Redis] = None
    
    async def connect(self):
        """Подключиться к Redis"""
        self.client = await redis.from_url(self.redis_url, decode_responses=True)
    
    async def disconnect(self):
        """Отключиться от Redis"""
        if self.client:
            await self.client.close()
    
    async def save_job(self, job_id: str, job_data: Dict[str, Any], ttl: int = 86400 * 7) -> bool:
        """
        Сохранить job метаданные в Redis
        
        Args:
            job_id: Job ID
            job_data: Job data dict
            ttl: Time to live in seconds (default 7 days)
        """
        if not self.client:
            return False
        
        key = f"job:{job_id}"
        await self.client.setex(key, ttl, json.dumps(job_data, default=str))
        return True
    
    async def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Получить job метаданные из Redis
        
        Args:
            job_id: Job ID
        """
        if not self.client:
            return None
        
        key = f"job:{job_id}"
        data = await self.client.get(key)
        if data:
            return json.loads(data)
        return None
    
    async def update_job(self, job_id: str, updates: Dict[str, Any]) -> bool:
        """Обновить job метаданные (merge)"""
        if not self.client:
            return False
        
        key = f"job:{job_id}"
        job_data = await self.get_job(job_id)
        if job_data:
            job_data.update(updates)
            await self.client.set(key, json.dumps(job_data, default=str))
            return True
        return False
    
    async def delete_job(self, job_id: str) -> bool:
        """Удалить job из Redis"""
        if not self.client:
            return False
        
        key = f"job:{job_id}"
        await self.client.delete(key)
        return True
    
    async def save_chunk(self, job_id: str, chunk_index: int, chunk_data: bytes) -> bool:
        """Сохранить обработанный чанк в Redis"""
        if not self.client:
            return False
        
        key = f"job:{job_id}:chunk:{chunk_index}"
        await self.client.set(key, chunk_data)
        return True
    
    async def get_chunk(self, job_id: str, chunk_index: int) -> Optional[bytes]:
        """Получить обработанный чанк из Redis"""
        if not self.client:
            return None
        
        key = f"job:{job_id}:chunk:{chunk_index}"
        return await self.client.get(key)
    
    async def get_all_chunks(self, job_id: str, total_chunks: int) -> Optional[list]:
        """
        Получить все чанки для job
        
        Returns:
            List of chunks in order, or None if any chunk is missing
        """
        if not self.client:
            return None
        
        chunks = []
        for i in range(total_chunks):
            chunk = await self.get_chunk(job_id, i)
            if chunk is None:
                return None  # Not all chunks are ready
            chunks.append(chunk)
        
        return chunks
    
    async def increment_processed(self, job_id: str) -> int:
        """Инкрементировать счётчик обработанных чанков"""
        if not self.client:
            return -1
        
        key = f"job:{job_id}:processed_count"
        return await self.client.incr(key)
    
    async def decrement_processed(self, job_id: str) -> int:
        """Декрементировать счётчик обработанных чанков"""
        if not self.client:
            return -1
        
        key = f"job:{job_id}:processed_count"
        return await self.client.decr(key)
    
    async def set_processed_count(self, job_id: str, count: int) -> bool:
        """Установить счётчик обработанных чанков"""
        if not self.client:
            return False
        
        key = f"job:{job_id}:processed_count"
        await self.client.set(key, count)
        return True
    
    async def get_processed_count(self, job_id: str) -> int:
        """Получить счётчик обработанных чанков"""
        if not self.client:
            return 0
        
        key = f"job:{job_id}:processed_count"
        count = await self.client.get(key)
        return int(count) if count else 0
    
    async def mark_chunk_processed(self, job_id: str, chunk_index: int) -> bool:
        """Отметить чанк как обработанный"""
        if not self.client:
            return False
        
        key = f"job:{job_id}:processed_chunks"
        await self.client.sadd(key, chunk_index)
        return True
    
    async def is_chunk_processed(self, job_id: str, chunk_index: int) -> bool:
        """Проверить, обработан ли чанк"""
        if not self.client:
            return False
        
        key = f"job:{job_id}:processed_chunks"
        result = await self.client.sismember(key, chunk_index)
        return bool(result)
    
    async def get_processed_chunks(self, job_id: str) -> set:
        """Получить set обработанных чанков"""
        if not self.client:
            return set()
        
        key = f"job:{job_id}:processed_chunks"
        return await self.client.smembers(key)
