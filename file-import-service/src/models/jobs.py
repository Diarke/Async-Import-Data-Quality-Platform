from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Job(Base):
    """Модель для хранения информации о job обработки файла"""
    __tablename__ = "jobs"

    job_id = Column(String(36), primary_key=True)
    filename = Column(String(255), nullable=False)
    status = Column(String(50), default="queued")  # queued, processing, completed, failed
    total_chunks = Column(Integer, default=0)
    processed_chunks = Column(Integer, default=0)
    failed_chunks = Column(Integer, default=0)
    client_id = Column(String(255), nullable=True)
    content_type = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    result_url = Column(String(500), nullable=True)
    error = Column(Text, nullable=True)
