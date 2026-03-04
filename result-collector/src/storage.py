import os
import logging
from pathlib import Path


logger = logging.getLogger(__name__)


def ensure_storage_dir(path: str) -> bool:
    """Create storage directory if it doesn't exist"""
    try:
        Path(path).mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        logger.error(f"Failed to create storage directory {path}: {e}")
        return False


def write_result_file(job_id: str, chunks: list, storage_path: str) -> str:
    """
    Write collected chunks to a result file.
    
    Args:
        job_id: Job identifier
        chunks: List of chunk bytes
        storage_path: Path to storage directory
    
    Returns:
        Full path to result file
    """
    ensure_storage_dir(storage_path)
    
    result_filename = f"{job_id}-result.csv"
    result_path = os.path.join(storage_path, result_filename)
    
    try:
        with open(result_path, 'wb') as f:
            for chunk in chunks:
                f.write(chunk)
        
        logger.info(f"Result file written: {result_path}")
        return result_path
        
    except Exception as e:
        logger.error(f"Failed to write result file: {e}")
        raise
