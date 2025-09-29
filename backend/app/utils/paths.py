"""
Path Utilities

Utilities for handling file paths, uploads, and directory management.
"""

import logging
import uuid
from pathlib import Path
from typing import Optional
import shutil

logger = logging.getLogger(__name__)

def ensure_upload_path(filename: str, upload_dir: str = "uploads") -> Path:
    """
    Ensure upload directory exists and return safe file path.
    
    Args:
        filename: Original filename
        upload_dir: Upload directory name
        
    Returns:
        Path object for the upload file
    """
    upload_path = Path(upload_dir)
    upload_path.mkdir(exist_ok=True)
    
    # Generate unique filename to avoid conflicts
    file_stem = Path(filename).stem
    file_suffix = Path(filename).suffix
    unique_id = str(uuid.uuid4())[:8]
    
    safe_filename = f"{file_stem}_{unique_id}{file_suffix}"
    full_path = upload_path / safe_filename
    
    logger.info(f"Generated upload path: {full_path}")
    return full_path

def ensure_temp_path(prefix: str = "temp", temp_dir: str = "temp") -> Path:
    """
    Create temporary file path with unique identifier.
    
    Args:
        prefix: Prefix for temporary file
        temp_dir: Temporary directory name
        
    Returns:
        Path object for temporary file
    """
    temp_path = Path(temp_dir)
    temp_path.mkdir(exist_ok=True)
    
    unique_id = str(uuid.uuid4())
    temp_filename = f"{prefix}_{unique_id}"
    full_path = temp_path / temp_filename
    
    logger.debug(f"Generated temp path: {full_path}")
    return full_path

def ensure_cache_path(cache_key: str, cache_dir: str = "cache") -> Path:
    """
    Ensure cache directory exists and return cache file path.
    
    Args:
        cache_key: Cache key for the file
        cache_dir: Cache directory name
        
    Returns:
        Path object for cache file
    """
    cache_path = Path(cache_dir)
    cache_path.mkdir(exist_ok=True)
    
    # Sanitize cache key for filesystem
    safe_key = "".join(c for c in cache_key if c.isalnum() or c in "._-")
    full_path = cache_path / safe_key
    
    logger.debug(f"Generated cache path: {full_path}")
    return full_path

def cleanup_temp_files(temp_dir: str = "temp", max_age_hours: int = 24):
    """
    Clean up old temporary files.
    
    Args:
        temp_dir: Temporary directory to clean
        max_age_hours: Maximum age of files to keep in hours
    """
    try:
        temp_path = Path(temp_dir)
        if not temp_path.exists():
            return
        
        import time
        current_time = time.time()
        max_age_seconds = max_age_hours * 3600
        
        cleaned_count = 0
        for file_path in temp_path.iterdir():
            if file_path.is_file():
                file_age = current_time - file_path.stat().st_mtime
                if file_age > max_age_seconds:
                    file_path.unlink()
                    cleaned_count += 1
                    logger.debug(f"Cleaned up temp file: {file_path}")
        
        if cleaned_count > 0:
            logger.info(f"Cleaned up {cleaned_count} temporary files")
            
    except Exception as e:
        logger.error(f"Failed to cleanup temp files: {e}")

def validate_file_path(file_path: str, allowed_extensions: Optional[list[str]] = None) -> bool:
    """
    Validate file path for security and format requirements.
    
    Args:
        file_path: File path to validate
        allowed_extensions: List of allowed file extensions
        
    Returns:
        True if file path is valid
    """
    try:
        path = Path(file_path)
        
        # Check if file exists
        if not path.exists():
            logger.warning(f"File does not exist: {file_path}")
            return False
        
        # Check if it's a file (not directory)
        if not path.is_file():
            logger.warning(f"Path is not a file: {file_path}")
            return False
        
        # Check file extension if specified
        if allowed_extensions:
            file_extension = path.suffix.lower()
            if file_extension not in [ext.lower() for ext in allowed_extensions]:
                logger.warning(f"File extension not allowed: {file_extension}")
                return False
        
        # Basic security check - no path traversal
        resolved_path = path.resolve()
        if ".." in str(resolved_path):
            logger.warning(f"Potential path traversal detected: {file_path}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"File path validation failed: {e}")
        return False

def get_file_info(file_path: str) -> dict:
    """
    Get basic information about a file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Dictionary with file information
    """
    try:
        path = Path(file_path)
        
        if not path.exists():
            return {"error": "File does not exist"}
        
        stat = path.stat()
        
        return {
            "name": path.name,
            "size_bytes": stat.st_size,
            "size_mb": round(stat.st_size / (1024 * 1024), 2),
            "extension": path.suffix,
            "modified_time": stat.st_mtime,
            "is_file": path.is_file(),
            "is_directory": path.is_dir()
        }
        
    except Exception as e:
        logger.error(f"Failed to get file info: {e}")
        return {"error": str(e)}
