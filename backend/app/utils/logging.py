"""
Logging Utilities

Structured logging configuration for EBA XBRL Validator.
Emits JSON logs with required metrics keys.
"""

import logging
import json
import sys
from datetime import datetime
from typing import Dict, Any, Optional
from pathlib import Path

class JSONFormatter(logging.Formatter):
    """Custom formatter for structured JSON logging."""
    
    def __init__(self):
        super().__init__()
        
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        
        # Base log entry
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        
        # Add extra fields from record
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
                          'filename', 'module', 'lineno', 'funcName', 'created', 
                          'msecs', 'relativeCreated', 'thread', 'threadName', 
                          'processName', 'process', 'getMessage', 'exc_info', 'exc_text', 'stack_info']:
                log_entry[key] = value
        
        return json.dumps(log_entry, default=str)

class ValidationLogger:
    """Logger for validation operations with structured metrics."""
    
    def __init__(self, logger_name: str):
        """
        Initialize validation logger.
        
        Args:
            logger_name: Name of the logger
        """
        self.logger = logging.getLogger(logger_name)
        
    def log_validation_start(self, trace_id: str, run_id: str, file_path: str, profile: str):
        """Log validation start event."""
        self.logger.info(
            "Validation started",
            extra={
                "trace_id": trace_id,
                "run_id": run_id,
                "file_path": file_path,
                "profile": profile,
                "event": "validation_start"
            }
        )
    
    def log_validation_complete(self, trace_id: str, run_id: str, duration_ms: int, 
                              facts_count: int, dmp_version: str, is_csv: bool, 
                              validation_status: str, errors_count: int = 0):
        """
        Log validation completion with required metrics.
        
        Emits all required metrics keys from development plan:
        trace_id, run_id, duration_ms, facts_count, dmp_version, is_csv
        """
        self.logger.info(
            "Validation completed",
            extra={
                "trace_id": trace_id,
                "run_id": run_id,
                "duration_ms": duration_ms,
                "facts_count": facts_count,
                "dmp_version": dmp_version,
                "is_csv": is_csv,
                "validation_status": validation_status,
                "errors_count": errors_count,
                "event": "validation_complete"
            }
        )
    
    def log_validation_error(self, trace_id: str, run_id: str, error: str, duration_ms: int = 0):
        """Log validation error event."""
        self.logger.error(
            "Validation failed",
            extra={
                "trace_id": trace_id,
                "run_id": run_id,
                "error": error,
                "duration_ms": duration_ms,
                "event": "validation_error"
            }
        )

def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None):
    """
    Set up structured logging configuration.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional log file path
    """
    
    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create JSON formatter
    json_formatter = JSONFormatter()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(json_formatter)
    root_logger.addHandler(console_handler)
    
    # File handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(json_formatter)
        root_logger.addHandler(file_handler)
    else:
        # Default file handler
        default_log_file = logs_dir / "eba_validator.log"
        file_handler = logging.FileHandler(default_log_file)
        file_handler.setFormatter(json_formatter)
        root_logger.addHandler(file_handler)
    
    # Log setup completion
    logger = logging.getLogger(__name__)
    logger.info("Structured logging configured", extra={"log_level": log_level})
