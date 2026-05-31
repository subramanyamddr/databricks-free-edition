# =============================================================================
# logger_utils.py
# Logging and timing utilities for pipeline execution
# =============================================================================

import logging
import time
from datetime import datetime


def get_logger(name, level="INFO", pipeline=None, env=None):
    """Create a configured logger for pipeline execution.
    
    Args:
        name: Logger name (typically module/layer name)
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        pipeline: Pipeline name for context
        env: Environment name for context
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Set level
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(numeric_level)
    
    # Avoid duplicate handlers if logger already exists
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(numeric_level)
        
        # Format with timestamp and context
        formatter = logging.Formatter(
            fmt='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    # Store context for use in log messages
    logger.pipeline = pipeline
    logger.env = env
    
    return logger


class PipelineTimer:
    """Context manager for timing pipeline operations.
    
    Usage:
        with PipelineTimer(log, "operation_name"):
            # code to time
            pass
    """
    
    def __init__(self, logger, operation_name):
        """Initialize the timer.
        
        Args:
            logger: Logger instance to write timing info
            operation_name: Name of the operation being timed
        """
        self.logger = logger
        self.operation_name = operation_name
        self.start_time = None
        self.end_time = None
    
    def __enter__(self):
        """Start the timer."""
        self.start_time = time.time()
        self.logger.info(
            f"Starting {self.operation_name}",
            extra={"operation": self.operation_name, "action": "start"}
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop the timer and log duration."""
        self.end_time = time.time()
        duration = self.end_time - self.start_time
        
        if exc_type is None:
            self.logger.info(
                f"Completed {self.operation_name} in {duration:.2f}s",
                extra={
                    "operation": self.operation_name,
                    "action": "complete",
                    "duration_seconds": duration
                }
            )
        else:
            self.logger.error(
                f"Failed {self.operation_name} after {duration:.2f}s: {exc_val}",
                extra={
                    "operation": self.operation_name,
                    "action": "failed",
                    "duration_seconds": duration,
                    "error": str(exc_val)
                }
            )
        
        # Don't suppress the exception
        return False
    
    @property
    def elapsed(self):
        """Get elapsed time if timer is running."""
        if self.start_time is None:
            return 0
        if self.end_time is None:
            return time.time() - self.start_time
        return self.end_time - self.start_time
