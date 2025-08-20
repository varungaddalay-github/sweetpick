"""
Logging configuration for the Sweet Morsels RAG application.
"""
import sys
import os
import logging
from src.utils.config import get_settings


def setup_logger():
    """Configure the application logger using Python's standard logging."""
    try:
        settings = get_settings()
    except Exception:
        # Fallback when settings are not available
        settings = type('Settings', (), {
            'log_level': os.getenv('LOG_LEVEL', 'INFO'),
            'environment': os.getenv('ENVIRONMENT', 'development')
        })()

    # Resolve log level
    level_name = settings.log_level if isinstance(settings.log_level, str) else 'INFO'
    log_level = getattr(logging, str(level_name).upper(), logging.INFO)

    logger = logging.getLogger("sweet_morsels")
    logger.setLevel(log_level)

    # Clear existing handlers to avoid duplicate logs
    logger.handlers.clear()
    logger.propagate = False

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler for production
    if getattr(settings, 'environment', 'development') == "production":
        try:
            os.makedirs("logs", exist_ok=True)
            file_handler = logging.FileHandler("logs/app.log")
            file_handler.setLevel(log_level)
            file_formatter = logging.Formatter(
                fmt="%Y-%m-%d %H:%M:%S | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s"
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
        except Exception:
            # If file handler setup fails, continue with console-only logging
            pass

    return logger


# Initialize logger
app_logger = setup_logger() 