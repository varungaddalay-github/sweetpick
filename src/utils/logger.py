"""
Logging configuration for the Sweet Morsels RAG application.
"""
import sys
from loguru import logger
from src.utils.config import get_settings


def setup_logger():
    """Configure the application logger."""
    settings = get_settings()
    
    # Remove default handler
    logger.remove()
    
    # Add console handler with custom format
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=settings.log_level,
        colorize=True
    )
    
    # Add file handler for production
    if settings.environment == "production":
        logger.add(
            "logs/app.log",
            rotation="10 MB",
            retention="7 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            level=settings.log_level
        )
    
    return logger


# Initialize logger
app_logger = setup_logger() 