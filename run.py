#!/usr/bin/env python3
"""
Run script for the Sweet Morsels RAG application.
"""
import sys
import uvicorn
from src.utils.config import get_settings


def check_python_version():
    """Check if Python version is compatible."""
    if sys.version_info < (3, 12, 2):
        print("❌ Error: Python 3.12.2 or higher is required")
        print(f"Current version: {sys.version}")
        sys.exit(1)
    print(f"✅ Python version: {sys.version}")


def main():
    """Start the FastAPI application."""
    # Check Python version first
    check_python_version()
    
    settings = get_settings()
    
    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.environment == "development",
        log_level=settings.log_level.lower()
    )


if __name__ == "__main__":
    main() 