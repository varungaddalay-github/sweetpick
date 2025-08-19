#!/usr/bin/env python3
"""
Run script for the optimized SweetPick API.
Includes comprehensive monitoring and performance optimizations.
"""
import asyncio
import uvicorn
import sys
import os
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.monitoring.metrics_collector import monitoring


async def initialize_optimized_api():
    """Initialize the optimized API components."""
    try:
        app_logger.info("🚀 Initializing Optimized SweetPick API...")
        
        # Initialize monitoring
        app_logger.info("📊 Initializing monitoring system...")
        
        # Start monitoring loop in background
        monitoring_task = asyncio.create_task(monitoring.start_monitoring_loop())
        
        app_logger.info("✅ Optimized API initialization completed")
        
        return monitoring_task
        
    except Exception as e:
        app_logger.error(f"❌ Error initializing optimized API: {e}")
        raise


def main():
    """Main function to run the optimized API."""
    try:
        # Load settings
        settings = get_settings()
        
        # Initialize monitoring
        monitoring_task = asyncio.run(initialize_optimized_api())
        
        # Configure uvicorn
        config = uvicorn.Config(
            "src.api.optimized_main:app",
            host="0.0.0.0",
            port=8000,
            reload=False,  # Disable reload for production
            log_level="info",
            access_log=True,
            workers=1,  # Single worker for now, can be scaled with load balancer
            loop="asyncio"
        )
        
        # Create server
        server = uvicorn.Server(config)
        
        app_logger.info("🌐 Starting Optimized SweetPick API Server...")
        app_logger.info(f"📡 Server will be available at: http://localhost:8000")
        app_logger.info(f"📚 API Documentation: http://localhost:8000/docs")
        app_logger.info(f"🏥 Health Check: http://localhost:8000/health")
        app_logger.info(f"📊 Monitoring: http://localhost:8000/monitoring")
        app_logger.info(f"📈 Statistics: http://localhost:8000/stats")
        
        # Run the server
        server.run()
        
    except KeyboardInterrupt:
        app_logger.info("🛑 Server stopped by user")
    except Exception as e:
        app_logger.error(f"❌ Error running optimized API: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
