"""
Vercel API function for Sweet Morsels Restaurant Recommendation System.
This is the main API endpoint that Vercel will execute.
"""

# Try to import the app, but provide a fallback if it fails
try:
    import sys
    import os
    # Add the parent directory to the path so we can import from src
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    from src.api.main import app
    print("✅ Successfully imported FastAPI app")
except Exception as e:
    print(f"❌ Failed to import main app: {e}")
    
    # Create a minimal fallback app
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    from fastapi.middleware.cors import CORSMiddleware
    
    app = FastAPI(title="Sweet Morsels - Fallback Mode")
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    @app.get("/")
    async def root():
        return JSONResponse(content={
            "message": "Sweet Morsels API - Fallback Mode", 
            "status": "running",
            "version": "1.0.0"
        })
    
    @app.get("/health")
    async def health():
        return JSONResponse(content={
            "status": "healthy", 
            "mode": "fallback",
            "timestamp": "2024-01-01T00:00:00Z"
        })
    
    @app.get("/error")
    async def show_error():
        return JSONResponse(content={
            "error": str(e), 
            "message": "Main app failed to load",
            "type": "import_error"
        })
    
    @app.get("/test")
    async def test():
        return JSONResponse(content={
            "message": "Test endpoint working",
            "deployment": "successful"
        })
    
    @app.get("/debug")
    async def debug():
        """Debug endpoint to check environment variables and configuration."""
        env_vars = {
            "OPENAI_API_KEY": "SET" if os.getenv("OPENAI_API_KEY") else "NOT SET",
            "SERPAPI_API_KEY": "SET" if os.getenv("SERPAPI_API_KEY") else "NOT SET",
            "MILVUS_URI": "SET" if os.getenv("MILVUS_URI") else "NOT SET",
            "MILVUS_TOKEN": "SET" if os.getenv("MILVUS_TOKEN") else "NOT SET",
            "ENVIRONMENT": os.getenv("ENVIRONMENT", "NOT SET"),
            "LOG_LEVEL": os.getenv("LOG_LEVEL", "NOT SET")
        }
        
        return JSONResponse(content={
            "status": "debug_info",
            "environment_variables": env_vars,
            "import_error": str(e),
            "python_path": sys.path[:3]  # First 3 paths
        })

# Vercel expects the FastAPI app to be available as 'app'
# No additional code needed - your complete system is imported!
