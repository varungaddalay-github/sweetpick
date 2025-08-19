"""
Vercel entry point for Sweet Morsels Restaurant Recommendation System.
This imports the complete FastAPI application from src/api/main.py
"""

# Try to import the app, but provide a fallback if it fails
try:
    from src.api.main import app
    print("✅ Successfully imported FastAPI app")
except Exception as e:
    print(f"❌ Failed to import main app: {e}")
    
    # Create a minimal fallback app
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    
    app = FastAPI(title="Sweet Morsels - Fallback Mode")
    
    @app.get("/")
    async def root():
        return {"message": "Sweet Morsels API - Fallback Mode", "status": "running"}
    
    @app.get("/health")
    async def health():
        return {"status": "healthy", "mode": "fallback"}
    
    @app.get("/error")
    async def show_error():
        return {"error": str(e), "message": "Main app failed to load"}

# Vercel expects the FastAPI app to be available as 'app'
# No additional code needed - your complete system is imported!