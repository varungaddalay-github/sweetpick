"""
Vercel entry point for Sweet Morsels Restaurant Recommendation System.
This imports the complete FastAPI application from src/api/main.py
"""

# Import the complete Sweet Morsels FastAPI application
from src.api.main import app

# Vercel expects the FastAPI app to be available as 'app'
# No additional code needed - your complete system is imported!