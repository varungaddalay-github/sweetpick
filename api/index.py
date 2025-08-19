"""
Vercel serverless function entry point for Sweet Morsels API.
"""
import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.api.main import app

# Vercel requires the app to be available as 'app' variable
# The FastAPI app is already configured with all routes and middleware
