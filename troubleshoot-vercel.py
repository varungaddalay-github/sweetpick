#!/usr/bin/env python3
"""
Vercel Deployment Troubleshooting Script
Run this locally to check for common issues before deploying.
"""

import os
import sys
import importlib
from pathlib import Path

def check_python_version():
    """Check Python version compatibility."""
    print("🐍 Checking Python version...")
    version = sys.version_info
    print(f"   Python {version.major}.{version.minor}.{version.micro}")
    
    if version.major == 3 and version.minor >= 9:
        print("   ✅ Python version is compatible with Vercel")
    else:
        print("   ⚠️  Python version might be too old for Vercel")
    
    return True

def check_imports():
    """Check if all required modules can be imported."""
    print("\n📦 Checking required imports...")
    
    required_modules = [
        'fastapi',
        'uvicorn',
        'pydantic',
        'openai',
        'pymilvus',
        'requests',
        'aiohttp',
        'tenacity',
        'loguru'
    ]
    
    failed_imports = []
    
    for module in required_modules:
        try:
            importlib.import_module(module)
            print(f"   ✅ {module}")
        except ImportError as e:
            print(f"   ❌ {module}: {e}")
            failed_imports.append(module)
    
    if failed_imports:
        print(f"\n   ⚠️  Failed imports: {failed_imports}")
        return False
    else:
        print("   ✅ All required modules can be imported")
        return True

def check_environment_variables():
    """Check if required environment variables are set."""
    print("\n🔑 Checking environment variables...")
    
    required_vars = [
        'OPENAI_API_KEY',
        'SERPAPI_API_KEY',
        'MILVUS_URI',
        'MILVUS_TOKEN'
    ]
    
    missing_vars = []
    
    for var in required_vars:
        if os.getenv(var):
            print(f"   ✅ {var}")
        else:
            print(f"   ❌ {var} (not set)")
            missing_vars.append(var)
    
    if missing_vars:
        print(f"\n   ⚠️  Missing environment variables: {missing_vars}")
        return False
    else:
        print("   ✅ All required environment variables are set")
        return True

def check_file_structure():
    """Check if all required files exist."""
    print("\n📁 Checking file structure...")
    
    required_files = [
        'index.py',
        'vercel.json',
        'requirements-vercel.txt',
        'src/api/main.py',
        'src/utils/config.py',
        'src/utils/logger.py'
    ]
    
    missing_files = []
    
    for file_path in required_files:
        if Path(file_path).exists():
            print(f"   ✅ {file_path}")
        else:
            print(f"   ❌ {file_path} (missing)")
            missing_files.append(file_path)
    
    if missing_files:
        print(f"\n   ⚠️  Missing files: {missing_files}")
        return False
    else:
        print("   ✅ All required files exist")
        return True

def test_app_import():
    """Test if the FastAPI app can be imported."""
    print("\n🚀 Testing app import...")
    
    try:
        # Add current directory to path
        sys.path.insert(0, os.getcwd())
        
        # Try to import the app
        from src.api.main import app
        print("   ✅ FastAPI app imported successfully")
        
        # Check if app has required attributes
        if hasattr(app, 'routes'):
            print("   ✅ App has routes defined")
        else:
            print("   ⚠️  App might not have routes")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Failed to import app: {e}")
        return False

def main():
    """Run all checks."""
    print("🔧 Vercel Deployment Troubleshooting")
    print("=" * 50)
    
    checks = [
        check_python_version(),
        check_imports(),
        check_environment_variables(),
        check_file_structure(),
        test_app_import()
    ]
    
    print("\n" + "=" * 50)
    print("📊 Summary:")
    
    if all(checks):
        print("✅ All checks passed! Your app should deploy successfully.")
    else:
        print("❌ Some checks failed. Please fix the issues above.")
        print("\n💡 Common solutions:")
        print("   1. Set environment variables in Vercel")
        print("   2. Check requirements-vercel.txt")
        print("   3. Verify file paths and imports")
        print("   4. Check Vercel function logs for specific errors")

if __name__ == "__main__":
    main()
