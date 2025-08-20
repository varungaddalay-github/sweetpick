#!/usr/bin/env python3
"""
Test script to verify HTTP-only mode is working correctly.
Tests that enhanced retrieval engine is disabled and HTTP client is used.
"""

import sys
import os
import asyncio
import json
from datetime import datetime

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_import_behavior():
    """Test that the main.py imports work correctly in HTTP-only mode."""
    print("🔍 Testing import behavior...")
    
    # Capture stdout to check for specific messages
    import io
    import contextlib
    
    captured_output = io.StringIO()
    
    with contextlib.redirect_stdout(captured_output):
        try:
            # Import main module - this should show HTTP-only messages
            from src.api import main
            print("✅ Main module imported successfully")
        except Exception as e:
            print(f"❌ Failed to import main module: {e}")
            return False
    
    output = captured_output.getvalue()
    print(f"📋 Captured output:\n{output}")
    
    # Check for expected messages
    expected_messages = [
        "ℹ️ Using HTTP-only mode - enhanced retrieval engine disabled",
        "✅ Milvus HTTP client available"
    ]
    
    for msg in expected_messages:
        if msg in output:
            print(f"✅ Found expected message: {msg}")
        else:
            print(f"❌ Missing expected message: {msg}")
            return False
    
    # Check that RETRIEVAL_AVAILABLE is False
    if hasattr(main, 'RETRIEVAL_AVAILABLE'):
        if main.RETRIEVAL_AVAILABLE == False:
            print("✅ RETRIEVAL_AVAILABLE is correctly set to False")
        else:
            print(f"❌ RETRIEVAL_AVAILABLE is {main.RETRIEVAL_AVAILABLE}, expected False")
            return False
    else:
        print("❌ RETRIEVAL_AVAILABLE not found in main module")
        return False
    
    return True

async def test_query_processing():
    """Test that query processing uses HTTP client path."""
    print("\n🔍 Testing query processing path...")
    
    try:
        from src.api import main
        from src.vector_db.milvus_http_client import MilvusHTTPClient
        
        # Mock a simple query request
        query_request = {
            "query": "I want Italian food in Manhattan",
            "user_location": None,
            "cuisine_preference": None,
            "price_range": None,
            "max_results": 5
        }
        
        # Check that the system would use HTTP client path
        if not main.RETRIEVAL_AVAILABLE:
            print("✅ System correctly configured to use HTTP client path")
            
            # Test that MilvusHTTPClient is available
            if main.MILVUS_AVAILABLE:
                print("✅ MilvusHTTPClient is available")
            else:
                print("❌ MilvusHTTPClient not available")
                return False
        else:
            print("❌ System still configured to use retrieval engine")
            return False
            
    except Exception as e:
        print(f"❌ Error testing query processing: {e}")
        return False
    
    return True

def test_no_enhanced_engine_import():
    """Test that enhanced retrieval engine is not being imported."""
    print("\n🔍 Testing that enhanced retrieval engine is not imported...")
    
    try:
        # Check if enhanced retrieval engine is in sys.modules
        enhanced_module_name = 'src.query_processing.enhanced_retrieval_engine'
        if enhanced_module_name in sys.modules:
            print(f"❌ Enhanced retrieval engine module is loaded: {enhanced_module_name}")
            return False
        else:
            print("✅ Enhanced retrieval engine module is not loaded")
        
        # Try to import it manually to confirm it exists but isn't auto-imported
        try:
            import src.query_processing.enhanced_retrieval_engine
            print("✅ Enhanced retrieval engine module exists but wasn't auto-imported")
        except ImportError as e:
            print(f"⚠️ Enhanced retrieval engine module not found: {e}")
        
    except Exception as e:
        print(f"❌ Error testing enhanced engine import: {e}")
        return False
    
    return True

async def test_http_client_functionality():
    """Test basic HTTP client functionality."""
    print("\n🔍 Testing HTTP client functionality...")
    
    try:
        from src.vector_db.milvus_http_client import MilvusHTTPClient
        
        # Create HTTP client instance
        client = MilvusHTTPClient()
        print("✅ MilvusHTTPClient created successfully")
        
        # Test basic functionality (without making actual API calls)
        if hasattr(client, '_get_sample_dishes'):
            print("✅ HTTP client has sample dishes method")
        else:
            print("❌ HTTP client missing sample dishes method")
            return False
            
    except Exception as e:
        print(f"❌ Error testing HTTP client: {e}")
        return False
    
    return True

async def main():
    """Run all tests."""
    print("🚀 Starting HTTP-only mode tests...")
    print("=" * 50)
    
    tests = [
        ("Import Behavior", test_import_behavior),
        ("Query Processing Path", test_query_processing),
        ("No Enhanced Engine Import", test_no_enhanced_engine_import),
        ("HTTP Client Functionality", test_http_client_functionality),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n🧪 Running: {test_name}")
        print("-" * 30)
        
        try:
            if asyncio.iscoroutinefunction(test_func):
                result = await test_func()
            else:
                result = test_func()
            
            results.append((test_name, result))
            
            if result:
                print(f"✅ {test_name}: PASSED")
            else:
                print(f"❌ {test_name}: FAILED")
                
        except Exception as e:
            print(f"❌ {test_name}: ERROR - {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY")
    print("=" * 50)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! HTTP-only mode is working correctly.")
        return True
    else:
        print("⚠️ Some tests failed. Please check the issues above.")
        return False

if __name__ == "__main__":
    # Set dummy environment variables for testing
    os.environ.setdefault('OPENAI_API_KEY', 'test-key')
    os.environ.setdefault('SERPAPI_KEY', 'test-key')
    os.environ.setdefault('MILVUS_URI', 'https://test.milvus.cloud')
    os.environ.setdefault('MILVUS_TOKEN', 'test-token')
    
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
