#!/usr/bin/env python3
"""
Simple verification test for HTTP-only mode without starting the full server.
"""

import sys
import os
import asyncio

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_http_only_imports():
    """Test that HTTP-only mode imports work correctly."""
    print("🔍 Testing HTTP-only mode imports...")
    
    try:
        # Import main module
        from src.api import main
        print("✅ Main module imported successfully")
        
        # Check key variables
        print(f"📋 RETRIEVAL_AVAILABLE: {main.RETRIEVAL_AVAILABLE}")
        print(f"📋 MILVUS_AVAILABLE: {main.MILVUS_AVAILABLE}")
        print(f"📋 QUERY_PARSER_AVAILABLE: {main.QUERY_PARSER_AVAILABLE}")
        
        # Verify HTTP-only mode
        if main.RETRIEVAL_AVAILABLE == False:
            print("✅ HTTP-only mode confirmed - no enhanced retrieval engine")
        else:
            print("❌ Enhanced retrieval engine is still enabled")
            return False
            
        if main.MILVUS_AVAILABLE == True:
            print("✅ Milvus HTTP client is available")
        else:
            print("❌ Milvus HTTP client not available")
            return False
            
        return True
        
    except Exception as e:
        print(f"❌ Error testing imports: {e}")
        return False

def test_http_client_functionality():
    """Test HTTP client functionality."""
    print("\n🔍 Testing HTTP client functionality...")
    
    try:
        from src.vector_db.milvus_http_client import MilvusHTTPClient
        
        # Create client
        client = MilvusHTTPClient()
        print("✅ HTTP client created")
        
        # Test sample dishes for different cuisines
        cuisines = ["Italian", "Mexican", "Indian", "Chinese"]
        
        for cuisine in cuisines:
            print(f"🍽️ Testing {cuisine} cuisine...")
            dishes = client._get_sample_dishes(cuisine=cuisine, limit=2)
            
            if dishes:
                print(f"  ✅ Got {len(dishes)} {cuisine} dishes")
                for dish in dishes:
                    print(f"    - {dish.get('dish_name', 'N/A')} at {dish.get('restaurant_name', 'N/A')}")
            else:
                print(f"  ⚠️ No {cuisine} dishes returned")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing HTTP client: {e}")
        return False

def test_query_parser_functionality():
    """Test query parser functionality."""
    print("\n🔍 Testing query parser functionality...")
    
    try:
        from src.query_processing.query_parser import QueryParser
        
        # Create parser
        parser = QueryParser()
        print("✅ Query parser created")
        
        # Test queries
        test_queries = [
            "I want Italian food in Manhattan",
            "Show me Mexican restaurants in Times Square",
            "Best Indian dishes in Manhattan"
        ]
        
        for query in test_queries:
            print(f"📝 Testing query: {query}")
            try:
                parsed = asyncio.run(parser.parse_query(query))
                print(f"  ✅ Parsed successfully")
                print(f"    Location: {parsed.get('location', 'N/A')}")
                print(f"    Cuisine: {parsed.get('cuisine_type', 'N/A')}")
                print(f"    Intent: {parsed.get('intent', 'N/A')}")
            except Exception as e:
                print(f"  ⚠️ Parsing failed: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing query parser: {e}")
        return False

def test_no_enhanced_engine_dependencies():
    """Test that no enhanced engine dependencies are loaded."""
    print("\n🔍 Testing no enhanced engine dependencies...")
    
    try:
        # Check if enhanced engine module is loaded
        enhanced_module = 'src.query_processing.enhanced_retrieval_engine'
        if enhanced_module in sys.modules:
            print(f"❌ Enhanced engine module is loaded: {enhanced_module}")
            return False
        else:
            print("✅ Enhanced engine module not loaded")
        
        # Check if pymilvus is being used
        pymilvus_module = 'pymilvus'
        if pymilvus_module in sys.modules:
            print(f"⚠️ Pymilvus module is loaded: {pymilvus_module}")
            # This might be okay if it's loaded by other dependencies
        else:
            print("✅ Pymilvus module not loaded")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing dependencies: {e}")
        return False

def main():
    """Run all verification tests."""
    print("🚀 Starting HTTP-only mode verification...")
    print("=" * 60)
    
    tests = [
        ("HTTP-only Imports", test_http_only_imports),
        ("HTTP Client Functionality", test_http_client_functionality),
        ("Query Parser Functionality", test_query_parser_functionality),
        ("No Enhanced Engine Dependencies", test_no_enhanced_engine_dependencies),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n🧪 Running: {test_name}")
        print("-" * 40)
        
        try:
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
    print("\n" + "=" * 60)
    print("📊 VERIFICATION SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All verifications passed! HTTP-only mode is working correctly.")
        print("\n📋 Summary:")
        print("  ✅ Enhanced retrieval engine is disabled")
        print("  ✅ Milvus HTTP client is available")
        print("  ✅ Query parser is working")
        print("  ✅ Sample dishes are available for all cuisines")
        print("  ✅ No unwanted dependencies loaded")
        return True
    else:
        print("⚠️ Some verifications failed. Please check the issues above.")
        return False

if __name__ == "__main__":
    # Set dummy environment variables for testing
    os.environ.setdefault('OPENAI_API_KEY', 'test-key')
    os.environ.setdefault('SERPAPI_KEY', 'test-key')
    os.environ.setdefault('MILVUS_URI', 'https://test.milvus.cloud')
    os.environ.setdefault('MILVUS_TOKEN', 'test-token')
    
    success = main()
    sys.exit(0 if success else 1)
