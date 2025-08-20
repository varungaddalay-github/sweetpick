#!/usr/bin/env python3
"""
Debug script to check field names and test direct queries.
"""

import asyncio
import os
import sys
import json

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

async def debug_field_names():
    """Debug field names and test direct queries."""
    print("🔍 Debugging Field Names and Queries...")
    
    try:
        from src.vector_db.milvus_http_client import MilvusHTTPClient
        
        client = MilvusHTTPClient()
        
        # List collections
        print("📋 Listing collections...")
        collections = await client.list_collections()
        print(f"Found collections: {collections}")
        
        if not collections:
            print("❌ No collections found")
            return
        
        # Test each collection
        for collection_name in collections:
            print(f"\n🔍 Testing collection: {collection_name}")
            
            # Get schema
            schema = await client._get_collection_schema(collection_name)
            if schema:
                print(f"✅ Schema found: {schema.get('fields', 'N/A')}")
                
                # Test a simple query with no filters to see what data exists
                print(f"🔍 Testing simple query (no filters)...")
                simple_results = await client._query_collection(collection_name, None, None, 3)
                print(f"📊 Simple query results: {len(simple_results)} results")
                
                if simple_results:
                    print(f"📋 Sample result keys: {list(simple_results[0].keys())}")
                    print(f"🍽️ Sample result: {json.dumps(simple_results[0], indent=2, default=str)}")
                    
                    # Check if this result has Mexican cuisine and Times Square
                    sample = simple_results[0]
                    cuisine = sample.get('cuisine_type', sample.get('cuisineType', ''))
                    neighborhood = sample.get('neighborhood', sample.get('neighborhoodName', ''))
                    city = sample.get('city', sample.get('cityName', ''))
                    
                    print(f"🔍 Sample data analysis:")
                    print(f"   Cuisine: {cuisine}")
                    print(f"   Neighborhood: {neighborhood}")
                    print(f"   City: {city}")
                    
                    # Test specific query for Mexican + Times Square
                    print(f"🔍 Testing specific query: Mexican + Times Square...")
                    specific_results = await client._query_collection(collection_name, "Mexican", "Times Square", 3)
                    print(f"📊 Specific query results: {len(specific_results)} results")
                    
                    if specific_results:
                        print(f"✅ Found matching data!")
                        for i, result in enumerate(specific_results):
                            print(f"Result {i+1}:")
                            print(f"  Restaurant: {result.get('restaurant_name', 'N/A')}")
                            print(f"  Dish: {result.get('dish_name', 'N/A')}")
                            print(f"  Cuisine: {result.get('cuisine_type', 'N/A')}")
                            print(f"  Neighborhood: {result.get('neighborhood', 'N/A')}")
                            print()
                    else:
                        print(f"❌ No matching data found")
                        
                        # Test with different field name variations
                        print(f"🔍 Testing with different field name variations...")
                        
                        # Try different cuisine field names
                        for cuisine_field in ['cuisine_type', 'cuisineType', 'cuisine']:
                            print(f"   Testing cuisine field: {cuisine_field}")
                            # Build custom filter
                            filter_string = f'{cuisine_field} == "Mexican"'
                            print(f"   Filter: {filter_string}")
                            
                            # Try different neighborhood field names
                            for neighborhood_field in ['neighborhood', 'neighborhoodName', 'neighborhood_name']:
                                print(f"   Testing neighborhood field: {neighborhood_field}")
                                full_filter = f'{cuisine_field} == "Mexican" and {neighborhood_field} == "Times Square"'
                                print(f"   Full filter: {full_filter}")
                                
                                # Test this specific filter
                                try:
                                    # Create custom query data
                                    query_data = {
                                        "collectionName": collection_name,
                                        "filter": full_filter,
                                        "limit": 3,
                                        "outputFields": ["*"]
                                    }
                                    
                                    result = await client._make_request("POST", "/v1/vector/query", query_data)
                                    parsed_result = client._parse_query_result(result)
                                    
                                    if parsed_result:
                                        print(f"   ✅ Found {len(parsed_result)} results with {cuisine_field} + {neighborhood_field}")
                                        break
                                    else:
                                        print(f"   ❌ No results with {cuisine_field} + {neighborhood_field}")
                                        
                                except Exception as e:
                                    print(f"   ❌ Error with {cuisine_field} + {neighborhood_field}: {e}")
                                    continue
            else:
                print(f"❌ No schema found for {collection_name}")
        
    except Exception as e:
        print(f"❌ Error debugging field names: {e}")
        import traceback
        traceback.print_exc()

async def main():
    """Main debug function."""
    print("🚀 Starting Field Name Debug...")
    print("=" * 50)
    
    await debug_field_names()
    
    print("\n✅ Field Name Debug Completed!")

if __name__ == "__main__":
    asyncio.run(main())
