#!/usr/bin/env python3
"""
Check what data is actually in the Milvus collections.
"""

import asyncio
import os
import sys
import json

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

async def check_actual_data():
    """Check what data is actually in the collections."""
    print("🔍 Checking Actual Data in Collections...")
    
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
        
        # Check each collection for actual data
        for collection_name in collections:
            print(f"\n🔍 Checking collection: {collection_name}")
            
            # Try to get a few records without any filters
            try:
                # Use the working query format we found in debug
                query_data = {
                    "collectionName": collection_name,
                    "filter": "",  # No filter to get all data
                    "limit": 5,
                    "outputFields": ["*"]
                }
                
                result = await client._make_request("POST", "/v1/vector/query", query_data)
                parsed_result = client._parse_query_result(result)
                
                if parsed_result:
                    print(f"✅ Found {len(parsed_result)} records in {collection_name}")
                    
                    # Show the first few records
                    for i, record in enumerate(parsed_result[:3]):
                        print(f"  Record {i+1}:")
                        
                        # Check for key fields
                        restaurant = record.get('restaurant_name', 'N/A')
                        cuisine = record.get('cuisine_type', 'N/A')
                        neighborhood = record.get('neighborhood', 'N/A')
                        city = record.get('city', 'N/A')
                        dish = record.get('dish_name', 'N/A')
                        
                        print(f"    Restaurant: {restaurant}")
                        print(f"    Cuisine: {cuisine}")
                        print(f"    Neighborhood: {neighborhood}")
                        print(f"    City: {city}")
                        print(f"    Dish: {dish}")
                        print()
                        
                        # Check if this matches what we're looking for
                        if cuisine.lower() == 'mexican' and neighborhood.lower() == 'times square':
                            print(f"    🎯 FOUND MATCH: Mexican + Times Square!")
                        
                else:
                    print(f"❌ No data found in {collection_name}")
                    
            except Exception as e:
                print(f"❌ Error querying {collection_name}: {e}")
                continue
        
    except Exception as e:
        print(f"❌ Error checking data: {e}")
        import traceback
        traceback.print_exc()

async def main():
    """Main function."""
    print("🚀 Starting Data Check...")
    print("=" * 50)
    
    await check_actual_data()
    
    print("\n✅ Data Check Completed!")

if __name__ == "__main__":
    asyncio.run(main())
