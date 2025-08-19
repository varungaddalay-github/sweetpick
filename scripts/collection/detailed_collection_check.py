#!/usr/bin/env python3
"""
Detailed check of all collections and their data.
"""
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.vector_db.milvus_client import MilvusClient
from pymilvus import utility, connections
from src.utils.config import get_settings

def check_all_collections():
    """Check all collections in detail."""
    print("🔍 DETAILED COLLECTION CHECK")
    print("=" * 60)
    
    # Connect directly to Milvus
    settings = get_settings()
    if settings.milvus_username and settings.milvus_password:
        connections.connect(
            alias="default",
            uri=settings.milvus_uri,
            user=settings.milvus_username,
            password=settings.milvus_password,
            db_name=settings.milvus_database
        )
    else:
        connections.connect(
            alias="default",
            uri=settings.milvus_uri,
            token=settings.milvus_token,
            db_name=settings.milvus_database
        )
    
    # List all collections
    all_collections = utility.list_collections()
    print(f"📋 All collections: {all_collections}")
    
    # Check each collection
    for collection_name in all_collections:
        print(f"\n🔍 COLLECTION: {collection_name}")
        print("-" * 40)
        
        try:
            collection = utility.get_collection(collection_name)
            stats = collection.get_statistics()
            print(f"   📊 Statistics: {stats}")
            
            # Get row count
            row_count = collection.num_entities
            print(f"   📈 Total rows: {row_count}")
            
            # Get schema
            schema = collection.schema
            print(f"   📋 Schema fields: {len(schema.fields)}")
            for field in schema.fields:
                print(f"      - {field.name}: {field.dtype}")
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    # Now check with our client
    print(f"\n🔍 CHECKING WITH MILVUS CLIENT")
    print("-" * 40)
    
    milvus_client = MilvusClient()
    
    # Check restaurants with different methods
    print("\n🏪 RESTAURANTS - Different query methods:")
    print("-" * 40)
    
    # Method 1: Direct collection access
    try:
        restaurants_collection = milvus_client._get_restaurants_collection()
        if restaurants_collection:
            total_restaurants = restaurants_collection.num_entities
            print(f"   Direct collection count: {total_restaurants}")
    except Exception as e:
        print(f"   Direct collection error: {e}")
    
    # Method 2: Search with filters
    try:
        restaurants_filter = milvus_client.search_restaurants_with_filters({}, limit=10000)
        print(f"   Search with filters: {len(restaurants_filter)}")
        
        # Check first few restaurants for neighborhood data
        print(f"\n   📍 First 5 restaurants with neighborhood data:")
        for i, restaurant in enumerate(restaurants_filter[:5], 1):
            print(f"      {i}. {restaurant.get('restaurant_name', 'N/A')}")
            print(f"         Neighborhood: '{restaurant.get('neighborhood', 'N/A')}'")
            print(f"         Cuisine: {restaurant.get('cuisine_type', 'N/A')}")
            print(f"         Is Famous: {restaurant.get('is_famous', False)}")
            print()
            
    except Exception as e:
        print(f"   Search with filters error: {e}")
    
    # Method 3: Vector search
    try:
        restaurants_vector = milvus_client.search_restaurants("restaurant", limit=10000)
        print(f"   Vector search: {len(restaurants_vector)}")
    except Exception as e:
        print(f"   Vector search error: {e}")
    
    # Check dishes
    print("\n🍽️  DISHES - Different query methods:")
    print("-" * 40)
    
    try:
        dishes_filter = milvus_client.search_dishes_with_filters({}, limit=10000)
        print(f"   Search with filters: {len(dishes_filter)}")
        
        # Check first few dishes for neighborhood data
        print(f"\n   📍 First 5 dishes with neighborhood data:")
        for i, dish in enumerate(dishes_filter[:5], 1):
            print(f"      {i}. {dish.get('dish_name', 'N/A')}")
            print(f"         Restaurant: {dish.get('restaurant_name', 'N/A')}")
            print(f"         Neighborhood: '{dish.get('neighborhood', 'N/A')}'")
            print(f"         Sentiment: {dish.get('sentiment_score', 'N/A')}")
            print()
            
    except Exception as e:
        print(f"   Dishes search error: {e}")

if __name__ == "__main__":
    check_all_collections()
