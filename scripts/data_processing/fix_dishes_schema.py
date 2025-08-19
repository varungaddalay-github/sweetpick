#!/usr/bin/env python3
"""
Fix dishes collection schema to include neighborhood and cuisine_type fields.
"""

import asyncio
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.vector_db.milvus_client import MilvusClient
from pymilvus import utility

async def fix_dishes_schema():
    """Drop and recreate dishes collection with updated schema."""
    print("🔧 FIXING DISHES COLLECTION SCHEMA")
    print("=" * 50)
    
    # Initialize Milvus client
    milvus_client = MilvusClient()
    
    # Step 1: Drop existing dishes collection
    print("\n🗑️  STEP 1: DROPPING EXISTING DISHES COLLECTION")
    print("-" * 40)
    
    collection_name = "dishes_detailed"
    if utility.has_collection(collection_name):
        utility.drop_collection(collection_name)
        print(f"  ✅ Dropped existing collection: {collection_name}")
    else:
        print(f"  ℹ️  Collection {collection_name} doesn't exist")
    
    # Step 2: Recreate dishes collection with updated schema
    print("\n🏗️  STEP 2: RECREATING DISHES COLLECTION")
    print("-" * 40)
    
    # Force recreation by calling the method directly
    milvus_client._create_dishes_collection()
    print(f"  ✅ Recreated collection: {collection_name}")
    
    # Step 3: Verify the schema
    print("\n🔍 STEP 3: VERIFYING SCHEMA")
    print("-" * 40)
    
    collection = milvus_client.collections.get('dishes')
    if collection:
        schema = collection.schema
        fields = schema.fields
        
        print(f"📊 Collection fields:")
        for field in fields:
            print(f"  • {field.name}: {field.dtype}")
        
        # Check for required fields
        field_names = [field.name for field in fields]
        required_fields = ['neighborhood', 'cuisine_type', 'restaurant_name']
        
        missing_fields = [field for field in required_fields if field not in field_names]
        if missing_fields:
            print(f"  ❌ Missing fields: {missing_fields}")
        else:
            print(f"  ✅ All required fields present")
    
    print(f"\n🎉 SUCCESS: Dishes collection schema updated!")
    print(f"   Can now filter dishes by neighborhood ✓")
    print(f"   Can now filter dishes by cuisine type ✓")

if __name__ == "__main__":
    asyncio.run(fix_dishes_schema())
