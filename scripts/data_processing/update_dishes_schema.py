#!/usr/bin/env python3
"""
Update dishes collection schema with hybrid fields.
"""

import asyncio
from src.vector_db.milvus_client import MilvusClient
from src.utils.logger import app_logger

async def update_dishes_schema():
    """Update dishes collection schema with hybrid fields."""
    
    print("🔄 Updating Dishes Collection Schema")
    print("="*45)
    
    milvus_client = MilvusClient()
    
    try:
        # Check if dishes collection exists
        from pymilvus import utility
        
        if utility.has_collection("dishes_detailed"):
            print("📊 Found existing dishes_detailed collection")
            
            # Get current collection
            from pymilvus import Collection
            collection = Collection("dishes_detailed")
            
            # Get current schema
            schema = collection.schema
            print(f"📋 Current schema has {len(schema.fields)} fields")
            
            # Check if hybrid fields already exist
            hybrid_fields = ['topic_mentions', 'topic_score', 'final_score', 'source', 'hybrid_insights']
            existing_hybrid_fields = []
            missing_hybrid_fields = []
            
            for field in schema.fields:
                if field.name in hybrid_fields:
                    existing_hybrid_fields.append(field.name)
                else:
                    missing_hybrid_fields.append(field.name)
            
            print(f"✅ Existing hybrid fields: {existing_hybrid_fields}")
            print(f"❌ Missing hybrid fields: {missing_hybrid_fields}")
            
            if len(existing_hybrid_fields) == len(hybrid_fields):
                print("✅ All hybrid fields already exist in schema!")
                return True
            
            print(f"\n⚠️  Schema needs to be updated with hybrid fields")
            print(f"   This will require dropping and recreating the collection")
            print(f"   All existing data will be lost!")
            
            # Ask for confirmation
            response = input("\nDo you want to proceed? (yes/no): ")
            if response.lower() != 'yes':
                print("❌ Schema update cancelled")
                return False
            
            # Drop the collection
            print(f"\n🗑️  Dropping existing dishes_detailed collection...")
            utility.drop_collection("dishes_detailed")
            print(f"   ✅ Collection dropped")
            
            # Remove from milvus_client collections dict
            if 'dishes' in milvus_client.collections:
                del milvus_client.collections['dishes']
            
        else:
            print("📊 No existing dishes_detailed collection found")
        
        # Create new collection with updated schema
        print(f"\n🏗️  Creating new dishes_detailed collection with hybrid fields...")
        milvus_client._create_dishes_collection()
        
        # Verify the new schema
        collection = Collection("dishes_detailed")
        schema = collection.schema
        
        print(f"📋 New schema has {len(schema.fields)} fields")
        
        # Check for hybrid fields
        hybrid_fields = ['topic_mentions', 'topic_score', 'final_score', 'source', 'hybrid_insights']
        found_hybrid_fields = []
        
        for field in schema.fields:
            if field.name in hybrid_fields:
                found_hybrid_fields.append(field.name)
                print(f"   ✅ {field.name}: {field.dtype}")
        
        if len(found_hybrid_fields) == len(hybrid_fields):
            print(f"\n🎉 Schema update successful!")
            print(f"   All {len(hybrid_fields)} hybrid fields added")
            return True
        else:
            print(f"\n❌ Schema update failed!")
            print(f"   Expected {len(hybrid_fields)} hybrid fields, found {len(found_hybrid_fields)}")
            return False
        
    except Exception as e:
        app_logger.error(f"Error updating dishes schema: {e}")
        print(f"❌ Error updating dishes schema: {e}")
        return False

async def main():
    """Main function."""
    
    success = await update_dishes_schema()
    
    if success:
        print(f"\n🎯 Next Steps:")
        print(f"   1. Run re-collection scripts to populate hybrid data")
        print(f"   2. Test hybrid data verification")
        print(f"   3. Verify API responses with hybrid data")
    else:
        print(f"\n❌ Schema update failed - manual intervention may be required")
    
    return success

if __name__ == "__main__":
    asyncio.run(main())
