#!/usr/bin/env python3
"""
Script to update the neighborhood analysis collection schema to include both top restaurants
"""

import asyncio
from pymilvus import utility
from src.vector_db.discovery_collections import DiscoveryCollections

async def update_neighborhood_schema():
    """Update the neighborhood analysis collection schema."""
    print("🔄 Updating Neighborhood Analysis Collection Schema")
    print("=" * 60)
    
    # Initialize collections
    dc = DiscoveryCollections()
    
    # Get the collection name
    collection_name = dc.collection_names['neighborhood_analysis']
    
    # Check if collection exists
    if utility.has_collection(collection_name):
        print(f"📋 Found existing collection: {collection_name}")
        
        # Get current count
        collection = dc.collections['neighborhood_analysis']
        current_count = collection.num_entities
        print(f"📊 Current records: {current_count}")
        
        if current_count > 0:
            print("⚠️  Warning: Collection has existing data!")
            response = input("Do you want to proceed and drop the collection? (y/N): ")
            if response.lower() != 'y':
                print("❌ Operation cancelled")
                return
        
        # Drop the collection
        print(f"🗑️  Dropping collection: {collection_name}")
        utility.drop_collection(collection_name)
        print("✅ Collection dropped successfully")
    else:
        print(f"📋 Collection {collection_name} does not exist")
    
    # Recreate the collection with new schema
    print(f"🏗️  Creating collection with new schema: {collection_name}")
    dc._create_neighborhood_analysis_collection()
    
    # Verify the new collection
    new_collection = dc.collections['neighborhood_analysis']
    print(f"✅ New collection created successfully")
    print(f"📊 New collection records: {new_collection.num_entities}")
    
    # Show the new schema fields
    print(f"\n📋 New Schema Fields:")
    for field in new_collection.schema.fields:
        print(f"   - {field.name}: {field.dtype}")
    
    print(f"\n🎯 New Structure:")
    print(f"   - Each record represents 1 restaurant")
    print(f"   - 25 neighborhood-cuisine combinations × 3 restaurants = 75 records")
    print(f"   - Each record includes: neighborhood, cuisine, restaurant, top dish")

if __name__ == "__main__":
    asyncio.run(update_neighborhood_schema())
