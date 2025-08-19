#!/usr/bin/env python3
"""
Manual data migration script to copy data from old collection to new hybrid collection.
"""

import asyncio
from pymilvus import utility, Collection

async def manual_data_migration():
    """Manually migrate data from old to new collection."""
    
    print("🔄 Manual Data Migration")
    print("="*30)
    
    try:
        # Connect to Milvus first
        from src.vector_db.milvus_client import MilvusClient
        milvus_client = MilvusClient()
        
        # Load old collection
        old_collection = Collection("dishes_detailed")
        old_collection.load()
        
        # Load new collection
        new_collection = Collection("dishes_detailed_hybrid")
        new_collection.load()
        
        print(f"📊 Old collection: {old_collection.num_entities} dishes")
        print(f"📊 New collection: {new_collection.num_entities} dishes")
        
        if old_collection.num_entities == 0:
            print("❌ Old collection is empty")
            return False
        
        if new_collection.num_entities > 0:
            print("⚠️  New collection already has data")
            response = input("Do you want to clear it and re-migrate? (yes/no): ")
            if response.lower() == 'yes':
                # Clear new collection
                new_collection.delete("dish_id != ''")
                print("   ✅ Cleared new collection")
            else:
                print("   ❌ Migration cancelled")
                return False
        
        # Migrate data in small chunks
        total_dishes = old_collection.num_entities
        chunk_size = 50
        total_migrated = 0
        
        print(f"\n🔄 Migrating {total_dishes} dishes...")
        
        for offset in range(0, total_dishes, chunk_size):
            limit = min(chunk_size, total_dishes - offset)
            
            print(f"   📦 Processing chunk {offset//chunk_size + 1}/{(total_dishes + chunk_size - 1)//chunk_size}")
            
            # Get chunk from old collection
            chunk_dishes = old_collection.query(
                expr="dish_id != ''",
                output_fields=["*"],
                limit=limit,
                offset=offset
            )
            
            if not chunk_dishes:
                print(f"   ⚠️  No dishes found in chunk {offset//chunk_size + 1}")
                continue
            
            # Prepare chunk with hybrid defaults
            migrated_chunk = []
            for dish in chunk_dishes:
                # Use existing sentiment_score as final_score for now
                final_score = dish.get('sentiment_score', 0.0)
                
                migrated_dish = {
                    **dish,  # Keep all existing fields
                    'topic_mentions': 0,  # Default: no topic mentions
                    'topic_score': 0.0,   # Default: no topic score
                    'final_score': final_score,  # Use existing sentiment score
                    'source': 'sentiment',  # Default: existing sentiment data
                    'hybrid_insights': {}  # Default: empty insights
                }
                migrated_chunk.append(migrated_dish)
            
            # Insert into new collection
            new_collection.insert(migrated_chunk)
            
            total_migrated += len(chunk_dishes)
            print(f"   ✅ Migrated {len(chunk_dishes)} dishes (Total: {total_migrated}/{total_dishes})")
            
            # Small delay
            await asyncio.sleep(0.1)
        
        print(f"\n🎉 Successfully migrated {total_migrated} dishes!")
        
        # Verify migration
        new_collection.load()
        final_count = new_collection.num_entities
        print(f"📊 Final count in new collection: {final_count}")
        
        if final_count == total_dishes:
            print(f"✅ Migration verification successful!")
            return True
        else:
            print(f"❌ Migration verification failed! Expected {total_dishes}, got {final_count}")
            return False
            
    except Exception as e:
        print(f"❌ Error during migration: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(manual_data_migration())
    
    if success:
        print(f"\n🎉 Manual migration completed successfully!")
        print(f"   Ready for hybrid re-collection!")
    else:
        print(f"\n❌ Manual migration failed!")
