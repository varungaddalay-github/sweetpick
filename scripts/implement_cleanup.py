#!/usr/bin/env python3
"""
Implement the actual data cleanup by removing zero-score restaurants.
"""
import asyncio
from src.vector_db.milvus_client import MilvusClient
from src.utils.logger import app_logger

async def implement_cleanup():
    """Implement the data cleanup by removing zero-score restaurants."""
    app_logger.info("🔄 Implementing data cleanup...")
    
    try:
        milvus_client = MilvusClient()
        
        # Get all restaurants
        all_restaurants = milvus_client.search_restaurants_with_filters({}, limit=1000)
        app_logger.info(f"📊 Found {len(all_restaurants)} total restaurants")
        
        # Filter out zero-score restaurants
        valid_restaurants = []
        zero_score_restaurants = []
        
        for restaurant in all_restaurants:
            quality_score = restaurant.get('quality_score', 0)
            if quality_score > 0.0:
                valid_restaurants.append(restaurant)
            else:
                zero_score_restaurants.append(restaurant)
                app_logger.info(f"❌ Removing: {restaurant.get('restaurant_name')} in {restaurant.get('city')}")
        
        app_logger.info(f"📊 Keeping {len(valid_restaurants)} valid restaurants")
        app_logger.info(f"📊 Removing {len(zero_score_restaurants)} zero-score restaurants")
        
        # Drop existing collection
        app_logger.info("🗑️ Dropping existing restaurants collection...")
        from pymilvus import utility
        utility.drop_collection("restaurants_enhanced")
        
        # Recreate collection
        app_logger.info("🏗️ Recreating restaurants collection...")
        milvus_client._create_restaurants_collection()
        
        # Re-insert valid restaurants
        app_logger.info(f"📥 Re-inserting {len(valid_restaurants)} valid restaurants...")
        
        collection = milvus_client.collections.get('restaurants')
        inserted_count = 0
        
        for restaurant in valid_restaurants:
            try:
                # Generate embedding
                embedding_text = milvus_client._create_restaurant_embedding_text(restaurant)
                embedding = await milvus_client._generate_embedding(embedding_text)
                
                # Transform
                transformed = milvus_client._transform_restaurant_entity_optimized(restaurant, embedding)
                
                # Insert
                collection.insert([transformed])
                inserted_count += 1
                
                if inserted_count % 10 == 0:
                    app_logger.info(f"📊 Inserted {inserted_count}/{len(valid_restaurants)} restaurants")
                
            except Exception as e:
                app_logger.error(f"❌ Error re-inserting {restaurant.get('restaurant_name')}: {e}")
                continue
        
        # Flush collection
        collection.flush()
        app_logger.info(f"✅ Successfully inserted {inserted_count} restaurants")
        
        # Verify the cleanup
        app_logger.info("🔍 Verifying cleanup...")
        final_restaurants = milvus_client.search_restaurants_with_filters({}, limit=1000)
        
        city_counts = {}
        for restaurant in final_restaurants:
            city = restaurant.get('city', 'Unknown')
            city_counts[city] = city_counts.get(city, 0) + 1
        
        app_logger.info(f"🏙️ Final city distribution:")
        for city, count in city_counts.items():
            app_logger.info(f"   {city}: {count} restaurants")
        
        app_logger.info("✅ Data cleanup completed successfully!")
        return True
        
    except Exception as e:
        app_logger.error(f"❌ Error implementing cleanup: {e}")
        return False

if __name__ == "__main__":
    print("🔧 This will remove all restaurants with zero quality scores.")
    print("📊 This will fix the issue where Manhattan restaurants appear in Hoboken.")
    
    response = input("Continue with cleanup? (y/N): ")
    if response.lower() == 'y':
        success = asyncio.run(implement_cleanup())
        if success:
            print("✅ Data cleanup completed successfully!")
            print("🎉 The 'restaurants in Hoboken' query should now work correctly!")
        else:
            print("❌ Data cleanup failed!")
    else:
        print("❌ Cleanup cancelled.")
