#!/usr/bin/env python3
"""
Data quality fix script to remove duplicate restaurants and ensure proper city assignments.
"""
import asyncio
from src.vector_db.milvus_client import MilvusClient
from src.utils.logger import app_logger

async def fix_data_quality():
    """Fix data quality issues in the restaurant database."""
    app_logger.info("🔧 Starting data quality fixes...")
    
    try:
        milvus_client = MilvusClient()
        
        # Get all restaurants
        all_restaurants = milvus_client.search_restaurants_with_filters({}, limit=1000)
        app_logger.info(f"📊 Found {len(all_restaurants)} total restaurants")
        
        # Group restaurants by restaurant_id to find duplicates
        restaurant_groups = {}
        for restaurant in all_restaurants:
            restaurant_id = restaurant.get('restaurant_id')
            if restaurant_id:
                if restaurant_id not in restaurant_groups:
                    restaurant_groups[restaurant_id] = []
                restaurant_groups[restaurant_id].append(restaurant)
        
        # Find duplicates
        duplicates = {rid: restaurants for rid, restaurants in restaurant_groups.items() if len(restaurants) > 1}
        
        # Also check for restaurants with same name but different IDs (like LOS TACOS No.1)
        name_groups = {}
        for restaurant in all_restaurants:
            name = restaurant.get('restaurant_name', '').lower().strip()
            if name:
                if name not in name_groups:
                    name_groups[name] = []
                name_groups[name].append(restaurant)
        
        name_duplicates = {name: restaurants for name, restaurants in name_groups.items() if len(restaurants) > 1}
        
        app_logger.info(f"🔍 Found {len(name_duplicates)} restaurants with same name but different IDs")
        
        # Show name duplicates
        for name, restaurants in name_duplicates.items():
            cities = [r.get('city', 'Unknown') for r in restaurants]
            if len(set(cities)) > 1:  # Only show if they're in different cities
                app_logger.warning(f"⚠️ Restaurant '{name}' appears in multiple cities: {cities}")
                for restaurant in restaurants:
                    app_logger.warning(f"   - {restaurant.get('city')}: {restaurant.get('restaurant_id')} (score: {restaurant.get('quality_score', 0)})")
        
        app_logger.info(f"🔍 Found {len(duplicates)} restaurants with duplicate entries")
        
        # Fix duplicates by keeping the best entry for each restaurant
        restaurants_to_keep = []
        restaurants_to_remove = []
        
        for restaurant_id, duplicate_restaurants in duplicates.items():
            app_logger.info(f"🔧 Processing duplicates for restaurant ID: {restaurant_id}")
            
            # Sort by quality score to keep the best one
            sorted_duplicates = sorted(duplicate_restaurants, 
                                     key=lambda x: x.get('quality_score', 0), 
                                     reverse=True)
            
            # Keep the one with highest quality score
            best_restaurant = sorted_duplicates[0]
            restaurants_to_keep.append(best_restaurant)
            
            # Mark others for removal
            for duplicate in sorted_duplicates[1:]:
                restaurants_to_remove.append(duplicate)
                app_logger.info(f"   ❌ Will remove: {duplicate.get('restaurant_name')} in {duplicate.get('city')} (score: {duplicate.get('quality_score', 0)})")
            
            app_logger.info(f"   ✅ Keeping: {best_restaurant.get('restaurant_name')} in {best_restaurant.get('city')} (score: {best_restaurant.get('quality_score', 0)})")
        
        # Also keep all non-duplicate restaurants
        for restaurant_id, restaurants in restaurant_groups.items():
            if len(restaurants) == 1:
                restaurants_to_keep.append(restaurants[0])
        
        app_logger.info(f"📊 Summary:")
        app_logger.info(f"   Total restaurants: {len(all_restaurants)}")
        app_logger.info(f"   Restaurants to keep: {len(restaurants_to_keep)}")
        app_logger.info(f"   Restaurants to remove: {len(restaurants_to_remove)}")
        
        # Verify city distribution after cleanup
        city_counts = {}
        for restaurant in restaurants_to_keep:
            city = restaurant.get('city', 'Unknown')
            city_counts[city] = city_counts.get(city, 0) + 1
        
        app_logger.info(f"🏙️ City distribution after cleanup:")
        for city, count in city_counts.items():
            app_logger.info(f"   {city}: {count} restaurants")
        
        # Ask for confirmation before proceeding
        response = input(f"\n⚠️ This will remove {len(restaurants_to_remove)} duplicate restaurants. Continue? (y/N): ")
        if response.lower() != 'y':
            app_logger.info("❌ Data cleanup cancelled by user")
            return
        
        # TODO: Implement actual deletion logic
        # For now, just report what would be done
        app_logger.info("🔧 Data cleanup analysis completed")
        app_logger.info("📝 To implement actual cleanup, we would:")
        app_logger.info("   1. Delete the restaurants_enhanced collection")
        app_logger.info("   2. Recreate it with the cleaned data")
        app_logger.info("   3. Re-insert only the restaurants_to_keep")
        
        return restaurants_to_keep, restaurants_to_remove
        
    except Exception as e:
        app_logger.error(f"❌ Error fixing data quality: {e}")
        return None, None

async def implement_data_cleanup(restaurants_to_keep, restaurants_to_remove):
    """Actually implement the data cleanup by recreating the collection."""
    app_logger.info("🔄 Implementing data cleanup...")
    
    try:
        milvus_client = MilvusClient()
        
        # Get collection reference
        collection = milvus_client.collections.get('restaurants')
        if not collection:
            app_logger.error("❌ Could not find restaurants collection")
            return False
        
        # Drop and recreate collection
        app_logger.info("🗑️ Dropping existing restaurants collection...")
        from pymilvus import utility
        utility.drop_collection("restaurants_enhanced")
        
        # Recreate collection
        app_logger.info("🏗️ Recreating restaurants collection...")
        milvus_client._create_restaurants_collection()
        
        # Re-insert cleaned data
        app_logger.info(f"📥 Re-inserting {len(restaurants_to_keep)} cleaned restaurants...")
        
        # Transform and insert restaurants
        for restaurant in restaurants_to_keep:
            try:
                # Generate embedding
                embedding_text = milvus_client._create_restaurant_embedding_text(restaurant)
                embedding = await milvus_client._generate_embedding(embedding_text)
                
                # Transform
                transformed = milvus_client._transform_restaurant_entity_optimized(restaurant, embedding)
                
                # Insert
                collection = milvus_client.collections.get('restaurants')
                collection.insert([transformed])
                
            except Exception as e:
                app_logger.error(f"❌ Error re-inserting {restaurant.get('restaurant_name')}: {e}")
                continue
        
        # Flush collection
        collection.flush()
        app_logger.info("✅ Data cleanup completed successfully")
        
        return True
        
    except Exception as e:
        app_logger.error(f"❌ Error implementing data cleanup: {e}")
        return False

if __name__ == "__main__":
    # First analyze the data
    restaurants_to_keep, restaurants_to_remove = asyncio.run(fix_data_quality())
    
    if restaurants_to_keep and restaurants_to_remove:
        # Ask if user wants to implement the cleanup
        response = input(f"\n🔧 Implement the data cleanup? This will recreate the collection. (y/N): ")
        if response.lower() == 'y':
            success = asyncio.run(implement_data_cleanup(restaurants_to_keep, restaurants_to_remove))
            if success:
                print("✅ Data cleanup completed successfully!")
            else:
                print("❌ Data cleanup failed!")
        else:
            print("❌ Data cleanup cancelled.")
