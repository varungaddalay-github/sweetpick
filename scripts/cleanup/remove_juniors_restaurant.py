#!/usr/bin/env python3
"""
Remove Junior's Restaurant & Bakery from collections.
"""
from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.vector_db.milvus_client import MilvusClient
from pymilvus import Collection, utility


def remove_juniors_restaurant():
    """Remove Junior's Restaurant & Bakery from all collections."""
    try:
        app_logger.info("🗑️  Removing Junior's Restaurant & Bakery...")
        
        # Initialize Milvus client
        milvus_client = MilvusClient()
        
        # Get all collections
        collections = utility.list_collections()
        app_logger.info(f"📚 Available collections: {collections}")
        
        # Target the specific restaurant ID
        target_restaurant_id = "ai_juniors_restaurant_&_bakery"
        
        total_removed = 0
        
        for collection_name in collections:
            app_logger.info(f"🔍 Processing collection: {collection_name}")
            
            try:
                collection = Collection(collection_name)
                collection.load()
                
                # Get schema to understand available fields
                schema = collection.schema
                available_fields = [field.name for field in schema.fields]
                app_logger.info(f"📋 Available fields: {available_fields}")
                
                # Get total count
                total_count = collection.num_entities
                app_logger.info(f"📊 Collection {collection_name} has {total_count} entities")
                
                # Find Junior's Restaurant records by restaurant_id
                restaurant_ids = []
                
                # Search for the specific restaurant ID
                if 'restaurant_id' in available_fields:
                    try:
                        # Use expression to find the restaurant by ID
                        expr = f"restaurant_id == '{target_restaurant_id}'"
                        results = collection.query(
                            expr=expr,
                            output_fields=available_fields[:5]  # Limit to first 5 fields
                        )
                        
                        for result in results:
                            # Use the primary key field (usually first field)
                            primary_key = available_fields[0]
                            restaurant_ids.append(result.get(primary_key))
                            app_logger.info(f"🗑️  Found Junior's Restaurant: {result}")
                            
                    except Exception as e:
                        app_logger.warning(f"⚠️  Error searching for restaurant ID '{target_restaurant_id}': {e}")
                
                # Remove duplicates
                restaurant_ids = list(set(restaurant_ids))
                
                if restaurant_ids:
                    app_logger.info(f"🗑️  Removing {len(restaurant_ids)} Junior's Restaurant entities from {collection_name}")
                    
                    # Delete the restaurant data using primary key
                    primary_key = available_fields[0]
                    expr = f"{primary_key} in {restaurant_ids}"
                    collection.delete(expr)
                    
                    total_removed += len(restaurant_ids)
                    app_logger.info(f"✅ Removed {len(restaurant_ids)} Junior's Restaurant entities from {collection_name}")
                else:
                    app_logger.info(f"✅ No Junior's Restaurant found in {collection_name}")
                
                collection.release()
                
            except Exception as e:
                app_logger.error(f"❌ Error processing collection {collection_name}: {e}")
                continue
        
        app_logger.info(f"🎉 Junior's Restaurant removal completed. Total removed: {total_removed}")
        return total_removed
        
    except Exception as e:
        app_logger.error(f"❌ Error during Junior's Restaurant removal: {e}")
        return 0
    finally:
        try:
            milvus_client.close()
        except:
            pass


def main():
    """Main removal function."""
    print("🗑️  Remove Junior's Restaurant & Bakery")
    print("=" * 50)
    
    try:
        removed_count = remove_juniors_restaurant()
        print(f"✅ Removed {removed_count} Junior's Restaurant entities")
        
        print(f"\n🎉 Removal completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during removal: {e}")
        return False
    
    return True


if __name__ == "__main__":
    main()
