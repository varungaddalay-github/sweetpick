# drop_collections.py
#!/usr/bin/env python3
"""
Utility to drop existing Milvus collections for schema migration.
"""
from pymilvus import connections, utility
from src.utils.config import get_settings
from src.utils.logger import app_logger

def drop_existing_collections():
    """Drop all existing collections to prepare for schema migration."""
    try:
        # Connect to Milvus
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
        
        app_logger.info("Connected to Milvus Cloud")
        
        # Get list of existing collections
        existing_collections = utility.list_collections()
        app_logger.info(f"Found existing collections: {existing_collections}")
        
        # Collections to drop
        collections_to_drop = []
        for collection_name in existing_collections:
            if collection_name in [
                "restaurants", "restaurants_enhanced",
                "dishes", "dishes_detailed", 
                "locations", "locations_metadata"
            ]:
                collections_to_drop.append(collection_name)
        
        if not collections_to_drop:
            app_logger.info("No collections to drop")
            return True
        
        # Confirm before dropping
        print(f"\nWARNING: About to drop {len(collections_to_drop)} collections:")
        for name in collections_to_drop:
            print(f"  - {name}")
        
        response = input("\nAre you sure you want to drop these collections? (yes/no): ")
        if response.lower() != 'yes':
            print("Operation cancelled")
            return False
        
        # Drop collections
        for collection_name in collections_to_drop:
            try:
                utility.drop_collection(collection_name)
                app_logger.info(f"✅ Dropped collection: {collection_name}")
                print(f"✅ Dropped: {collection_name}")
            except Exception as e:
                app_logger.error(f"❌ Error dropping {collection_name}: {e}")
                print(f"❌ Error dropping {collection_name}: {e}")
        
        # Verify collections are dropped
        remaining_collections = utility.list_collections()
        app_logger.info(f"Remaining collections: {remaining_collections}")
        print(f"\nRemaining collections: {remaining_collections}")
        
        return True
        
    except Exception as e:
        app_logger.error(f"Error in drop_collections: {e}")
        print(f"Error: {e}")
        return False
    finally:
        try:
            connections.disconnect("default")
        except:
            pass

if __name__ == "__main__":
    print("🗑️  Milvus Collection Drop Utility")
    print("=" * 40)
    success = drop_existing_collections()
    if success:
        print("\n✅ Collection drop completed successfully!")
    else:
        print("\n❌ Collection drop failed or was cancelled")