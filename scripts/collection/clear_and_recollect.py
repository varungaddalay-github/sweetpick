#!/usr/bin/env python3
"""
Clear existing data and re-run collection with fixed quality_score calculation.
"""
from src.vector_db.milvus_client import MilvusClient
from src.utils.logger import app_logger
from pymilvus import utility
import subprocess
import sys

def clear_collections():
    """Clear all data from existing collections."""
    try:
        print("🗑️  Clearing existing collection data...")
        
        # Initialize Milvus client
        milvus_client = MilvusClient()
        
        # Get existing collections
        existing_collections = utility.list_collections()
        print(f"Found collections: {existing_collections}")
        
        # Clear data from each collection (but keep the schema)
        collections_to_clear = [
            "restaurants_enhanced", "dishes_detailed", "locations_metadata"
        ]
        
        for collection_name in collections_to_clear:
            if collection_name in existing_collections:
                try:
                    # Drop and recreate to clear data
                    utility.drop_collection(collection_name)
                    print(f"✅ Cleared: {collection_name}")
                except Exception as e:
                    print(f"⚠️  Error clearing {collection_name}: {e}")
        
        print("🏗️  Collections will be recreated during data collection...")
        return True
        
    except Exception as e:
        print(f"❌ Error clearing collections: {e}")
        return False

def run_data_collection():
    """Run the data collection pipeline."""
    try:
        print("\n🚀 Starting data collection with fixed quality_score...")
        
        # Run the data collection script
        result = subprocess.run([
            sys.executable, "run_data_collection.py"
        ], capture_output=False, text=True)
        
        if result.returncode == 0:
            print("✅ Data collection completed successfully!")
            return True
        else:
            print(f"❌ Data collection failed with return code: {result.returncode}")
            return False
            
    except Exception as e:
        print(f"❌ Error running data collection: {e}")
        return False

def main():
    """Main function to clear and re-collect data."""
    print("🔄 Clear and Re-collect Data with Fixed Quality Score")
    print("=" * 55)
    
    # Step 1: Clear existing data
    if not clear_collections():
        print("❌ Failed to clear collections")
        return False
    
    # Step 2: Re-run data collection
    print("\n" + "=" * 55)
    if not run_data_collection():
        print("❌ Failed to run data collection")
        return False
    
    print("\n🎉 Successfully cleared and re-collected data with quality_score!")
    print("📊 Your restaurants should now have proper quality_score values")
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)