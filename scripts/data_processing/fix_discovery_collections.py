#!/usr/bin/env python3
"""
Fix Discovery Collections - Drop and recreate collections with proper vector fields.
This script resolves the SchemaNotReadyException issue.
"""

import asyncio
import sys
import os
from typing import List, Dict, Any

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.vector_db.discovery_collections import DiscoveryCollections
from src.utils.logger import app_logger
from pymilvus import utility

class DiscoveryCollectionsFixer:
    """Fix discovery collections by recreating them with proper schemas."""
    
    def __init__(self):
        self.discovery_collections = DiscoveryCollections()
        
    def fix_collections(self):
        """Drop and recreate all discovery collections."""
        print("🔧 FIXING DISCOVERY COLLECTIONS")
        print("=" * 50)
        
        collection_names = [
            'discovery_popular_dishes',
            'discovery_famous_restaurants', 
            'discovery_neighborhood_analysis',
            'discovery_checkpoints'
        ]
        
        for collection_name in collection_names:
            try:
                print(f"\n🔍 Checking collection: {collection_name}")
                
                if utility.has_collection(collection_name):
                    print(f"   ❌ Dropping existing collection: {collection_name}")
                    utility.drop_collection(collection_name)
                    print(f"   ✅ Dropped collection: {collection_name}")
                else:
                    print(f"   ℹ️ Collection does not exist: {collection_name}")
                    
            except Exception as e:
                print(f"   ⚠️ Error handling collection {collection_name}: {e}")
        
        print(f"\n🔄 Recreating collections...")
        
        # Reinitialize collections (this will recreate them)
        try:
            self.discovery_collections._initialize_collections()
            print("✅ Collections recreated successfully!")
        except Exception as e:
            print(f"❌ Error recreating collections: {e}")
            return False
        
        # Verify collections exist and have proper schemas
        print(f"\n🔍 Verifying collections...")
        for collection_name in collection_names:
            try:
                if utility.has_collection(collection_name):
                    collection = self.discovery_collections.collections.get(collection_name.split('_', 1)[1])
                    if collection:
                        schema = collection.schema
                        fields = schema.fields
                        vector_fields = [f for f in fields if f.dtype.name == 'FLOAT_VECTOR']
                        print(f"   ✅ {collection_name}: {len(vector_fields)} vector fields")
                    else:
                        print(f"   ⚠️ {collection_name}: Collection exists but not in registry")
                else:
                    print(f"   ❌ {collection_name}: Collection not found")
            except Exception as e:
                print(f"   ❌ {collection_name}: Error verifying - {e}")
        
        return True

def main():
    """Main function to fix discovery collections."""
    fixer = DiscoveryCollectionsFixer()
    success = fixer.fix_collections()
    
    if success:
        print(f"\n🎉 Discovery collections fixed successfully!")
        print(f"   You can now run the AI-driven discovery engine.")
    else:
        print(f"\n❌ Failed to fix discovery collections.")
        print(f"   Please check the logs for more details.")

if __name__ == "__main__":
    main()
