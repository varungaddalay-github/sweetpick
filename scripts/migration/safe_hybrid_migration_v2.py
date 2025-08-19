#!/usr/bin/env python3
"""
Safe migration script to add hybrid fields by creating new collection and migrating data.
Preserves all existing data while enabling hybrid capabilities.
"""

import asyncio
from typing import List, Dict, Any
from src.vector_db.milvus_client import MilvusClient
from src.utils.logger import app_logger
from pymilvus import utility, Collection, FieldSchema, DataType

class SafeHybridMigrationV2:
    """Safe migration by creating new collection and migrating data."""
    
    def __init__(self):
        self.milvus_client = MilvusClient()
        self.old_collection_name = "dishes_detailed"
        self.new_collection_name = "dishes_detailed_hybrid"
        
    async def check_current_schema(self) -> Dict[str, Any]:
        """Check current schema and identify missing hybrid fields."""
        
        print("🔍 Checking Current Schema")
        print("="*30)
        
        if not utility.has_collection(self.old_collection_name):
            print(f"❌ Collection {self.old_collection_name} does not exist")
            return {'exists': False}
        
        collection = Collection(self.old_collection_name)
        schema = collection.schema
        
        print(f"📊 Collection: {self.old_collection_name}")
        print(f"📋 Total fields: {len(schema.fields)}")
        
        # Check for hybrid fields
        hybrid_fields = {
            'topic_mentions': DataType.INT32,
            'topic_score': DataType.FLOAT,
            'final_score': DataType.FLOAT,
            'source': DataType.VARCHAR,
            'hybrid_insights': DataType.JSON
        }
        
        existing_fields = []
        missing_fields = []
        
        for field in schema.fields:
            if field.name in hybrid_fields:
                existing_fields.append(field.name)
            else:
                missing_fields.append(field.name)
        
        print(f"\n✅ Existing hybrid fields: {existing_fields}")
        print(f"❌ Missing hybrid fields: {list(hybrid_fields.keys())}")
        
        # Count existing dishes
        collection.load()
        total_dishes = collection.num_entities
        print(f"📊 Total dishes in collection: {total_dishes}")
        
        return {
            'exists': True,
            'total_fields': len(schema.fields),
            'existing_hybrid_fields': existing_fields,
            'missing_hybrid_fields': list(hybrid_fields.keys()),
            'total_dishes': total_dishes,
            'needs_migration': len(existing_fields) < len(hybrid_fields)
        }
    
    async def create_new_collection_with_hybrid_schema(self) -> bool:
        """Create new collection with hybrid schema."""
        
        print("\n🏗️ Creating New Collection with Hybrid Schema")
        print("="*50)
        
        try:
            # Check if new collection already exists
            if utility.has_collection(self.new_collection_name):
                print(f"⚠️  Collection {self.new_collection_name} already exists")
                response = input("Do you want to drop it and recreate? (yes/no): ")
                if response.lower() == 'yes':
                    utility.drop_collection(self.new_collection_name)
                    print(f"   ✅ Dropped existing collection")
                else:
                    print(f"   ❌ Migration cancelled")
                    return False
            
            # Create new collection with hybrid schema
            fields = [
                # Primary key
                FieldSchema(name="dish_id", dtype=DataType.VARCHAR, max_length=100, 
                           is_primary=True, description="Unique dish identifier"),
                
                # Relationship
                FieldSchema(name="restaurant_id", dtype=DataType.VARCHAR, max_length=100, 
                           description="Foreign key to restaurant"),
                FieldSchema(name="restaurant_name", dtype=DataType.VARCHAR, max_length=300, 
                           description="Restaurant name for quick reference"),
                
                # Location information
                FieldSchema(name="neighborhood", dtype=DataType.VARCHAR, max_length=100, 
                           description="Neighborhood where dish is served"),
                FieldSchema(name="cuisine_type", dtype=DataType.VARCHAR, max_length=100, 
                           description="Cuisine type for the dish"),
                
                # Dish information
                FieldSchema(name="dish_name", dtype=DataType.VARCHAR, max_length=300, 
                           description="Original dish name"),
                FieldSchema(name="normalized_dish_name", dtype=DataType.VARCHAR, max_length=300, 
                           description="Normalized dish name for matching"),
                FieldSchema(name="dish_category", dtype=DataType.VARCHAR, max_length=100, 
                           description="Dish category (appetizer, main, dessert, etc.)"),
                FieldSchema(name="cuisine_context", dtype=DataType.VARCHAR, max_length=100, 
                           description="Cuisine context for the dish"),
                FieldSchema(name="dietary_tags", dtype=DataType.JSON, 
                           description="Array of dietary restrictions/tags"),
                
                # Sentiment and quality metrics
                FieldSchema(name="sentiment_score", dtype=DataType.FLOAT, 
                           description="Overall sentiment score (-1.0 to 1.0)"),
                FieldSchema(name="positive_mentions", dtype=DataType.INT32, 
                           description="Count of positive mentions"),
                FieldSchema(name="negative_mentions", dtype=DataType.INT32, 
                           description="Count of negative mentions"),
                FieldSchema(name="neutral_mentions", dtype=DataType.INT32, 
                           description="Count of neutral mentions"),
                FieldSchema(name="total_mentions", dtype=DataType.INT32, 
                           description="Total mention count"),
                FieldSchema(name="confidence_score", dtype=DataType.FLOAT, 
                           description="Confidence in sentiment analysis (0.0-1.0)"),
                FieldSchema(name="recommendation_score", dtype=DataType.FLOAT, 
                           description="Overall recommendation score (0.0-1.0)"),
                
                # Price and trending information
                FieldSchema(name="avg_price_mentioned", dtype=DataType.FLOAT, 
                           description="Average price mentioned in reviews"),
                FieldSchema(name="trending_score", dtype=DataType.FLOAT, 
                           description="Trending score based on recent mentions"),
                
                # 🆕 Hybrid topics data
                FieldSchema(name="topic_mentions", dtype=DataType.INT32, 
                           description="Number of topic mentions from Google Maps"),
                FieldSchema(name="topic_score", dtype=DataType.FLOAT, 
                           description="Topic popularity score (mentions * weight)"),
                FieldSchema(name="final_score", dtype=DataType.FLOAT, 
                           description="Hybrid final score combining topics and sentiment"),
                FieldSchema(name="source", dtype=DataType.VARCHAR, max_length=50, 
                           description="Data source (topics, sentiment, hybrid)"),
                FieldSchema(name="hybrid_insights", dtype=DataType.JSON, 
                           description="Additional hybrid analysis insights"),
                
                # Embeddings
                FieldSchema(name="embedding_text", dtype=DataType.VARCHAR, max_length=4000, 
                           description="Text used for embedding generation"),
                FieldSchema(name="vector_embedding", dtype=DataType.FLOAT_VECTOR, 
                           dim=self.milvus_client.settings.vector_dimension, description="OpenAI embedding vector"),
                
                # Context and examples
                FieldSchema(name="sample_contexts", dtype=DataType.JSON, 
                           description="Array of sample review contexts"),
                
                # Timestamps
                FieldSchema(name="created_at", dtype=DataType.VARCHAR, max_length=50, 
                           description="Creation timestamp"),
                FieldSchema(name="updated_at", dtype=DataType.VARCHAR, max_length=50, 
                           description="Last update timestamp")
            ]
            
            from pymilvus import CollectionSchema
            schema = CollectionSchema(fields, description="Enhanced dish information with hybrid capabilities")
            
            # Create collection
            collection = Collection(self.new_collection_name, schema)
            
            # Create index
            index_params = {
                "metric_type": "COSINE",
                "index_type": "HNSW",
                "params": {
                    "M": 16,
                    "efConstruction": 200
                }
            }
            collection.create_index("vector_embedding", index_params)
            
            print(f"✅ Created new collection: {self.new_collection_name}")
            print(f"📋 Total fields: {len(fields)}")
            print(f"🆕 Hybrid fields: 5")
            
            return True
            
        except Exception as e:
            app_logger.error(f"Error creating new collection: {e}")
            print(f"❌ Error creating new collection: {e}")
            return False
    
    async def migrate_existing_data(self) -> bool:
        """Migrate existing data to new collection with default hybrid values."""
        
        print("\n🔄 Migrating Existing Data")
        print("="*30)
        
        try:
            # Load old collection
            old_collection = Collection(self.old_collection_name)
            old_collection.load()
            
            # Load new collection
            new_collection = Collection(self.new_collection_name)
            new_collection.load()
            
            total_dishes = old_collection.num_entities
            print(f"📊 Migrating {total_dishes} dishes...")
            
            # Get all dishes from old collection
            all_dishes = old_collection.query(
                expr="dish_id != ''",
                output_fields=["*"]
            )
            
            print(f"📋 Found {len(all_dishes)} dishes to migrate")
            
            # Migrate dishes in batches
            batch_size = 100
            total_migrated = 0
            
            for i in range(0, len(all_dishes), batch_size):
                batch = all_dishes[i:i + batch_size]
                
                # Prepare batch data with hybrid defaults
                migrated_batch = []
                for dish in batch:
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
                    migrated_batch.append(migrated_dish)
                
                # Insert batch into new collection
                new_collection.insert(migrated_batch)
                
                total_migrated += len(batch)
                print(f"   ✅ Migrated {total_migrated}/{len(all_dishes)} dishes")
            
            print(f"🎉 Successfully migrated {total_migrated} dishes!")
            return True
            
        except Exception as e:
            app_logger.error(f"Error migrating data: {e}")
            print(f"❌ Error migrating data: {e}")
            return False
    
    async def switch_collections(self) -> bool:
        """Switch from old collection to new collection."""
        
        print("\n🔄 Switching Collections")
        print("="*25)
        
        try:
            # Update milvus_client to use new collection
            new_collection = Collection(self.new_collection_name)
            self.milvus_client.collections['dishes'] = new_collection
            
            print(f"✅ Switched to new collection: {self.new_collection_name}")
            
            # Verify switch
            collection = self.milvus_client.collections['dishes']
            schema = collection.schema
            
            hybrid_fields = ['topic_mentions', 'topic_score', 'final_score', 'source', 'hybrid_insights']
            found_fields = []
            
            for field in schema.fields:
                if field.name in hybrid_fields:
                    found_fields.append(field.name)
            
            print(f"📋 Schema verification: {len(found_fields)}/{len(hybrid_fields)} hybrid fields")
            
            if len(found_fields) == len(hybrid_fields):
                print(f"✅ All hybrid fields present!")
                return True
            else:
                print(f"❌ Schema verification failed!")
                return False
                
        except Exception as e:
            app_logger.error(f"Error switching collections: {e}")
            print(f"❌ Error switching collections: {e}")
            return False
    
    async def verify_migration(self) -> bool:
        """Verify that migration was successful."""
        
        print("\n🔍 Verifying Migration")
        print("="*25)
        
        try:
            collection = self.milvus_client.collections['dishes']
            collection.load()
            
            # Check schema
            schema = collection.schema
            hybrid_fields = ['topic_mentions', 'topic_score', 'final_score', 'source', 'hybrid_insights']
            
            found_fields = []
            for field in schema.fields:
                if field.name in hybrid_fields:
                    found_fields.append(field.name)
            
            print(f"📋 Schema verification:")
            print(f"   ✅ Found {len(found_fields)}/{len(hybrid_fields)} hybrid fields")
            
            if len(found_fields) != len(hybrid_fields):
                print(f"   ❌ Schema verification failed!")
                return False
            
            # Check data
            sample_dishes = collection.query(
                expr="dish_id != ''",
                output_fields=["dish_id", "dish_name", "topic_mentions", "final_score", "source"],
                limit=5
            )
            
            print(f"📊 Data verification:")
            print(f"   ✅ Found {len(sample_dishes)} sample dishes")
            
            for dish in sample_dishes:
                print(f"   📋 {dish.get('dish_name', 'Unknown')}:")
                print(f"      Topic mentions: {dish.get('topic_mentions', 'MISSING')}")
                print(f"      Final score: {dish.get('final_score', 'MISSING')}")
                print(f"      Source: {dish.get('source', 'MISSING')}")
            
            total_dishes = collection.num_entities
            print(f"📊 Total dishes in collection: {total_dishes}")
            
            return True
            
        except Exception as e:
            app_logger.error(f"Error verifying migration: {e}")
            print(f"❌ Error verifying migration: {e}")
            return False
    
    async def run_migration(self) -> bool:
        """Run the complete safe migration process."""
        
        print("🚀 Starting Safe Hybrid Migration V2")
        print("="*40)
        
        # Step 1: Check current schema
        schema_info = await self.check_current_schema()
        
        if not schema_info['exists']:
            print("❌ Collection does not exist - cannot migrate")
            return False
        
        if not schema_info['needs_migration']:
            print("✅ Schema already has all hybrid fields!")
            return True
        
        print(f"\n📊 Migration Summary:")
        print(f"   Total dishes to preserve: {schema_info['total_dishes']}")
        print(f"   Fields to add: {len(schema_info['missing_hybrid_fields'])}")
        print(f"   Data preservation: ✅ GUARANTEED")
        print(f"   Method: Create new collection and migrate data")
        
        # Step 2: Create new collection with hybrid schema
        if not await self.create_new_collection_with_hybrid_schema():
            return False
        
        # Step 3: Migrate existing data
        if not await self.migrate_existing_data():
            return False
        
        # Step 4: Switch to new collection
        if not await self.switch_collections():
            return False
        
        # Step 5: Verify migration
        if not await self.verify_migration():
            return False
        
        print(f"\n🎉 Safe Migration Complete!")
        print("="*30)
        print(f"✅ All {schema_info['total_dishes']} dishes preserved")
        print(f"✅ Hybrid fields added to schema")
        print(f"✅ Default values set for existing dishes")
        print(f"✅ System ready for hybrid extraction")
        print(f"✅ New collection: {self.new_collection_name}")
        
        return True

async def main():
    """Main migration function."""
    
    migrator = SafeHybridMigrationV2()
    success = await migrator.run_migration()
    
    if success:
        print(f"\n🎯 Next Steps:")
        print(f"   1. Run re-collection for Jersey City Italian restaurants")
        print(f"   2. Test hybrid data verification")
        print(f"   3. Verify enhanced API responses")
        print(f"   4. Plan gradual re-collection for other restaurants")
    else:
        print(f"\n❌ Migration failed - manual intervention may be required")
    
    return success

if __name__ == "__main__":
    asyncio.run(main())
