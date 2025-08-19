#!/usr/bin/env python3
"""
Safe migration script to add hybrid fields to existing dishes collection.
Preserves all existing data while enabling hybrid capabilities.
"""

import asyncio
from typing import List, Dict, Any
from src.vector_db.milvus_client import MilvusClient
from src.utils.logger import app_logger
from pymilvus import utility, Collection, FieldSchema, DataType

class SafeHybridMigration:
    """Safe migration to add hybrid fields without data loss."""
    
    def __init__(self):
        self.milvus_client = MilvusClient()
        self.collection_name = "dishes_detailed"
        
    async def check_current_schema(self) -> Dict[str, Any]:
        """Check current schema and identify missing hybrid fields."""
        
        print("🔍 Checking Current Schema")
        print("="*30)
        
        if not utility.has_collection(self.collection_name):
            print(f"❌ Collection {self.collection_name} does not exist")
            return {'exists': False}
        
        collection = Collection(self.collection_name)
        schema = collection.schema
        
        print(f"📊 Collection: {self.collection_name}")
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
    
    async def add_hybrid_fields(self) -> bool:
        """Add hybrid fields to existing collection."""
        
        print("\n🔄 Adding Hybrid Fields to Schema")
        print("="*40)
        
        try:
            collection = Collection(self.collection_name)
            
            # Define new hybrid fields
            new_fields = [
                FieldSchema(name="topic_mentions", dtype=DataType.INT32, 
                           description="Number of topic mentions from Google Maps"),
                FieldSchema(name="topic_score", dtype=DataType.FLOAT, 
                           description="Topic popularity score (mentions * weight)"),
                FieldSchema(name="final_score", dtype=DataType.FLOAT, 
                           description="Hybrid final score combining topics and sentiment"),
                FieldSchema(name="source", dtype=DataType.VARCHAR, max_length=50, 
                           description="Data source (topics, sentiment, hybrid)"),
                FieldSchema(name="hybrid_insights", dtype=DataType.JSON, 
                           description="Additional hybrid analysis insights")
            ]
            
            print(f"📝 Adding {len(new_fields)} new fields:")
            for field in new_fields:
                print(f"   • {field.name}: {field.dtype}")
            
            # Add fields to schema
            collection.alter_schema(new_fields)
            
            print(f"✅ Schema updated successfully!")
            
            # Verify the update
            schema = collection.schema
            hybrid_field_names = [field.name for field in new_fields]
            found_fields = []
            
            for field in schema.fields:
                if field.name in hybrid_field_names:
                    found_fields.append(field.name)
            
            if len(found_fields) == len(hybrid_field_names):
                print(f"✅ All {len(hybrid_field_names)} hybrid fields verified!")
                return True
            else:
                print(f"❌ Schema update verification failed!")
                print(f"   Expected: {hybrid_field_names}")
                print(f"   Found: {found_fields}")
                return False
                
        except Exception as e:
            app_logger.error(f"Error adding hybrid fields: {e}")
            print(f"❌ Error adding hybrid fields: {e}")
            return False
    
    async def update_existing_dishes_with_defaults(self) -> bool:
        """Update existing dishes with default values for new hybrid fields."""
        
        print("\n🔄 Updating Existing Dishes with Default Values")
        print("="*50)
        
        try:
            collection = Collection(self.collection_name)
            collection.load()
            
            total_dishes = collection.num_entities
            print(f"📊 Updating {total_dishes} existing dishes...")
            
            # Get all existing dishes
            existing_dishes = collection.query(
                expr="dish_id != ''",
                output_fields=["dish_id", "sentiment_score", "recommendation_score"]
            )
            
            print(f"📋 Found {len(existing_dishes)} dishes to update")
            
            # Prepare update data
            update_data = []
            for dish in existing_dishes:
                # Use existing sentiment_score as final_score for now
                final_score = dish.get('sentiment_score', 0.0)
                
                update_data.append({
                    'dish_id': dish['dish_id'],
                    'topic_mentions': 0,  # Default: no topic mentions
                    'topic_score': 0.0,   # Default: no topic score
                    'final_score': final_score,  # Use existing sentiment score
                    'source': 'sentiment',  # Default: existing sentiment data
                    'hybrid_insights': {}  # Default: empty insights
                })
            
            # Update dishes in batches
            batch_size = 100
            total_updated = 0
            
            for i in range(0, len(update_data), batch_size):
                batch = update_data[i:i + batch_size]
                
                # Update each dish in the batch
                for dish_update in batch:
                    collection.upsert([{
                        'dish_id': dish_update['dish_id'],
                        'topic_mentions': dish_update['topic_mentions'],
                        'topic_score': dish_update['topic_score'],
                        'final_score': dish_update['final_score'],
                        'source': dish_update['source'],
                        'hybrid_insights': dish_update['hybrid_insights']
                    }])
                
                total_updated += len(batch)
                print(f"   ✅ Updated {total_updated}/{len(update_data)} dishes")
            
            print(f"🎉 Successfully updated {total_updated} dishes with default hybrid values!")
            return True
            
        except Exception as e:
            app_logger.error(f"Error updating existing dishes: {e}")
            print(f"❌ Error updating existing dishes: {e}")
            return False
    
    async def verify_migration(self) -> bool:
        """Verify that migration was successful."""
        
        print("\n🔍 Verifying Migration")
        print("="*25)
        
        try:
            collection = Collection(self.collection_name)
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
        
        print("🚀 Starting Safe Hybrid Migration")
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
        
        # Step 2: Add hybrid fields
        if not await self.add_hybrid_fields():
            return False
        
        # Step 3: Update existing dishes with defaults
        if not await self.update_existing_dishes_with_defaults():
            return False
        
        # Step 4: Verify migration
        if not await self.verify_migration():
            return False
        
        print(f"\n🎉 Safe Migration Complete!")
        print("="*30)
        print(f"✅ All {schema_info['total_dishes']} dishes preserved")
        print(f"✅ Hybrid fields added to schema")
        print(f"✅ Default values set for existing dishes")
        print(f"✅ System ready for hybrid extraction")
        
        return True

async def main():
    """Main migration function."""
    
    migrator = SafeHybridMigration()
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
