#!/usr/bin/env python3
"""
Migration script to add neighborhood field to restaurants collection.
Since Milvus doesn't support adding fields to existing collections,
we'll create a new collection with the updated schema.
"""
import asyncio
from src.vector_db.milvus_client import MilvusClient
from src.utils.logger import app_logger

async def migrate_restaurant_schema():
    """Migrate restaurant schema to include neighborhood field."""
    app_logger.info("🔄 Starting restaurant schema migration...")
    
    try:
        milvus_client = MilvusClient()
        
        # Check if we have existing restaurants
        existing_restaurants = milvus_client.search_restaurants_with_filters({}, 1000)
        app_logger.info(f"📊 Found {len(existing_restaurants)} existing restaurants")
        
        if not existing_restaurants:
            app_logger.info("✅ No existing restaurants to migrate")
            return True
        
        # Create new collection with updated schema
        app_logger.info("🏗️ Creating new collection with neighborhood field...")
        
        # Drop old collection if it exists
        from pymilvus import utility
        if utility.has_collection("restaurants_enhanced_v2"):
            utility.drop_collection("restaurants_enhanced_v2")
            app_logger.info("🗑️ Dropped old v2 collection")
        
        # Create new collection with neighborhood field
        from pymilvus import Collection, CollectionSchema, FieldSchema, DataType
        from src.utils.config import get_settings
        
        settings = get_settings()
        
        fields = [
            # Primary key
            FieldSchema(name="restaurant_id", dtype=DataType.VARCHAR, max_length=100, 
                       is_primary=True, description="Unique restaurant identifier"),
            
            # Core restaurant information with better constraints
            FieldSchema(name="restaurant_name", dtype=DataType.VARCHAR, max_length=300, 
                       description="Restaurant name"),
            FieldSchema(name="google_place_id", dtype=DataType.VARCHAR, max_length=150, 
                       description="Google Places API ID"),
            FieldSchema(name="full_address", dtype=DataType.VARCHAR, max_length=500, 
                       description="Complete address"),
            FieldSchema(name="city", dtype=DataType.VARCHAR, max_length=100, 
                       description="City name"),
            FieldSchema(name="neighborhood", dtype=DataType.VARCHAR, max_length=150, 
                       description="Neighborhood name"),
            
            # Location with better precision
            FieldSchema(name="latitude", dtype=DataType.DOUBLE, 
                       description="Latitude coordinate (double precision)"),
            FieldSchema(name="longitude", dtype=DataType.DOUBLE, 
                       description="Longitude coordinate (double precision)"),
            
            # Cuisine information
            FieldSchema(name="cuisine_type", dtype=DataType.VARCHAR, max_length=100, 
                       description="Primary cuisine type"),
            FieldSchema(name="sub_cuisines", dtype=DataType.JSON, 
                       description="Array of sub-cuisine types"),
            
            # Rating and quality metrics
            FieldSchema(name="rating", dtype=DataType.FLOAT, 
                       description="Average rating (0.0-5.0)"),
            FieldSchema(name="review_count", dtype=DataType.INT64, 
                       description="Total number of reviews"),
            FieldSchema(name="quality_score", dtype=DataType.FLOAT, 
                       description="Calculated quality score (rating * log(reviews+1))"),
            
            # Business information
            FieldSchema(name="price_range", dtype=DataType.INT32, 
                       description="Price range (1-4)"),
            FieldSchema(name="operating_hours", dtype=DataType.JSON, 
                       description="Operating hours by day"),
            FieldSchema(name="meal_types", dtype=DataType.JSON, 
                       description="Supported meal types array"),
            FieldSchema(name="phone", dtype=DataType.VARCHAR, max_length=50, 
                       description="Phone number"),
            FieldSchema(name="website", dtype=DataType.VARCHAR, max_length=500, 
                       description="Website URL"),
            
            # System metadata
            FieldSchema(name="fallback_tier", dtype=DataType.INT32, 
                       description="Fallback tier (1-4)"),
            
            # Embeddings
            FieldSchema(name="embedding_text", dtype=DataType.VARCHAR, max_length=4000, 
                       description="Text used for embedding generation"),
            FieldSchema(name="vector_embedding", dtype=DataType.FLOAT_VECTOR, 
                       dim=settings.vector_dimension, description="OpenAI embedding vector"),
            
            # Timestamps
            FieldSchema(name="created_at", dtype=DataType.VARCHAR, max_length=50, 
                       description="Creation timestamp"),
            FieldSchema(name="updated_at", dtype=DataType.VARCHAR, max_length=50, 
                       description="Last update timestamp")
        ]
        
        schema = CollectionSchema(fields, description="Enhanced restaurant information with neighborhood support")
        new_collection = Collection("restaurants_enhanced_v2", schema)
        
        # Create index
        index_params = {
            "metric_type": "COSINE",
            "index_type": "HNSW",
            "params": {
                "M": 16,
                "efConstruction": 200
            }
        }
        new_collection.create_index("vector_embedding", index_params)
        
        app_logger.info("✅ Created new collection with neighborhood field")
        
        # Migrate existing data
        app_logger.info("🔄 Migrating existing restaurant data...")
        
        migrated_count = 0
        for restaurant in existing_restaurants:
            try:
                # Add empty neighborhood field to existing restaurants
                restaurant['neighborhood'] = ''
                
                # Transform and insert
                embedding_text = milvus_client._create_restaurant_embedding_text(restaurant)
                embedding = await milvus_client._generate_embedding(embedding_text)
                transformed = milvus_client._transform_restaurant_entity_optimized(restaurant, embedding)
                
                new_collection.insert([transformed])
                migrated_count += 1
                
                if migrated_count % 10 == 0:
                    app_logger.info(f"📊 Migrated {migrated_count}/{len(existing_restaurants)} restaurants")
                    
            except Exception as e:
                app_logger.error(f"❌ Error migrating restaurant {restaurant.get('restaurant_name', 'Unknown')}: {e}")
                continue
        
        new_collection.flush()
        app_logger.info(f"✅ Successfully migrated {migrated_count} restaurants")
        
        # Update the client to use the new collection
        milvus_client.collections['restaurants'] = new_collection
        app_logger.info("🔄 Updated client to use new collection")
        
        return True
        
    except Exception as e:
        app_logger.error(f"❌ Migration failed: {e}")
        return False

if __name__ == "__main__":
    asyncio.run(migrate_restaurant_schema())
