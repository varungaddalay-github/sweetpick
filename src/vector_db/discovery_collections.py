#!/usr/bin/env python3
"""
Milvus Collections for AI-Driven Discovery Engine
Defines schemas and operations for discovery-specific collections.
"""

import json
import uuid
import asyncio
from typing import List, Dict, Optional, Any, Tuple
from pymilvus import connections, Collection, CollectionSchema, FieldSchema, DataType, utility
from pymilvus.exceptions import MilvusException
from openai import AsyncOpenAI
from src.utils.config import get_settings
from src.utils.logger import app_logger
from datetime import datetime
import time


class DiscoveryCollections:
    """Manages Milvus collections for AI-driven discovery data."""
    
    def __init__(self):
        self.settings = get_settings()
        self.openai_client = AsyncOpenAI(api_key=self.settings.openai_api_key)
        self.collections = {}
        self._embedding_cache = {}
        
        # Collection names
        self.collection_names = {
            'popular_dishes': 'discovery_popular_dishes',
            'famous_restaurants': 'discovery_famous_restaurants', 
            'neighborhood_analysis': 'discovery_neighborhood_analysis',
            'discovery_checkpoints': 'discovery_checkpoints'
        }
        
        # Connect to Milvus first
        self._connect()
        self._initialize_collections()
    
    def _connect(self):
        """Connect to Milvus Cloud."""
        try:
            # Validate URI format
            if not self.settings.milvus_uri.startswith(('http://', 'https://')):
                raise ValueError(
                    f"Invalid Milvus URI format: {self.settings.milvus_uri}. "
                    "Expected format: https://your-cluster.zillizcloud.com"
                )
            
            # Connect to Milvus Cloud
            if self.settings.milvus_username and self.settings.milvus_password:
                # Use username/password authentication
                connections.connect(
                    alias="default",
                    uri=self.settings.milvus_uri,
                    user=self.settings.milvus_username,
                    password=self.settings.milvus_password,
                    db_name=self.settings.milvus_database
                )
                app_logger.info(f"Connected to Milvus Cloud using username/password: {self.settings.milvus_uri}")
            else:
                # Use API token authentication
                connections.connect(
                    alias="default",
                    uri=self.settings.milvus_uri,
                    token=self.settings.milvus_token,
                    db_name=self.settings.milvus_database
                )
                app_logger.info(f"Connected to Milvus Cloud using API token: {self.settings.milvus_uri}")
            
        except Exception as e:
            app_logger.error(f"Failed to connect to Milvus Cloud: {e}")
            app_logger.error(f"URI: {self.settings.milvus_uri}")
            app_logger.error(f"Database: {self.settings.milvus_database}")
            if self.settings.milvus_username:
                app_logger.error(f"Username: {self.settings.milvus_username}")
            else:
                app_logger.error("Using API token authentication")
            raise
    
    def _initialize_collections(self):
        """Initialize all discovery collections."""
        try:
            app_logger.info("Initializing discovery collections...")
            
            # Create collections if they don't exist
            self._create_popular_dishes_collection()
            self._create_famous_restaurants_collection()
            self._create_neighborhood_analysis_collection()
            
            # Create checkpoints collection separately (no vector fields)
            try:
                self._create_discovery_checkpoints_collection()
            except Exception as checkpoint_error:
                app_logger.warning(f"Checkpoints collection initialization failed: {checkpoint_error}")
                # Continue without checkpoints collection
            
            app_logger.info("Discovery collections initialized successfully")
            
        except Exception as e:
            app_logger.error(f"Error initializing discovery collections: {e}")
            # Don't raise the error, just log it and continue
            # This allows the system to work even if some collections fail to initialize
            app_logger.warning("Some collections may not be fully initialized, but continuing...")
    
    def _create_popular_dishes_collection(self):
        """Create collection for popular dishes discovered by AI."""
        collection_name = self.collection_names['popular_dishes']
        
        if utility.has_collection(collection_name):
            self.collections['popular_dishes'] = Collection(collection_name)
            app_logger.info(f"Using existing collection: {collection_name}")
            return
        
        fields = [
            # Primary key
            FieldSchema(name="dish_id", dtype=DataType.VARCHAR, max_length=100, 
                       is_primary=True, description="Unique dish identifier"),
            
            # Core dish information
            FieldSchema(name="dish_name", dtype=DataType.VARCHAR, max_length=300, 
                       description="Dish name"),
            FieldSchema(name="normalized_dish_name", dtype=DataType.VARCHAR, max_length=300, 
                       description="Normalized dish name for matching"),
            
            # Location context
            FieldSchema(name="city", dtype=DataType.VARCHAR, max_length=100, 
                       description="City where dish is popular"),
            FieldSchema(name="neighborhoods", dtype=DataType.JSON, 
                       description="Array of neighborhoods where dish is popular"),
            
            # Popularity metrics
            FieldSchema(name="popularity_score", dtype=DataType.FLOAT, 
                       description="AI-calculated popularity score (0.0-1.0)"),
            FieldSchema(name="frequency", dtype=DataType.INT32, 
                       description="Frequency of mentions across restaurants"),
            FieldSchema(name="avg_sentiment", dtype=DataType.FLOAT, 
                       description="Average sentiment score across mentions"),
            FieldSchema(name="cultural_significance", dtype=DataType.VARCHAR, max_length=500, 
                       description="Cultural significance description"),
            
            # Restaurant associations
            FieldSchema(name="top_restaurants", dtype=DataType.JSON, 
                       description="Array of restaurant names serving this dish"),
            FieldSchema(name="restaurant_count", dtype=DataType.INT32, 
                       description="Number of restaurants serving this dish"),
            
            # Cuisine and category
            FieldSchema(name="primary_cuisine", dtype=DataType.VARCHAR, max_length=100, 
                       description="Primary cuisine type"),
            FieldSchema(name="cuisine_types", dtype=DataType.JSON, 
                       description="Array of cuisine types where dish appears"),
            FieldSchema(name="dish_category", dtype=DataType.VARCHAR, max_length=100, 
                       description="Dish category (appetizer, main, dessert, etc.)"),
            
            # AI analysis metadata
            FieldSchema(name="reasoning", dtype=DataType.VARCHAR, max_length=1000, 
                       description="AI reasoning for popularity"),
            FieldSchema(name="discovery_method", dtype=DataType.VARCHAR, max_length=100, 
                       description="Method used for discovery"),
            FieldSchema(name="confidence_score", dtype=DataType.FLOAT, 
                       description="Confidence in popularity assessment"),
            
            # Embeddings for similarity search
            FieldSchema(name="embedding_text", dtype=DataType.VARCHAR, max_length=4000, 
                       description="Text used for embedding generation"),
            FieldSchema(name="vector_embedding", dtype=DataType.FLOAT_VECTOR, 
                       dim=self.settings.vector_dimension, description="OpenAI embedding vector"),
            
            # Timestamps
            FieldSchema(name="created_at", dtype=DataType.VARCHAR, max_length=50, 
                       description="Creation timestamp"),
            FieldSchema(name="updated_at", dtype=DataType.VARCHAR, max_length=50, 
                       description="Last update timestamp"),
            FieldSchema(name="discovery_timestamp", dtype=DataType.VARCHAR, max_length=50, 
                       description="When dish was discovered")
        ]
        
        schema = CollectionSchema(fields, description="Popular dishes discovered by AI analysis")
        collection = Collection(collection_name, schema)
        
        # Create indexes
        self._create_popular_dishes_indexes(collection)
        
        self.collections['popular_dishes'] = collection
        app_logger.info(f"Created collection: {collection_name}")
    
    def _create_famous_restaurants_collection(self):
        """Create collection for famous restaurants discovered by AI."""
        collection_name = self.collection_names['famous_restaurants']
        
        if utility.has_collection(collection_name):
            self.collections['famous_restaurants'] = Collection(collection_name)
            app_logger.info(f"Using existing collection: {collection_name}")
            return
        
        fields = [
            # Primary key
            FieldSchema(name="restaurant_id", dtype=DataType.VARCHAR, max_length=100, 
                       is_primary=True, description="Unique restaurant identifier"),
            
            # Core restaurant information
            FieldSchema(name="restaurant_name", dtype=DataType.VARCHAR, max_length=300, 
                       description="Restaurant name"),
            FieldSchema(name="city", dtype=DataType.VARCHAR, max_length=100, 
                       description="City where restaurant is located"),
            FieldSchema(name="neighborhood", dtype=DataType.VARCHAR, max_length=150, 
                       description="Neighborhood location"),
            FieldSchema(name="full_address", dtype=DataType.VARCHAR, max_length=500, 
                       description="Complete address"),
            
            # Fame metrics
            FieldSchema(name="fame_score", dtype=DataType.FLOAT, 
                       description="AI-calculated fame score (0.0-1.0)"),
            FieldSchema(name="famous_dish", dtype=DataType.VARCHAR, max_length=300, 
                       description="Dish that made restaurant famous"),
            FieldSchema(name="dish_popularity", dtype=DataType.FLOAT, 
                       description="Popularity score of the famous dish"),
            
            # Restaurant quality metrics
            FieldSchema(name="rating", dtype=DataType.FLOAT, 
                       description="Average rating (0.0-5.0)"),
            FieldSchema(name="review_count", dtype=DataType.INT64, 
                       description="Total number of reviews"),
            FieldSchema(name="quality_score", dtype=DataType.FLOAT, 
                       description="Quality score using logarithmic scaling"),
            
            # Cuisine and business info
            FieldSchema(name="cuisine_type", dtype=DataType.VARCHAR, max_length=100, 
                       description="Primary cuisine type"),
            FieldSchema(name="price_range", dtype=DataType.INT32, 
                       description="Price range (1-4)"),
            FieldSchema(name="phone", dtype=DataType.VARCHAR, max_length=50, 
                       description="Phone number"),
            FieldSchema(name="website", dtype=DataType.VARCHAR, max_length=500, 
                       description="Website URL"),
            
            # Discovery metadata
            FieldSchema(name="discovery_method", dtype=DataType.VARCHAR, max_length=100, 
                       description="Method used for discovery"),
            FieldSchema(name="fame_indicators", dtype=DataType.JSON, 
                       description="Array of fame indicators found"),
            FieldSchema(name="cultural_significance", dtype=DataType.VARCHAR, max_length=500, 
                       description="Cultural significance description"),
            
            # Embeddings
            FieldSchema(name="embedding_text", dtype=DataType.VARCHAR, max_length=4000, 
                       description="Text used for embedding generation"),
            FieldSchema(name="vector_embedding", dtype=DataType.FLOAT_VECTOR, 
                       dim=self.settings.vector_dimension, description="OpenAI embedding vector"),
            
            # Timestamps
            FieldSchema(name="created_at", dtype=DataType.VARCHAR, max_length=50, 
                       description="Creation timestamp"),
            FieldSchema(name="updated_at", dtype=DataType.VARCHAR, max_length=50, 
                       description="Last update timestamp"),
            FieldSchema(name="discovery_timestamp", dtype=DataType.VARCHAR, max_length=50, 
                       description="When restaurant was discovered")
        ]
        
        schema = CollectionSchema(fields, description="Famous restaurants discovered by AI analysis")
        collection = Collection(collection_name, schema)
        
        # Create indexes
        self._create_famous_restaurants_indexes(collection)
        
        self.collections['famous_restaurants'] = collection
        app_logger.info(f"Created collection: {collection_name}")
    
    def _create_neighborhood_analysis_collection(self):
        """Create collection for neighborhood-cuisine analysis results."""
        collection_name = self.collection_names['neighborhood_analysis']
        
        if utility.has_collection(collection_name):
            self.collections['neighborhood_analysis'] = Collection(collection_name)
            app_logger.info(f"Using existing collection: {collection_name}")
            return
        
        fields = [
            # Primary key
            FieldSchema(name="analysis_id", dtype=DataType.VARCHAR, max_length=150, 
                       is_primary=True, description="Unique analysis identifier"),
            
            # Location context
            FieldSchema(name="city", dtype=DataType.VARCHAR, max_length=100, 
                       description="City name"),
            FieldSchema(name="neighborhood", dtype=DataType.VARCHAR, max_length=150, 
                       description="Neighborhood name"),
            FieldSchema(name="cuisine_type", dtype=DataType.VARCHAR, max_length=100, 
                       description="Cuisine type analyzed"),
            
            # Restaurant information (individual restaurant)
            FieldSchema(name="restaurant_rank", dtype=DataType.INT32, 
                       description="Rank of this restaurant (1 or 2)"),
            FieldSchema(name="restaurant_id", dtype=DataType.VARCHAR, max_length=100, 
                       description="ID of restaurant"),
            FieldSchema(name="restaurant_name", dtype=DataType.VARCHAR, max_length=300, 
                       description="Name of restaurant"),
            FieldSchema(name="rating", dtype=DataType.FLOAT, 
                       description="Rating of restaurant"),
            FieldSchema(name="review_count", dtype=DataType.INT64, 
                       description="Review count of restaurant"),
            FieldSchema(name="hybrid_quality_score", dtype=DataType.FLOAT, 
                       description="Hybrid quality score of restaurant"),
            
            # Top dish information (from this restaurant)
            FieldSchema(name="top_dish_name", dtype=DataType.VARCHAR, max_length=300, 
                       description="Name of top dish"),
            FieldSchema(name="top_dish_final_score", dtype=DataType.FLOAT, 
                       description="Final score of top dish"),
            FieldSchema(name="top_dish_sentiment_score", dtype=DataType.FLOAT, 
                       description="Sentiment score of top dish"),
            FieldSchema(name="top_dish_topic_mentions", dtype=DataType.INT32, 
                       description="Topic mentions of top dish"),
            
            # Additional metadata
            FieldSchema(name="total_dishes", dtype=DataType.INT32, 
                       description="Total dishes extracted from this restaurant"),
            
            # Analysis metadata
            FieldSchema(name="restaurants_analyzed", dtype=DataType.INT32, 
                       description="Number of restaurants analyzed"),
            FieldSchema(name="dishes_extracted", dtype=DataType.INT32, 
                       description="Number of dishes extracted"),
            FieldSchema(name="analysis_confidence", dtype=DataType.FLOAT, 
                       description="Confidence in analysis results"),
            
            # Embeddings
            FieldSchema(name="embedding_text", dtype=DataType.VARCHAR, max_length=4000, 
                       description="Text used for embedding generation"),
            FieldSchema(name="vector_embedding", dtype=DataType.FLOAT_VECTOR, 
                       dim=self.settings.vector_dimension, description="OpenAI embedding vector"),
            
            # Timestamps
            FieldSchema(name="created_at", dtype=DataType.VARCHAR, max_length=50, 
                       description="Creation timestamp"),
            FieldSchema(name="updated_at", dtype=DataType.VARCHAR, max_length=50, 
                       description="Last update timestamp"),
            FieldSchema(name="analysis_timestamp", dtype=DataType.VARCHAR, max_length=50, 
                       description="When analysis was performed")
        ]
        
        schema = CollectionSchema(fields, description="Neighborhood-cuisine analysis results")
        collection = Collection(collection_name, schema)
        
        # Create indexes
        self._create_neighborhood_analysis_indexes(collection)
        
        self.collections['neighborhood_analysis'] = collection
        app_logger.info(f"Created collection: {collection_name}")
    
    def _create_discovery_checkpoints_collection(self):
        """Create collection for discovery process checkpoints."""
        collection_name = self.collection_names['discovery_checkpoints']
        
        if utility.has_collection(collection_name):
            self.collections['discovery_checkpoints'] = Collection(collection_name)
            app_logger.info(f"Using existing collection: {collection_name}")
            return
        
        fields = [
            # Primary key
            FieldSchema(name="checkpoint_id", dtype=DataType.VARCHAR, max_length=100, 
                       is_primary=True, description="Unique checkpoint identifier"),
            
            # Checkpoint metadata
            FieldSchema(name="city", dtype=DataType.VARCHAR, max_length=100, 
                       description="City being processed"),
            FieldSchema(name="phase", dtype=DataType.VARCHAR, max_length=100, 
                       description="Discovery phase"),
            FieldSchema(name="status", dtype=DataType.VARCHAR, max_length=50, 
                       description="Processing status"),
            
            # Progress tracking
            FieldSchema(name="processed_restaurants", dtype=DataType.JSON, 
                       description="Array of processed restaurant IDs"),
            FieldSchema(name="processed_neighborhoods", dtype=DataType.JSON, 
                       description="Array of processed neighborhoods"),
            FieldSchema(name="last_restaurant_count", dtype=DataType.INT32, 
                       description="Last restaurant count processed"),
            FieldSchema(name="last_dish_count", dtype=DataType.INT32, 
                       description="Last dish count processed"),
            
            # Error handling
            FieldSchema(name="error_message", dtype=DataType.VARCHAR, max_length=1000, 
                       description="Error message if failed"),
            FieldSchema(name="retry_count", dtype=DataType.INT32, 
                       description="Number of retry attempts"),
            
            # Timestamps
            FieldSchema(name="created_at", dtype=DataType.VARCHAR, max_length=50, 
                       description="Creation timestamp"),
            FieldSchema(name="updated_at", dtype=DataType.VARCHAR, max_length=50, 
                       description="Last update timestamp"),
            FieldSchema(name="checkpoint_timestamp", dtype=DataType.VARCHAR, max_length=50, 
                       description="Checkpoint timestamp")
        ]
        
        schema = CollectionSchema(fields, description="Discovery process checkpoints")
        collection = Collection(collection_name, schema)
        
        # Create indexes (checkpoints don't need vector indexes)
        self._create_checkpoints_indexes(collection)
        
        self.collections['discovery_checkpoints'] = collection
        app_logger.info(f"Created collection: {collection_name}")
    
    def _create_popular_dishes_indexes(self, collection: Collection):
        """Create indexes for popular dishes collection."""
        try:
            # Vector index
            index_params = {
                "metric_type": "COSINE",
                "index_type": "HNSW",
                "params": {
                    "M": 16,
                    "efConstruction": 200
                }
            }
            collection.create_index("vector_embedding", index_params)
            
            # Scalar indexes (only numeric fields support STL_SORT)
            collection.create_index("popularity_score", {"index_type": "STL_SORT"})
            collection.create_index("frequency", {"index_type": "STL_SORT"})
            collection.create_index("avg_sentiment", {"index_type": "STL_SORT"})
            collection.create_index("restaurant_count", {"index_type": "STL_SORT"})
            collection.create_index("confidence_score", {"index_type": "STL_SORT"})
            
            app_logger.info("Created indexes for popular_dishes collection")
            
        except Exception as e:
            app_logger.warning(f"Index creation failed for popular_dishes: {e}")
    
    def _create_famous_restaurants_indexes(self, collection: Collection):
        """Create indexes for famous restaurants collection."""
        try:
            # Vector index
            index_params = {
                "metric_type": "COSINE",
                "index_type": "HNSW",
                "params": {
                    "M": 16,
                    "efConstruction": 200
                }
            }
            collection.create_index("vector_embedding", index_params)
            
            # Scalar indexes (only numeric fields support STL_SORT)
            collection.create_index("fame_score", {"index_type": "STL_SORT"})
            collection.create_index("dish_popularity", {"index_type": "STL_SORT"})
            collection.create_index("rating", {"index_type": "STL_SORT"})
            collection.create_index("review_count", {"index_type": "STL_SORT"})
            collection.create_index("quality_score", {"index_type": "STL_SORT"})
            collection.create_index("price_range", {"index_type": "STL_SORT"})
            
            app_logger.info("Created indexes for famous_restaurants collection")
            
        except Exception as e:
            app_logger.warning(f"Index creation failed for famous_restaurants: {e}")
    
    def _create_neighborhood_analysis_indexes(self, collection: Collection):
        """Create indexes for neighborhood analysis collection."""
        try:
            # Vector index
            index_params = {
                "metric_type": "COSINE",
                "index_type": "HNSW",
                "params": {
                    "M": 16,
                    "efConstruction": 200
                }
            }
            collection.create_index("vector_embedding", index_params)
            
            # Scalar indexes (only numeric fields support STL_SORT)
            collection.create_index("top_restaurant_rating", {"index_type": "STL_SORT"})
            collection.create_index("top_restaurant_review_count", {"index_type": "STL_SORT"})
            collection.create_index("top_restaurant_quality_score", {"index_type": "STL_SORT"})
            collection.create_index("top_dish_final_score", {"index_type": "STL_SORT"})
            collection.create_index("top_dish_sentiment_score", {"index_type": "STL_SORT"})
            collection.create_index("top_dish_topic_mentions", {"index_type": "STL_SORT"})
            collection.create_index("restaurants_analyzed", {"index_type": "STL_SORT"})
            collection.create_index("dishes_extracted", {"index_type": "STL_SORT"})
            collection.create_index("analysis_confidence", {"index_type": "STL_SORT"})
            
            app_logger.info("Created indexes for neighborhood_analysis collection")
            
        except Exception as e:
            app_logger.warning(f"Index creation failed for neighborhood_analysis: {e}")
    
    def _create_checkpoints_indexes(self, collection: Collection):
        """Create indexes for checkpoints collection."""
        try:
            # Scalar indexes (only numeric fields support STL_SORT)
            collection.create_index("last_restaurant_count", {"index_type": "STL_SORT"})
            collection.create_index("last_dish_count", {"index_type": "STL_SORT"})
            collection.create_index("retry_count", {"index_type": "STL_SORT"})
            
            app_logger.info("Created indexes for discovery_checkpoints collection")
            
        except Exception as e:
            app_logger.warning(f"Index creation failed for discovery_checkpoints: {e}")
    
    async def upsert_popular_dish(self, dish_data: Dict[str, Any]) -> bool:
        """Upsert a popular dish record."""
        try:
            collection = self.collections['popular_dishes']
            
            # Generate embedding if not present
            if 'vector_embedding' not in dish_data:
                embedding = await self._generate_embedding(dish_data.get('embedding_text', ''))
                dish_data['vector_embedding'] = embedding
            
            # Prepare data for insertion
            insert_data = self._prepare_popular_dish_data(dish_data)
            
            # Upsert (insert or update)
            collection.upsert(insert_data)
            
            app_logger.info(f"Upserted popular dish: {dish_data.get('dish_name', 'Unknown')}")
            return True
            
        except Exception as e:
            app_logger.error(f"Error upserting popular dish: {e}")
            return False
    
    async def upsert_famous_restaurant(self, restaurant_data: Dict[str, Any]) -> bool:
        """Upsert a famous restaurant record."""
        try:
            collection = self.collections['famous_restaurants']
            
            # Generate embedding if not present
            if 'vector_embedding' not in restaurant_data:
                embedding = await self._generate_embedding(restaurant_data.get('embedding_text', ''))
                restaurant_data['vector_embedding'] = embedding
            
            # Prepare data for insertion
            insert_data = self._prepare_famous_restaurant_data(restaurant_data)
            
            # Upsert (insert or update)
            collection.upsert(insert_data)
            
            app_logger.info(f"Upserted famous restaurant: {restaurant_data.get('restaurant_name', 'Unknown')}")
            return True
            
        except Exception as e:
            app_logger.error(f"Error upserting famous restaurant: {e}")
            return False
    
    async def upsert_neighborhood_analysis(self, analysis_data: Dict[str, Any]) -> bool:
        """Upsert a neighborhood analysis record."""
        try:
            collection = self.collections['neighborhood_analysis']
            
            # Generate embedding if not present
            if 'vector_embedding' not in analysis_data:
                embedding = await self._generate_embedding(analysis_data.get('embedding_text', ''))
                analysis_data['vector_embedding'] = embedding
            
            # Prepare data for insertion
            insert_data = self._prepare_neighborhood_analysis_data(analysis_data)
            
            # Upsert (insert or update)
            collection.upsert(insert_data)
            
            app_logger.info(f"Upserted neighborhood analysis: {analysis_data.get('analysis_id', 'Unknown')}")
            return True
            
        except Exception as e:
            app_logger.error(f"Error upserting neighborhood analysis: {e}")
            return False
    
    async def save_checkpoint(self, checkpoint_data: Dict[str, Any]) -> bool:
        """Save a discovery checkpoint."""
        try:
            collection = self.collections['discovery_checkpoints']
            
            # Prepare data for insertion
            insert_data = self._prepare_checkpoint_data(checkpoint_data)
            
            # Insert checkpoint
            collection.insert(insert_data)
            
            app_logger.info(f"Saved checkpoint: {checkpoint_data.get('checkpoint_id', 'Unknown')}")
            return True
            
        except Exception as e:
            app_logger.error(f"Error saving checkpoint: {e}")
            return False
    
    async def _generate_embedding(self, text: str) -> List[float]:
        """Generate embedding for text."""
        if not text:
            return [0.0] * self.settings.vector_dimension
        
        # Check cache
        cache_key = f"embedding:{hash(text)}"
        if cache_key in self._embedding_cache:
            return self._embedding_cache[cache_key]
        
        try:
            response = await self.openai_client.embeddings.create(
                model="text-embedding-3-small",
                input=text
            )
            
            embedding = response.data[0].embedding
            self._embedding_cache[cache_key] = embedding
            
            return embedding
            
        except Exception as e:
            app_logger.error(f"Error generating embedding: {e}")
            return [0.0] * self.settings.vector_dimension
    
    def _prepare_popular_dish_data(self, dish_data: Dict[str, Any]) -> List[List]:
        """Prepare popular dish data for Milvus insertion."""
        # Milvus expects data in column format: [[field1_values], [field2_values], ...]
        return [
            [dish_data.get('dish_id', '')],
            [dish_data.get('dish_name', '')],
            [dish_data.get('normalized_dish_name', '')],
            [dish_data.get('city', '')],
            [dish_data.get('neighborhoods', [])],
            [dish_data.get('popularity_score', 0.0)],
            [dish_data.get('frequency', 0)],
            [dish_data.get('avg_sentiment', 0.0)],
            [dish_data.get('cultural_significance', '')],
            [dish_data.get('top_restaurants', [])],
            [dish_data.get('restaurant_count', 0)],
            [dish_data.get('primary_cuisine', '')],
            [dish_data.get('cuisine_types', [])],
            [dish_data.get('dish_category', '')],
            [dish_data.get('reasoning', '')],
            [dish_data.get('discovery_method', '')],
            [dish_data.get('confidence_score', 0.0)],
            [dish_data.get('embedding_text', '')],
            [dish_data.get('vector_embedding', [])],
            [dish_data.get('created_at', '')],
            [dish_data.get('updated_at', '')],
            [dish_data.get('discovery_timestamp', '')]
        ]
    
    def _prepare_famous_restaurant_data(self, restaurant_data: Dict[str, Any]) -> List[List]:
        """Prepare famous restaurant data for Milvus insertion."""
        # Milvus expects data in column format: [[field1_values], [field2_values], ...]
        return [
            [restaurant_data.get('restaurant_id', '')],
            [restaurant_data.get('restaurant_name', '')],
            [restaurant_data.get('city', '')],
            [restaurant_data.get('neighborhood', '')],
            [restaurant_data.get('full_address', '')],
            [restaurant_data.get('fame_score', 0.0)],
            [restaurant_data.get('famous_dish', '')],
            [restaurant_data.get('dish_popularity', 0.0)],
            [restaurant_data.get('rating', 0.0)],
            [restaurant_data.get('review_count', 0)],
            [restaurant_data.get('quality_score', 0.0)],
            [restaurant_data.get('cuisine_type', '')],
            [restaurant_data.get('price_range', 2)],
            [restaurant_data.get('phone', '')],
            [restaurant_data.get('website', '')],
            [restaurant_data.get('discovery_method', '')],
            [restaurant_data.get('fame_indicators', [])],
            [restaurant_data.get('cultural_significance', '')],
            [restaurant_data.get('embedding_text', '')],
            [restaurant_data.get('vector_embedding', [])],
            [restaurant_data.get('created_at', '')],
            [restaurant_data.get('updated_at', '')],
            [restaurant_data.get('discovery_timestamp', '')]
        ]
    
    def _prepare_neighborhood_analysis_data(self, analysis_data: Dict[str, Any]) -> List[List]:
        """Prepare neighborhood analysis data for Milvus insertion."""
        # Milvus expects data in column format: [[field1_values], [field2_values], ...]
        # For single record insertion, each field gets a list with one value
        return [
            [analysis_data.get('analysis_id', '')],
            [analysis_data.get('city', '')],
            [analysis_data.get('neighborhood', '')],
            [analysis_data.get('cuisine_type', '')],
            [analysis_data.get('restaurant_rank', 1)],
            [analysis_data.get('restaurant_id', '')],
            [analysis_data.get('restaurant_name', '')],
            [analysis_data.get('rating', 0.0)],
            [analysis_data.get('review_count', 0)],
            [analysis_data.get('hybrid_quality_score', 0.0)],
            [analysis_data.get('top_dish_name', '')],
            [analysis_data.get('top_dish_final_score', 0.0)],
            [analysis_data.get('top_dish_sentiment_score', 0.0)],
            [analysis_data.get('top_dish_topic_mentions', 0)],
            [analysis_data.get('total_dishes', 0)],
            [analysis_data.get('restaurants_analyzed', 1)],
            [analysis_data.get('dishes_extracted', 0)],
            [analysis_data.get('analysis_confidence', 0.0)],
            [analysis_data.get('embedding_text', '')],
            [analysis_data.get('vector_embedding', [])],
            [analysis_data.get('created_at', '')],
            [analysis_data.get('updated_at', '')],
            [analysis_data.get('analysis_timestamp', '')]
        ]
    
    def _prepare_checkpoint_data(self, checkpoint_data: Dict[str, Any]) -> List[List]:
        """Prepare checkpoint data for Milvus insertion."""
        # Milvus expects data in column format: [[field1_values], [field2_values], ...]
        return [
            [checkpoint_data.get('checkpoint_id', '')],
            [checkpoint_data.get('city', '')],
            [checkpoint_data.get('phase', '')],
            [checkpoint_data.get('status', '')],
            [checkpoint_data.get('processed_restaurants', [])],
            [checkpoint_data.get('processed_neighborhoods', [])],
            [checkpoint_data.get('last_restaurant_count', 0)],
            [checkpoint_data.get('last_dish_count', 0)],
            [checkpoint_data.get('error_message', '')],
            [checkpoint_data.get('retry_count', 0)],
            [checkpoint_data.get('created_at', '')],
            [checkpoint_data.get('updated_at', '')],
            [checkpoint_data.get('checkpoint_timestamp', '')]
        ]


# Convenience functions for the optimized discovery engine
async def create_discovery_collections():
    """Create all discovery collections."""
    collections = DiscoveryCollections()
    return collections


async def main():
    """Test function to create discovery collections."""
    try:
        collections = await create_discovery_collections()
        print("✅ Discovery collections created successfully!")
        
        # List all collections
        all_collections = utility.list_collections()
        print(f"📋 All collections: {all_collections}")
        
    except Exception as e:
        print(f"❌ Error creating discovery collections: {e}")


if __name__ == "__main__":
    asyncio.run(main())
