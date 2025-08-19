"""
Milvus client for vector database operations.
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
from serpapi import GoogleSearch
from src.data_collection.cache_manager import CacheManager


class MilvusClient:
    """Client for Milvus vector database operations."""
    
    def __init__(self):
        self.settings = get_settings()
        self.cache_manager = CacheManager()
        self.api_calls = 0
        self.start_time = time.time()
        self.reviews_per_restaurant = 20  # Increased from default
        self.connection = None
        self.collections = {}
        self.openai_client = AsyncOpenAI(api_key=self.settings.openai_api_key)
        self._embedding_cache = {}
        
        # Add batch processing configuration
        self.embedding_batch_size = 50  # OpenAI API batch size
        self.insert_batch_size = 100    # Milvus insert batch size
        
        # Performance tracking
        self.stats = {
            "embeddings_generated": 0,
            "embeddings_cached": 0,
            "batch_inserts": 0,
            "failed_inserts": 0
        }
        
        self._connect()
    
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
            
            # Initialize collections
            self._initialize_collections()
            
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
        """Initialize all required collections."""
        try:
            # Check existing collections first
            existing_collections = utility.list_collections()
            app_logger.info(f"Found existing collections: {existing_collections}")
            
            # Only create collections if they don't exist and we have room
            collections_to_create = []
            
            if "restaurants_enhanced" not in existing_collections:
                collections_to_create.append("restaurants")
            if "dishes_detailed" not in existing_collections and "dishes_detailed_hybrid" not in existing_collections:
                collections_to_create.append("dishes")
            if "locations_metadata" not in existing_collections:
                collections_to_create.append("locations")
            
            # Also check for simple collection names (for backward compatibility)
            if "restaurants" not in existing_collections and "restaurants_enhanced" not in existing_collections:
                collections_to_create.append("restaurants")
            if "dishes" not in existing_collections and "dishes_detailed" not in existing_collections and "dishes_detailed_hybrid" not in existing_collections:
                collections_to_create.append("dishes")
            
            # Check if we have room for new collections (free tier limit: 5)
            if len(existing_collections) + len(collections_to_create) > 5:
                app_logger.warning(f"Collection limit reached ({len(existing_collections)}/5). Using existing collections.")
                # Try to use existing collections that might be compatible
                for name in ["restaurants_enhanced", "dishes_detailed_hybrid", "dishes_detailed", "locations_metadata"]:
                    if name in existing_collections:
                        try:
                            collection = Collection(name)
                            if name.startswith("restaurants"):
                                self.collections['restaurants'] = collection
                            elif name.startswith("dishes"):
                                self.collections['dishes'] = collection
                            elif name.startswith("locations"):
                                self.collections['locations'] = collection
                            app_logger.info(f"Using existing collection: {name}")
                        except Exception as e:
                            app_logger.warning(f"Failed to load existing collection {name}: {e}")
                
                # Ensure we have a dishes collection
                if 'dishes' not in self.collections:
                    # Prefer hybrid collection
                    if "dishes_detailed_hybrid" in existing_collections:
                        self.collections['dishes'] = Collection("dishes_detailed_hybrid")
                        app_logger.info("Using dishes_detailed_hybrid collection")
                    elif "dishes_detailed" in existing_collections:
                        self.collections['dishes'] = Collection("dishes_detailed")
                        app_logger.info("Using dishes_detailed collection")
                return
            
            # Ensure we have a dishes collection (even if not creating new ones)
            if 'dishes' not in self.collections:
                # Prefer hybrid collection
                if "dishes_detailed_hybrid" in existing_collections:
                    self.collections['dishes'] = Collection("dishes_detailed_hybrid")
                    app_logger.info("Using dishes_detailed_hybrid collection")
                elif "dishes_detailed" in existing_collections:
                    self.collections['dishes'] = Collection("dishes_detailed")
                    app_logger.info("Using dishes_detailed collection")
            
            # Create needed collections
            for collection_name in collections_to_create:
                if collection_name == "restaurants":
                    self._create_restaurants_collection()
                elif collection_name == "dishes":
                    self._create_dishes_collection()
                elif collection_name == "locations":
                    self._create_locations_collection()
            
            app_logger.info("Collections initialized successfully")
            
        except Exception as e:
            app_logger.error(f"Error initializing collections: {e}")
            app_logger.warning("Collections will be created when first needed")
    
    def _create_restaurants_collection(self):
        """Create restaurants_enhanced collection with improved schema."""
        collection_name = "restaurants_enhanced"
        
        if utility.has_collection(collection_name):
            self.collections['restaurants'] = Collection(collection_name)
            app_logger.info(f"Using existing collection: {collection_name}")
            return
        
        # Enhanced schema with best practices
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
                       dim=self.settings.vector_dimension, description="OpenAI embedding vector"),
            
            # Timestamps
            FieldSchema(name="created_at", dtype=DataType.VARCHAR, max_length=50, 
                       description="Creation timestamp"),
            FieldSchema(name="updated_at", dtype=DataType.VARCHAR, max_length=50, 
                       description="Last update timestamp")
        ]
        
        schema = CollectionSchema(fields, description="Enhanced restaurant information with embeddings")
        
        # Create collection
        collection = Collection(collection_name, schema)
        
        # Enhanced indexing strategy
        index_params = {
            "metric_type": "COSINE",
            "index_type": "HNSW",  # Better performance than IVF_FLAT
            "params": {
                "M": 16,               # Number of connections for each new element
                "efConstruction": 200  # Size of dynamic candidate list
            }
        }
        collection.create_index("vector_embedding", index_params)
        # Scalar indexes to accelerate filters
        try:
            collection.create_index("city", {"index_type": "STL_SORT"})
            collection.create_index("cuisine_type", {"index_type": "STL_SORT"})
        except Exception as e:
            app_logger.warning(f"Scalar index creation skipped/failed (restaurants): {e}")
        
        self.collections['restaurants'] = collection
        app_logger.info(f"Created enhanced collection: {collection_name}")
    
    def _create_dishes_collection(self):
        """Create dishes_detailed collection with improved schema."""
        # Prefer hybrid collection if it exists
        if utility.has_collection("dishes_detailed_hybrid"):
            collection_name = "dishes_detailed_hybrid"
            self.collections['dishes'] = Collection(collection_name)
            app_logger.info(f"Using existing hybrid collection: {collection_name}")
            return
        elif utility.has_collection("dishes_detailed"):
            collection_name = "dishes_detailed"
            self.collections['dishes'] = Collection(collection_name)
            app_logger.info(f"Using existing collection: {collection_name}")
            return
        
        # Create new collection (prefer hybrid name)
        collection_name = "dishes_detailed_hybrid"
        
        # Enhanced schema with best practices
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
            
            # Dish information with better constraints
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
            
            # Hybrid topics data
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
                       dim=self.settings.vector_dimension, description="OpenAI embedding vector"),
            
            # Context and examples
            FieldSchema(name="sample_contexts", dtype=DataType.JSON, 
                       description="Array of sample review contexts"),
            
            # Timestamps
            FieldSchema(name="created_at", dtype=DataType.VARCHAR, max_length=50, 
                       description="Creation timestamp"),
            FieldSchema(name="updated_at", dtype=DataType.VARCHAR, max_length=50, 
                       description="Last update timestamp")
        ]
        
        schema = CollectionSchema(fields, description="Enhanced dish information with embeddings")
        
        # Create collection
        collection = Collection(collection_name, schema)
        
        # Enhanced indexing strategy
        index_params = {
            "metric_type": "COSINE",
            "index_type": "HNSW",  # Better performance than IVF_FLAT
            "params": {
                "M": 16,               # Number of connections for each new element
                "efConstruction": 200  # Size of dynamic candidate list
            }
        }
        collection.create_index("vector_embedding", index_params)
        # Scalar indexes to accelerate filters on dishes
        try:
            collection.create_index("cuisine_type", {"index_type": "STL_SORT"})
            collection.create_index("neighborhood", {"index_type": "STL_SORT"})
            collection.create_index("topic_mentions", {"index_type": "STL_SORT"})
            collection.create_index("final_score", {"index_type": "STL_SORT"})
        except Exception as e:
            app_logger.warning(f"Scalar index creation skipped/failed (dishes): {e}")
        
        self.collections['dishes'] = collection
        app_logger.info(f"Created enhanced collection: {collection_name}")
    
    def _create_locations_collection(self):
        """Create locations_metadata collection with improved schema."""
        collection_name = "locations_metadata"
        
        if utility.has_collection(collection_name):
            self.collections['locations'] = Collection(collection_name)
            app_logger.info(f"Using existing collection: {collection_name}")
            return
        
        # Enhanced schema with best practices
        fields = [
            # Primary key
            FieldSchema(name="location_id", dtype=DataType.VARCHAR, max_length=100, 
                       is_primary=True, description="Unique location identifier"),
            
            # Location information
            FieldSchema(name="city", dtype=DataType.VARCHAR, max_length=100, 
                       description="City name"),
            FieldSchema(name="neighborhood", dtype=DataType.VARCHAR, max_length=150, 
                       description="Neighborhood or area name"),
            
            # Aggregated statistics
            FieldSchema(name="restaurant_count", dtype=DataType.INT32, 
                       description="Number of restaurants in this location"),
            FieldSchema(name="avg_rating", dtype=DataType.FLOAT, 
                       description="Average rating of restaurants in location"),
            
            # Distribution data
            FieldSchema(name="cuisine_distribution", dtype=DataType.JSON, 
                       description="Distribution of cuisine types"),
            FieldSchema(name="popular_cuisines", dtype=DataType.JSON, 
                       description="Array of popular cuisines in order"),
            FieldSchema(name="price_distribution", dtype=DataType.JSON, 
                       description="Distribution of price ranges"),
            FieldSchema(name="geographic_bounds", dtype=DataType.JSON, 
                       description="Geographic boundaries (lat/lng bounds)"),
            
            # Embeddings
            FieldSchema(name="embedding_text", dtype=DataType.VARCHAR, max_length=4000, 
                       description="Text used for embedding generation"),
            FieldSchema(name="vector_embedding", dtype=DataType.FLOAT_VECTOR, 
                       dim=self.settings.vector_dimension, description="OpenAI embedding vector")
        ]
        
        schema = CollectionSchema(fields, description="Enhanced location metadata with embeddings")
        
        # Create collection
        collection = Collection(collection_name, schema)
        
        # Enhanced indexing strategy
        index_params = {
            "metric_type": "COSINE",
            "index_type": "HNSW",  # Better performance than IVF_FLAT
            "params": {
                "M": 16,               # Number of connections for each new element
                "efConstruction": 200  # Size of dynamic candidate list
            }
        }
        collection.create_index("vector_embedding", index_params)
        # Scalar indexes for locations
        try:
            collection.create_index("city", {"index_type": "STL_SORT"})
            collection.create_index("neighborhood", {"index_type": "STL_SORT"})
        except Exception as e:
            app_logger.warning(f"Scalar index creation skipped/failed (locations): {e}")
        
        self.collections['locations'] = collection
        app_logger.info(f"Created enhanced collection: {collection_name}")
    
    async def insert_restaurants(self, restaurants: List[Dict]) -> bool:
        """Insert restaurants into the collection. (SYNCHRONOUS - removed async)"""
        if not restaurants:
            return True
        
        try:
            # Get the collection
            collection = self._get_restaurants_collection()
            if not collection:
                app_logger.error("No restaurants collection available")
                return False
            
            # Prepare data
            data = []
            for restaurant in restaurants:
                # Generate embedding text
                embedding_text = self._create_restaurant_embedding_text(restaurant)
                
                # Generate vector embedding using OpenAI
                vector_embedding = await self._generate_embedding(embedding_text)
                
                # DEBUG: Print raw restaurant data
                app_logger.info("🔍 DEBUG: Raw restaurant data before processing:")
                app_logger.info(f"   sub_cuisines: {type(restaurant.get('sub_cuisines', [])).__name__} = {restaurant.get('sub_cuisines', [])}")
                app_logger.info(f"   operating_hours: {type(restaurant.get('operating_hours', {})).__name__} = {restaurant.get('operating_hours', {})}")
                app_logger.info(f"   meal_types: {type(restaurant.get('meal_types', [])).__name__} = {restaurant.get('meal_types', [])}")
                
                # Process operating_hours: convert string to dict if needed
                operating_hours = restaurant.get('operating_hours', {})
                if isinstance(operating_hours, str):
                    # Convert string like "Closed ⋅ Opens 5 PM Wed" to a simple dict
                    operating_hours = {"status": operating_hours}
                elif not isinstance(operating_hours, dict):
                    operating_hours = {}
                
                # Ensure all JSON fields are proper Python objects (Milvus will handle JSON conversion)
                sub_cuisines = restaurant.get('sub_cuisines', [])
                if not isinstance(sub_cuisines, list):
                    sub_cuisines = []
                
                meal_types = restaurant.get('meal_types', [])
                if not isinstance(meal_types, list):
                    meal_types = []
                
                # Extract coordinates properly with double precision
                latitude = restaurant.get('latitude', 0.0)
                longitude = restaurant.get('longitude', 0.0)
                
                # Handle coordinates from different possible formats
                if 'coordinates' in restaurant:
                    coords = restaurant['coordinates']
                    if isinstance(coords, dict):
                        latitude = coords.get('latitude', latitude)
                        longitude = coords.get('longitude', longitude)
                    elif isinstance(coords, list) and len(coords) >= 2:
                        latitude = coords[0]
                        longitude = coords[1]
                
                # Ensure numeric types are correct with proper casting
                rating = float(restaurant.get('rating', 0.0))
                review_count = int(restaurant.get('review_count', 0))
                quality_score = float(restaurant.get('quality_score', 0.0))
                price_range = int(restaurant.get('price_range', 2))  # Will be converted to INT32
                fallback_tier = int(restaurant.get('fallback_tier', 2))  # Will be converted to INT32
                
                # Convert coordinates to float (will be stored as DOUBLE)
                latitude = float(latitude)
                longitude = float(longitude)
                
                data.append([
                    restaurant.get('restaurant_id', str(uuid.uuid4())),
                    restaurant.get('restaurant_name', ''),
                    restaurant.get('google_place_id', ''),
                    restaurant.get('full_address', ''),
                    restaurant.get('city', ''),
                    restaurant.get('neighborhood', ''),  # Add neighborhood field
                    latitude,
                    longitude,
                    restaurant.get('cuisine_type', ''),
                    sub_cuisines,  # List - Milvus will convert to JSON
                    rating,
                    review_count,
                    quality_score,
                    price_range,
                    operating_hours,  # Dict - Milvus will convert to JSON
                    meal_types,  # List - Milvus will convert to JSON
                    restaurant.get('phone', ''),
                    restaurant.get('website', ''),
                    fallback_tier,
                    embedding_text,
                    vector_embedding,
                    restaurant.get('created_at', ''),
                    restaurant.get('updated_at', '')
                ])
            
            # Insert data - convert to row-based entity format that Milvus expects
            if data:
                # Convert to list of entity dictionaries (modern Milvus format)
                entities = []
                field_names = [
                    "restaurant_id", "restaurant_name", "google_place_id", "full_address", 
                    "city", "neighborhood", "latitude", "longitude", "cuisine_type", "sub_cuisines", 
                    "rating", "review_count", "quality_score", "price_range", "operating_hours", 
                    "meal_types", "phone", "website", "fallback_tier", 
                    "embedding_text", "vector_embedding", "created_at", "updated_at"
                ]
                
                # Convert each row to an entity dictionary
                for row in data:
                    entity = {}
                    for i, field_name in enumerate(field_names):
                        if i < len(row):
                            entity[field_name] = row[i]
                    entities.append(entity)
                
                # DEBUG: Print the first entity being inserted
                if entities:
                    app_logger.info("🔍 DEBUG: Sample entity being inserted to Milvus:")
                    for field_name, value in entities[0].items():
                        if field_name == 'vector_embedding':
                            app_logger.info(f"   {field_name}: {type(value).__name__} (length: {len(value)})")
                        elif field_name in ['sub_cuisines', 'operating_hours', 'meal_types']:
                            app_logger.info(f"   {field_name}: {type(value).__name__} = {value}")
                        else:
                            app_logger.info(f"   {field_name}: {type(value).__name__} = {value}")
                
                # DEBUG: Print schema info
                app_logger.info("🔍 DEBUG: Collection schema:")
                for field in collection.schema.fields:
                    app_logger.info(f"   {field.name}: {field.dtype} (max_length: {getattr(field, 'max_length', None)})")
                
                # Try partitioned insert by cuisine for better pruning
                try:
                    cuisine_to_rows: Dict[str, List[Dict]] = {}
                    for e in entities:
                        cuisine = e.get("cuisine_type") or "_default"
                        cuisine_to_rows.setdefault(cuisine, []).append(e)
                    for cuisine, rows in cuisine_to_rows.items():
                        p = self._ensure_dishes_partition(cuisine)
                        if p and p != "_default":
                            collection.insert(rows, partition_name=p)
                        else:
                            collection.insert(rows)
                except Exception as e:
                    app_logger.warning(f"Partitioned insert failed, fallback to default insert: {e}")
                    collection.insert(entities)
                collection.flush()
            
            app_logger.info(f"Inserted {len(restaurants)} restaurants")
            return True
            
        except Exception as e:
            app_logger.error(f"Error inserting restaurants: {e}")
            return False
    
    async def insert_dishes(self, dishes: List[Dict]) -> bool:
        """Insert dishes into the collection. (SYNCHRONOUS - removed async)"""
        if not dishes:
            return True
        
        try:
            # Get the collection
            collection = self._get_dishes_collection()
            if not collection:
                app_logger.error("No dishes collection available")
                return False
            
            # Prepare data
            data = []
            for dish in dishes:
                # Generate embedding text
                embedding_text = self._create_dish_embedding_text(dish)
                
                # Generate vector embedding using OpenAI
                vector_embedding = await self._generate_embedding(embedding_text)
                
                # DEBUG: Print raw dish data
                app_logger.info("🔍 DEBUG: Raw dish data before processing:")
                app_logger.info(f"   dietary_tags: {type(dish.get('dietary_tags', [])).__name__} = {dish.get('dietary_tags', [])}")
                app_logger.info(f"   sample_contexts: {type(dish.get('sample_contexts', [])).__name__} = {dish.get('sample_contexts', [])}")
                
                # Ensure all JSON fields are proper Python objects (Milvus will handle JSON conversion)
                dietary_tags = dish.get('dietary_tags', [])
                if not isinstance(dietary_tags, list):
                    dietary_tags = []
                
                sample_contexts = dish.get('sample_contexts', [])
                if not isinstance(sample_contexts, list):
                    sample_contexts = []
                
                # Ensure numeric types are correct with proper casting
                sentiment_score = float(dish.get('sentiment_score', 0.0))
                positive_mentions = int(dish.get('positive_mentions', 0))  # Will be converted to INT32
                negative_mentions = int(dish.get('negative_mentions', 0))  # Will be converted to INT32
                neutral_mentions = int(dish.get('neutral_mentions', 0))  # Will be converted to INT32
                total_mentions = int(dish.get('total_mentions', 0))  # Will be converted to INT32
                confidence_score = float(dish.get('confidence_score', 0.5))
                recommendation_score = float(dish.get('recommendation_score', 0.0))
                avg_price_mentioned = float(dish.get('avg_price_mentioned', 0.0))
                trending_score = float(dish.get('trending_score', 0.0))
                # Hybrid fields
                topic_mentions = int(dish.get('topic_mentions', 0))
                topic_score = float(dish.get('topic_score', 0.0))
                final_score = float(dish.get('final_score', dish.get('sentiment_score', 0.0)))
                source = dish.get('source', 'hybrid')
                hybrid_insights = dish.get('hybrid_insights', {})
                
                data.append([
                    dish.get('dish_id', str(uuid.uuid4())),
                    dish.get('restaurant_id', ''),
                    dish.get('restaurant_name', ''),
                    dish.get('neighborhood', ''),
                    dish.get('cuisine_type', ''),
                    dish.get('dish_name', ''),
                    dish.get('normalized_dish_name', ''),
                    dish.get('dish_category', 'main'),  # Fixed: use 'dish_category' not 'category'
                    dish.get('cuisine_context', ''),
                    dietary_tags,  # List - Milvus will convert to JSON
                    sentiment_score,
                    positive_mentions,
                    negative_mentions,
                    neutral_mentions,
                    total_mentions,
                    confidence_score,
                    recommendation_score,
                    avg_price_mentioned,
                    trending_score,
                    # Hybrid fields
                    topic_mentions,
                    topic_score,
                    final_score,
                    source,
                    hybrid_insights,
                    embedding_text,
                    vector_embedding,
                    sample_contexts,  # List - Milvus will convert to JSON
                    dish.get('created_at', ''),
                    dish.get('updated_at', '')
                ])
            
            # Insert data - convert to row-based entity format that Milvus expects
            if data:
                # Convert to list of entity dictionaries (modern Milvus format)
                entities = []
                field_names = [
                    "dish_id", "restaurant_id", "restaurant_name", "neighborhood", "cuisine_type",
                    "dish_name", "normalized_dish_name", "dish_category", "cuisine_context", "dietary_tags", 
                    "sentiment_score", "positive_mentions", "negative_mentions", "neutral_mentions", "total_mentions",
                    "confidence_score", "recommendation_score", "avg_price_mentioned", "trending_score",
                    # Hybrid fields
                    "topic_mentions", "topic_score", "final_score", "source", "hybrid_insights",
                    "embedding_text", "vector_embedding", "sample_contexts", "created_at", "updated_at"
                ]
                
                # Convert each row to an entity dictionary
                for row in data:
                    entity = {}
                    for i, field_name in enumerate(field_names):
                        if i < len(row):
                            entity[field_name] = row[i]
                    entities.append(entity)
                
                # DEBUG: Print the first entity being inserted
                if entities:
                    app_logger.info("🔍 DEBUG: Sample dish entity being inserted to Milvus:")
                    for field_name, value in entities[0].items():
                        if field_name == 'vector_embedding':
                            app_logger.info(f"   {field_name}: {type(value).__name__} (length: {len(value)})")
                        elif field_name in ['dietary_tags', 'sample_contexts']:
                            app_logger.info(f"   {field_name}: {type(value).__name__} = {value}")
                        else:
                            app_logger.info(f"   {field_name}: {type(value).__name__} = {value}")
                
                # DEBUG: Print schema info
                app_logger.info("🔍 DEBUG: Dish collection schema:")
                for field in collection.schema.fields:
                    app_logger.info(f"   {field.name}: {field.dtype} (max_length: {getattr(field, 'max_length', None)})")
                
                collection.insert(entities)
                collection.flush()
            
            app_logger.info(f"Inserted {len(dishes)} dishes")
            return True
            
        except Exception as e:
            app_logger.error(f"Error inserting dishes: {e}")
            return False
    
    def _get_restaurants_collection(self) -> Optional[Collection]:
        """Get restaurants collection with fallback logic."""
        # Try to get from stored collections
        if 'restaurants' in self.collections:
            return self.collections['restaurants']
        
        # Try to get from Milvus directly
        try:
            if utility.has_collection("restaurants_enhanced"):
                collection = Collection("restaurants_enhanced")
                self.collections['restaurants'] = collection
                return collection
            elif utility.has_collection("restaurants"):
                collection = Collection("restaurants")
                self.collections['restaurants'] = collection
                return collection
        except Exception as e:
            app_logger.error(f"Error getting restaurants collection: {e}")
        
        return None
    
    def _get_dishes_collection(self) -> Optional[Collection]:
        """Get dishes collection with fallback logic."""
        # Try to get from stored collections
        if 'dishes' in self.collections:
            return self.collections['dishes']
        
        # Try to get from Milvus directly
        try:
            if utility.has_collection("dishes_detailed"):
                collection = Collection("dishes_detailed")
                self.collections['dishes'] = collection
                return collection
            elif utility.has_collection("dishes"):
                collection = Collection("dishes")
                self.collections['dishes'] = collection
                return collection
        except Exception as e:
            app_logger.error(f"Error getting dishes collection: {e}")
        
        return None
    
    def search_restaurants_with_filters(self, filters: Dict = None, limit: int = 10) -> List[Dict]:
        """Search restaurants by filters only (no vector similarity)."""
        try:
            collection = self._get_restaurants_collection()
            if not collection:
                app_logger.error("No restaurants collection available for search")
                return []
                
            collection.load()
            
            # Build filter expression
            filter_expr = self._build_filter_expression(filters) if filters else ""
            
            # Execute query
            results = collection.query(
                expr=filter_expr,
                limit=limit,
                output_fields=["restaurant_id", "restaurant_name", "city", "neighborhood", "cuisine_type", "rating", "review_count", "quality_score"]
            )
            
            # Process results
            restaurants = []
            for result in results:
                restaurant = {
                    'restaurant_id': result.get('restaurant_id'),
                    'restaurant_name': result.get('restaurant_name'),
                    'city': result.get('city'),
                    'neighborhood': result.get('neighborhood', ''),
                    'cuisine_type': result.get('cuisine_type'),
                    'rating': result.get('rating'),
                    'review_count': result.get('review_count'),
                    'quality_score': result.get('quality_score')
                }
                restaurants.append(restaurant)
            
            # Sort by quality score
            restaurants.sort(key=lambda x: x.get('quality_score', 0), reverse=True)
            
            return restaurants
            
        except Exception as e:
            app_logger.error(f"Error searching restaurants with filters: {e}")
            return []

    def search_restaurants(self, query_vector: List[float], filters: Dict = None, limit: int = 10) -> List[Dict]:
        """Search restaurants by vector similarity."""
        try:
            collection = self._get_restaurants_collection()
            if not collection:
                app_logger.error("No restaurants collection available for search")
                return []
                
            collection.load()
            
            # Build search parameters
            search_params = {
                "metric_type": "COSINE",
                "params": {"nprobe": 10}
            }
            
            # Execute search
            results = collection.search(
                data=[query_vector],
                anns_field="vector_embedding",
                param=search_params,
                limit=limit,
                expr=self._build_filter_expression(filters) if filters else None,
                output_fields=["restaurant_id", "restaurant_name", "city", "cuisine_type", "rating", "review_count", "quality_score"]
            )
            
            # Process results
            restaurants = []
            for hits in results:
                for hit in hits:
                    restaurant = {
                        'restaurant_id': hit.entity.get('restaurant_id'),
                        'restaurant_name': hit.entity.get('restaurant_name'),
                        'city': hit.entity.get('city'),
                        'neighborhood': '',  # Not available in current schema
                        'cuisine_type': hit.entity.get('cuisine_type'),
                        'rating': hit.entity.get('rating'),
                        'review_count': hit.entity.get('review_count'),
                        'quality_score': hit.entity.get('quality_score'),
                        'similarity_score': hit.score
                    }
                    restaurants.append(restaurant)
            
            restaurants.sort(key=lambda x: x.get('quality_score', 0), reverse=True)
            
            return restaurants
            
        except Exception as e:
            app_logger.error(f"Error searching restaurants: {e}")
            return []
    
    def search_dishes(self, query_vector: List[float], filters: Dict = None, limit: int = 10) -> List[Dict]:
        """Search dishes by vector similarity."""
        try:
            collection = self._get_dishes_collection()
            if not collection:
                app_logger.error("No dishes collection available for search")
                return []
                
            collection.load()
            
            # Build search parameters
            search_params = {
                "metric_type": "COSINE",
                "params": {"nprobe": 10}
            }
            
            # Execute search
            results = collection.search(
                data=[query_vector],
                anns_field="vector_embedding",
                param=search_params,
                limit=limit,
                expr=self._build_filter_expression(filters) if filters else None,
                output_fields=[
                    "dish_id", "dish_name", "restaurant_id", "restaurant_name",
                    "sentiment_score", "recommendation_score",
                    "topic_mentions", "topic_score", "final_score", "source", "hybrid_insights"
                ]
            )
            
            # Process results with deduplication
            seen_dishes = {}
            for hits in results:
                for hit in hits:
                    dish_key = (hit.entity.get('dish_name'), hit.entity.get('restaurant_id'))
                    current_score = float(hit.entity.get('final_score') or hit.entity.get('recommendation_score') or 0)
                    
                    if dish_key not in seen_dishes or current_score > seen_dishes[dish_key]["score"]:
                        dish = {
                            'dish_id': hit.entity.get('dish_id'),
                            'dish_name': hit.entity.get('dish_name'),
                            'restaurant_id': hit.entity.get('restaurant_id'),
                            'restaurant_name': hit.entity.get('restaurant_name'),
                            'sentiment_score': hit.entity.get('sentiment_score'),
                            'recommendation_score': hit.entity.get('recommendation_score'),
                            'topic_mentions': hit.entity.get('topic_mentions'),
                            'topic_score': hit.entity.get('topic_score'),
                            'final_score': hit.entity.get('final_score'),
                            'source': hit.entity.get('source'),
                            'hybrid_insights': hit.entity.get('hybrid_insights'),
                            'similarity_score': hit.score
                        }
                        seen_dishes[dish_key] = {
                            "dish": dish,
                            "score": current_score
                        }
            
            # Convert back to list and sort by score
            dishes = [item["dish"] for item in seen_dishes.values()]
            dishes.sort(key=lambda d: float(d.get('final_score', d.get('recommendation_score', 0)) or 0), reverse=True)
            
            return dishes
            
        except Exception as e:
            app_logger.error(f"Error searching dishes: {e}")
            return []
    
    def search_dishes_with_filters(self, filters: Dict = None, limit: int = 10) -> List[Dict]:
        """Search dishes by filters only (no vector similarity)."""
        try:
            collection = self._get_dishes_collection()
            if not collection:
                app_logger.error("No dishes collection available for search")
                return []
                
            collection.load()
            
            # Build filter expression
            filter_expr = self._build_filter_expression(filters) if filters else ""
            
            # Execute query
            results = collection.query(
                expr=filter_expr,
                limit=limit,
                output_fields=[
                    "dish_id", "dish_name", "restaurant_id", "restaurant_name",
                    "neighborhood", "cuisine_type",
                    "sentiment_score", "recommendation_score",
                    "total_mentions", "positive_mentions", "negative_mentions", "neutral_mentions",
                    "dish_category", "cuisine_context", "confidence_score", "avg_price_mentioned",
                    "trending_score",
                    # Hybrid fields
                    "topic_mentions", "topic_score", "final_score", "source", "hybrid_insights",
                    # Context
                    "sample_contexts", "created_at", "updated_at"
                ]
            )
            
            return results
            
        except Exception as e:
            app_logger.error(f"Error searching dishes: {e}")
            return []

    def search_dishes_with_topics(self, cuisine: str = None, neighborhood: str = None, limit: int = 10, order_by: str = "final_score") -> List[Dict]:
        """Search dishes prioritizing hybrid topic fields. Filters by cuisine/neighborhood if provided.

        Note: City is not stored on dishes; city-level filtering should be done at the restaurants layer.
        """
        try:
            collection = self._get_dishes_collection()
            if not collection:
                app_logger.error("No dishes collection available for search")
                return []

            collection.load()

            filters: Dict[str, Any] = {}
            if cuisine:
                filters["cuisine_type"] = cuisine
            if neighborhood:
                filters["neighborhood"] = neighborhood

            expr = self._build_filter_expression(filters) if filters else ""

            # Pull more rows then sort client-side by order_by to ensure correct ordering
            fetch_limit = max(limit * 3, 50)
            # Choose partition by cuisine when available
            partition_names = None
            if cuisine:
                p = self._get_cuisine_partition_name(cuisine)
                try:
                    if utility.has_partition(collection.name, p):
                        partition_names = [p]
                except Exception:
                    partition_names = None

            rows = collection.query(
                expr=expr,
                limit=fetch_limit,
                partition_names=partition_names,
                output_fields=[
                    "dish_id", "dish_name", "restaurant_id", "restaurant_name",
                    "neighborhood", "cuisine_type",
                    "topic_mentions", "topic_score", "final_score", "source", "hybrid_insights",
                    "sentiment_score", "recommendation_score"
                ]
            )

            # Deduplicate by (dish_name, restaurant_id) and keep the highest scoring version
            seen_dishes = {}
            for row in rows:
                dish_key = (row.get("dish_name"), row.get("restaurant_id"))
                current_score = float(row.get(order_by, 0) or 0)
                
                if dish_key not in seen_dishes or current_score > seen_dishes[dish_key]["score"]:
                    seen_dishes[dish_key] = {
                        "row": row,
                        "score": current_score
                    }
            
            # Convert back to list and sort by score
            unique_rows = [item["row"] for item in seen_dishes.values()]
            unique_rows.sort(key=lambda r: float(r.get(order_by, 0) or 0), reverse=True)
            
            return unique_rows[:limit]
        except Exception as e:
            app_logger.error(f"Error searching dishes with topics: {e}")
            return []

    # --------------------
    # Partition utilities
    # --------------------
    def _sanitize_partition_label(self, label: str) -> str:
        safe = ''.join(ch if ch.isalnum() or ch == '_' else '_' for ch in (label or '').strip().lower())
        return safe[:255] or "unknown"

    def _get_cuisine_partition_name(self, cuisine: str) -> str:
        return f"cuisine_{self._sanitize_partition_label(cuisine)}"

    def _ensure_dishes_partition(self, cuisine: str) -> str:
        """Ensure a partition for given cuisine exists; return its name."""
        collection = self._get_dishes_collection()
        if not collection or not cuisine:
            return "_default"
        p = self._get_cuisine_partition_name(cuisine)
        try:
            if not utility.has_partition(collection.name, p):
                collection.create_partition(p)
                app_logger.info(f"Created dishes partition: {p}")
        except Exception as e:
            app_logger.warning(f"Partition ensure failed ({p}): {e}")
        return p
    
    def _create_restaurant_embedding_text(self, restaurant: Dict) -> str:
        """Create embedding text for restaurant."""
        parts = [
            restaurant.get('restaurant_name', ''),
            restaurant.get('cuisine_type', ''),
            restaurant.get('city', ''),
            ' '.join(restaurant.get('meal_types', [])),
            ' '.join(restaurant.get('sub_cuisines', []))
        ]
        
        return ' '.join(filter(None, parts))
    
    def _create_dish_embedding_text(self, dish: Dict) -> str:
        """Create embedding text for dish."""
        parts = [
            dish.get('dish_name', ''),
            dish.get('normalized_dish_name', ''),
            dish.get('dish_category', ''),
            dish.get('cuisine_context', ''),
            ' '.join(dish.get('dietary_tags', []))
        ]
        
        return ' '.join(filter(None, parts))
    
    async def _generate_embedding(self, text: str) -> List[float]:
        """Generate embedding for text using OpenAI."""
        
        # Add caching for common queries
        if text in self._embedding_cache:
            return self._embedding_cache[text]

        try:
            response = await self.openai_client.embeddings.create(
                model=self.settings.embedding_model,
                input=text
            )
            
            embedding = response.data[0].embedding
            self._embedding_cache[text] = embedding
            return embedding
            
        except Exception as e:
            app_logger.error(f"Error generating embedding: {e}")
            # Return zero vector as fallback
            return [0.0] * self.settings.vector_dimension
    
    def _build_filter_expression(self, filters: Dict) -> str:
        """Build filter expression for Milvus search."""
        expressions = []
        
        for key, value in filters.items():
            if isinstance(value, (list, tuple)):
                # Handle array filters
                if value:
                    expressions.append(f'{key} in {value}')
            elif isinstance(value, dict):
                # Handle range filters
                if 'min' in value and 'max' in value:
                    expressions.append(f'{key} >= {value["min"]} and {key} <= {value["max"]}')
                elif 'min' in value:
                    expressions.append(f'{key} >= {value["min"]}')
                elif 'max' in value:
                    expressions.append(f'{key} <= {value["max"]}')
            else:
                # Handle exact match
                expressions.append(f'{key} == "{value}"')
        
        return ' and '.join(expressions) if expressions else None
    

    def get_collection_stats(self) -> Dict[str, Any]:
        """Get statistics for all collections."""
        stats = {}
        
        for name, collection in self.collections.items():
            try:
                stats[name] = {
                    'num_entities': collection.num_entities,
                    'schema': str(collection.schema),
                    'indexes': collection.indexes
                }
            except Exception as e:
                app_logger.error(f"Error getting stats for {name}: {e}")
                stats[name] = {'error': str(e)}
        
        # If no collections exist yet, return empty stats
        if not self.collections:
            stats['status'] = 'No collections initialized yet'
        
        return stats
    
    async def insert_restaurants_optimized(self, restaurants: List[Dict]) -> bool:
        """
        Optimized restaurant insertion with batch embedding generation.
        Essential optimizations for SweetPick scale.
        """
        if not restaurants:
            return True
        
        app_logger.info(f"🚀 Optimized insert: {len(restaurants)} restaurants")
        
        try:
            collection = self._get_restaurants_collection()
            if not collection:
                app_logger.error("No restaurants collection available")
                return False
            
            # Step 1: Batch generate embeddings (ESSENTIAL - saves API costs)
            app_logger.info("🔄 Generating embeddings in batches...")
            embedding_texts = [self._create_restaurant_embedding_text(r) for r in restaurants]
            embeddings = await self._generate_embeddings_batch(embedding_texts)
            
            # Step 2: Transform all entities
            app_logger.info("🔄 Transforming entities...")
            entities = []
            for restaurant, embedding in zip(restaurants, embeddings):
                try:
                    entity = self._transform_restaurant_entity_optimized(restaurant, embedding)
                    entities.append(entity)
                except Exception as e:
                    app_logger.warning(f"⚠️ Failed to transform restaurant {restaurant.get('restaurant_name', 'Unknown')}: {e}")
                    continue
            
            if not entities:
                app_logger.warning("No valid entities to insert")
                return False
            
            # Step 3: Batch insert (USEFUL - reduces network overhead)
            app_logger.info(f"💾 Batch inserting {len(entities)} restaurants...")
            success = await self._batch_insert_entities(collection, entities, "restaurants")
            
            app_logger.info(f"✅ Restaurant insert completed: {len(entities)} entities")
            return success
            
        except Exception as e:
            app_logger.error(f"❌ Error in optimized restaurant insert: {e}")
            return False
    
    async def insert_dishes_optimized(self, dishes: List[Dict]) -> bool:
        """
        Optimized dish insertion with batch embedding generation.
        Essential optimizations for SweetPick scale.
        """
        if not dishes:
            return True
        
        app_logger.info(f"🚀 Optimized insert: {len(dishes)} dishes")
        
        try:
            collection = self._get_dishes_collection()
            if not collection:
                app_logger.error("No dishes collection available")
                return False
            
            # Step 1: Batch generate embeddings
            app_logger.info("🔄 Generating embeddings in batches...")
            embedding_texts = [self._create_dish_embedding_text(d) for d in dishes]
            embeddings = await self._generate_embeddings_batch(embedding_texts)
            
            # Step 2: Transform all entities
            app_logger.info("🔄 Transforming entities...")
            entities = []
            for dish, embedding in zip(dishes, embeddings):
                try:
                    entity = self._transform_dish_entity_optimized(dish, embedding)
                    entities.append(entity)
                except Exception as e:
                    app_logger.warning(f"⚠️ Failed to transform dish {dish.get('dish_name', 'Unknown')}: {e}")
                    continue
            
            if not entities:
                app_logger.warning("No valid entities to insert")
                return False
            
            # Step 3: Batch insert
            app_logger.info(f"💾 Batch inserting {len(entities)} dishes...")
            success = await self._batch_insert_entities(collection, entities, "dishes")
            
            app_logger.info(f"✅ Dish insert completed: {len(entities)} entities")
            return success
            
        except Exception as e:
            app_logger.error(f"❌ Error in optimized dish insert: {e}")
            return False
    
    async def _generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        ESSENTIAL OPTIMIZATION: Generate embeddings in batches to save API costs.
        """
        if not texts:
            return []
        
        embeddings = []
        total_batches = (len(texts) + self.embedding_batch_size - 1) // self.embedding_batch_size
        
        app_logger.info(f"📝 Generating {len(texts)} embeddings in {total_batches} batches")
        
        for i in range(0, len(texts), self.embedding_batch_size):
            batch_texts = texts[i:i + self.embedding_batch_size]
            batch_num = i // self.embedding_batch_size + 1
            
            app_logger.info(f"🔄 Processing embedding batch {batch_num}/{total_batches} ({len(batch_texts)} texts)")
            
            try:
                # Check cache first for each text
                batch_embeddings = []
                texts_to_generate = []
                cached_indices = {}
                
                for j, text in enumerate(batch_texts):
                    if text in self._embedding_cache:
                        batch_embeddings.append(None)  # Placeholder
                        cached_indices[j] = self._embedding_cache[text]
                        self.stats["embeddings_cached"] += 1
                    else:
                        batch_embeddings.append(None)  # Placeholder
                        texts_to_generate.append((j, text))
                
                # Generate embeddings for non-cached texts
                if texts_to_generate:
                    app_logger.info(f"🔄 Generating {len(texts_to_generate)} new embeddings (cached: {len(cached_indices)})")
                    
                    texts_only = [item[1] for item in texts_to_generate]
                    response = await self.openai_client.embeddings.create(
                        model=self.settings.embedding_model,
                        input=texts_only
                    )
                    
                    # Store in cache and result
                    for k, (original_index, text) in enumerate(texts_to_generate):
                        embedding = response.data[k].embedding
                        self._embedding_cache[text] = embedding
                        batch_embeddings[original_index] = embedding
                        self.stats["embeddings_generated"] += 1
                
                # Fill in cached embeddings
                for j, embedding in cached_indices.items():
                    batch_embeddings[j] = embedding
                
                embeddings.extend(batch_embeddings)
                
            except Exception as e:
                app_logger.error(f"❌ Error generating embedding batch {batch_num}: {e}")
                # Fallback to zero vectors
                fallback_embeddings = [[0.0] * self.settings.vector_dimension] * len(batch_texts)
                embeddings.extend(fallback_embeddings)
        
        app_logger.info(f"✅ Generated {len(embeddings)} embeddings")
        return embeddings
    
    async def _batch_insert_entities(self, collection, entities: List[Dict], entity_type: str) -> bool:
        """
        USEFUL OPTIMIZATION: Insert entities in batches to reduce network overhead.
        """
        if not entities:
            return True
        
        try:
            # Insert in batches
            total_batches = (len(entities) + self.insert_batch_size - 1) // self.insert_batch_size
            
            if total_batches > 1:
                app_logger.info(f"📦 Inserting {len(entities)} {entity_type} in {total_batches} batches")
            
            for i in range(0, len(entities), self.insert_batch_size):
                batch = entities[i:i + self.insert_batch_size]
                batch_num = i // self.insert_batch_size + 1
                
                if total_batches > 1:
                    app_logger.info(f"💾 Inserting batch {batch_num}/{total_batches} ({len(batch)} {entity_type})")
                
                # Simple retry logic
                for attempt in range(3):
                    try:
                        collection.insert(batch)
                        self.stats["batch_inserts"] += 1
                        break
                    except Exception as e:
                        if attempt == 2:  # Last attempt
                            app_logger.error(f"❌ Failed to insert batch {batch_num} after 3 attempts: {e}")
                            self.stats["failed_inserts"] += 1
                            return False
                        app_logger.warning(f"⚠️ Batch {batch_num} attempt {attempt + 1} failed: {e}")
                        await asyncio.sleep(1)  # Brief pause before retry
            
            # Single flush at the end
            collection.flush()
            app_logger.info(f"✅ Batch insert completed successfully")
            return True
            
        except Exception as e:
            app_logger.error(f"❌ Critical error in batch insert: {e}")
            self.stats["failed_inserts"] += 1
            return False
    
    def _transform_restaurant_entity_optimized(self, restaurant: Dict, embedding: List[float]) -> Dict:
        """Transform restaurant with proper type handling and size limits."""
        current_time = datetime.now().isoformat()
        
        # Safe type conversions
        def safe_float(value, default=0.0):
            try:
                return float(value) if value is not None else default
            except (ValueError, TypeError):
                return default
        
        def safe_int(value, default=0):
            try:
                return int(value) if value is not None else default
            except (ValueError, TypeError):
                return default
        
        def safe_str(value, max_length=None):
            result = str(value) if value is not None else ''
            return result[:max_length] if max_length else result
        
        return {
            'restaurant_id': safe_str(restaurant.get('restaurant_id', f"rest_{str(uuid.uuid4())[:8]}")),
            'restaurant_name': safe_str(restaurant.get('restaurant_name', ''), 300),
            'google_place_id': safe_str(restaurant.get('google_place_id', restaurant.get('place_id', '')), 150),
            'full_address': safe_str(restaurant.get('full_address', restaurant.get('address', '')), 500),
            'city': safe_str(restaurant.get('city', ''), 100),
            'neighborhood': safe_str(restaurant.get('neighborhood', ''), 150),
            'latitude': safe_float(restaurant.get('latitude')),
            'longitude': safe_float(restaurant.get('longitude')),
            'cuisine_type': safe_str(restaurant.get('cuisine_type', ''), 100),
            'sub_cuisines': restaurant.get('sub_cuisines', []) if isinstance(restaurant.get('sub_cuisines'), list) else [],
            'rating': safe_float(restaurant.get('rating')),
            'review_count': safe_int(restaurant.get('review_count')),
            'quality_score': safe_float(restaurant.get('quality_score')),
            'price_range': safe_int(restaurant.get('price_range', 2)),
            'operating_hours': restaurant.get('operating_hours', {}) if isinstance(restaurant.get('operating_hours'), dict) else {},
            'meal_types': restaurant.get('meal_types', []) if isinstance(restaurant.get('meal_types'), list) else [],
            'phone': safe_str(restaurant.get('phone', ''), 50),
            'website': safe_str(restaurant.get('website', ''), 500),
            'fallback_tier': safe_int(restaurant.get('fallback_tier', 2)),
            'embedding_text': self._create_restaurant_embedding_text(restaurant)[:4000],
            'vector_embedding': embedding,
            'created_at': safe_str(restaurant.get('created_at', current_time)),
            'updated_at': safe_str(restaurant.get('updated_at', current_time))
        }
    
    def _transform_dish_entity_optimized(self, dish: Dict, embedding: List[float]) -> Dict:
        """Transform dish with proper type handling and size limits."""
        current_time = datetime.now().isoformat()
        
        # Safe type conversions (reuse from above)
        def safe_float(value, default=0.0):
            try:
                return float(value) if value is not None else default
            except (ValueError, TypeError):
                return default
        
        def safe_int(value, default=0):
            try:
                return int(value) if value is not None else default
            except (ValueError, TypeError):
                return default
        
        def safe_str(value, max_length=None):
            result = str(value) if value is not None else ''
            return result[:max_length] if max_length else result
        
        # Ensure list fields are proper lists
        dietary_tags = dish.get('dietary_tags', [])
        if not isinstance(dietary_tags, list):
            dietary_tags = []
        
        sample_contexts = dish.get('sample_contexts', [])
        if not isinstance(sample_contexts, list):
            sample_contexts = []
        
        return {
            'dish_id': safe_str(dish.get('dish_id', str(uuid.uuid4()))),
            'restaurant_id': safe_str(dish.get('restaurant_id', '')),
            'dish_name': safe_str(dish.get('dish_name', ''), 300),
            'normalized_dish_name': safe_str(dish.get('normalized_dish_name', ''), 300),
            'dish_category': safe_str(dish.get('dish_category', 'main'), 100),
            'cuisine_context': safe_str(dish.get('cuisine_context', ''), 200),
            'dietary_tags': dietary_tags,
            'sentiment_score': safe_float(dish.get('sentiment_score', 0.0)),
            'positive_mentions': safe_int(dish.get('positive_mentions', 0)),
            'negative_mentions': safe_int(dish.get('negative_mentions', 0)),
            'neutral_mentions': safe_int(dish.get('neutral_mentions', 0)),
            'total_mentions': safe_int(dish.get('total_mentions', 0)),
            'confidence_score': safe_float(dish.get('confidence_score', 0.5)),
            'recommendation_score': safe_float(dish.get('recommendation_score', 0.0)),
            'avg_price_mentioned': safe_float(dish.get('avg_price_mentioned', 0.0)),
            'trending_score': safe_float(dish.get('trending_score', 0.0)),
            'embedding_text': self._create_dish_embedding_text(dish)[:4000],
            'vector_embedding': embedding,
            'sample_contexts': sample_contexts,
            'created_at': safe_str(dish.get('created_at', current_time)),
            'updated_at': safe_str(dish.get('updated_at', current_time))
        }
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        cache_hit_rate = 0.0
        total_embeddings = self.stats["embeddings_generated"] + self.stats["embeddings_cached"]
        if total_embeddings > 0:
            cache_hit_rate = self.stats["embeddings_cached"] / total_embeddings
        
        return {
            **self.stats,
            "cache_hit_rate": cache_hit_rate,
            "cache_size": len(self._embedding_cache)
        }
    
    def close(self):
        """Close Milvus connection."""
        try:
            connections.disconnect("default")
            app_logger.info("Disconnected from Milvus")
        except Exception as e:
            app_logger.error(f"Error disconnecting from Milvus: {e}")

    # ==================== LOCATION METADATA METHODS ====================
    
    async def insert_location_metadata(self, location_data: List[Dict]) -> bool:
        """Insert location metadata into the collection."""
        if not location_data:
            return True
        
        try:
            collection = self._get_locations_collection()
            if not collection:
                app_logger.error("No locations collection available")
                return False
            
            # Prepare data
            data = []
            for location in location_data:
                # Generate embedding text
                embedding_text = self._create_location_embedding_text(location)
                
                # Generate vector embedding using OpenAI
                vector_embedding = await self._generate_embedding(embedding_text)
                
                # Transform location data
                transformed_location = self._transform_location_entity(location, vector_embedding)
                data.append(transformed_location)
            
            # Insert data
            collection.insert(data)
            collection.flush()
            
            app_logger.info(f"✅ Inserted {len(data)} location metadata records")
            return True
            
        except Exception as e:
            app_logger.error(f"❌ Error inserting location metadata: {e}")
            return False
    
    def _get_locations_collection(self) -> Optional[Collection]:
        """Get the locations metadata collection."""
        if 'locations' not in self.collections:
            try:
                collection_name = "locations_metadata"
                if utility.has_collection(collection_name):
                    self.collections['locations'] = Collection(collection_name)
                    app_logger.info(f"Connected to existing collection: {collection_name}")
                else:
                    app_logger.error(f"Collection {collection_name} does not exist")
                    return None
            except Exception as e:
                app_logger.error(f"Error getting locations collection: {e}")
                return None
        
        return self.collections['locations']
    
    def _create_location_embedding_text(self, location: Dict) -> str:
        """Create embedding text for location metadata."""
        city = location.get('city', '')
        neighborhood = location.get('neighborhood', '')
        popular_cuisines = location.get('popular_cuisines', [])
        cuisine_distribution = location.get('cuisine_distribution', {})
        
        # Create descriptive text for embedding
        embedding_text = f"Location: {city}"
        if neighborhood:
            embedding_text += f" in {neighborhood}"
        
        if popular_cuisines:
            embedding_text += f". Popular cuisines: {', '.join(popular_cuisines[:5])}"
        
        if cuisine_distribution:
            top_cuisines = sorted(cuisine_distribution.items(), key=lambda x: x[1], reverse=True)[:3]
            embedding_text += f". Cuisine distribution: {', '.join([f'{cuisine}({count})' for cuisine, count in top_cuisines])}"
        
        embedding_text += f". Restaurant count: {location.get('restaurant_count', 0)}"
        embedding_text += f". Average rating: {location.get('avg_rating', 0.0):.1f}"
        
        return embedding_text
    
    def _transform_location_entity(self, location: Dict, embedding: List[float]) -> Dict:
        """Transform location data with proper type handling."""
        current_time = datetime.now().isoformat()
        
        def safe_float(value, default=0.0):
            try:
                return float(value) if value is not None else default
            except (ValueError, TypeError):
                return default
        
        def safe_int(value, default=0):
            try:
                return int(value) if value is not None else default
            except (ValueError, TypeError):
                return default
        
        def safe_str(value, max_length=None):
            result = str(value) if value is not None else ''
            return result[:max_length] if max_length else result
        
        # Ensure JSON fields are proper Python objects
        cuisine_distribution = location.get('cuisine_distribution', {})
        if not isinstance(cuisine_distribution, dict):
            cuisine_distribution = {}
        
        popular_cuisines = location.get('popular_cuisines', [])
        if not isinstance(popular_cuisines, list):
            popular_cuisines = []
        
        price_distribution = location.get('price_distribution', {})
        if not isinstance(price_distribution, dict):
            price_distribution = {}
        
        geographic_bounds = location.get('geographic_bounds', {})
        if not isinstance(geographic_bounds, dict):
            geographic_bounds = {}
        
        return {
            'location_id': safe_str(location.get('location_id', str(uuid.uuid4()))),
            'city': safe_str(location.get('city', ''), 100),
            'neighborhood': safe_str(location.get('neighborhood', ''), 150),
            'restaurant_count': safe_int(location.get('restaurant_count', 0)),
            'avg_rating': safe_float(location.get('avg_rating', 0.0)),
            'cuisine_distribution': cuisine_distribution,
            'popular_cuisines': popular_cuisines,
            'price_distribution': price_distribution,
            'geographic_bounds': geographic_bounds,
            'embedding_text': self._create_location_embedding_text(location)[:4000],
            'vector_embedding': embedding
        }
    
    async def search_locations(self, query: str, city: Optional[str] = None, 
                        max_results: int = 10) -> List[Dict]:
        """Search locations by text similarity and filters."""
        try:
            collection = self._get_locations_collection()
            if not collection:
                app_logger.error("No locations collection available")
                return []
            
            # Generate query embedding
            query_embedding = await self._generate_embedding(query)
            
            # Build search parameters
            search_params = {
                "metric_type": "COSINE",
                "params": {"ef": 64}
            }
            
            # Build filter expression
            filter_expr = None
            if city:
                filter_expr = f'city == "{city}"'
            
            # Perform search
            results = collection.search(
                data=[query_embedding],
                anns_field="vector_embedding",
                param=search_params,
                limit=max_results,
                expr=filter_expr,
                output_fields=["location_id", "city", "neighborhood", "restaurant_count", 
                              "avg_rating", "popular_cuisines", "cuisine_distribution"]
            )
            
            # Transform results
            locations = []
            for hits in results:
                for hit in hits:
                    location_data = {
                        'location_id': hit.get('location_id') or '',
                        'city': hit.get('city') or '',
                        'neighborhood': hit.get('neighborhood') or '',
                        'restaurant_count': hit.get('restaurant_count') or 0,
                        'avg_rating': hit.get('avg_rating') or 0.0,
                        'popular_cuisines': hit.get('popular_cuisines') or [],
                        'cuisine_distribution': hit.get('cuisine_distribution') or {},
                        'similarity_score': hit.score
                    }
                    locations.append(location_data)
            
            app_logger.info(f"🔍 Found {len(locations)} locations for query: {query}")
            return locations
            
        except Exception as e:
            app_logger.error(f"❌ Error searching locations: {e}")
            return []
    
    def get_location_by_id(self, location_id: str) -> Optional[Dict]:
        """Get location metadata by ID."""
        try:
            collection = self._get_locations_collection()
            if not collection:
                return None
            
            results = collection.query(
                expr=f'location_id == "{location_id}"',
                output_fields=["location_id", "city", "neighborhood", "restaurant_count", 
                              "avg_rating", "popular_cuisines", "cuisine_distribution", 
                              "price_distribution", "geographic_bounds"]
            )
            
            if results:
                return results[0]
            return None
            
        except Exception as e:
            app_logger.error(f"❌ Error getting location by ID: {e}")
            return None
    
    def get_neighborhoods_for_city(self, city: str) -> List[Dict]:
        """Get all neighborhoods for a specific city."""
        try:
            collection = self._get_locations_collection()
            if not collection:
                return []
            
            results = collection.query(
                expr=f'city == "{city}"',
                output_fields=["location_id", "city", "neighborhood", "restaurant_count", 
                              "avg_rating", "popular_cuisines", "cuisine_distribution"]
            )
            
            # Sort by restaurant count (most popular first)
            results.sort(key=lambda x: x.get('restaurant_count', 0), reverse=True)
            
            app_logger.info(f"🏙️ Found {len(results)} neighborhoods for {city}")
            return results
            
        except Exception as e:
            app_logger.error(f"❌ Error getting neighborhoods for city: {e}")
            return []
    
    def get_location_statistics(self, city: str, neighborhood: Optional[str] = None) -> Dict:
        """Get aggregated statistics for a location."""
        try:
            collection = self._get_locations_collection()
            if not collection:
                return {}
            
            # Build filter expression
            if neighborhood:
                filter_expr = f'city == "{city}" and neighborhood == "{neighborhood}"'
            else:
                filter_expr = f'city == "{city}"'
            
            results = collection.query(
                expr=filter_expr,
                output_fields=["restaurant_count", "avg_rating", "popular_cuisines", 
                              "cuisine_distribution", "price_distribution"]
            )
            
            if not results:
                return {}
            
            # Aggregate statistics
            total_restaurants = sum(r.get('restaurant_count', 0) for r in results)
            avg_rating = sum(r.get('avg_rating', 0) * r.get('restaurant_count', 0) for r in results) / total_restaurants if total_restaurants > 0 else 0
            
            # Merge cuisine distributions
            merged_cuisine_dist = {}
            for result in results:
                cuisine_dist = result.get('cuisine_distribution', {})
                for cuisine, count in cuisine_dist.items():
                    merged_cuisine_dist[cuisine] = merged_cuisine_dist.get(cuisine, 0) + count
            
            # Get top cuisines
            top_cuisines = sorted(merged_cuisine_dist.items(), key=lambda x: x[1], reverse=True)[:5]
            
            return {
                'city': city,
                'neighborhood': neighborhood,
                'total_restaurants': total_restaurants,
                'average_rating': round(avg_rating, 2),
                'top_cuisines': [cuisine for cuisine, _ in top_cuisines],
                'cuisine_distribution': merged_cuisine_dist
            }
            
        except Exception as e:
            app_logger.error(f"❌ Error getting location statistics: {e}")
            return {}
    
    def has_collection(self, collection_name: str) -> bool:
        """Check if a collection exists."""
        try:
            return utility.has_collection(collection_name)
        except Exception as e:
            app_logger.error(f"Error checking collection {collection_name}: {e}")
            return False
    
    def _get_collection_by_name(self, collection_name: str):
        """Get a collection by name."""
        try:
            return Collection(collection_name)
        except Exception as e:
            app_logger.error(f"Error getting collection {collection_name}: {e}")
            return None
    
    def search_collection(self, collection_name: str, query_vector: List[float], 
                         filter_expr: str = None, limit: int = 10, 
                         output_fields: List[str] = None) -> List:
        """Generic search method for any collection."""
        try:
            collection = self._get_collection_by_name(collection_name)
            if not collection:
                app_logger.error(f"Collection {collection_name} not found")
                return []
                
            collection.load()
            
            # Build search parameters
            search_params = {
                "metric_type": "COSINE",
                "params": {"nprobe": 10}
            }
            
            # Execute search
            results = collection.search(
                data=[query_vector],
                anns_field="vector_embedding",
                param=search_params,
                expr=filter_expr,
                limit=limit,
                output_fields=output_fields or ["*"]
            )
            
            return results
            
        except Exception as e:
            app_logger.error(f"Error searching collection {collection_name}: {e}")
            return []

