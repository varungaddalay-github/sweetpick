"""
Optimized Milvus client with bulk operations, connection pooling, and enhanced performance.
"""
import asyncio
import json
import uuid
import time
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass
from contextlib import asynccontextmanager
from pymilvus import connections, Collection, CollectionSchema, FieldSchema, DataType, utility
from pymilvus.exceptions import MilvusException
from openai import AsyncOpenAI
from src.utils.config import get_settings
from src.utils.logger import app_logger
from datetime import datetime
import numpy as np
import hashlib


@dataclass
class ConnectionConfig:
    """Configuration for Milvus connections."""
    max_connections: int = 20
    min_connections: int = 5
    connection_timeout: int = 30
    idle_timeout: int = 300
    max_retries: int = 3
    retry_delay: float = 1.0


@dataclass
class BulkConfig:
    """Configuration for bulk operations."""
    batch_size: int = 1000
    max_concurrent_batches: int = 5
    embedding_batch_size: int = 50
    insert_timeout: int = 60


class ConnectionPool:
    """Connection pool for Milvus operations."""
    
    def __init__(self, config: ConnectionConfig):
        self.config = config
        self.connections = asyncio.Queue(maxsize=config.max_connections)
        self.active_connections = 0
        self.lock = asyncio.Lock()
        self.settings = get_settings()
    
    async def initialize(self):
        """Initialize the connection pool."""
        app_logger.info(f"🔧 Initializing connection pool with {self.config.min_connections} connections")
        
        for _ in range(self.config.min_connections):
            connection = await self._create_connection()
            await self.connections.put(connection)
    
    async def _create_connection(self):
        """Create a new Milvus connection."""
        try:
            # Create connection alias
            alias = f"connection_{uuid.uuid4().hex[:8]}"
            
            if self.settings.milvus_username and self.settings.milvus_password:
                connections.connect(
                    alias=alias,
                    uri=self.settings.milvus_uri,
                    user=self.settings.milvus_username,
                    password=self.settings.milvus_password,
                    db_name=self.settings.milvus_database
                )
            else:
                connections.connect(
                    alias=alias,
                    uri=self.settings.milvus_uri,
                    token=self.settings.milvus_token,
                    db_name=self.settings.milvus_database
                )
            
            return alias
            
        except Exception as e:
            app_logger.error(f"Error creating connection: {e}")
            raise
    
    @asynccontextmanager
    async def get_connection(self):
        """Get a connection from the pool."""
        connection = None
        try:
            # Try to get existing connection
            try:
                connection = await asyncio.wait_for(
                    self.connections.get(), 
                    timeout=self.config.connection_timeout
                )
            except asyncio.TimeoutError:
                # Create new connection if pool is empty
                async with self.lock:
                    if self.active_connections < self.config.max_connections:
                        connection = await self._create_connection()
                        self.active_connections += 1
                    else:
                        raise Exception("Connection pool exhausted")
            
            yield connection
            
        except Exception as e:
            app_logger.error(f"Error with connection: {e}")
            raise
        finally:
            if connection:
                # Return connection to pool
                try:
                    await self.connections.put(connection)
                except asyncio.QueueFull:
                    # Pool is full, close connection
                    await self._close_connection(connection)
                    async with self.lock:
                        self.active_connections -= 1
    
    async def _close_connection(self, alias: str):
        """Close a Milvus connection."""
        try:
            connections.disconnect(alias)
        except Exception as e:
            app_logger.warning(f"Error closing connection {alias}: {e}")


class OptimizedMilvusClient:
    """Optimized Milvus client with bulk operations and connection pooling."""
    
    def __init__(self, connection_config: ConnectionConfig = None, bulk_config: BulkConfig = None):
        self.settings = get_settings()
        self.connection_config = connection_config or ConnectionConfig()
        self.bulk_config = bulk_config or BulkConfig()
        
        # Initialize connection pool
        self.connection_pool = ConnectionPool(self.connection_config)
        
        # OpenAI client for embeddings
        self.openai_client = AsyncOpenAI(api_key=self.settings.openai_api_key)
        
        # Embedding cache
        self._embedding_cache = {}
        self._cache_hits = 0
        self._cache_misses = 0
        
        # Statistics
        self.stats = {
            "bulk_inserts": 0,
            "failed_inserts": 0,
            "embeddings_generated": 0,
            "embeddings_cached": 0,
            "connection_errors": 0,
            "processing_time": 0.0
        }
        
        # Initialize collections
        self.collections = {}
        self._initialize_collections()
    
    async def initialize(self):
        """Initialize the client."""
        await self.connection_pool.initialize()
        app_logger.info("✅ Optimized Milvus client initialized")
    
    def _initialize_collections(self):
        """Initialize collections with optimized schemas."""
        try:
            existing_collections = utility.list_collections()
            app_logger.info(f"Found existing collections: {existing_collections}")
            
            # Initialize restaurants collection
            if "restaurants_enhanced" in existing_collections:
                self.collections['restaurants'] = Collection("restaurants_enhanced")
            else:
                self._create_optimized_restaurants_collection()
            
            # Initialize dishes collection
            if "dishes_detailed" in existing_collections:
                self.collections['dishes'] = Collection("dishes_detailed")
            else:
                self._create_optimized_dishes_collection()
            
            app_logger.info("Collections initialized successfully")
            
        except Exception as e:
            app_logger.error(f"Error initializing collections: {e}")
    
    def _create_optimized_restaurants_collection(self):
        """Create optimized restaurants collection."""
        collection_name = "restaurants_enhanced"
        
        # Enhanced schema with better field constraints
        fields = [
            FieldSchema(name="restaurant_id", dtype=DataType.VARCHAR, max_length=100, 
                       is_primary=True, description="Unique restaurant identifier"),
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
            FieldSchema(name="latitude", dtype=DataType.DOUBLE, description="Latitude coordinate"),
            FieldSchema(name="longitude", dtype=DataType.DOUBLE, description="Longitude coordinate"),
            FieldSchema(name="cuisine_type", dtype=DataType.VARCHAR, max_length=100, 
                       description="Primary cuisine type"),
            FieldSchema(name="sub_cuisines", dtype=DataType.JSON, description="Array of sub-cuisine types"),
            FieldSchema(name="rating", dtype=DataType.FLOAT, description="Average rating (0.0-5.0)"),
            FieldSchema(name="review_count", dtype=DataType.INT64, description="Total number of reviews"),
            FieldSchema(name="quality_score", dtype=DataType.FLOAT, description="Calculated quality score"),
            FieldSchema(name="price_range", dtype=DataType.INT32, description="Price range (1-4)"),
            FieldSchema(name="operating_hours", dtype=DataType.JSON, description="Operating hours by day"),
            FieldSchema(name="meal_types", dtype=DataType.JSON, description="Supported meal types array"),
            FieldSchema(name="phone", dtype=DataType.VARCHAR, max_length=50, description="Phone number"),
            FieldSchema(name="website", dtype=DataType.VARCHAR, max_length=500, description="Website URL"),
            FieldSchema(name="fallback_tier", dtype=DataType.INT32, description="Fallback tier (1-4)"),
            FieldSchema(name="embedding_text", dtype=DataType.VARCHAR, max_length=8000, 
                       description="Text used for embedding generation"),
            FieldSchema(name="vector_embedding", dtype=DataType.FLOAT_VECTOR, 
                       dim=self.settings.vector_dimension, description="OpenAI embedding vector"),
            FieldSchema(name="created_at", dtype=DataType.VARCHAR, max_length=50, 
                       description="Creation timestamp"),
            FieldSchema(name="updated_at", dtype=DataType.VARCHAR, max_length=50, 
                       description="Last update timestamp")
        ]
        
        schema = CollectionSchema(fields, description="Enhanced restaurant information with embeddings")
        collection = Collection(collection_name, schema)
        
        # Optimized indexing parameters
        index_params = {
            "metric_type": "COSINE",
            "index_type": "HNSW",
            "params": {
                "M": 32,               # Increased for better recall
                "efConstruction": 400, # Increased for better build quality
                "ef": 64               # Search parameter
            }
        }
        collection.create_index("vector_embedding", index_params)
        
        self.collections['restaurants'] = collection
        app_logger.info(f"Created optimized collection: {collection_name}")
    
    def _create_optimized_dishes_collection(self):
        """Create optimized dishes collection."""
        collection_name = "dishes_detailed"
        
        # Enhanced schema with better field constraints
        fields = [
            FieldSchema(name="dish_id", dtype=DataType.VARCHAR, max_length=100, 
                       is_primary=True, description="Unique dish identifier"),
            FieldSchema(name="restaurant_id", dtype=DataType.VARCHAR, max_length=100, 
                       description="Foreign key to restaurant"),
            FieldSchema(name="dish_name", dtype=DataType.VARCHAR, max_length=300, 
                       description="Original dish name"),
            FieldSchema(name="normalized_dish_name", dtype=DataType.VARCHAR, max_length=300, 
                       description="Normalized dish name for matching"),
            FieldSchema(name="dish_category", dtype=DataType.VARCHAR, max_length=100, 
                       description="Dish category (appetizer, main, dessert, etc.)"),
            FieldSchema(name="cuisine_context", dtype=DataType.VARCHAR, max_length=100, 
                       description="Cuisine context for the dish"),
            FieldSchema(name="dietary_tags", dtype=DataType.JSON, description="Array of dietary restrictions/tags"),
            FieldSchema(name="sentiment_score", dtype=DataType.FLOAT, description="Overall sentiment score (-1.0 to 1.0)"),
            FieldSchema(name="positive_mentions", dtype=DataType.INT32, description="Count of positive mentions"),
            FieldSchema(name="negative_mentions", dtype=DataType.INT32, description="Count of negative mentions"),
            FieldSchema(name="neutral_mentions", dtype=DataType.INT32, description="Count of neutral mentions"),
            FieldSchema(name="total_mentions", dtype=DataType.INT32, description="Total mention count"),
            FieldSchema(name="confidence_score", dtype=DataType.FLOAT, description="Confidence in sentiment analysis (0.0-1.0)"),
            FieldSchema(name="recommendation_score", dtype=DataType.FLOAT, description="Overall recommendation score (0.0-1.0)"),
            FieldSchema(name="avg_price_mentioned", dtype=DataType.FLOAT, description="Average price mentioned in reviews"),
            FieldSchema(name="trending_score", dtype=DataType.FLOAT, description="Trending score based on recent mentions"),
            FieldSchema(name="embedding_text", dtype=DataType.VARCHAR, max_length=8000, 
                       description="Text used for embedding generation"),
            FieldSchema(name="vector_embedding", dtype=DataType.FLOAT_VECTOR, 
                       dim=self.settings.vector_dimension, description="OpenAI embedding vector"),
            FieldSchema(name="sample_contexts", dtype=DataType.JSON, description="Array of sample review contexts"),
            FieldSchema(name="created_at", dtype=DataType.VARCHAR, max_length=50, 
                       description="Creation timestamp"),
            FieldSchema(name="updated_at", dtype=DataType.VARCHAR, max_length=50, 
                       description="Last update timestamp")
        ]
        
        schema = CollectionSchema(fields, description="Enhanced dish information with embeddings")
        collection = Collection(collection_name, schema)
        
        # Optimized indexing parameters
        index_params = {
            "metric_type": "COSINE",
            "index_type": "HNSW",
            "params": {
                "M": 32,               # Increased for better recall
                "efConstruction": 400, # Increased for better build quality
                "ef": 64               # Search parameter
            }
        }
        collection.create_index("vector_embedding", index_params)
        
        self.collections['dishes'] = collection
        app_logger.info(f"Created optimized collection: {collection_name}")
    
    async def insert_restaurants_bulk(self, restaurants: List[Dict]) -> bool:
        """Insert restaurants in optimized batches."""
        if not restaurants:
            return True
        
        start_time = time.time()
        
        try:
            app_logger.info(f"💾 Bulk inserting {len(restaurants)} restaurants")
            
            # Process in batches
            batches = [restaurants[i:i + self.bulk_config.batch_size] 
                      for i in range(0, len(restaurants), self.bulk_config.batch_size)]
            
            # Process batches with limited concurrency
            semaphore = asyncio.Semaphore(self.bulk_config.max_concurrent_batches)
            
            async def process_batch(batch: List[Dict]):
                async with semaphore:
                    return await self._insert_restaurant_batch(batch)
            
            tasks = [process_batch(batch) for batch in batches]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count successful inserts
            successful_inserts = sum(1 for result in results if result is True)
            failed_inserts = len(results) - successful_inserts
            
            self.stats["bulk_inserts"] += successful_inserts
            self.stats["failed_inserts"] += failed_inserts
            self.stats["processing_time"] += time.time() - start_time
            
            app_logger.info(f"✅ Bulk insert completed: {successful_inserts} successful, {failed_inserts} failed")
            
            return failed_inserts == 0
            
        except Exception as e:
            app_logger.error(f"❌ Error in bulk restaurant insert: {e}")
            self.stats["failed_inserts"] += 1
            return False
    
    async def _insert_restaurant_batch(self, batch: List[Dict]) -> bool:
        """Insert a batch of restaurants."""
        try:
            # Prepare data for insertion
            prepared_data = []
            for restaurant in batch:
                prepared = self._prepare_restaurant_for_insertion(restaurant)
                if prepared:
                    prepared_data.append(prepared)
            
            if not prepared_data:
                return True
            
            # Generate embeddings in batches
            embeddings = await self._generate_embeddings_batch(
                [r["embedding_text"] for r in prepared_data]
            )
            
            # Add embeddings to data
            for i, embedding in enumerate(embeddings):
                prepared_data[i]["vector_embedding"] = embedding
            
            # Insert using connection pool
            async with self.connection_pool.get_connection() as connection:
                from pymilvus import Collection
                collection = Collection('restaurants_enhanced', using=connection)
                collection.load()
                
                # Prepare data for Milvus - use entity format
                entities = prepared_data  # Use prepared data directly as entities
                
                # Insert (synchronous method)
                collection.insert(entities)
                collection.flush()
            
            return True
            
        except Exception as e:
            app_logger.error(f"Error inserting restaurant batch: {e}")
            return False
    
    async def insert_dishes_bulk(self, dishes: List[Dict]) -> bool:
        """Insert dishes in optimized batches."""
        if not dishes:
            return True
        
        start_time = time.time()
        
        try:
            app_logger.info(f"💾 Bulk inserting {len(dishes)} dishes")
            
            # Process in batches
            batches = [dishes[i:i + self.bulk_config.batch_size] 
                      for i in range(0, len(dishes), self.bulk_config.batch_size)]
            
            # Process batches with limited concurrency
            semaphore = asyncio.Semaphore(self.bulk_config.max_concurrent_batches)
            
            async def process_batch(batch: List[Dict]):
                async with semaphore:
                    return await self._insert_dish_batch(batch)
            
            tasks = [process_batch(batch) for batch in batches]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Count successful inserts
            successful_inserts = sum(1 for result in results if result is True)
            failed_inserts = len(results) - successful_inserts
            
            self.stats["bulk_inserts"] += successful_inserts
            self.stats["failed_inserts"] += failed_inserts
            self.stats["processing_time"] += time.time() - start_time
            
            app_logger.info(f"✅ Bulk dish insert completed: {successful_inserts} successful, {failed_inserts} failed")
            
            return failed_inserts == 0
            
        except Exception as e:
            app_logger.error(f"❌ Error in bulk dish insert: {e}")
            self.stats["failed_inserts"] += 1
            return False
    
    async def _insert_dish_batch(self, batch: List[Dict]) -> bool:
        """Insert a batch of dishes."""
        try:
            # Prepare data for insertion
            prepared_data = []
            for dish in batch:
                prepared = self._prepare_dish_for_insertion(dish)
                if prepared:
                    prepared_data.append(prepared)
            
            if not prepared_data:
                return True
            
            # Generate embeddings in batches
            embeddings = await self._generate_embeddings_batch(
                [d["embedding_text"] for d in prepared_data]
            )
            
            # Add embeddings to data
            for i, embedding in enumerate(embeddings):
                prepared_data[i]["vector_embedding"] = embedding
            
            # Insert using connection pool
            async with self.connection_pool.get_connection() as connection:
                from pymilvus import Collection
                collection = Collection('dishes_detailed', using=connection)
                collection.load()
                
                # Prepare data for Milvus - use entity format
                entities = prepared_data  # Use prepared data directly as entities
                
                # Insert (synchronous method)
                collection.insert(entities)
                collection.flush()
            
            return True
            
        except Exception as e:
            app_logger.error(f"Error inserting dish batch: {e}")
            return False
    
    async def _generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for a batch of texts with caching."""
        if not texts:
            return []
        
        # Check cache first
        embeddings = []
        uncached_texts = []
        uncached_indices = []
        
        for i, text in enumerate(texts):
            cache_key = self._get_cache_key(text)
            if cache_key in self._embedding_cache:
                embeddings.append(self._embedding_cache[cache_key])
                self._cache_hits += 1
            else:
                embeddings.append(None)  # Placeholder
                uncached_texts.append(text)
                uncached_indices.append(i)
                self._cache_misses += 1
        
        # Generate embeddings for uncached texts
        if uncached_texts:
            try:
                # Process in smaller batches for OpenAI API
                batch_size = self.bulk_config.embedding_batch_size
                all_new_embeddings = []
                
                for i in range(0, len(uncached_texts), batch_size):
                    batch_texts = uncached_texts[i:i + batch_size]
                    
                    response = await self.openai_client.embeddings.create(
                        model=self.settings.embedding_model,
                        input=batch_texts
                    )
                    
                    batch_embeddings = [data.embedding for data in response.data]
                    all_new_embeddings.extend(batch_embeddings)
                    
                    # Add delay between batches
                    if i + batch_size < len(uncached_texts):
                        await asyncio.sleep(0.1)
                
                # Cache new embeddings and update results
                for i, (text, embedding) in enumerate(zip(uncached_texts, all_new_embeddings)):
                    cache_key = self._get_cache_key(text)
                    self._embedding_cache[cache_key] = embedding
                    embeddings[uncached_indices[i]] = embedding
                
                self.stats["embeddings_generated"] += len(all_new_embeddings)
                
            except Exception as e:
                app_logger.error(f"Error generating embeddings: {e}")
                # Return zeros for failed embeddings
                for i in uncached_indices:
                    embeddings[i] = [0.0] * self.settings.vector_dimension
        
        return embeddings
    
    def _get_cache_key(self, text: str) -> str:
        """Generate cache key for text."""
        return hashlib.md5(text.encode()).hexdigest()
    
    def _prepare_restaurant_for_insertion(self, restaurant: Dict) -> Optional[Dict]:
        """Prepare restaurant data for insertion."""
        try:
            # Generate restaurant ID if not present
            restaurant_id = restaurant.get('restaurant_id')
            if not restaurant_id:
                restaurant_id = f"rest_{str(uuid.uuid4())[:8]}"
            
            # Create embedding text
            embedding_text = self._create_restaurant_embedding_text(restaurant)
            
            # Prepare data
            prepared = {
                'restaurant_id': str(restaurant_id),
                'restaurant_name': str(restaurant.get('restaurant_name', ''))[:300],
                'google_place_id': str(restaurant.get('google_place_id', ''))[:150],
                'full_address': str(restaurant.get('full_address', ''))[:500],
                'city': str(restaurant.get('city', ''))[:100],
                'neighborhood': str(restaurant.get('neighborhood', ''))[:150],
                'latitude': float(restaurant.get('latitude', 0.0)),
                'longitude': float(restaurant.get('longitude', 0.0)),
                'cuisine_type': str(restaurant.get('cuisine_type', ''))[:100],
                'sub_cuisines': restaurant.get('sub_cuisines', []),
                'rating': float(restaurant.get('rating', 0.0)),
                'review_count': int(restaurant.get('review_count', 0)),
                'quality_score': float(restaurant.get('quality_score', 0.0)),
                'price_range': int(restaurant.get('price_range', 2)),
                'operating_hours': restaurant.get('operating_hours', {}),
                'meal_types': restaurant.get('meal_types', []),
                'phone': str(restaurant.get('phone', ''))[:50],
                'website': str(restaurant.get('website', ''))[:500],
                'fallback_tier': int(restaurant.get('fallback_tier', 2)),
                'embedding_text': embedding_text[:8000],
                'created_at': str(restaurant.get('created_at', datetime.now().isoformat()))[:50],
                'updated_at': str(restaurant.get('updated_at', datetime.now().isoformat()))[:50]
            }
            
            return prepared
            
        except Exception as e:
            app_logger.error(f"Error preparing restaurant data: {e}")
            return None
    
    def _prepare_dish_for_insertion(self, dish: Dict) -> Optional[Dict]:
        """Prepare dish data for insertion."""
        try:
            # Generate dish ID if not present
            dish_id = dish.get('dish_id')
            if not dish_id:
                dish_name = dish.get('dish_name', 'unknown')
                restaurant_id = dish.get('restaurant_id', 'unknown')
                dish_id = f"dish_{hash(f'{restaurant_id}_{dish_name}') % 1000000}"
            
            # Create embedding text
            embedding_text = self._create_dish_embedding_text(dish)
            
            # Prepare data
            prepared = {
                'dish_id': str(dish_id),
                'restaurant_id': str(dish.get('restaurant_id', '')),
                'dish_name': str(dish.get('dish_name', ''))[:300],
                'normalized_dish_name': str(dish.get('normalized_dish_name', dish.get('dish_name', '')))[:300],
                'dish_category': str(dish.get('dish_category', 'main'))[:100],
                'cuisine_context': str(dish.get('cuisine_context', ''))[:100],
                'dietary_tags': dish.get('dietary_tags', []),
                'sentiment_score': float(dish.get('sentiment_score', 0.0)),
                'positive_mentions': int(dish.get('positive_mentions', 0)),
                'negative_mentions': int(dish.get('negative_mentions', 0)),
                'neutral_mentions': int(dish.get('neutral_mentions', 0)),
                'total_mentions': int(dish.get('total_mentions', 1)),
                'confidence_score': float(dish.get('confidence_score', 0.5)),
                'recommendation_score': float(dish.get('recommendation_score', 0.0)),
                'avg_price_mentioned': float(dish.get('avg_price_mentioned', 0.0)),
                'trending_score': float(dish.get('trending_score', 0.0)),
                'embedding_text': embedding_text[:8000],
                'sample_contexts': dish.get('sample_contexts', []),
                'created_at': str(dish.get('created_at', datetime.now().isoformat()))[:50],
                'updated_at': str(dish.get('updated_at', datetime.now().isoformat()))[:50]
            }
            
            return prepared
            
        except Exception as e:
            app_logger.error(f"Error preparing dish data: {e}")
            return None
    
    def _create_restaurant_embedding_text(self, restaurant: Dict) -> str:
        """Create embedding text for restaurant."""
        parts = [
            restaurant.get('restaurant_name', ''),
            restaurant.get('cuisine_type', ''),
            restaurant.get('city', ''),
            restaurant.get('neighborhood', ''),
            ' '.join(restaurant.get('sub_cuisines', [])),
            ' '.join(restaurant.get('meal_types', [])),
            f"Rating: {restaurant.get('rating', 0)}",
            f"Price range: {restaurant.get('price_range', 2)}"
        ]
        return ' '.join(filter(None, parts))
    
    def _create_dish_embedding_text(self, dish: Dict) -> str:
        """Create embedding text for dish."""
        parts = [
            dish.get('dish_name', ''),
            dish.get('normalized_dish_name', ''),
            dish.get('dish_category', ''),
            dish.get('cuisine_context', ''),
            ' '.join(dish.get('dietary_tags', [])),
            f"Sentiment: {dish.get('sentiment_score', 0)}",
            f"Recommendation score: {dish.get('recommendation_score', 0)}"
        ]
        return ' '.join(filter(None, parts))
    
    def _prepare_milvus_data(self, data: List[Dict]) -> List:
        """Prepare data for Milvus insertion."""
        if not data:
            return []
        
        # Get field names from first item
        field_names = list(data[0].keys())
        
        # Prepare data in Milvus format
        milvus_data = []
        for field_name in field_names:
            if field_name == 'vector_embedding':
                # Handle vector embeddings specially - ensure they are 1536 dimensions
                field_data = []
                for item in data:
                    embedding = item.get(field_name)
                    if embedding and len(embedding) == self.settings.vector_dimension:
                        field_data.append(embedding)
                    else:
                        # Generate default embedding if missing or wrong dimension
                        field_data.append([0.0] * self.settings.vector_dimension)
            else:
                field_data = [item.get(field_name) for item in data]
            milvus_data.append(field_data)
        
        return milvus_data
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get client statistics."""
        stats = self.stats.copy()
        stats.update({
            "cache_hits": self._cache_hits,
            "cache_misses": self._cache_misses,
            "cache_hit_rate": self._cache_hits / (self._cache_hits + self._cache_misses) if (self._cache_hits + self._cache_misses) > 0 else 0.0
        })
        return stats
    
    async def search_restaurants_with_filters(self, filters: Dict = None, limit: int = 10) -> List[Dict]:
        """Search restaurants with filters using vector similarity."""
        try:
            if not filters:
                filters = {}
            
            # Create search query from filters
            search_text = self._create_search_text_from_filters(filters)
            
            # Generate embedding for search
            embedding = await self._generate_embedding(search_text)
            
            # Search using connection pool
            async with self.connection_pool.get_connection() as connection:
                from pymilvus import Collection
                collection = Collection('restaurants_enhanced', using=connection)
                collection.load()
                
                # Build search parameters
                search_params = {
                    "metric_type": "COSINE",
                    "params": {"ef": 64}
                }
                
                # Execute search
                results = collection.search(
                    data=[embedding],
                    anns_field="vector_embedding",
                    param=search_params,
                    limit=limit,
                    output_fields=["restaurant_id", "restaurant_name", "city", "cuisine_type", 
                                 "rating", "review_count", "price_range", "neighborhood"]
                )
                
                # Convert results to list of dicts
                restaurants = []
                for hits in results:
                    for hit in hits:
                        restaurant = {
                            "restaurant_id": hit.entity.get("restaurant_id"),
                            "restaurant_name": hit.entity.get("restaurant_name"),
                            "city": hit.entity.get("city"),
                            "cuisine_type": hit.entity.get("cuisine_type"),
                            "rating": hit.entity.get("rating"),
                            "review_count": hit.entity.get("review_count"),
                            "price_range": hit.entity.get("price_range"),
                            "neighborhood": hit.entity.get("neighborhood"),
                            "similarity_score": hit.score
                        }
                        restaurants.append(restaurant)
                
                return restaurants
                
        except Exception as e:
            app_logger.error(f"Error searching restaurants: {e}")
            return []
    
    async def search_dishes(self, query_vector: List[float], filters: Dict = None, limit: int = 10) -> List[Dict]:
        """Search dishes using vector similarity."""
        try:
            if not filters:
                filters = {}
            
            # Search using connection pool
            async with self.connection_pool.get_connection() as connection:
                from pymilvus import Collection
                collection = Collection('dishes_detailed', using=connection)
                collection.load()
                
                # Build search parameters
                search_params = {
                    "metric_type": "COSINE",
                    "params": {"ef": 64}
                }
                
                # Execute search
                results = collection.search(
                    data=[query_vector],
                    anns_field="vector_embedding",
                    param=search_params,
                    limit=limit,
                    output_fields=["dish_id", "dish_name", "restaurant_id", "sentiment_score", 
                                 "recommendation_score", "cuisine_context"]
                )
                
                # Convert results to list of dicts
                dishes = []
                for hits in results:
                    for hit in hits:
                        dish = {
                            "dish_id": hit.entity.get("dish_id"),
                            "dish_name": hit.entity.get("dish_name"),
                            "restaurant_id": hit.entity.get("restaurant_id"),
                            "sentiment_score": hit.entity.get("sentiment_score"),
                            "recommendation_score": hit.entity.get("recommendation_score"),
                            "cuisine_context": hit.entity.get("cuisine_context"),
                            "similarity_score": hit.score
                        }
                        dishes.append(dish)
                
                return dishes
                
        except Exception as e:
            app_logger.error(f"Error searching dishes: {e}")
            return []
    
    def _create_search_text_from_filters(self, filters: Dict) -> str:
        """Create search text from filters."""
        parts = []
        
        if filters.get("city"):
            parts.append(filters["city"])
        if filters.get("cuisine_type"):
            parts.append(filters["cuisine_type"])
        if filters.get("neighborhood"):
            parts.append(filters["neighborhood"])
        if filters.get("dish_name"):
            parts.append(filters["dish_name"])
        
        return " ".join(parts) if parts else "restaurant food"
    
    async def _generate_embedding(self, text: str) -> List[float]:
        """Generate embedding for a single text."""
        try:
            response = await self.openai_client.embeddings.create(
                model=self.settings.embedding_model,
                input=[text]
            )
            return response.data[0].embedding
        except Exception as e:
            app_logger.error(f"Error generating embedding: {e}")
            return [0.0] * self.settings.vector_dimension

    async def close(self):
        """Close the client and connections."""
        try:
            # Close all connections in pool
            while not self.connection_pool.connections.empty():
                connection = await self.connection_pool.connections.get()
                await self.connection_pool._close_connection(connection)
            
            app_logger.info("✅ Optimized Milvus client closed")
        except Exception as e:
            app_logger.error(f"Error closing client: {e}")
