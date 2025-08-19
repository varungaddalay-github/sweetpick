"""
Parallel data collection with optimized processing for SweetPick RAG system.
Implements async batch processing, streaming ingestion, and rate limiting.
"""
import asyncio
import time
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass
from collections import deque
import hashlib
from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.data_collection.serpapi_collector import SerpAPICollector
from src.data_collection.data_validator import DataValidator
from src.processing.dish_extractor import DishExtractor
from src.processing.sentiment_analyzer import SentimentAnalyzer
from src.vector_db.milvus_client import MilvusClient


@dataclass
class ProcessingConfig:
    """Configuration for parallel processing."""
    max_concurrent_restaurants: int = 10
    max_concurrent_reviews: int = 20
    max_concurrent_sentiment: int = 30
    batch_size: int = 50
    rate_limit_delay: float = 0.2  # 200ms between API calls
    max_retries: int = 3
    retry_delay: float = 1.0


class RateLimiter:
    """Distributed rate limiter for API calls."""
    
    def __init__(self, max_calls_per_second: int = 5):
        self.max_calls_per_second = max_calls_per_second
        self.call_times = deque()
        self.lock = asyncio.Lock()
    
    async def acquire(self):
        """Acquire rate limit permission."""
        async with self.lock:
            now = time.time()
            
            # Remove old calls
            while self.call_times and now - self.call_times[0] > 1.0:
                self.call_times.popleft()
            
            # Check if we can make a call
            if len(self.call_times) >= self.max_calls_per_second:
                # Wait until we can make another call
                wait_time = 1.0 - (now - self.call_times[0])
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                    now = time.time()
            
            # Record this call
            self.call_times.append(now)


class ParallelDataCollector:
    """Optimized parallel data collector with streaming capabilities."""
    
    def __init__(self, config: ProcessingConfig = None):
        self.config = config or ProcessingConfig()
        self.settings = get_settings()
        
        # Initialize components
        self.serpapi_collector = SerpAPICollector()
        self.data_validator = DataValidator()
        self.dish_extractor = DishExtractor()
        self.sentiment_analyzer = SentimentAnalyzer()
        self.milvus_client = MilvusClient()
        
        # Rate limiters
        self.serpapi_limiter = RateLimiter(max_calls_per_second=5)
        self.openai_limiter = RateLimiter(max_calls_per_second=10)
        
        # Processing semaphores
        self.restaurant_semaphore = asyncio.Semaphore(self.config.max_concurrent_restaurants)
        self.review_semaphore = asyncio.Semaphore(self.config.max_concurrent_reviews)
        self.sentiment_semaphore = asyncio.Semaphore(self.config.max_concurrent_sentiment)
        
        # Statistics
        self.stats = {
            "restaurants_processed": 0,
            "dishes_extracted": 0,
            "reviews_processed": 0,
            "errors": 0,
            "processing_time": 0.0
        }
    
    async def collect_data_parallel(self, city: str, cuisine: str) -> Dict[str, Any]:
        """Collect data with parallel processing."""
        start_time = time.time()
        
        try:
            app_logger.info(f"🚀 Starting parallel data collection for {city}, {cuisine}")
            
            # Step 1: Collect restaurants in parallel
            restaurants = await self._collect_restaurants_parallel(city, cuisine)
            
            if not restaurants:
                app_logger.warning(f"No restaurants found for {city}, {cuisine}")
                return {"success": False, "error": "No restaurants found"}
            
            # Step 2: Process restaurants and extract dishes in parallel
            processed_data = await self._process_restaurants_parallel(restaurants)
            
            # Step 3: Store data in batches
            storage_result = await self._store_data_batches(processed_data)
            
            # Update statistics
            self.stats["processing_time"] = time.time() - start_time
            
            app_logger.info(f"✅ Parallel collection completed: {self.stats}")
            
            return {
                "success": True,
                "restaurants_processed": self.stats["restaurants_processed"],
                "dishes_extracted": self.stats["dishes_extracted"],
                "processing_time": self.stats["processing_time"],
                "storage_result": storage_result
            }
            
        except Exception as e:
            app_logger.error(f"❌ Error in parallel collection: {e}")
            self.stats["errors"] += 1
            return {"success": False, "error": str(e)}
    
    async def _collect_restaurants_parallel(self, city: str, cuisine: str) -> List[Dict]:
        """Collect restaurants with rate limiting and parallel processing."""
        app_logger.info(f"🔍 Collecting restaurants for {city}, {cuisine}")
        
        # Use rate limiter for SerpAPI calls
        await self.serpapi_limiter.acquire()
        
        try:
            restaurants = await self.serpapi_collector.search_restaurants(
                city=city,
                cuisine=cuisine,
                max_results=self.config.batch_size
            )
            
            app_logger.info(f"📝 Found {len(restaurants)} restaurants")
            return restaurants
            
        except Exception as e:
            app_logger.error(f"Error collecting restaurants: {e}")
            return []
    
    async def _process_restaurants_parallel(self, restaurants: List[Dict]) -> Dict[str, List]:
        """Process restaurants in parallel with streaming."""
        app_logger.info(f"🔄 Processing {len(restaurants)} restaurants in parallel")
        
        # Create tasks for parallel processing
        tasks = []
        for restaurant in restaurants:
            task = asyncio.create_task(
                self._process_single_restaurant(restaurant)
            )
            tasks.append(task)
        
        # Process in batches to avoid overwhelming the system
        batch_size = self.config.max_concurrent_restaurants
        all_restaurants = []
        all_dishes = []
        
        for i in range(0, len(tasks), batch_size):
            batch_tasks = tasks[i:i + batch_size]
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, Exception):
                    app_logger.error(f"Error processing restaurant: {result}")
                    self.stats["errors"] += 1
                    continue
                
                if result and result.get("restaurant"):
                    all_restaurants.append(result["restaurant"])
                    all_dishes.extend(result.get("dishes", []))
        
        self.stats["restaurants_processed"] = len(all_restaurants)
        self.stats["dishes_extracted"] = len(all_dishes)
        
        return {
            "restaurants": all_restaurants,
            "dishes": all_dishes
        }
    
    async def _process_single_restaurant(self, restaurant: Dict) -> Optional[Dict]:
        """Process a single restaurant with all its data."""
        async with self.restaurant_semaphore:
            try:
                # Ensure neighborhood field is present
                if 'neighborhood' not in restaurant:
                    restaurant['neighborhood'] = ""
                
                # Validate restaurant
                if not self.data_validator.validate_restaurant(restaurant):
                    app_logger.warning(f"Invalid restaurant data: {restaurant.get('restaurant_name', 'Unknown')}")
                    return None
                
                # Collect reviews in parallel
                reviews = await self._collect_reviews_parallel(restaurant)
                
                if not reviews:
                    app_logger.warning(f"No reviews found for {restaurant.get('restaurant_name', 'Unknown')}")
                    return {"restaurant": restaurant, "dishes": []}
                
                # Extract dishes in parallel
                dishes = await self._extract_dishes_parallel(reviews, restaurant["restaurant_id"])
                
                # Analyze sentiment in parallel
                dishes_with_sentiment = await self._analyze_sentiment_parallel(dishes, reviews)
                
                return {
                    "restaurant": restaurant,
                    "dishes": dishes_with_sentiment
                }
                
            except Exception as e:
                app_logger.error(f"Error processing restaurant {restaurant.get('restaurant_name', 'Unknown')}: {e}")
                return None
    
    async def _collect_reviews_parallel(self, restaurant: Dict) -> List[Dict]:
        """Collect reviews with rate limiting."""
        async with self.review_semaphore:
            await self.serpapi_limiter.acquire()
            
            try:
                reviews = await self.serpapi_collector.get_restaurant_reviews(
                    restaurant,  # Pass the full restaurant dictionary
                    self.settings.max_reviews_per_restaurant
                )
                
                # Validate reviews
                valid_reviews, _ = self.data_validator.validate_review_batch(reviews)
                return valid_reviews
                
            except Exception as e:
                app_logger.error(f"Error collecting reviews: {e}")
                return []
    
    async def _extract_dishes_parallel(self, reviews: List[Dict], restaurant_id: str) -> List[Dict]:
        """Extract dishes from reviews in parallel."""
        if not reviews:
            return []
        
        try:
            dishes = await self.dish_extractor.extract_dishes_from_reviews(reviews)
            
            # Add restaurant_id to dishes
            for dish in dishes:
                dish["restaurant_id"] = restaurant_id
            
            return dishes
            
        except Exception as e:
            app_logger.error(f"Error extracting dishes: {e}")
            return []
    
    async def _analyze_sentiment_parallel(self, dishes: List[Dict], reviews: List[Dict]) -> List[Dict]:
        """Analyze sentiment for dishes in parallel."""
        if not dishes:
            return []
        
        # Create tasks for parallel sentiment analysis
        tasks = []
        for dish in dishes:
            task = asyncio.create_task(
                self._analyze_single_dish_sentiment(dish, reviews)
            )
            tasks.append(task)
        
        # Process in batches
        batch_size = self.config.max_concurrent_sentiment
        results = []
        
        for i in range(0, len(tasks), batch_size):
            batch_tasks = tasks[i:i + batch_size]
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, Exception):
                    app_logger.error(f"Error analyzing sentiment: {result}")
                    continue
                if result:
                    results.append(result)
        
        return results
    
    async def _analyze_single_dish_sentiment(self, dish: Dict, reviews: List[Dict]) -> Optional[Dict]:
        """Analyze sentiment for a single dish."""
        async with self.sentiment_semaphore:
            await self.openai_limiter.acquire()
            
            try:
                sentiment = await self.sentiment_analyzer.analyze_dish_sentiment(
                    dish["dish_name"], reviews
                )
                
                dish.update(sentiment)
                return dish
                
            except Exception as e:
                app_logger.error(f"Error analyzing sentiment for {dish.get('dish_name', 'Unknown')}: {e}")
                return dish  # Return dish without sentiment
    
    async def _store_data_batches(self, data: Dict[str, List]) -> Dict[str, Any]:
        """Store data in optimized batches."""
        restaurants = data.get("restaurants", [])
        dishes = data.get("dishes", [])
        
        app_logger.info(f"💾 Storing {len(restaurants)} restaurants and {len(dishes)} dishes")
        
        try:
            # Store restaurants in batches
            restaurants_stored = 0
            if restaurants:
                batch_size = self.config.batch_size
                for i in range(0, len(restaurants), batch_size):
                    batch = restaurants[i:i + batch_size]
                    success = await self.milvus_client.insert_restaurants(batch)
                    if success:
                        restaurants_stored += len(batch)
            
            # Store dishes in batches
            dishes_stored = 0
            if dishes:
                batch_size = self.config.batch_size
                for i in range(0, len(dishes), batch_size):
                    batch = dishes[i:i + batch_size]
                    success = await self.milvus_client.insert_dishes(batch)
                    if success:
                        dishes_stored += len(batch)
            
            return {
                "restaurants_stored": restaurants_stored,
                "dishes_stored": dishes_stored
            }
            
        except Exception as e:
            app_logger.error(f"Error storing data: {e}")
            return {
                "restaurants_stored": 0,
                "dishes_stored": 0,
                "error": str(e)
            }
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get processing statistics."""
        return self.stats.copy()
