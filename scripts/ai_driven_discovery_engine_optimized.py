#!/usr/bin/env python3
"""
Optimized AI-Driven Discovery Engine - Incremental, Redis-Enhanced, Efficient
Builds on existing AI capabilities with incremental processing and advanced caching.
"""

import asyncio
import json
import math
from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from src.data_collection.serpapi_collector import SerpAPICollector
from src.processing.hybrid_dish_extractor import HybridDishExtractor
from src.processing.topics_hybrid_dish_extractor import TopicsHybridDishExtractor
from src.processing.sentiment_analyzer import SentimentAnalyzer
from src.vector_db.milvus_client import MilvusClient
from src.vector_db.discovery_collections import DiscoveryCollections
from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.data_collection.cache_manager import CacheManager
from openai import AsyncOpenAI


@dataclass
class DiscoveryCheckpoint:
    """Checkpoint data for incremental discovery."""
    city: str
    phase: str  # 'popular_dishes', 'famous_restaurants', 'neighborhood_analysis'
    timestamp: str
    processed_restaurants: List[str]
    processed_neighborhoods: List[str]
    last_restaurant_count: int
    last_dish_count: int
    status: str  # 'in_progress', 'completed', 'failed'


@dataclass
class DiscoveryStats:
    """Enhanced statistics tracking."""
    cities_processed: int = 0
    popular_dishes_found: int = 0
    famous_restaurants_discovered: int = 0
    neighborhood_restaurants_analyzed: int = 0
    total_dishes_extracted: int = 0
    ai_queries_made: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    incremental_updates: int = 0
    api_calls_saved: int = 0
    processing_time_seconds: float = 0.0
    # API call tracking
    serpapi_calls: int = 0
    openai_calls: int = 0
    yelp_calls: int = 0


class OptimizedAIDrivenDiscoveryEngine:
    """
    Optimized AI-Driven Discovery Engine with incremental processing and advanced caching.
    """
    
    def __init__(self):
        """Initialize the optimized AI-driven discovery engine."""
        # Get settings
        self.settings = get_settings()
        
        # Initialize components
        self.serpapi_collector = SerpAPICollector()
        self.hybrid_extractor = HybridDishExtractor()
        self.topics_extractor = TopicsHybridDishExtractor()
        self.openai_client = AsyncOpenAI(api_key=self.settings.openai_api_key)
        self.cache_manager = CacheManager()
        
        # Initialize discovery collections
        self.discovery_collections = DiscoveryCollections()
        
        # Statistics tracking
        self.stats = DiscoveryStats()
        
        # Configuration
        self.max_dishes_per_restaurant = 10
        self.max_restaurants_per_neighborhood = 20
        self.cache_ttl = 3600  # 1 hour
        
        # Cache keys
        self.cache_keys = {
            'restaurants': 'discovery:restaurants:{city}:{cuisine}',
            'dishes': 'discovery:dishes:{restaurant_id}',
            'ai_analysis': 'discovery:ai_analysis:{city}:{analysis_type}',
            'famous_restaurants': 'discovery:famous:{city}',
            'neighborhood_analysis': 'discovery:neighborhood:{city}:{neighborhood}',
            'checkpoint': 'discovery:checkpoint:{city}'
        }
        
        # Comprehensive cuisine-dish mapping with multi-word variations
        self.cuisine_dish_mapping = {
            'italian': {
                'base_dishes': ['pizza', 'pasta', 'lasagna', 'risotto', 'calzone', 'ravioli', 'gnocchi', 'osso buco'],
                'variations': {
                    'pizza': ['margherita pizza', 'pepperoni pizza', 'quattro formaggi pizza', 'diavola pizza', 'marinara pizza'],
                    'pasta': ['spaghetti carbonara', 'fettuccine alfredo', 'penne arrabiata', 'linguine vongole', 'rigatoni bolognese'],
                    'lasagna': ['beef lasagna', 'vegetarian lasagna', 'spinach lasagna', 'mushroom lasagna'],
                    'risotto': ['mushroom risotto', 'seafood risotto', 'saffron risotto', 'truffle risotto']
                },
                'search_terms': ['pizza', 'pasta', 'lasagna', 'risotto', 'calzone', 'ravioli', 'gnocchi', 'osso buco', 'tiramisu', 'cannoli']
            },
            'indian': {
                'base_dishes': ['biryani', 'curry', 'tandoori', 'naan', 'samosa', 'dal', 'roti', 'paratha', 'kebab', 'paneer'],
                'variations': {
                    'biryani': ['chicken biryani', 'mutton biryani', 'paneer biryani', 'vegetable biryani', 'lamb biryani', 'beef biryani'],
                    'curry': ['butter chicken', 'chicken tikka masala', 'dal makhani', 'palak paneer', 'chana masala', 'aloo gobi'],
                    'tandoori': ['tandoori chicken', 'tandoori fish', 'tandoori paneer', 'tandoori lamb'],
                    'naan': ['garlic naan', 'butter naan', 'cheese naan', 'plain naan'],
                    'samosa': ['vegetable samosa', 'chicken samosa', 'potato samosa']
                },
                'search_terms': ['biryani', 'curry', 'tandoori', 'naan', 'samosa', 'dal', 'roti', 'paratha', 'kebab', 'paneer', 'gulab jamun', 'rasmalai']
            },
            'chinese': {
                'base_dishes': ['dim sum', 'kung pao', 'sweet and sour', 'lo mein', 'fried rice', 'peking duck', 'mapo tofu', 'wonton'],
                'variations': {
                    'dim sum': ['shrimp dim sum', 'pork dim sum', 'vegetable dim sum', 'chicken dim sum'],
                    'kung pao': ['kung pao chicken', 'kung pao shrimp', 'kung pao tofu', 'kung pao beef'],
                    'sweet and sour': ['sweet and sour chicken', 'sweet and sour pork', 'sweet and sour fish'],
                    'lo mein': ['chicken lo mein', 'beef lo mein', 'shrimp lo mein', 'vegetable lo mein'],
                    'fried rice': ['chicken fried rice', 'beef fried rice', 'shrimp fried rice', 'vegetable fried rice'],
                    'peking duck': ['peking duck', 'roasted duck', 'crispy duck']
                },
                'search_terms': ['dim sum', 'kung pao', 'sweet and sour', 'lo mein', 'fried rice', 'peking duck', 'mapo tofu', 'wonton', 'egg roll', 'fortune cookie']
            },
            'american': {
                'base_dishes': ['burger', 'hot dog', 'cheesecake', 'steak', 'fries', 'sandwich', 'chicken wings', 'mac and cheese'],
                'variations': {
                    'burger': ['cheeseburger', 'bacon burger', 'veggie burger', 'turkey burger', 'salmon burger'],
                    'hot dog': ['chicago hot dog', 'new york hot dog', 'bacon wrapped hot dog'],
                    'steak': ['ribeye steak', 'filet mignon', 'strip steak', 'porterhouse steak'],
                    'sandwich': ['pastrami sandwich', 'corned beef sandwich', 'reuben sandwich', 'club sandwich'],
                    'chicken wings': ['buffalo wings', 'bbq wings', 'honey garlic wings', 'teriyaki wings']
                },
                'search_terms': ['burger', 'hot dog', 'cheesecake', 'steak', 'fries', 'sandwich', 'chicken wings', 'mac and cheese', 'apple pie', 'brownie']
            },
            'mexican': {
                'base_dishes': ['taco', 'burrito', 'enchilada', 'quesadilla', 'guacamole', 'fajita', 'tamale', 'churro'],
                'variations': {
                    'taco': ['fish taco', 'chicken taco', 'beef taco', 'pork taco', 'vegetable taco'],
                    'burrito': ['chicken burrito', 'beef burrito', 'vegetable burrito', 'shrimp burrito'],
                    'enchilada': ['chicken enchilada', 'beef enchilada', 'cheese enchilada', 'vegetable enchilada'],
                    'quesadilla': ['chicken quesadilla', 'beef quesadilla', 'cheese quesadilla', 'vegetable quesadilla'],
                    'fajita': ['chicken fajita', 'beef fajita', 'shrimp fajita', 'vegetable fajita']
                },
                'search_terms': ['taco', 'burrito', 'enchilada', 'quesadilla', 'guacamole', 'fajita', 'tamale', 'churro', 'salsa', 'margarita']
            },
            'thai': {
                'base_dishes': ['pad thai', 'green curry', 'red curry', 'tom yum', 'pad see ew', 'mango sticky rice'],
                'variations': {
                    'pad thai': ['chicken pad thai', 'shrimp pad thai', 'tofu pad thai', 'vegetable pad thai'],
                    'green curry': ['chicken green curry', 'beef green curry', 'shrimp green curry', 'vegetable green curry'],
                    'red curry': ['chicken red curry', 'beef red curry', 'shrimp red curry', 'vegetable red curry'],
                    'tom yum': ['tom yum soup', 'tom yum goong', 'tom yum gai']
                },
                'search_terms': ['pad thai', 'green curry', 'red curry', 'tom yum', 'pad see ew', 'mango sticky rice', 'thai iced tea']
            },
            'japanese': {
                'base_dishes': ['sushi', 'ramen', 'tempura', 'teriyaki', 'udon', 'bento', 'miso soup'],
                'variations': {
                    'sushi': ['california roll', 'spicy tuna roll', 'salmon roll', 'dragon roll', 'philadelphia roll'],
                    'ramen': ['tonkotsu ramen', 'miso ramen', 'shoyu ramen', 'vegetable ramen'],
                    'tempura': ['shrimp tempura', 'vegetable tempura', 'chicken tempura'],
                    'teriyaki': ['chicken teriyaki', 'beef teriyaki', 'salmon teriyaki']
                },
                'search_terms': ['sushi', 'ramen', 'tempura', 'teriyaki', 'udon', 'bento', 'miso soup', 'green tea']
            },
            'mediterranean': {
                'base_dishes': ['falafel', 'hummus', 'shawarma', 'kebab', 'tabbouleh', 'baklava'],
                'variations': {
                    'falafel': ['falafel wrap', 'falafel plate', 'falafel sandwich'],
                    'shawarma': ['chicken shawarma', 'beef shawarma', 'lamb shawarma'],
                    'kebab': ['chicken kebab', 'beef kebab', 'lamb kebab', 'vegetable kebab'],
                    'hummus': ['classic hummus', 'roasted red pepper hummus', 'garlic hummus']
                },
                'search_terms': ['falafel', 'hummus', 'shawarma', 'kebab', 'tabbouleh', 'baklava', 'pita bread']
            }
        }
        
        # Supported cities and cuisines
        self.supported_cities = ["Manhattan", "Jersey City", "Hoboken"]
        self.supported_cuisines = ["Italian", "Indian", "Chinese", "American", "Mexican"]
        
        # Top neighborhoods per city
        self.top_neighborhoods = {
            "Manhattan": ["Times Square", "SoHo", "Chelsea", "Upper East Side", "Lower East Side"],
            "Jersey City": ["Downtown JC", "Journal Square", "The Heights", "Newport", "Grove Street"],
            "Hoboken": ["Washington Street", "Downtown Hoboken", "Uptown Hoboken", "Midtown Hoboken", "Hoboken Waterfront"]
        }
        
        # Enhanced statistics
        self.stats = DiscoveryStats()
        
        # Cache keys for Redis
        self.cache_keys = {
            'restaurants': 'discovery:restaurants:{city}',
            'dishes': 'discovery:dishes:{restaurant_id}',
            'popular_dishes': 'discovery:popular_dishes:{city}',
            'famous_restaurants': 'discovery:famous_restaurants:{city}',
            'neighborhood_analysis': 'discovery:neighborhood:{city}:{neighborhood}:{cuisine}',
            'checkpoint': 'discovery:checkpoint:{city}',
            'last_update': 'discovery:last_update:{city}',
            'ai_analysis': 'discovery:ai_analysis:{city}:{analysis_type}'
        }
        
        # Processing limits for efficiency
        self.max_restaurants_per_city = 30
        self.max_dishes_per_restaurant = 6  # Updated to 6 dishes per restaurant
        self.cache_ttl = 3600 * 24 * 7  # 7 days
        self.checkpoint_ttl = 3600 * 24 * 30  # 30 days
        
        # Quality filters for neighborhood analysis
        self.min_review_count_high = 1000  # For rating > 4.2
        self.min_rating_high = 4.2
        self.min_review_count_medium = 700  # For rating > 4.4
        self.min_rating_medium = 4.4
        
        # Famous restaurants limit
        self.max_famous_restaurants = 15  # Allow up to 15 restaurants (3 per dish for 5 dishes)
    
    async def run_incremental_discovery(self, 
                                      cities: Optional[List[str]] = None,
                                      since_timestamp: Optional[str] = None,
                                      force_full: bool = False) -> Dict[str, Any]:
        """
        Run incremental AI-driven discovery with Redis caching and checkpointing.
        
        Args:
            cities: List of cities to process
            since_timestamp: Only process data since this timestamp
            force_full: Force full reprocessing ignoring cache
            
        Returns:
            Dictionary with discovery results
        """
        if cities is None:
            cities = self.supported_cities
        
        start_time = datetime.now()
        app_logger.info(f"🚀 Starting Optimized AI-Driven Discovery for cities: {cities}")
        print(f"🚀 OPTIMIZED AI-DRIVEN DISCOVERY ENGINE")
        print("=" * 60)
        print(f"📅 Since: {since_timestamp or 'Beginning of time'}")
        print(f"🔄 Mode: {'Full' if force_full else 'Incremental'}")
        
        all_results = {}
        
        for city in cities:
            if city not in self.supported_cities:
                app_logger.warning(f"⚠️ City {city} not supported, skipping")
                continue
            
            print(f"\n🏙️ PROCESSING: {city.upper()}")
            print("-" * 40)
            
            try:
                # Check if we can resume from checkpoint
                checkpoint = await self._load_checkpoint(city)
                if checkpoint and not force_full:
                    print(f"📋 Resuming from checkpoint: {checkpoint.phase}")
                    city_results = await self._resume_from_checkpoint(city, checkpoint, since_timestamp)
                else:
                    city_results = await self._discover_city_data_incremental(city, since_timestamp, force_full)
                
                all_results[city] = city_results
                self.stats.cities_processed += 1
                
                print(f"✅ {city}: {len(city_results.get('popular_dishes', []))} popular dishes, "
                      f"{len(city_results.get('famous_restaurants', []))} famous restaurants")
                
            except Exception as e:
                app_logger.error(f"❌ Error processing {city}: {e}")
                print(f"❌ {city}: Error - {e}")
                await self._save_checkpoint(city, 'failed', str(e))
        
        # Results are now saved incrementally after each phase
        # No need for final save since data is already in Milvus
        
        # Calculate processing time
        self.stats.processing_time_seconds = (datetime.now() - start_time).total_seconds()
        
        # Print enhanced statistics
        self._print_enhanced_stats()
        
        return all_results
    
    async def _discover_city_data_incremental(self, city: str, since_timestamp: str = None, force_full: bool = False) -> Dict[str, Any]:
        """Discover city data with incremental processing."""
        
        # Check cache first
        cache_key = self.cache_keys['popular_dishes'].format(city=city)
        cached_results = await self.cache_manager.get(cache_key)
        
        if cached_results and not force_full:
            self.stats.cache_hits += 1
            print(f"📋 Using cached results for {city}")
            return cached_results
        
        self.stats.cache_misses += 1
        
        print(f"\n📋 PHASE 1: POPULAR DISHES → FAMOUS RESTAURANTS")
        print("-" * 50)
        
        # Phase 1: Discover popular dishes
        await self._save_checkpoint(city, 'popular_dishes', 'in_progress')
        popular_dishes = await self._discover_popular_dishes_incremental(city, since_timestamp)
        print(f"🍽️ Found {len(popular_dishes)} popular dishes")
        
        # Save Phase 1 popular dishes immediately
        await self._save_phase1_popular_dishes(popular_dishes, city)
        
        # Phase 1: Discover famous restaurants
        await self._save_checkpoint(city, 'famous_restaurants', 'in_progress')
        famous_restaurants = await self._discover_famous_restaurants_incremental(city, popular_dishes, since_timestamp)
        print(f"🏆 Found {len(famous_restaurants)} famous restaurants")
        
        # Save Phase 1 famous restaurants immediately
        await self._save_phase1_famous_restaurants(famous_restaurants)
        
        print(f"\n🏘️ PHASE 2: NEIGHBORHOODS + CUISINES → TOP RESTAURANTS + DISHES")
        print("-" * 60)
        
        # Phase 2: Analyze neighborhoods and cuisines
        await self._save_checkpoint(city, 'neighborhood_analysis', 'in_progress')
        neighborhood_analysis = await self._analyze_neighborhoods_incremental(city, since_timestamp)
        print(f"🏘️ Analyzed {len(neighborhood_analysis)} neighborhood-cuisine combinations")
        
        # Save Phase 2 neighborhood analysis immediately
        await self._save_phase2_neighborhood_analysis(neighborhood_analysis)
        
        results = {
            'popular_dishes': popular_dishes,
            'famous_restaurants': famous_restaurants,
            'neighborhood_analysis': neighborhood_analysis,
            'city': city,
            'discovery_timestamp': datetime.now().isoformat(),
            'incremental': not force_full,
            'since_timestamp': since_timestamp
        }
        
        # Cache results
        await self.cache_manager.set(cache_key, results, expire=self.cache_ttl)
        await self._save_checkpoint(city, 'completed', 'completed')
        
        return results
    
    async def _discover_popular_dishes_incremental(self, city: str, since_timestamp: str = None) -> List[Dict[str, Any]]:
        """Discover popular dishes using OpenAI directly."""
        
        # Check AI analysis cache
        cache_key = self.cache_keys['ai_analysis'].format(city=city, analysis_type='popular_dishes')
        cached_analysis = await self.cache_manager.get(cache_key)
        
        if cached_analysis:
            self.stats.cache_hits += 1
            return cached_analysis
        
        self.stats.cache_misses += 1
        
        # Use OpenAI to discover popular dishes directly
        popular_dishes = await self._ai_discover_popular_dishes(city)
        
        # Cache results
        await self.cache_manager.set(cache_key, popular_dishes, expire=self.cache_ttl)
        
        self.stats.popular_dishes_found += len(popular_dishes)
        return popular_dishes
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def _search_neighborhood_restaurants_hybrid(self, city: str, neighborhood: str, cuisine: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search for restaurants using Google Reviews API as main engine, Yelp for enhancement."""
        
        try:
            # Step 1: Get restaurants from Google Reviews API (main source)
            google_restaurants = await self._get_google_restaurants_with_fallback(city, neighborhood, cuisine, limit)
            
            # Step 2: Get restaurants from Yelp API (enhancement)
            yelp_restaurants = await self._get_yelp_restaurants_with_fallback(city, neighborhood, cuisine, limit)
            
            # Step 3: Merge and deduplicate restaurants
            all_restaurants = []
            seen_names = set()
            
            # Add Google restaurants first (main source)
            for restaurant in google_restaurants:
                restaurant_name = restaurant.get('restaurant_name', '').lower()
                if restaurant_name and restaurant_name not in seen_names:
                    restaurant['source'] = 'google'
                    all_restaurants.append(restaurant)
                    seen_names.add(restaurant_name)
            
            # Add Yelp restaurants (enhancement)
            for restaurant in yelp_restaurants:
                restaurant_name = restaurant.get('restaurant_name', '').lower()
                if restaurant_name and restaurant_name not in seen_names:
                    restaurant['source'] = 'yelp'
                    all_restaurants.append(restaurant)
                    seen_names.add(restaurant_name)
            
            # Step 4: Calculate quality scores for each restaurant BEFORE hybrid scoring
            for restaurant in all_restaurants:
                # Calculate individual quality score first
                rating = restaurant.get('rating', 0)
                review_count = restaurant.get('review_count', 0)
                import math
                quality_score = rating * math.log(review_count + 1)
                restaurant['quality_score'] = quality_score
            
            # Step 5: Calculate hybrid quality scores and sort
            for restaurant in all_restaurants:
                # Get the source-specific quality scores
                google_data = None
                yelp_data = None
                
                if restaurant.get('source') == 'google':
                    google_data = restaurant
                elif restaurant.get('source') == 'yelp':
                    yelp_data = restaurant
                
                # Calculate hybrid quality score using SerpAPI collector's method
                hybrid_score = self.serpapi_collector.calculate_hybrid_quality_score(google_data, yelp_data)
                restaurant['hybrid_quality_score'] = hybrid_score
            
            # Sort by hybrid quality score and return top results
            all_restaurants.sort(key=lambda x: x.get('hybrid_quality_score', 0), reverse=True)
            
            return all_restaurants[:limit]
            
        except Exception as e:
            app_logger.error(f"Error in hybrid restaurant search: {e}")
            # Fallback to Google-only search
            try:
                app_logger.info(f"🔄 Fallback: Using Google-only search for {neighborhood}, {city}")
                google_restaurants = await self.serpapi_collector.search_restaurants(
                    city=f"{neighborhood}, {city}",
                    cuisine=cuisine,
                    max_results=limit
                )
                return google_restaurants[:limit] if google_restaurants else []
            except Exception as fallback_error:
                app_logger.error(f"Fallback also failed: {fallback_error}")
                return []
    
    async def _get_google_restaurants_with_fallback(self, city: str, neighborhood: str, cuisine: str, limit: int) -> List[Dict[str, Any]]:
        """Get Google restaurants with retry and fallback logic."""
        try:
            return await self.serpapi_collector.search_restaurants(
                city=f"{neighborhood}, {city}",
                cuisine=cuisine,
                max_results=limit
            )
        except Exception as e:
            app_logger.warning(f"Google search failed for {neighborhood}, {city}: {e}")
            # Fallback: try without neighborhood
            try:
                app_logger.info(f"🔄 Google fallback: Searching {city} without neighborhood")
                return await self.serpapi_collector.search_restaurants(
                    city=city,
                    cuisine=cuisine,
                    max_results=limit
                )
            except Exception as fallback_error:
                app_logger.error(f"Google fallback also failed: {fallback_error}")
                return []

    async def _get_yelp_restaurants_with_fallback(self, city: str, neighborhood: str, cuisine: str, limit: int) -> List[Dict[str, Any]]:
        """Get Yelp restaurants with retry and fallback logic."""
        try:
            return await self.serpapi_collector._search_yelp_restaurants(
                city=f"{neighborhood}, {city}",
                cuisine=cuisine,
                limit=limit
            )
        except Exception as e:
            app_logger.warning(f"Yelp search failed for {neighborhood}, {city}: {e}")
            # Fallback: try without neighborhood
            try:
                app_logger.info(f"🔄 Yelp fallback: Searching {city} without neighborhood")
                return await self.serpapi_collector._search_yelp_restaurants(
                    city=city,
                    cuisine=cuisine,
                    limit=limit
                )
            except Exception as fallback_error:
                app_logger.error(f"Yelp fallback also failed: {fallback_error}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=6))
    async def _extract_restaurant_dishes_advanced(self, restaurant: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract dishes using topics + sentiment analysis (advanced approach)."""
        
        try:
            restaurant_id = restaurant.get('restaurant_id', '')
            restaurant_name = restaurant.get('restaurant_name', '')
            cuisine_type = restaurant.get('cuisine_type', '')
            neighborhood = restaurant.get('neighborhood', '')
            city = restaurant.get('city', '')
            
            # Get restaurant details with reviews and topics
            restaurant_details = await self._get_restaurant_details_with_fallback(restaurant)
            
            if not restaurant_details or not restaurant_details.get('reviews'):
                # Fallback to simple extraction if no review data
                app_logger.info(f"🔄 Fallback: No review data for {restaurant_name}, using simple extraction")
                return await self._extract_restaurant_dishes_simple(restaurant)
            
            # Use topics + sentiment analysis
            dishes = await self.topics_extractor.extract_dishes_hybrid(restaurant_details)
            
            # Add metadata for compatibility
            for dish in dishes:
                dish.update({
                    'restaurant_id': restaurant_id,
                    'restaurant_name': restaurant_name,
                    'cuisine_type': cuisine_type,
                    'neighborhood': neighborhood,
                    'city': city
                })
            
            return dishes
            
        except Exception as e:
            app_logger.error(f"Error in advanced dish extraction: {e}")
            # Fallback to simple extraction
            return await self._extract_restaurant_dishes_simple(restaurant)

    async def _get_restaurant_details_with_fallback(self, restaurant: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Get restaurant details with retry and fallback logic."""
        try:
            # Use reviews_data_id if available, otherwise fall back to restaurant_id
            data_id = restaurant.get('reviews_data_id') or restaurant.get('restaurant_id', '')
            if not data_id:
                app_logger.warning(f"No data_id available for restaurant {restaurant.get('restaurant_name', 'Unknown')}")
                return None
                
            return await self.serpapi_collector.get_place_details(data_id)
        except Exception as e:
            app_logger.warning(f"Failed to get restaurant details for {restaurant.get('restaurant_name', 'Unknown')}: {e}")
            return None

    async def _extract_restaurant_dishes_simple(self, restaurant: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract dishes using old system approach (manual + comprehensive)."""
        
        try:
            restaurant_id = restaurant.get('restaurant_id', '')
            restaurant_name = restaurant.get('restaurant_name', '')
            cuisine_type = restaurant.get('cuisine_type', '')
            neighborhood = restaurant.get('neighborhood', '')
            city = restaurant.get('city', '')
            
            dishes = []
            
            # Define comprehensive cuisine dishes (from old system)
            cuisine_dishes = {
                'italian': [
                    {"name": "Margherita Pizza", "search_terms": ["margherita pizza", "margherita"], "category": "main"},
                    {"name": "Spaghetti Carbonara", "search_terms": ["carbonara", "spaghetti carbonara"], "category": "main"},
                    {"name": "Lasagna", "search_terms": ["lasagna", "lasagne"], "category": "main"},
                    {"name": "Italian Sub", "search_terms": ["italian sub", "italian sandwich"], "category": "main"},
                    {"name": "Bruschetta", "search_terms": ["bruschetta"], "category": "appetizer"},
                    {"name": "Tiramisu", "search_terms": ["tiramisu"], "category": "dessert"}
                ],
                'american': [
                    {"name": "Cheeseburger", "search_terms": ["cheeseburger", "burger"], "category": "main"},
                    {"name": "Chicken Wings", "search_terms": ["wings", "chicken wings"], "category": "main"},
                    {"name": "Mac and Cheese", "search_terms": ["mac and cheese", "macaroni"], "category": "main"},
                    {"name": "BBQ Ribs", "search_terms": ["ribs", "bbq ribs"], "category": "main"},
                    {"name": "Bar Food", "search_terms": ["bar food", "pub food"], "category": "main"},
                    {"name": "Hot Dog", "search_terms": ["hot dog", "hotdog"], "category": "main"}
                ],
                'indian': [
                    {"name": "Butter Chicken", "search_terms": ["butter chicken", "murgh makhani"], "category": "main"},
                    {"name": "Tikka Masala", "search_terms": ["tikka masala", "chicken tikka"], "category": "main"},
                    {"name": "Biryani", "search_terms": ["biryani", "biryani rice"], "category": "main"},
                    {"name": "Naan", "search_terms": ["naan", "garlic naan"], "category": "bread"},
                    {"name": "Samosas", "search_terms": ["samosa", "samosas"], "category": "appetizer"}
                ],
                'mexican': [
                    {"name": "Tacos", "search_terms": ["taco", "tacos"], "category": "main"},
                    {"name": "Guacamole", "search_terms": ["guacamole", "guac"], "category": "appetizer"},
                    {"name": "Quesadillas", "search_terms": ["quesadilla", "quesadillas"], "category": "main"},
                    {"name": "Enchiladas", "search_terms": ["enchilada", "enchiladas"], "category": "main"},
                    {"name": "Burritos", "search_terms": ["burrito", "burritos"], "category": "main"}
                ],
                'chinese': [
                    {"name": "Dim Sum", "search_terms": ["dim sum", "dimsum"], "category": "main"},
                    {"name": "Kung Pao Chicken", "search_terms": ["kung pao chicken", "kung pao"], "category": "main"},
                    {"name": "Lo Mein", "search_terms": ["lo mein", "lo mein noodles"], "category": "main"},
                    {"name": "Peking Duck", "search_terms": ["peking duck", "peking duck"], "category": "main"},
                    {"name": "Wonton Soup", "search_terms": ["wonton soup", "wonton"], "category": "main"}
                ]
            }
            
            # Manual dish extraction based on cuisine (old system approach)
            cuisine_lower = cuisine_type.lower()
            if cuisine_lower in cuisine_dishes:
                for dish_info in cuisine_dishes[cuisine_lower]:
                    dish_name = dish_info['name']
                    
                    # Create comprehensive dish entry (old system format)
                    dish = {
                        "dish_name": dish_name,
                        "restaurant_id": restaurant_id,
                        "restaurant_name": restaurant_name,
                        "cuisine_type": cuisine_type,
                        "category": dish_info['category'],
                        "neighborhood": neighborhood,
                        "city": city,
                        "sentiment_score": 0.7,  # Default positive score
                        "recommendation_score": 0.8,  # Default high recommendation
                        "mention_count": 1,  # Default mention count
                        "confidence_score": 0.9,
                        "search_terms": dish_info['search_terms'],
                        "final_score": 0.8  # For compatibility with current system
                    }
                    
                    dishes.append(dish)
            
            return dishes
            
        except Exception as e:
            app_logger.error(f"Error in dish extraction (old system): {e}")
            return []
    
    # Removed unused methods: _get_restaurants_incremental, _extract_dishes_batch, _extract_restaurant_dishes_cached
    # These are no longer needed since we use OpenAI directly for popular dishes and famous restaurants
    
    def _merge_and_normalize_dishes(self, dishes: List[Dict[str, Any]], cuisine_type: str) -> List[Dict[str, Any]]:
        """Merge and normalize dishes, handling multi-word variations."""
        normalized_dishes = {}
        
        for dish in dishes:
            dish_name = dish.get('dish_name', '')
            if not dish_name:
                continue
            
            # Normalize dish name
            normalized_name = self._normalize_dish_name(dish_name, cuisine_type)
            
            # Create unique key for deduplication
            dish_key = f"{normalized_name}_{cuisine_type}"
            
            if dish_key not in normalized_dishes:
                # First occurrence of this dish
                dish['normalized_name'] = normalized_name
                dish['dish_key'] = dish_key
                normalized_dishes[dish_key] = dish
            else:
                # Merge with existing dish
                existing_dish = normalized_dishes[dish_key]
                
                # Keep the more specific dish name if available
                if len(dish_name) > len(existing_dish.get('dish_name', '')):
                    existing_dish['dish_name'] = dish_name
                
                # Merge confidence scores
                existing_confidence = existing_dish.get('confidence', 0.0)
                new_confidence = dish.get('confidence', 0.0)
                existing_dish['confidence'] = max(existing_confidence, new_confidence)
                
                # Merge sources
                existing_source = existing_dish.get('source', '')
                new_source = dish.get('source', '')
                if new_source and new_source not in existing_source:
                    existing_dish['source'] = f"{existing_source}+{new_source}" if existing_source else new_source
                
                # Merge review snippets
                existing_snippet = existing_dish.get('review_snippet', '')
                new_snippet = dish.get('review_snippet', '')
                if new_snippet and new_snippet not in existing_snippet:
                    existing_dish['review_snippet'] = f"{existing_snippet}; {new_snippet}" if existing_snippet else new_snippet
        
        # Sort by confidence and return
        sorted_dishes = list(normalized_dishes.values())
        sorted_dishes.sort(key=lambda x: x.get('confidence', 0.0), reverse=True)
        
        return sorted_dishes
    
    async def _enhance_restaurants_parallel(self, restaurants: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Enhance restaurants with SerpAPI data in parallel."""
        enhanced_restaurants = []
        
        # Process in parallel with rate limiting
        semaphore = asyncio.Semaphore(3)  # Limit concurrent API calls
        
        async def enhance_restaurant(restaurant):
            async with semaphore:
                try:
                    place_details = await self.serpapi_collector.get_place_details(
                        restaurant.get('restaurant_id', '')
                    )
                    
                    if place_details:
                        restaurant.update(place_details)
                    
                    restaurant['last_updated'] = datetime.now().isoformat()
                    return restaurant
                    
                except Exception as e:
                    app_logger.error(f"Error enhancing restaurant {restaurant.get('name', '')}: {e}")
                    restaurant['last_updated'] = datetime.now().isoformat()
                    return restaurant
        
        # Process restaurants in parallel
        tasks = [enhance_restaurant(restaurant) for restaurant in restaurants]
        enhanced_restaurants = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions
        enhanced_restaurants = [r for r in enhanced_restaurants if not isinstance(r, Exception)]
        
        return enhanced_restaurants
    
    # Removed _ai_analyze_popular_dishes_cached method - replaced with _ai_discover_popular_dishes
    
    async def _discover_famous_restaurants_incremental(self, city: str, popular_dishes: List[Dict], since_timestamp: str = None) -> List[Dict[str, Any]]:
        """Discover famous restaurants using OpenAI instead of Yelp."""
        
        # Check cache
        cache_key = self.cache_keys['famous_restaurants'].format(city=city)
        cached_restaurants = await self.cache_manager.get(cache_key)
        
        if cached_restaurants:
            self.stats.cache_hits += 1
            return cached_restaurants
        
        self.stats.cache_misses += 1
        
        try:
            # Use OpenAI to find famous restaurants for the city
            famous_restaurants = await self._ai_discover_famous_restaurants(city, popular_dishes)
            
            # Sort by fame score
            famous_restaurants.sort(key=lambda x: x.get('fame_score', 0), reverse=True)
            
            # Cache results
            await self.cache_manager.set(cache_key, famous_restaurants, expire=self.cache_ttl)
            
            self.stats.famous_restaurants_discovered += len(famous_restaurants)
            return famous_restaurants[:self.max_famous_restaurants]  # Return top 3 only
            
        except Exception as e:
            app_logger.error(f"Error discovering famous restaurants for {city}: {e}")
            return []
    
    async def _ai_discover_popular_dishes(self, city: str) -> List[Dict[str, Any]]:
        """Use OpenAI to discover popular dishes for a city."""
        
        try:
            prompt = f"""
You are an expert food analyst for {city}. Based on your knowledge of {city}'s culinary scene, identify the most popular dishes.

CITY: {city}

TASK: Identify the top 5 most popular dishes in {city} based on:
1. Cultural significance and local popularity
2. Historical importance to the city
3. Media mentions and tourist appeal
4. Local food culture and traditions
5. Restaurant prevalence and demand

For each dish, provide:
- Exact dish name
- Cultural significance
- Why it's popular in this city
- Typical restaurants that serve it

Return a JSON array with this exact structure:
[
    {{
        "dish_name": "string",
        "popularity_score": float (0.8-1.0),
        "frequency": int (estimated mentions across restaurants),
        "avg_sentiment": float (0.7-1.0),
        "cultural_significance": "string",
        "top_restaurants": ["array of restaurant names"],
        "reasoning": "string"
    }}
]

Focus on dishes that are:
- Iconic to {city}
- Frequently mentioned in food guides
- Have significant cultural impact
- Are tourist favorites
- Represent local food culture

For {city}, consider dishes like:
- New York Pizza, Pastrami Sandwich, Bagel with Lox (Manhattan)
- Italian Sub, Pizza, Deli Sandwiches (Jersey City)
- Pizza, Italian Food, Deli Sandwiches (Hoboken)
"""
            
            response = await self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a food expert specializing in city-specific cuisine analysis. Always respond with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=2000,
                timeout=30.0  # 30 second timeout
            )
            self._track_api_call('openai')
            
            result = response.choices[0].message.content.strip()
            
            # Debug logging
            app_logger.info(f"OpenAI response for popular dishes: {result[:200]}...")
            
            # Validate response
            if not result or result.strip() == "":
                app_logger.error("OpenAI returned empty response")
                return []
            
            try:
                # Handle markdown-wrapped JSON responses
                if result.startswith('```json'):
                    # Extract JSON from markdown code blocks
                    json_start = result.find('[')
                    json_end = result.rfind(']') + 1
                    if json_start != -1 and json_end != 0:
                        result = result[json_start:json_end]
                elif result.startswith('```'):
                    # Handle other markdown code blocks
                    lines = result.split('\n')
                    json_lines = []
                    in_json = False
                    for line in lines:
                        if line.strip().startswith('```'):
                            if not in_json:
                                in_json = True
                            else:
                                break
                        elif in_json:
                            json_lines.append(line)
                    result = '\n'.join(json_lines)
                
                popular_dishes = json.loads(result)
            except json.JSONDecodeError as e:
                app_logger.error(f"Failed to parse OpenAI JSON response: {e}")
                app_logger.error(f"Response content: {result}")
                return []
            
            self.stats.ai_queries_made += 1
            return popular_dishes
            
        except Exception as e:
            app_logger.error(f"Error in AI discovery of popular dishes: {e}")
            return []

    async def _ai_discover_famous_restaurants(self, city: str, popular_dishes: List[Dict]) -> List[Dict[str, Any]]:
        """Use OpenAI to discover famous restaurants for a city."""
        
        try:
            # Prepare popular dishes summary for context
            dishes_summary = []
            for dish in popular_dishes[:5]:  # Top 5 dishes for context
                dishes_summary.append({
                    "dish_name": dish.get('dish_name', ''),
                    "popularity_score": dish.get('popularity_score', 0.0),
                    "cultural_significance": dish.get('cultural_significance', '')
                })
            
            prompt = f"""
You are an expert food critic and restaurant historian for {city}. Based on the popular dishes and your knowledge of {city}'s culinary scene, identify the most famous restaurants.

CITY: {city}
POPULAR DISHES: {json.dumps(dishes_summary, indent=2)}

TASK: Identify the top 5 most famous restaurants in {city} that are known for:
1. High ratings (4.5+ stars typically)
2. Large number of reviews (1000+ typically)
3. Cultural significance and historical importance
4. Being featured in media, guidebooks, or food shows
5. Serving the popular dishes identified above

For each restaurant, provide:
- Exact restaurant name
- What they're famous for (specific dish or cuisine)
- Estimated rating (4.0-5.0)
- Estimated review count (1000-10000+)
- Neighborhood/location
- Brief reason for fame

Return a JSON array with this exact structure:
[
    {{
        "restaurant_name": "string",
        "famous_for": "string (specific dish or cuisine)",
        "estimated_rating": float (4.0-5.0),
        "estimated_review_count": int (1000+),
        "neighborhood": "string",
        "address": "string (if known)",
        "reason_for_fame": "string",
        "cuisine_type": "string"
    }}
]

Focus on restaurants that are:
- Iconic and well-known
- Have stood the test of time
- Are frequently mentioned in food guides
- Have significant cultural impact
- Serve the popular dishes identified above

For {city}, consider restaurants like:
- Joe's Pizza, Katz's Delicatessen, Russ & Daughters (Manhattan)
- Razza, Ani Ramen, Porta (Jersey City)
- Carlo's Bakery, Fiore's, La Isla (Hoboken)
"""
            
            response = await self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a food expert specializing in famous restaurants. Always respond with valid JSON. Be specific and accurate with restaurant names and details."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=2000,
                timeout=30.0  # 30 second timeout
            )
            self._track_api_call('openai')
            
            result = response.choices[0].message.content.strip()
            
            # Debug logging
            app_logger.info(f"OpenAI response for famous restaurants: {result[:200]}...")
            
            # Validate response
            if not result or result.strip() == "":
                app_logger.error("OpenAI returned empty response for famous restaurants")
                return []
            
            try:
                # Handle markdown-wrapped JSON responses
                if result.startswith('```json'):
                    # Extract JSON from markdown code blocks
                    json_start = result.find('[')
                    json_end = result.rfind(']') + 1
                    if json_start != -1 and json_end != 0:
                        result = result[json_start:json_end]
                elif result.startswith('```'):
                    # Handle other markdown code blocks
                    lines = result.split('\n')
                    json_lines = []
                    in_json = False
                    for line in lines:
                        if line.strip().startswith('```'):
                            if not in_json:
                                in_json = True
                            else:
                                break
                        elif in_json:
                            json_lines.append(line)
                    result = '\n'.join(json_lines)
                
                ai_restaurants = json.loads(result)
            except json.JSONDecodeError as e:
                app_logger.error(f"Failed to parse OpenAI JSON response for famous restaurants: {e}")
                app_logger.error(f"Response content: {result}")
                return []
            
            # Convert to our standard format
            famous_restaurants = []
            for ai_restaurant in ai_restaurants:
                # Calculate fame score based on AI-provided data
                fame_score = self._calculate_ai_fame_score(ai_restaurant)
                
                famous_restaurant = {
                    'restaurant_id': f"ai_{ai_restaurant['restaurant_name'].lower().replace(' ', '_').replace('\'', '')}",
                    'restaurant_name': ai_restaurant['restaurant_name'],
                    'city': city,
                    'cuisine_type': ai_restaurant.get('cuisine_type', ''),
                    'rating': ai_restaurant.get('estimated_rating', 4.5),
                    'review_count': ai_restaurant.get('estimated_review_count', 1000),
                    'famous_dish': ai_restaurant.get('famous_for', ''),
                    'fame_score': fame_score,
                    'dish_popularity': 0.9,  # High popularity for AI-suggested restaurants
                    'location': ai_restaurant.get('address', ''),
                    'neighborhood': ai_restaurant.get('neighborhood', ''),
                    'discovery_method': 'ai_driven',
                    'reason_for_fame': ai_restaurant.get('reason_for_fame', ''),
                    'ai_generated': True
                }
                
                famous_restaurants.append(famous_restaurant)
            
            self.stats.ai_queries_made += 1
            return famous_restaurants
            
        except Exception as e:
            app_logger.error(f"Error in AI discovery of famous restaurants: {e}")
            return []
    
    def _calculate_ai_fame_score(self, ai_restaurant: Dict[str, Any]) -> float:
        """Calculate fame score for AI-discovered restaurants."""
        try:
            rating = ai_restaurant.get('estimated_rating', 4.5)
            review_count = ai_restaurant.get('estimated_review_count', 1000)
            
            # Normalize metrics
            rating_score = rating / 5.0
            review_score = min(review_count / 5000.0, 1.0)  # Cap at 5000 reviews
            
            # Name fame indicators (same as before)
            name_fame = self._calculate_name_fame(ai_restaurant.get('restaurant_name', ''))
            
            # Reason for fame analysis
            reason = ai_restaurant.get('reason_for_fame', '').lower()
            fame_indicators = ['iconic', 'famous', 'legendary', 'historic', 'award-winning', 'celebrity', 'media']
            fame_bonus = sum(1 for indicator in fame_indicators if indicator in reason) * 0.1
            
            # Weighted combination
            fame_score = (
                rating_score * 0.3 +
                review_score * 0.2 +
                name_fame * 0.3 +
                fame_bonus * 0.2
            )
            
            return min(fame_score, 1.0)
            
        except Exception as e:
            app_logger.error(f"Error calculating AI fame score: {e}")
            return 0.7  # Default high score for AI-suggested restaurants
    
    async def _analyze_neighborhoods_incremental(self, city: str, since_timestamp: str = None) -> List[Dict[str, Any]]:
        """Analyze neighborhoods with incremental processing."""
        neighborhoods = self.top_neighborhoods.get(city, [])
        analysis_results = []
        
        for neighborhood in neighborhoods[:5]:  # Top 5 neighborhoods
            for cuisine in self.supported_cuisines:
                try:
                    # Check cache first
                    cache_key = self.cache_keys['neighborhood_analysis'].format(
                        city=city, neighborhood=neighborhood, cuisine=cuisine
                    )
                    
                    cached_analysis = await self.cache_manager.get(cache_key)
                    if cached_analysis:
                        self.stats.cache_hits += 1
                        analysis_results.append(cached_analysis)
                        continue
                    
                    self.stats.cache_misses += 1
                    
                    result = await self._analyze_neighborhood_cuisine_cached(city, neighborhood, cuisine)
                    if result:
                        # Cache result
                        await self.cache_manager.set(cache_key, result, expire=self.cache_ttl)
                        analysis_results.append(result)
                        
                except Exception as e:
                    app_logger.error(f"Error analyzing {neighborhood} {cuisine}: {e}")
        
        self.stats.neighborhood_restaurants_analyzed += len(analysis_results)
        return analysis_results
    
    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=6))
    async def _analyze_neighborhood_cuisine_cached(self, city: str, neighborhood: str, cuisine: str) -> Optional[Dict[str, Any]]:
        """Analyze neighborhood-cuisine combination with caching."""
        try:
            # Search for restaurants using Google Reviews API as main engine, Yelp for enhancement
            restaurants = await self._search_neighborhood_restaurants_hybrid(
                city=city,
                neighborhood=neighborhood,
                cuisine=cuisine,
                limit=20
            )
            self._track_api_call('serpapi')
            
            if not restaurants:
                return None
            
            # Apply quality filters for neighborhood analysis
            filtered_restaurants = self._filter_restaurants_by_quality(restaurants)
            
            if not filtered_restaurants:
                return None
            
            # Find the top 3 restaurants using hybrid quality scoring
            top_restaurants = sorted(filtered_restaurants, 
                                   key=lambda r: r.get('hybrid_quality_score', self._calculate_restaurant_quality_score(r)), 
                                   reverse=True)[:3]
            
            if not top_restaurants:
                return None
            
            # Analyze all top 3 restaurants
            restaurant_analyses = []
            for i, restaurant in enumerate(top_restaurants):
                # Extract dishes using topics + sentiment analysis (advanced approach)
                dishes = await self._extract_restaurant_dishes_advanced(restaurant)
                
                if dishes:
                    # Find the best dish for this restaurant
                    best_dish = max(dishes, key=lambda d: d.get('final_score', d.get('recommendation_score', 0)))
                    
                    restaurant_analysis = {
                        'rank': i + 1,
                        'restaurant_id': restaurant.get('restaurant_id', ''),
                        'restaurant_name': restaurant.get('restaurant_name', restaurant.get('name', '')),
                        'rating': restaurant.get('rating', 0.0),
                        'review_count': restaurant.get('review_count', 0),
                        'hybrid_quality_score': restaurant.get('hybrid_quality_score', self._calculate_restaurant_quality_score(restaurant)),
                        'top_dish': {
                            'dish_name': best_dish.get('dish_name', ''),
                            'final_score': best_dish.get('final_score', 0.0),
                            'sentiment_score': best_dish.get('sentiment_score', 0.0),
                            'topic_mentions': best_dish.get('topic_mentions', 0)
                        },
                        'total_dishes': len(dishes)
                    }
                    restaurant_analyses.append(restaurant_analysis)
            
            if not restaurant_analyses:
                return None
            
            # Get the overall top restaurant and dish for backward compatibility
            top_restaurant = top_restaurants[0]
            top_dish = restaurant_analyses[0]['top_dish']
            
            return {
                'city': city,
                'neighborhood': neighborhood,
                'cuisine_type': cuisine,
                'top_restaurant': {
                    'restaurant_id': top_restaurant.get('restaurant_id', ''),
                    'restaurant_name': top_restaurant.get('restaurant_name', top_restaurant.get('name', '')),
                    'rating': top_restaurant.get('rating', 0.0),
                    'review_count': top_restaurant.get('review_count', 0),
                    'hybrid_quality_score': top_restaurant.get('hybrid_quality_score', self._calculate_restaurant_quality_score(top_restaurant))
                },
                'top_dish': top_dish,
                'top_restaurants': restaurant_analyses,  # New field with all 3 restaurants
                'analysis_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            app_logger.error(f"Error analyzing {neighborhood} {cuisine}: {e}")
            # Enhanced error handling with fallback
            try:
                app_logger.info(f"🔄 Attempting fallback analysis for {neighborhood} {cuisine}")
                # Try with simplified approach
                return await self._analyze_neighborhood_cuisine_fallback(city, neighborhood, cuisine)
            except Exception as fallback_error:
                app_logger.error(f"Fallback analysis also failed for {neighborhood} {cuisine}: {fallback_error}")
                return None
    
    async def _analyze_neighborhood_cuisine_fallback(self, city: str, neighborhood: str, cuisine: str) -> Optional[Dict[str, Any]]:
        """Fallback analysis method with simplified approach."""
        try:
            app_logger.info(f"🔄 Using fallback analysis for {neighborhood} {cuisine}")
            
            # Simplified approach: just get basic restaurant data
            restaurants = await self.serpapi_collector.search_restaurants(
                city=city,
                cuisine=cuisine,
                max_results=5  # Reduced limit for fallback
            )
            
            if not restaurants:
                return None
            
            # Use the first restaurant with simple dish extraction
            top_restaurant = restaurants[0]
            top_restaurant['neighborhood'] = neighborhood
            top_restaurant['city'] = city
            
            # Use simple dish extraction
            top_dishes = await self._extract_restaurant_dishes_simple(top_restaurant)
            
            if not top_dishes:
                return None
            
            # Use the first dish
            best_dish = top_dishes[0]
            
            return {
                'city': city,
                'neighborhood': neighborhood,
                'cuisine_type': cuisine,
                'top_restaurant': {
                    'restaurant_id': top_restaurant.get('restaurant_id', ''),
                    'restaurant_name': top_restaurant.get('restaurant_name', ''),
                    'rating': top_restaurant.get('rating', 0.0),
                    'review_count': top_restaurant.get('review_count', 0)
                },
                'top_dish': {
                    'dish_name': best_dish.get('dish_name', ''),
                    'final_score': best_dish.get('final_score', 0.0),
                    'sentiment_score': best_dish.get('sentiment_score', 0.0),
                    'topic_mentions': best_dish.get('topic_mentions', 0)
                },
                'analysis_timestamp': datetime.now().isoformat(),
                'fallback_used': True
            }
            
        except Exception as e:
            app_logger.error(f"Fallback analysis failed: {e}")
            return None

    def _calculate_restaurant_quality_score(self, restaurant: Dict[str, Any]) -> float:
        """Calculate restaurant quality score using logarithmic review count scaling."""
        rating = restaurant.get('rating', 0.0)
        review_count = restaurant.get('review_count', 0)
        
        # Use log10(review_count + 1) to handle 0 reviews and provide diminishing returns
        log_review_count = math.log10(review_count + 1)
        
        # Calculate quality score: rating * log10(review_count + 1)
        quality_score = rating * log_review_count
        
        return quality_score
    
    async def _save_checkpoint(self, city: str, phase: str, status: str, error_message: str = None) -> None:
        """Save checkpoint to Redis."""
        checkpoint = DiscoveryCheckpoint(
            city=city,
            phase=phase,
            timestamp=datetime.now().isoformat(),
            processed_restaurants=[],
            processed_neighborhoods=[],
            last_restaurant_count=0,
            last_dish_count=0,
            status=status
        )
        
        cache_key = self.cache_keys['checkpoint'].format(city=city)
        await self.cache_manager.set(cache_key, asdict(checkpoint), expire=self.checkpoint_ttl)
    
    async def _load_checkpoint(self, city: str) -> Optional[DiscoveryCheckpoint]:
        """Load checkpoint from Redis."""
        cache_key = self.cache_keys['checkpoint'].format(city=city)
        checkpoint_data = await self.cache_manager.get(cache_key)
        
        if checkpoint_data:
            return DiscoveryCheckpoint(**checkpoint_data)
        return None
    
    async def _resume_from_checkpoint(self, city: str, checkpoint: DiscoveryCheckpoint, since_timestamp: str = None) -> Dict[str, Any]:
        """Resume processing from checkpoint."""
        print(f"📋 Resuming {city} from phase: {checkpoint.phase}")
        
        if checkpoint.phase == 'popular_dishes':
            # Resume from popular dishes discovery
            popular_dishes = await self._discover_popular_dishes_incremental(city, since_timestamp)
            famous_restaurants = await self._discover_famous_restaurants_incremental(city, popular_dishes, since_timestamp)
            neighborhood_analysis = await self._analyze_neighborhoods_incremental(city, since_timestamp)
        elif checkpoint.phase == 'famous_restaurants':
            # Resume from famous restaurants discovery
            popular_dishes = await self._discover_popular_dishes_incremental(city, since_timestamp)
            famous_restaurants = await self._discover_famous_restaurants_incremental(city, popular_dishes, since_timestamp)
            neighborhood_analysis = await self._analyze_neighborhoods_incremental(city, since_timestamp)
        else:
            # Complete the process
            popular_dishes = await self._discover_popular_dishes_incremental(city, since_timestamp)
            famous_restaurants = await self._discover_famous_restaurants_incremental(city, popular_dishes, since_timestamp)
            neighborhood_analysis = await self._analyze_neighborhoods_incremental(city, since_timestamp)
        
        return {
            'popular_dishes': popular_dishes,
            'famous_restaurants': famous_restaurants,
            'neighborhood_analysis': neighborhood_analysis,
            'city': city,
            'discovery_timestamp': datetime.now().isoformat(),
            'incremental': True,
            'resumed_from_checkpoint': checkpoint.phase
        }
    
    async def _save_phase1_popular_dishes(self, popular_dishes: List[Dict[str, Any]], city: str) -> None:
        """Save Phase 1 popular dishes immediately after discovery."""
        try:
            app_logger.info(f"💾 Saving {len(popular_dishes)} popular dishes for {city}")
            
            for dish in popular_dishes:
                await self._upsert_popular_dish(dish, city)
            
            app_logger.info(f"✅ Phase 1 popular dishes saved for {city}")
            
        except Exception as e:
            app_logger.error(f"Error saving Phase 1 popular dishes for {city}: {e}")
    
    async def _save_phase1_famous_restaurants(self, famous_restaurants: List[Dict[str, Any]]) -> None:
        """Save Phase 1 famous restaurants immediately after discovery."""
        try:
            app_logger.info(f"💾 Saving {len(famous_restaurants)} famous restaurants")
            
            for restaurant in famous_restaurants:
                await self._upsert_famous_restaurant(restaurant)
            
            app_logger.info("✅ Phase 1 famous restaurants saved")
            
        except Exception as e:
            app_logger.error(f"Error saving Phase 1 famous restaurants: {e}")
    
    async def _save_phase2_neighborhood_analysis(self, neighborhood_analysis: List[Dict[str, Any]]) -> None:
        """Save Phase 2 neighborhood analysis immediately after discovery."""
        try:
            total_records = 0
            for analysis in neighborhood_analysis:
                # Get both restaurants from the analysis
                top_restaurants = analysis.get('top_restaurants', [])
                
                # Create separate records for each restaurant
                for i, restaurant_analysis in enumerate(top_restaurants):
                    # Create individual restaurant record
                    restaurant_record = {
                        'city': analysis.get('city', ''),
                        'neighborhood': analysis.get('neighborhood', ''),
                        'cuisine_type': analysis.get('cuisine_type', ''),
                        'restaurant_rank': restaurant_analysis.get('rank', i + 1),
                        'restaurant_id': restaurant_analysis.get('restaurant_id', ''),
                        'restaurant_name': restaurant_analysis.get('restaurant_name', ''),
                        'rating': restaurant_analysis.get('rating', 0.0),
                        'review_count': restaurant_analysis.get('review_count', 0),
                        'hybrid_quality_score': restaurant_analysis.get('hybrid_quality_score', 0.0),
                        'top_dish_name': restaurant_analysis.get('top_dish', {}).get('dish_name', ''),
                        'top_dish_final_score': restaurant_analysis.get('top_dish', {}).get('final_score', 0.0),
                        'top_dish_sentiment_score': restaurant_analysis.get('top_dish', {}).get('sentiment_score', 0.0),
                        'top_dish_topic_mentions': restaurant_analysis.get('top_dish', {}).get('topic_mentions', 0),
                        'total_dishes': restaurant_analysis.get('total_dishes', 0),
                        'analysis_timestamp': analysis.get('analysis_timestamp', datetime.now().isoformat())
                    }
                    
                    await self._upsert_individual_restaurant_analysis(restaurant_record)
                    total_records += 1
            
            app_logger.info(f"✅ Phase 2 neighborhood analysis saved: {total_records} restaurant records")
            
        except Exception as e:
            app_logger.error(f"Error saving Phase 2 neighborhood analysis: {e}")
    
    async def _save_discovery_results_incremental(self, all_results: Dict[str, Any]) -> None:
        """Save discovery results with incremental updates."""
        try:
            app_logger.info("💾 Saving incremental discovery results to Milvus")
            
            for city, results in all_results.items():
                # Save popular dishes
                popular_dishes = results.get('popular_dishes', [])
                for dish in popular_dishes:
                    await self._upsert_popular_dish(dish, city)
                
                # Save famous restaurants
                famous_restaurants = results.get('famous_restaurants', [])
                for restaurant in famous_restaurants:
                    await self._upsert_famous_restaurant(restaurant)
                
                # Save neighborhood analysis
                neighborhood_analysis = results.get('neighborhood_analysis', [])
                for analysis in neighborhood_analysis:
                    await self._upsert_neighborhood_analysis(analysis)
            
            app_logger.info("✅ Incremental discovery results saved to Milvus")
            
        except Exception as e:
            app_logger.error(f"Error saving incremental discovery results: {e}")
    
    async def _upsert_popular_dish(self, dish: Dict[str, Any], city: str) -> None:
        """Upsert popular dish (update if exists, insert if new)."""
        try:
            # Prepare dish record for discovery collection
            dish_record = {
                'dish_id': f"popular_{city}_{hash(dish.get('dish_name', '')) % 1000000}",
                'dish_name': dish.get('dish_name', ''),
                'normalized_dish_name': dish.get('dish_name', '').lower(),
                'city': city,
                'neighborhoods': dish.get('neighborhoods', []),
                'popularity_score': dish.get('popularity_score', 0.0),
                'frequency': dish.get('frequency', 0),
                'avg_sentiment': dish.get('avg_sentiment', 0.0),
                'cultural_significance': dish.get('cultural_significance', ''),
                'top_restaurants': dish.get('top_restaurants', []),
                'restaurant_count': len(dish.get('top_restaurants', [])),
                'primary_cuisine': dish.get('primary_cuisine', ''),
                'cuisine_types': dish.get('cuisine_types', []),
                'dish_category': dish.get('dish_category', ''),
                'reasoning': dish.get('reasoning', ''),
                'discovery_method': 'ai_analysis',
                'confidence_score': dish.get('confidence_score', 0.0),
                'embedding_text': f"{dish.get('dish_name', '')} {dish.get('cultural_significance', '')} {city}",
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat(),
                'discovery_timestamp': datetime.now().isoformat()
            }
            
            # Upsert to discovery collection
            success = await self.discovery_collections.upsert_popular_dish(dish_record)
            if success:
                self.stats.incremental_updates += 1
            
        except Exception as e:
            app_logger.error(f"Error upserting popular dish: {e}")
    
    async def _upsert_famous_restaurant(self, restaurant: Dict[str, Any]) -> None:
        """Upsert famous restaurant."""
        try:
            # Calculate quality score using logarithmic scaling
            quality_score = self._calculate_restaurant_quality_score(restaurant)
            
            # Prepare restaurant record for discovery collection
            restaurant_record = {
                'restaurant_id': restaurant.get('restaurant_id', ''),
                'restaurant_name': restaurant.get('restaurant_name', ''),
                'city': restaurant.get('city', ''),
                'neighborhood': restaurant.get('neighborhood', ''),
                'full_address': restaurant.get('location', ''),
                'fame_score': restaurant.get('fame_score', 0.0),
                'famous_dish': restaurant.get('famous_dish', ''),
                'dish_popularity': restaurant.get('dish_popularity', 0.0),
                'rating': restaurant.get('rating', 0.0),
                'review_count': restaurant.get('review_count', 0),
                'quality_score': quality_score,
                'cuisine_type': restaurant.get('cuisine_type', ''),
                'price_range': restaurant.get('price_range', 2),
                'phone': restaurant.get('phone', ''),
                'website': restaurant.get('website', ''),
                'discovery_method': restaurant.get('discovery_method', ''),
                'fame_indicators': restaurant.get('fame_indicators', []),
                'cultural_significance': restaurant.get('cultural_significance', ''),
                'embedding_text': f"{restaurant.get('restaurant_name', '')} {restaurant.get('famous_dish', '')} {restaurant.get('city', '')}",
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat(),
                'discovery_timestamp': datetime.now().isoformat()
            }
            
            # Upsert to discovery collection
            success = await self.discovery_collections.upsert_famous_restaurant(restaurant_record)
            if success:
                self.stats.incremental_updates += 1
            
        except Exception as e:
            app_logger.error(f"Error upserting famous restaurant: {e}")
    
    async def _upsert_individual_restaurant_analysis(self, restaurant_record: Dict[str, Any]) -> None:
        """Upsert individual restaurant analysis."""
        try:
            # Create unique analysis ID for this restaurant
            analysis_id = f"restaurant_{restaurant_record.get('city', '')}_{restaurant_record.get('neighborhood', '')}_{restaurant_record.get('cuisine_type', '')}_{restaurant_record.get('restaurant_rank', 1)}"
            
            # Prepare analysis record for discovery collection
            analysis_record = {
                'analysis_id': analysis_id,
                'city': restaurant_record.get('city', ''),
                'neighborhood': restaurant_record.get('neighborhood', ''),
                'cuisine_type': restaurant_record.get('cuisine_type', ''),
                'restaurant_rank': restaurant_record.get('restaurant_rank', 1),
                'restaurant_id': restaurant_record.get('restaurant_id', ''),
                'restaurant_name': restaurant_record.get('restaurant_name', ''),
                'rating': restaurant_record.get('rating', 0.0),
                'review_count': restaurant_record.get('review_count', 0),
                'hybrid_quality_score': restaurant_record.get('hybrid_quality_score', 0.0),
                'top_dish_name': restaurant_record.get('top_dish_name', ''),
                'top_dish_final_score': restaurant_record.get('top_dish_final_score', 0.0),
                'top_dish_sentiment_score': restaurant_record.get('top_dish_sentiment_score', 0.0),
                'top_dish_topic_mentions': restaurant_record.get('top_dish_topic_mentions', 0),
                'total_dishes': restaurant_record.get('total_dishes', 0),
                'analysis_confidence': 0.8, # High confidence for top selection
                'embedding_text': f"{restaurant_record.get('neighborhood', '')} {restaurant_record.get('cuisine_type', '')} {restaurant_record.get('restaurant_name', '')} {restaurant_record.get('top_dish_name', '')}",
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat(),
                'analysis_timestamp': restaurant_record.get('analysis_timestamp', datetime.now().isoformat())
            }
            
            # Upsert to discovery collection
            success = await self.discovery_collections.upsert_neighborhood_analysis(analysis_record)
            if success:
                self.stats.incremental_updates += 1
            
        except Exception as e:
            app_logger.error(f"Error upserting individual restaurant analysis: {e}")
    
    async def _upsert_neighborhood_analysis(self, analysis: Dict[str, Any]) -> None:
        """Upsert neighborhood analysis with both top restaurants."""
        try:
            # Get both restaurants from the analysis
            top_restaurants = analysis.get('top_restaurants', [])
            first_restaurant = top_restaurants[0] if len(top_restaurants) > 0 else {}
            second_restaurant = top_restaurants[1] if len(top_restaurants) > 1 else {}
            
            # Prepare analysis record for discovery collection
            analysis_record = {
                'analysis_id': f"neighborhood_{analysis.get('city', '')}_{analysis.get('neighborhood', '')}_{analysis.get('cuisine_type', '')}",
                'city': analysis.get('city', ''),
                'neighborhood': analysis.get('neighborhood', ''),
                'cuisine_type': analysis.get('cuisine_type', ''),
                # First restaurant (top)
                'top_restaurant_id': first_restaurant.get('restaurant_id', ''),
                'top_restaurant_name': first_restaurant.get('restaurant_name', ''),
                'top_restaurant_rating': first_restaurant.get('rating', 0.0),
                'top_restaurant_review_count': first_restaurant.get('review_count', 0),
                'top_restaurant_quality_score': first_restaurant.get('hybrid_quality_score', 0.0),
                # Second restaurant
                'second_restaurant_id': second_restaurant.get('restaurant_id', ''),
                'second_restaurant_name': second_restaurant.get('restaurant_name', ''),
                'second_restaurant_rating': second_restaurant.get('rating', 0.0),
                'second_restaurant_review_count': second_restaurant.get('review_count', 0),
                'second_restaurant_quality_score': second_restaurant.get('hybrid_quality_score', 0.0),
                # First restaurant's top dish
                'top_dish_name': first_restaurant.get('top_dish', {}).get('dish_name', ''),
                'top_dish_final_score': first_restaurant.get('top_dish', {}).get('final_score', 0.0),
                'top_dish_sentiment_score': first_restaurant.get('top_dish', {}).get('sentiment_score', 0.0),
                'top_dish_topic_mentions': first_restaurant.get('top_dish', {}).get('topic_mentions', 0),
                # Second restaurant's top dish
                'second_dish_name': second_restaurant.get('top_dish', {}).get('dish_name', ''),
                'second_dish_final_score': second_restaurant.get('top_dish', {}).get('final_score', 0.0),
                'second_dish_sentiment_score': second_restaurant.get('top_dish', {}).get('sentiment_score', 0.0),
                'second_dish_topic_mentions': second_restaurant.get('top_dish', {}).get('topic_mentions', 0),
                'restaurants_analyzed': len(top_restaurants),
                'dishes_extracted': sum(r.get('total_dishes', 0) for r in top_restaurants),
                'analysis_confidence': 0.8, # High confidence for top selection
                'embedding_text': f"{analysis.get('neighborhood', '')} {analysis.get('cuisine_type', '')} {first_restaurant.get('restaurant_name', '')} {second_restaurant.get('restaurant_name', '')}",
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat(),
                'analysis_timestamp': analysis.get('analysis_timestamp', datetime.now().isoformat())
            }
            
            # Upsert to discovery collection
            success = await self.discovery_collections.upsert_neighborhood_analysis(analysis_record)
            if success:
                self.stats.incremental_updates += 1
            
        except Exception as e:
            app_logger.error(f"Error upserting neighborhood analysis: {e}")
    
    def _prepare_dish_summary(self, all_dishes: List[Dict]) -> Dict[str, Any]:
        """Prepare dish data summary for AI analysis."""
        dish_counts = {}
        dish_sentiments = {}
        dish_restaurants = {}
        
        for dish in all_dishes:
            dish_name = dish.get('dish_name', '').lower()
            if not dish_name:
                continue
            
            # Count frequency
            dish_counts[dish_name] = dish_counts.get(dish_name, 0) + 1
            
            # Collect sentiment scores
            if dish_name not in dish_sentiments:
                dish_sentiments[dish_name] = []
            dish_sentiments[dish_name].append(dish.get('sentiment_score', 0.0))
            
            # Collect restaurant names
            if dish_name not in dish_restaurants:
                dish_restaurants[dish_name] = set()
            dish_restaurants[dish_name].add(dish.get('restaurant_name', ''))
        
        return {
            'dish_counts': dish_counts,
            'dish_sentiments': {k: sum(v)/len(v) for k, v in dish_sentiments.items()},
            'dish_restaurants': {k: list(v) for k, v in dish_restaurants.items()}
        }
    
    # Note: _merge_dish_results method replaced by _merge_and_normalize_dishes for better multi-word dish handling
    
    def _restaurant_likely_serves_dish(self, restaurant: Dict[str, Any], dish_name: str) -> bool:
        """Check if a restaurant likely serves a specific dish using comprehensive cuisine mapping."""
        cuisine_type = restaurant.get('cuisine_type', '').lower()
        dish_name_lower = dish_name.lower()
        
        # Find matching cuisine
        for cuisine, mapping in self.cuisine_dish_mapping.items():
            if cuisine in cuisine_type:
                # Check base dishes
                for base_dish in mapping['base_dishes']:
                    if base_dish in dish_name_lower:
                        return True
                
                # Check dish variations
                for base_dish, variations in mapping['variations'].items():
                    for variation in variations:
                        if variation in dish_name_lower:
                            return True
                
                # Check search terms
                for search_term in mapping['search_terms']:
                    if search_term in dish_name_lower:
                        return True
        
        return False
    
    def _extract_cuisine_specific_dishes(self, reviews: List[Dict], cuisine_type: str) -> List[Dict[str, Any]]:
        """Extract cuisine-specific dishes from reviews using comprehensive mapping."""
        extracted_dishes = []
        cuisine_type_lower = cuisine_type.lower()
        
        # Find matching cuisine mapping
        cuisine_mapping = None
        for cuisine, mapping in self.cuisine_dish_mapping.items():
            if cuisine in cuisine_type_lower:
                cuisine_mapping = mapping
                break
        
        if not cuisine_mapping:
            return extracted_dishes
        
        # Process each review
        for review in reviews:
            review_text = review.get('text', '').lower()
            
            # Check for base dishes
            for base_dish in cuisine_mapping['base_dishes']:
                if base_dish in review_text:
                    dish_data = {
                        'dish_name': base_dish,
                        'normalized_name': base_dish,
                        'cuisine_type': cuisine_type,
                        'source': 'base_dish',
                        'confidence': 0.8,
                        'review_snippet': self._extract_dish_snippet(review_text, base_dish)
                    }
                    extracted_dishes.append(dish_data)
            
            # Check for dish variations
            for base_dish, variations in cuisine_mapping['variations'].items():
                for variation in variations:
                    if variation in review_text:
                        dish_data = {
                            'dish_name': variation,
                            'normalized_name': base_dish,  # Use base dish for normalization
                            'cuisine_type': cuisine_type,
                            'source': 'dish_variation',
                            'confidence': 0.9,  # Higher confidence for specific variations
                            'review_snippet': self._extract_dish_snippet(review_text, variation)
                        }
                        extracted_dishes.append(dish_data)
        
        # Deduplicate and sort by confidence
        unique_dishes = {}
        for dish in extracted_dishes:
            dish_key = dish['normalized_name']
            if dish_key not in unique_dishes or dish['confidence'] > unique_dishes[dish_key]['confidence']:
                unique_dishes[dish_key] = dish
        
        return list(unique_dishes.values())
    
    def _extract_dish_snippet(self, review_text: str, dish_name: str, context_words: int = 10) -> str:
        """Extract a snippet of text around the dish mention."""
        try:
            dish_index = review_text.find(dish_name.lower())
            if dish_index == -1:
                return ""
            
            start = max(0, dish_index - context_words * 5)  # Approximate word boundary
            end = min(len(review_text), dish_index + len(dish_name) + context_words * 5)
            
            snippet = review_text[start:end].strip()
            return snippet
            
        except Exception:
            return ""
    
    def _normalize_dish_name(self, dish_name: str, cuisine_type: str) -> str:
        """Normalize dish name to base dish for better matching."""
        cuisine_type_lower = cuisine_type.lower()
        
        for cuisine, mapping in self.cuisine_dish_mapping.items():
            if cuisine in cuisine_type_lower:
                # Check if it's a variation of a base dish
                for base_dish, variations in mapping['variations'].items():
                    if dish_name.lower() in [v.lower() for v in variations]:
                        return base_dish
                
                # Check if it's a base dish
                if dish_name.lower() in [d.lower() for d in mapping['base_dishes']]:
                    return dish_name.lower()
        
        return dish_name.lower()
    
    def _filter_restaurants_by_quality(self, restaurants: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter restaurants based on quality criteria for neighborhood analysis."""
        filtered_restaurants = []
        
        for restaurant in restaurants:
            rating = restaurant.get('rating', 0.0)
            review_count = restaurant.get('review_count', 0)
            
            # Apply quality filters
            if review_count >= self.min_review_count_high and rating >= self.min_rating_high:
                # High volume, good rating
                filtered_restaurants.append(restaurant)
            elif review_count >= self.min_review_count_medium and rating >= self.min_rating_medium:
                # Medium volume, excellent rating
                filtered_restaurants.append(restaurant)
        
        return filtered_restaurants
    
    def _track_api_call(self, api_type: str):
        """Track API calls for monitoring."""
        if api_type == 'serpapi':
            self.stats.serpapi_calls += 1
        elif api_type == 'openai':
            self.stats.openai_calls += 1
        elif api_type == 'yelp':
            self.stats.yelp_calls += 1
    
    async def _calculate_fame_score(self, restaurant: Dict[str, Any], dish_name: str) -> float:
        """Calculate fame score for a restaurant based on dish and restaurant metrics."""
        try:
            # Base score from restaurant metrics
            rating = restaurant.get('rating', 0.0)
            review_count = restaurant.get('review_count', 0)
            
            # Normalize metrics
            rating_score = rating / 5.0
            review_score = min(review_count / 1000.0, 1.0)  # Cap at 1000 reviews
            
            # Name fame indicators
            name_fame = self._calculate_name_fame(restaurant.get('name', ''))
            
            # Dish-specific fame
            dish_fame = self._calculate_dish_fame(dish_name, restaurant)
            
            # Weighted combination
            fame_score = (
                rating_score * 0.3 +
                review_score * 0.2 +
                name_fame * 0.3 +
                dish_fame * 0.2
            )
            
            return min(fame_score, 1.0)
            
        except Exception as e:
            app_logger.error(f"Error calculating fame score: {e}")
            return 0.0
    
    def _calculate_name_fame(self, restaurant_name: str) -> float:
        """Calculate fame score based on restaurant name patterns."""
        name_lower = restaurant_name.lower()
        
        # Fame indicators
        fame_indicators = [
            'joe\'s', 'joe\'s pizza', 'lombardi', 'grimaldi', 'junior\'s',
            'katz\'s', 'russ & daughters', 'gray\'s papaya', 'papaya king',
            'nathan\'s', 'sabrett', 'eileen\'s', 'lady m'
        ]
        
        for indicator in fame_indicators:
            if indicator in name_lower:
                return 0.9
        
        return 0.1
    
    def _calculate_dish_fame(self, dish_name: str, restaurant: Dict[str, Any]) -> float:
        """Calculate dish-specific fame score."""
        dish_lower = dish_name.lower()
        
        # Famous dish patterns
        famous_dishes = [
            'new york pizza', 'margherita pizza', 'pepperoni pizza',
            'everything bagel', 'lox bagel', 'cream cheese bagel',
            'pastrami sandwich', 'corned beef sandwich', 'reuben sandwich',
            'chicken biryani', 'mutton biryani', 'butter chicken',
            'dim sum', 'peking duck', 'kung pao chicken'
        ]
        
        for famous_dish in famous_dishes:
            if famous_dish in dish_lower:
                return 0.8
        
        return 0.3
    
    def _print_enhanced_stats(self) -> None:
        """Print enhanced discovery statistics."""
        print(f"\n📊 ENHANCED DISCOVERY STATISTICS")
        print("=" * 50)
        print(f"Cities processed: {self.stats.cities_processed}")
        print(f"Popular dishes found: {self.stats.popular_dishes_found}")
        print(f"Famous restaurants discovered: {self.stats.famous_restaurants_discovered}")
        print(f"Neighborhood restaurants analyzed: {self.stats.neighborhood_restaurants_analyzed}")
        print(f"Total dishes extracted: {self.stats.total_dishes_extracted}")
        print(f"AI queries made: {self.stats.ai_queries_made}")
        print(f"Cache hits: {self.stats.cache_hits}")
        print(f"Cache misses: {self.stats.cache_misses}")
        print(f"Incremental updates: {self.stats.incremental_updates}")
        print(f"API calls saved: {self.stats.api_calls_saved}")
        print(f"Processing time: {self.stats.processing_time_seconds:.2f} seconds")
        
        # API call breakdown
        print(f"\n📞 API CALL BREAKDOWN")
        print("-" * 30)
        print(f"SerpAPI calls: {self.stats.serpapi_calls}")
        print(f"OpenAI calls: {self.stats.openai_calls}")
        print(f"Yelp calls: {self.stats.yelp_calls}")
        total_api_calls = self.stats.serpapi_calls + self.stats.openai_calls + self.stats.yelp_calls
        print(f"Total API calls: {total_api_calls}")
        
        # Calculate efficiency metrics
        if self.stats.cache_hits + self.stats.cache_misses > 0:
            cache_hit_rate = (self.stats.cache_hits / (self.stats.cache_hits + self.stats.cache_misses)) * 100
            print(f"Cache hit rate: {cache_hit_rate:.1f}%")
        
        if self.stats.processing_time_seconds > 0:
            efficiency = (self.stats.popular_dishes_found + self.stats.famous_restaurants_discovered) / self.stats.processing_time_seconds
            print(f"Efficiency: {efficiency:.2f} discoveries/second")
            api_calls_per_minute = (total_api_calls / self.stats.processing_time_seconds) * 60
            print(f"API calls per minute: {api_calls_per_minute:.1f}")


async def main():
    """Main function to run the optimized AI-driven discovery engine."""
    engine = OptimizedAIDrivenDiscoveryEngine()
    
    # Run incremental discovery for all supported cities
    results = await engine.run_incremental_discovery(
        cities=["Manhattan", "Jersey City", "Hoboken"],  # Process all supported cities
        since_timestamp=None,  # Process all data
        force_full=False       # Use incremental mode
    )
    
    # Save results to file for inspection
    with open('optimized_discovery_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n✅ Optimized discovery completed! Results saved to optimized_discovery_results.json")


if __name__ == "__main__":
    asyncio.run(main())
