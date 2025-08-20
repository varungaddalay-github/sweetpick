"""
SerpAPI integration for restaurant discovery and review collection.
"""
import asyncio
import json
import time
from typing import List, Dict, Optional, Any
from serpapi import GoogleSearch
from tenacity import retry, stop_after_attempt, wait_exponential
from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.data_collection.cache_manager import CacheManager
from src.data_collection.yelp_collector import YelpCollector

# City coordinates map for SerpAPI location parameter
CITY_COORDINATES_MAP = {
    "Jersey City": "@40.7178,-74.0431,14z",
    "Hoboken": "@40.7445,-74.0324,14z",
    "New York": "@40.7128,-74.0060,14z",
    "Newark": "@40.7357,-74.1724,14z",
    "Secaucus": "@40.7895,-74.0646,14z",
    "Staten Island": "@40.5795,-74.1502,14z",
    "Bronx": "@40.8448,-73.8648,14z",
    "Brooklyn": "@40.6782,-73.9442,14z",
    "Queens": "@40.7282,-73.7949,14z",
    "Manhattan": "@40.7589,-73.9851,14z",
    "San Francisco": "@37.7749,-122.4194,14z",
    "Hyderabad": "@17.3850,78.4867,14z"
}

# City size tiers for dynamic scaling
CITY_SIZE_TIERS = {
    "small": {
        "max_restaurants": 30,
        "review_limit": 5,
        "min_rating": 4.0,
        "min_reviews": 500
    },
    "medium": {
        "max_restaurants": 50,
        "review_limit": 8,
        "min_rating": 4.2,
        "min_reviews": 1000
    },
    "large": {
        "max_restaurants": 100,
        "review_limit": 12,
        "min_rating": 4.4,
        "min_reviews": 2000
    },
    "mega": {
        "max_restaurants": 200,
        "review_limit": 15,
        "min_rating": 4.5,
        "min_reviews": 3000
    }
}

# City size mapping based on known characteristics
CITY_SIZE_MAP = {
    "Secaucus": "small",
    "Staten Island": "small", 
    "Hoboken": "medium",
    "Jersey City": "medium",
    "Newark": "medium",
    "Bronx": "medium",
    "Brooklyn": "large",
    "Queens": "large",
    "San Francisco": "large",
    "Hyderabad": "mega",
    "New York": "mega",
    "Manhattan": "mega"
}

class SerpAPICollector:
    """Collector for restaurant data and reviews using SerpAPI."""
    
    def __init__(self):
        self.settings = get_settings()
        self.cache_manager = CacheManager()
        self.api_calls = 0
        self.start_time = time.time()
        self.reviews_per_restaurant = 20  # Increased from default
        self.yelp_collector = YelpCollector() if self.settings.yelp_api_key else None
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def search_restaurants(self, city: str, cuisine: str, max_results: int = 50, location: Optional[str] = None) -> List[Dict]:
        """Search for restaurants in a specific city and cuisine (original method)."""
        # Include location in cache key if provided
        location_suffix = f":{location}" if location else ""
        cache_key = f"restaurants:{city}:{cuisine}:{max_results}{location_suffix}"
        
        # Check cache first
        cached_result = await self.cache_manager.get(cache_key)
        if cached_result:
            app_logger.info(f"Using cached restaurant data for {city}, {cuisine}")
            return cached_result
        
        # Build search query and coordinates based on location parameter
        neighborhood = ""  # Default empty neighborhood
        if location:
            # Extract neighborhood from location (e.g., "Manhattan in Hell's Kitchen" -> "Hell's Kitchen")
            if " in " in location:
                neighborhood = location.split(" in ")[1]
                search_query = f"{cuisine} restaurants in {neighborhood}"
                app_logger.info(f"Searching restaurants in {neighborhood} for {cuisine} cuisine")
                
                # Use neighborhood-specific coordinates
                try:
                    from src.data_collection.neighborhood_coordinates import get_neighborhood_coordinates, format_coordinates_for_serpapi
                    coords = get_neighborhood_coordinates(city, neighborhood)
                    if coords:
                        ll_param = format_coordinates_for_serpapi(coords["lat"], coords["lng"], coords["zoom"])
                        app_logger.info(f"Using neighborhood coordinates: {ll_param}")
                    else:
                        ll_param = CITY_COORDINATES_MAP.get(city, "@40.7178,-74.0431,14z")
                        app_logger.warning(f"No coordinates found for {neighborhood}, using city default")
                except ImportError:
                    ll_param = CITY_COORDINATES_MAP.get(city, "@40.7178,-74.0431,14z")
                    app_logger.warning("Neighborhood coordinates module not found, using city default")
            else:
                search_query = f"{cuisine} restaurants in {location}"
                app_logger.info(f"Searching restaurants in {location} for {cuisine} cuisine")
                ll_param = CITY_COORDINATES_MAP.get(city, "@40.7178,-74.0431,14z")
        else:
            search_query = f"{cuisine} restaurants in {city}"
            app_logger.info(f"Searching restaurants in {city} for {cuisine} cuisine")
            ll_param = CITY_COORDINATES_MAP.get(city, "@40.7178,-74.0431,14z")
        
        search_params = {
            "engine": "google_maps",
            "q": search_query,
            "type": "search",
            "api_key": self.settings.serpapi_key,
            "ll": ll_param
        }
        
        try:
            search = GoogleSearch(search_params)
            results = search.get_dict()

            google_restaurants: List[Dict] = []
            
            # Handle both local_results and place_results structures
            if "local_results" in results:
                app_logger.info(f"Found {len(results['local_results'])} raw Google Maps results")
                for result in results["local_results"][:max_results]:
                    restaurant = self._parse_restaurant_result(result, city, cuisine, neighborhood)
                    if restaurant and self._meets_criteria(restaurant):
                        # Get the proper data_id for reviews
                        restaurant = await self._enrich_restaurant_data(restaurant, result)
                        restaurant["source"] = "google_maps"
                        google_restaurants.append(restaurant)
            elif "place_results" in results:
                # Handle single place result (like in debug output)
                app_logger.info(f"Found place_results (single result)")
                result = results["place_results"]
                restaurant = self._parse_restaurant_result(result, city, cuisine, neighborhood)
                if restaurant and self._meets_criteria(restaurant):
                    # Get the proper data_id for reviews
                    restaurant = await self._enrich_restaurant_data(restaurant, result)
                    restaurant["source"] = "google_maps"
                    google_restaurants.append(restaurant)
            else:
                app_logger.warning(f"No local_results or place_results found in response")
                app_logger.debug(f"Available keys in response: {list(results.keys())}")
                if "error" in results:
                    app_logger.error(f"SerpAPI error: {results['error']}")

            # Source-specific quality checks: city consistency + dedupe for Google
            if google_restaurants:
                try:
                    from src.data_collection.data_validator import DataValidator
                    dv = DataValidator()
                    google_restaurants = dv.filter_google_city_and_dedupe(
                        google_restaurants, city=city, neighborhood=neighborhood or None
                    )
                except Exception as ve:
                    app_logger.warning(f"Google city/dedupe filtering skipped: {ve}")

            merged: List[Dict] = list(google_restaurants)

            # Optionally augment with Yelp via SerpAPI
            if self.settings.serpapi_enable_yelp:
                try:
                    yelp_results = await self._search_yelp_restaurants(city, cuisine, limit=self.settings.serpapi_yelp_limit)
                    if yelp_results:
                        # Source-specific quality checks: city consistency + dedupe for Yelp
                        try:
                            from src.data_collection.data_validator import DataValidator
                            dv = DataValidator()
                            yelp_results = dv.filter_yelp_city_and_dedupe(
                                yelp_results, city=city, neighborhood=neighborhood or None
                            )
                        except Exception as ve:
                            app_logger.warning(f"Yelp city/dedupe filtering skipped: {ve}")

                        merged = self._merge_dedupe_sources(google_restaurants, yelp_results, max_total=max_results)
                        app_logger.info(f"Merged results: Google={len(google_restaurants)}, Yelp={len(yelp_results)}, Merged={len(merged)}")
                except Exception as ye:
                    app_logger.warning(f"Yelp enrichment failed: {ye}")
            
            # Optionally augment with direct Yelp API for better neighborhood data
            if self.yelp_collector and location and " in " in location:
                try:
                    neighborhood = location.split(" in ")[1].strip()
                    app_logger.info(f"🔍 Using direct Yelp API for neighborhood: {neighborhood}")
                    
                    yelp_neighborhood_results = await self.yelp_collector.search_by_neighborhood(
                        city, neighborhood, cuisine, limit=min(20, max_results)
                    )
                    
                    if yelp_neighborhood_results:
                        # Merge with existing results, prioritizing neighborhood-specific results
                        merged = self._merge_dedupe_sources(merged, yelp_neighborhood_results, max_total=max_results)
                        app_logger.info(f"Enhanced with Yelp neighborhood data: {len(yelp_neighborhood_results)} restaurants in {neighborhood}")
                        
                except Exception as ye:
                    app_logger.warning(f"Direct Yelp API enrichment failed: {ye}")

            # Cache the results
            await self.cache_manager.set(cache_key, merged, expire=3600)  # 1 hour
            self._track_api_call()

            app_logger.info(f"Found {len(merged)} restaurants in {city} for {cuisine}")
            return merged

        except Exception as e:
            app_logger.error(f"Error searching restaurants in {city} for {cuisine}: {e}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def search_restaurants_with_reviews(self, city: str, cuisine: str, max_results: int = 50, 
                                            location: Optional[str] = None, incremental=False, 
                                            review_threshold=300, ranking_threshold=0.1) -> List[Dict]:
        """Search for restaurants and collect reviews for top 3 qualifying restaurants."""
        # Include location in cache key if provided
        location_suffix = f":{location}" if location else ""
        cache_key = f"restaurants_with_reviews:{city}:{cuisine}:{max_results}{location_suffix}"
        
        # Check cache first
        cached_result = await self.cache_manager.get(cache_key)
        if cached_result and not incremental:
            app_logger.info(f"Using cached restaurant data with reviews for {city}, {cuisine}")
            return cached_result
        
        # Step 1: Get restaurants using the original method
        restaurants = await self.search_restaurants(city, cuisine, max_results, location)
        
        if not restaurants:
            app_logger.warning(f"No restaurants found for {cuisine} in {city}")
            return []
        
        # Step 1.5: Perform data quality checks
        await self._perform_data_quality_checks(restaurants, city, cuisine)
        
        # Step 1.5: Handle incremental updates if enabled
        if incremental and cached_result:
            app_logger.info(f"🔄 Incremental mode: Checking for review updates...")
            restaurants = await self._apply_incremental_updates(restaurants, cached_result, review_threshold, ranking_threshold=0.1)
        
        # Step 2: Re-rank restaurants with fresh data
        restaurants = await self._rerank_restaurants_with_fresh_data(restaurants)
        
        # Step 3: Get TOP 5 restaurants and collect their reviews
        # Prefer entries that can fetch reviews (have reviews_data_id) and from Google source
        candidates = [r for r in restaurants if r.get('reviews_data_id')]
        if not candidates:
            candidates = [r for r in restaurants if r.get('source') == 'google_maps'] or restaurants
        
        # Debug: Log restaurant data for troubleshooting
        app_logger.info(f"🔍 Restaurant candidates for review collection:")
        for i, restaurant in enumerate(restaurants[:5], 1):
            app_logger.info(f"  {i}. {restaurant.get('restaurant_name', 'Unknown')}")
            app_logger.info(f"     Source: {restaurant.get('source', 'unknown')}")
            app_logger.info(f"     Data ID: {restaurant.get('reviews_data_id', 'None')}")
            app_logger.info(f"     Place ID: {restaurant.get('google_place_id', 'None')}")
            app_logger.info(f"     Rating: {restaurant.get('rating', 'None')}")
        
        top_restaurants = candidates[:5]
        app_logger.info(f"Selected top 5 restaurants for review collection:")
        
        for i, restaurant in enumerate(top_restaurants, 1):
            app_logger.info(f"{i}. {restaurant['restaurant_name']} - "
                           f"Rating: {restaurant.get('rating')}, "
                           f"Reviews: {restaurant.get('review_count')}, "
                           f"Quality Score: {restaurant.get('quality_score', 0):.2f}")
        
        # Step 4: Click into each top restaurant and collect reviews
        restaurants_with_reviews = []
        
        for restaurant in top_restaurants:
            app_logger.info(f"🏪 Clicking into: {restaurant['restaurant_name']}")
            
            try:
                # Fetch reviews for this restaurant (this is the "click")
                reviews = await self.get_restaurant_reviews(restaurant, max_reviews=40)
                
                # Add reviews to the restaurant data
                restaurant['reviews'] = reviews
                restaurant['reviews_collected'] = len(reviews)
                
                app_logger.info(f"✅ Collected {len(reviews)} reviews for {restaurant['restaurant_name']}")
                restaurants_with_reviews.append(restaurant)
                
                # Rate limiting between restaurants
                await asyncio.sleep(2)
                
            except Exception as e:
                app_logger.error(f"❌ Error collecting reviews for {restaurant['restaurant_name']}: {e}")
                # Still add the restaurant but mark review collection as failed
                restaurant['reviews'] = []
                restaurant['reviews_collected'] = 0
                restaurant['review_error'] = str(e)
                restaurants_with_reviews.append(restaurant)
        
        # Cache the results
        await self.cache_manager.set(cache_key, restaurants_with_reviews, expire=3600)
        self._track_api_call()
        
        app_logger.info(f"🎉 Completed collection for {len(restaurants_with_reviews)} restaurants with reviews")
        return restaurants_with_reviews

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def get_restaurant_reviews(self, restaurant: Dict, max_reviews: int = 40) -> Dict[str, Any]:
        """Get reviews and topics for a specific restaurant using proper data_id."""
        
        restaurant_name = restaurant.get('restaurant_name', 'Unknown')
        # Prefer explicit review/place ids; fall back to restaurant_id if it looks like a Google hex id
        data_id = restaurant.get("reviews_data_id") or restaurant.get("google_place_id")
        if not data_id:
            rid = restaurant.get("restaurant_id")
            if rid and "0x" in str(rid):
                data_id = rid
        
        if not data_id:
            app_logger.warning(f"No data_id found for restaurant: {restaurant_name}")
            app_logger.info(f"   Trying to find reviews using restaurant name and location...")
            
            # Try to find the restaurant using a search query
            try:
                location = restaurant.get('full_address', '')
                neighborhood = restaurant.get('neighborhood', '')
                
                # Build better search query with neighborhood context
                if neighborhood and location:
                    search_query = f"{restaurant_name} {neighborhood} {location}"
                elif neighborhood:
                    search_query = f"{restaurant_name} {neighborhood}"
                elif location:
                    search_query = f"{restaurant_name} {location}"
                else:
                    search_query = restaurant_name
                
                app_logger.info(f"   Searching for: {search_query}")
                
                # Use neighborhood-specific coordinates if available
                ll_param = "@40.7178,-74.0431,14z"  # Default
                if neighborhood:
                    try:
                        from src.data_collection.neighborhood_coordinates import get_neighborhood_coordinates, format_coordinates_for_serpapi
                        city = restaurant.get('city', 'Manhattan')
                        coords = get_neighborhood_coordinates(city, neighborhood)
                        if coords:
                            ll_param = format_coordinates_for_serpapi(coords["lat"], coords["lng"], coords["zoom"])
                            app_logger.info(f"   Using neighborhood coordinates: {ll_param}")
                    except ImportError:
                        pass
                
                # Use a simple search to try to get the data_id
                search_params = {
                    "engine": "google_maps",
                    "q": search_query,
                    "type": "search",
                    "api_key": self.settings.serpapi_key,
                    "ll": ll_param
                }
                
                search = GoogleSearch(search_params)
                results = search.get_dict()
                
                if "local_results" in results and results["local_results"]:
                    # Take the first result
                    first_result = results["local_results"][0]
                    data_id = (first_result.get("data_id") or 
                              first_result.get("place_id") or 
                              first_result.get("data_cid") or
                              first_result.get("cid"))
                    
                    if data_id:
                        app_logger.info(f"   ✅ Found data_id: {data_id}")
                    else:
                        app_logger.warning(f"   ❌ Still no data_id found")
                        return {'reviews': [], 'topics': []}
                else:
                    app_logger.warning(f"   ❌ No search results found")
                    return {'reviews': [], 'topics': []}
                    
            except Exception as e:
                app_logger.error(f"   ❌ Error searching for restaurant: {e}")
                return {'reviews': [], 'topics': []}
        
        cache_key = f"reviews:{data_id}:{max_reviews}"
        
        # Check cache first
        cached_result = await self.cache_manager.get(cache_key)
        if cached_result:
            app_logger.info(f"Using cached reviews for {restaurant_name}")
            return cached_result
        
        app_logger.info(f"📖 Reading reviews for: {restaurant_name}")
        app_logger.info(f"   Data ID: {data_id}")
        
        all_reviews = []
        all_topics = []
        
        # The debug showed the direct reviews API works well, so try that first
        if data_id and "0x" in str(data_id):  # Check if we have the hex format data_id
            app_logger.info(f"   Trying direct reviews API...")
            reviews_result = await self._fetch_reviews_direct(data_id, max_reviews)
            if isinstance(reviews_result, dict):
                all_reviews = reviews_result.get('reviews', [])
                all_topics = reviews_result.get('topics', [])
            else:
                all_reviews = reviews_result
        
        # If no reviews from direct API, try place details
        if not all_reviews:
            app_logger.info(f"   Trying place details API...")
            all_reviews = await self._fetch_reviews_via_place_details(data_id, max_reviews)
        
        # Cache the results
        result = {'reviews': all_reviews, 'topics': all_topics}
        await self.cache_manager.set(cache_key, result, expire=7200)
        self._track_api_call()
        
        app_logger.info(f"   ✅ Collected {len(all_reviews)} reviews and {len(all_topics)} topics")
        return result

    async def _fetch_reviews_direct(self, data_id: str, max_reviews: int) -> Dict[str, Any]:
        """Fetch reviews and topics using the direct reviews API."""
        try:
            search_params = {
                "engine": "google_maps_reviews",
                "data_id": data_id,
                "api_key": self.settings.serpapi_key
            }
            
            search = GoogleSearch(search_params)
            results = search.get_dict()
            
            app_logger.debug(f"Direct reviews API response keys: {list(results.keys())}")
            
            reviews = []
            topics = []
            
            # Extract reviews
            if "reviews" in results and isinstance(results["reviews"], list):
                reviews_data = results["reviews"]
                app_logger.info(f"Found {len(reviews_data)} reviews in direct API")
                
                for review_data in reviews_data[:max_reviews]:
                    parsed_review = self._parse_review_result(review_data)
                    if parsed_review:
                        reviews.append(parsed_review)
            else:
                app_logger.warning(f"No 'reviews' list found. Available keys: {list(results.keys())}")
            
            # Extract topics
            if "topics" in results and isinstance(results["topics"], list):
                topics_data = results["topics"]
                app_logger.info(f"Found {len(topics_data)} topics in direct API")
                
                for topic_data in topics_data:
                    parsed_topic = self._parse_topic_result(topic_data)
                    if parsed_topic:
                        topics.append(parsed_topic)
            else:
                app_logger.warning(f"No 'topics' list found. Available keys: {list(results.keys())}")
            
            return {'reviews': reviews, 'topics': topics}
            
        except Exception as e:
            app_logger.error(f"Error in direct reviews fetch: {e}")
            return {'reviews': [], 'topics': []}
    
    def _parse_topic_result(self, topic_data: Dict) -> Dict:
        """Parse topic data from Google Maps API response."""
        try:
            return {
                'keyword': topic_data.get('keyword', ''),
                'mentions': topic_data.get('mentions', 0),
                'id': topic_data.get('id', '')
            }
        except Exception as e:
            app_logger.error(f"Error parsing topic data: {e}")
            return None

    async def get_place_details(self, data_id: str) -> Optional[Dict[str, Any]]:
        """Get place details including reviews and metadata using google_maps_reviews engine."""
        try:
            # Use the google_maps_reviews engine with data_id (not place_id)
            search_params = {
                "engine": "google_maps_reviews",
                "data_id": data_id,
                "api_key": self.settings.serpapi_key
            }
            
            search = GoogleSearch(search_params)
            results = search.get_dict()
            
            app_logger.debug(f"Place details API response keys: {list(results.keys())}")
            
            # Extract reviews
            reviews = []
            if "reviews" in results and isinstance(results["reviews"], list):
                reviews_data = results["reviews"]
                app_logger.info(f"Found {len(reviews_data)} reviews in google_maps_reviews API")
                
                for review_data in reviews_data[:20]:  # Limit to 20 reviews
                    parsed_review = self._parse_review_result(review_data)
                    if parsed_review:
                        reviews.append(parsed_review)
            
            # Extract topics
            topics = []
            if "topics" in results and isinstance(results["topics"], list):
                topics_data = results["topics"]
                app_logger.info(f"Found {len(topics_data)} topics in google_maps_reviews API")
                
                for topic_data in topics_data:
                    parsed_topic = self._parse_topic_result(topic_data)
                    if parsed_topic:
                        topics.append(parsed_topic)
            
            return {
                "data_id": data_id,
                "reviews": reviews,
                "topics": topics,
                "metadata": results
            }
            
        except Exception as e:
            app_logger.error(f"Error in get_place_details: {e}")
            return None

    async def _fetch_reviews_via_place_details(self, place_id: str, max_reviews: int) -> List[Dict]:
        """Fetch reviews via place details API."""
        try:
            search_params = {
                "engine": "google_maps",
                "place_id": place_id,
                "api_key": self.settings.serpapi_key
            }
            
            search = GoogleSearch(search_params)
            results = search.get_dict()
            
            app_logger.debug(f"Place details API response keys: {list(results.keys())}")
            
            reviews = []
            
            if "place_results" in results:
                place_results = results["place_results"]
                
                # Based on debug output, user_reviews is a dict with 'most_relevant' containing reviews
                if "user_reviews" in place_results:
                    user_reviews = place_results["user_reviews"]
                    app_logger.debug(f"user_reviews type: {type(user_reviews)}")
                    
                    if isinstance(user_reviews, dict):
                        # Try different sections within user_reviews
                        for section in ["most_relevant", "summary", "recent"]:
                            if section in user_reviews and isinstance(user_reviews[section], list):
                                reviews_data = user_reviews[section]
                                app_logger.info(f"Found {len(reviews_data)} reviews in user_reviews['{section}']")
                                
                                for review_data in reviews_data[:max_reviews]:
                                    parsed_review = self._parse_review_result(review_data)
                                    if parsed_review:
                                        reviews.append(parsed_review)
                                
                                # Use the first section that has reviews
                                if reviews:
                                    break
            
            if not reviews:
                app_logger.warning(f"No reviews found in place_results")
            
            return reviews
            
        except Exception as e:
            app_logger.error(f"Error in place details reviews fetch: {e}")
            return []

    async def _enrich_restaurant_data(self, restaurant: Dict, raw_result: Dict) -> Dict:
        """Enrich restaurant data with proper identifiers for review collection."""
        try:
            # Try different possible identifiers from the raw result
            data_id = (raw_result.get("data_id") or 
                      raw_result.get("place_id") or 
                      raw_result.get("data_cid") or
                      raw_result.get("cid"))
            
            restaurant["reviews_data_id"] = data_id
            
            # Update restaurant_id if we have a proper data_id
            if data_id and data_id != "":
                restaurant["restaurant_id"] = data_id
                restaurant["google_place_id"] = data_id
            
            # Add debug logging to see what's happening
            app_logger.debug(f"Enriching restaurant {restaurant['restaurant_name']}:")
            app_logger.debug(f"  Raw result keys: {list(raw_result.keys())}")
            app_logger.debug(f"  data_id from raw_result: {raw_result.get('data_id')}")
            app_logger.debug(f"  place_id from raw_result: {raw_result.get('place_id')}")
            app_logger.debug(f"  data_cid from raw_result: {raw_result.get('data_cid')}")
            app_logger.debug(f"  cid from raw_result: {raw_result.get('cid')}")
            app_logger.debug(f"  Final data_id assigned: {data_id}")
            app_logger.debug(f"  Updated restaurant_id: {restaurant.get('restaurant_id')}")
            
            return restaurant
            
        except Exception as e:
            app_logger.error(f"Error enriching restaurant data: {e}")
            return restaurant

    async def collect_top_restaurants_with_reviews(self) -> Dict[str, List[Dict]]:
        """Collect top restaurants with reviews for all supported cities and cuisines."""
        all_data = {}
        
        for city in self.settings.supported_cities:
            all_data[city] = []
            
            for cuisine in self.settings.supported_cuisines:
                try:
                    app_logger.info(f"🔍 Processing {cuisine} restaurants in {city}")
                    
                    # Get restaurants with reviews (this does the full workflow)
                    restaurants_with_reviews = await self.search_restaurants_with_reviews(
                        city, cuisine, self.settings.max_restaurants_per_city
                    )
                    
                    # Add cuisine context to each restaurant
                    for restaurant in restaurants_with_reviews:
                        restaurant['search_cuisine'] = cuisine
                    
                    all_data[city].extend(restaurants_with_reviews)
                    
                    # Rate limiting between cuisines
                    await asyncio.sleep(3)
                    
                except Exception as e:
                    app_logger.error(f"Error processing {cuisine} in {city}: {e}")
                    continue
        
        # Summary logging
        total_restaurants = sum(len(city_data) for city_data in all_data.values())
        total_reviews = sum(
            sum(len(restaurant.get('reviews', [])) for restaurant in city_data)
            for city_data in all_data.values()
        )
        
        app_logger.info(f"🏁 Collection Complete!")
        app_logger.info(f"   Total Restaurants: {total_restaurants}")
        app_logger.info(f"   Total Reviews: {total_reviews}")
        
        return all_data

    def _parse_restaurant_result(self, result: Dict, city: str, cuisine: str, neighborhood: str = "") -> Optional[Dict]:
        """Parse a restaurant search result."""
        try:
            # Extract rating and review count
            rating = None
            review_count = None
            
            if "rating" in result:
                rating = float(result["rating"])
            
            if "reviews" in result:
                review_data = result["reviews"]
                
                # Handle both string and int cases
                if isinstance(review_data, int):
                    review_count = review_data
                elif isinstance(review_data, str):
                    # Extract number from text like "1,234 reviews"
                    import re
                    match = re.search(r'(\d+(?:,\d+)*)', review_data)
                    if match:
                        review_count = int(match.group(1).replace(',', ''))
                else:
                    app_logger.warning(f"Unexpected reviews data type: {type(review_data)} for {result.get('title', 'unknown')}")
            
            # Generate a proper restaurant ID
            place_id = result.get("place_id", "")
            if not place_id or place_id == "":
                # Generate a unique ID based on restaurant name and location
                import hashlib
                unique_string = f"{result.get('title', '')}_{city}_{cuisine}"
                place_id = hashlib.md5(unique_string.encode()).hexdigest()[:16]
            
            return {
                "restaurant_id": place_id,
                "restaurant_name": result.get("title", ""),
                "google_place_id": result.get("place_id", ""),
                "full_address": result.get("address", ""),
                "city": city,
                "neighborhood": neighborhood,  # Set directly from search parameters
                "latitude": self._extract_latitude(result),
                "longitude": self._extract_longitude(result),
                "cuisine_type": cuisine,
                "rating": rating,
                "review_count": review_count,
                "price_range": self._extract_price_range(result.get("price", "")),
                "phone": result.get("phone", ""),
                "website": result.get("website", ""),
                "operating_hours": result.get("hours", {}),
                "meal_types": self._extract_meal_types(result.get("hours", {})),
                "fallback_tier": self._calculate_fallback_tier(rating, review_count)
            }
        except Exception as e:
            app_logger.error(f"Error parsing restaurant result: {e}")
            app_logger.error(f"Problematic result: {result}")
            return None
    
    def _parse_review_result(self, review: Dict) -> Optional[Dict]:
        """Parse a review result based on the actual SerpAPI structure."""
        try:
            # Based on debug output, handle user field
            user_info = review.get("user", {})
            if isinstance(user_info, dict):
                user_name = user_info.get("name", "")
            else:
                # For place details API, username might be directly in review
                user_name = review.get("username", "")
            
            # Handle review text - based on debug, it's in 'snippet' or 'description'
            review_text = (review.get("snippet", "") or 
                          review.get("description", "") or
                          review.get("extracted_snippet", "") or
                          review.get("text", ""))
            
            # Handle rating
            rating = review.get("rating")
            if rating is not None:
                try:
                    rating = float(rating)
                except (ValueError, TypeError):
                    rating = None
            
            # Handle date
            date = review.get("date", "")
            
            # Handle review ID
            review_id = review.get("review_id", review.get("link", ""))
            
            # Handle likes/helpful (not visible in debug output but common)
            likes = review.get("likes", 0)
            
            parsed_review = {
                "review_id": review_id,
                "user_name": user_name,
                "rating": rating,
                "date": date,
                "text": review_text,
                "likes": likes,
                "translated": review.get("translated", False)
            }
            
            # Only return if we have meaningful content
            if parsed_review["text"] or parsed_review["rating"] is not None:
                app_logger.debug(f"Parsed review: {user_name} - {rating} stars - {len(review_text)} chars")
                return parsed_review
            else:
                app_logger.debug(f"Skipping review with no content: {review}")
                return None
                
        except Exception as e:
            app_logger.error(f"Error parsing review result: {e}")
            app_logger.error(f"Problematic review: {review}")
            return None
    
    def _meets_criteria(self, restaurant: Dict) -> bool:
        """Check if restaurant meets the minimum criteria."""
        if not restaurant.get("rating") or restaurant["rating"] < self.settings.min_rating:
            return False
        
        if not restaurant.get("review_count") or restaurant["review_count"] < self.settings.min_reviews:
            return False
        
        return True
    
    def _extract_price_range(self, price_text: str) -> int:
        """Extract price range from text (1-4 scale)."""
        if not price_text:
            return 2
        
        price_map = {"$": 1, "$$": 2, "$$$": 3, "$$$$": 4}
        return price_map.get(price_text, 2)
    
    def _extract_meal_types(self, hours: Dict) -> List[str]:
        """Extract meal types from operating hours."""
        meal_types = []
        if not hours:
            return meal_types
        
        # Simple heuristic based on opening times
        # This could be enhanced with more sophisticated logic
        return ["lunch", "dinner"]  # Default assumption
    
    def _extract_latitude(self, result: Dict) -> float:
        """Extract latitude from SerpAPI result."""
        try:
            # Check for gps_coordinates first (Google Maps API format)
            if "gps_coordinates" in result:
                gps = result["gps_coordinates"]
                if isinstance(gps, dict) and "latitude" in gps:
                    return float(gps["latitude"])
            
            # Fallback to direct latitude field
            if "latitude" in result:
                return float(result["latitude"])
            
            # No coordinates found
            return 0.0
            
        except (ValueError, TypeError):
            return 0.0
    
    def _extract_longitude(self, result: Dict) -> float:
        """Extract longitude from SerpAPI result."""
        try:
            # Check for gps_coordinates first (Google Maps API format)
            if "gps_coordinates" in result:
                gps = result["gps_coordinates"]
                if isinstance(gps, dict) and "longitude" in gps:
                    return float(gps["longitude"])
            
            # Fallback to direct longitude field
            if "longitude" in result:
                return float(result["longitude"])
            
            # No coordinates found
            return 0.0
            
        except (ValueError, TypeError):
            return 0.0
    
    def _calculate_fallback_tier(self, rating: float, review_count: int) -> int:
        """Calculate fallback tier based on rating and review count."""
        if rating >= 4.2 and review_count >= 500:
            return 1  # Premium
        elif rating >= 4.0 and review_count >= 250:
            return 2  # Good
        elif rating >= 3.8 and review_count >= 100:
            return 3  # Acceptable
        else:
            return 4  # Below threshold
    
    async def _perform_data_quality_checks(self, restaurants: List[Dict], city: str, cuisine: str):
        """Perform comprehensive data quality checks for each data source."""
        from src.data_collection.data_validator import DataValidator
        
        validator = DataValidator()
        
        app_logger.info(f"🔍 Performing data quality checks for {city}, {cuisine}")
        
        # Separate restaurants by source
        google_restaurants = [r for r in restaurants if r.get('source') == 'google_maps']
        yelp_restaurants = [r for r in restaurants if r.get('source') == 'yelp']
        merged_restaurants = [r for r in restaurants if r.get('source') == 'merged']
        
        # Validate Google Maps data
        if google_restaurants:
            google_quality = validator.validate_google_maps_data(google_restaurants)
            app_logger.info(f"📊 Google Maps Quality Report for {city}, {cuisine}:")
            app_logger.info(f"   Overall Score: {google_quality['quality_score']:.2f}/1.0")
            app_logger.info(f"   Checks Passed: {google_quality['checks_passed']}/4")
            app_logger.info(f"   Detailed Scores: {google_quality['detailed_checks']}")
            if google_quality['recommendations']:
                app_logger.info(f"   Recommendations: {google_quality['recommendations']}")
        
        # Validate Yelp data
        if yelp_restaurants:
            yelp_quality = validator.validate_yelp_data(yelp_restaurants)
            app_logger.info(f"📊 Yelp Quality Report for {city}, {cuisine}:")
            app_logger.info(f"   Overall Score: {yelp_quality['quality_score']:.2f}/1.0")
            app_logger.info(f"   Checks Passed: {yelp_quality['checks_passed']}/4")
            app_logger.info(f"   Detailed Scores: {yelp_quality['detailed_checks']}")
            if yelp_quality['recommendations']:
                app_logger.info(f"   Recommendations: {yelp_quality['recommendations']}")
        
        # Validate merged data
        if merged_restaurants:
            merged_quality = validator.validate_merged_data(merged_restaurants)
            app_logger.info(f"📊 Merged Data Quality Report for {city}, {cuisine}:")
            app_logger.info(f"   Overall Score: {merged_quality['quality_score']:.2f}/1.0")
            app_logger.info(f"   Checks Passed: {merged_quality['checks_passed']}/4")
            app_logger.info(f"   Detailed Scores: {merged_quality['detailed_checks']}")
            if merged_quality['recommendations']:
                app_logger.info(f"   Recommendations: {merged_quality['recommendations']}")
        
        # Overall quality summary
        total_restaurants = len(restaurants)
        if total_restaurants > 0:
            app_logger.info(f"📊 Overall Quality Summary for {city}, {cuisine}:")
            app_logger.info(f"   Total Restaurants: {total_restaurants}")
            app_logger.info(f"   Google Maps: {len(google_restaurants)} restaurants")
            app_logger.info(f"   Yelp: {len(yelp_restaurants)} restaurants")
            app_logger.info(f"   Merged: {len(merged_restaurants)} restaurants")

    def _track_api_call(self):
        """Track API calls for cost monitoring."""
        self.api_calls += 1
        elapsed_time = time.time() - self.start_time
        
        if self.api_calls % 10 == 0:
            app_logger.info(f"API calls: {self.api_calls}, elapsed: {elapsed_time:.2f}s, cost: ${self.api_calls * 0.015:.2f}")
    
    async def _apply_incremental_updates(self, new_restaurants: List[Dict], cached_restaurants: List[Dict], 
                                       review_threshold: int, ranking_threshold: float = 0.1) -> List[Dict]:
        """Apply incremental updates with smart re-ranking based on metadata changes."""
        app_logger.info(f"🔄 Applying incremental updates (review threshold: {review_threshold}, ranking threshold: {ranking_threshold})")
        
        # Step 1: Check if re-ranking is needed based on metadata changes
        needs_reranking = await self._should_rerank(new_restaurants, cached_restaurants, ranking_threshold)
        
        if needs_reranking:
            app_logger.info(f"🔄 Significant metadata changes detected - re-ranking restaurants")
            reranked_restaurants = await self._rerank_restaurants_with_fresh_data(new_restaurants)
            
            # Track ranking changes if we have cached data
            if cached_restaurants:
                self._track_ranking_changes(cached_restaurants, reranked_restaurants)
        else:
            app_logger.info(f"⏭️  Minimal metadata changes - using cached ranking order")
            reranked_restaurants = new_restaurants
        
        # Step 2: Apply incremental review collection logic
        cached_lookup = {}
        for restaurant in cached_restaurants:
            key = restaurant.get('restaurant_name', '').lower().strip()
            cached_lookup[key] = restaurant
        
        updated_restaurants = []
        reviews_updated = 0
        reviews_skipped = 0
        
        for reranked_restaurant in reranked_restaurants:
            restaurant_name = reranked_restaurant.get('restaurant_name', '').lower().strip()
            new_review_count = reranked_restaurant.get('review_count', 0)
            
            if restaurant_name in cached_lookup:
                cached_restaurant = cached_lookup[restaurant_name]
                cached_review_count = cached_restaurant.get('review_count', 0)
                review_diff = abs(new_review_count - cached_review_count)
                
                if review_diff > review_threshold:
                    # Significant change - use fresh data but keep cached reviews temporarily
                    app_logger.info(f"   📈 {reranked_restaurant.get('restaurant_name')}: "
                                  f"Review count changed {cached_review_count} → {new_review_count} "
                                  f"(diff: {review_diff}) - will re-fetch reviews")
                    
                    # Use fresh metadata but preserve cached reviews
                    updated_restaurant = reranked_restaurant.copy()
                    updated_restaurant['reviews'] = cached_restaurant.get('reviews', [])
                    updated_restaurant['reviews_collected'] = cached_restaurant.get('reviews_collected', 0)
                    updated_restaurant['needs_review_update'] = True
                    updated_restaurants.append(updated_restaurant)
                    reviews_updated += 1
                else:
                    # Insignificant change - use fresh metadata with cached reviews
                    app_logger.info(f"   ⏭️  {reranked_restaurant.get('restaurant_name')}: "
                                  f"Review count change {cached_review_count} → {new_review_count} "
                                  f"(diff: {review_diff}) - using cached reviews with fresh ranking")
                    
                    # Use fresh metadata but keep cached reviews
                    updated_restaurant = reranked_restaurant.copy()
                    updated_restaurant['reviews'] = cached_restaurant.get('reviews', [])
                    updated_restaurant['reviews_collected'] = cached_restaurant.get('reviews_collected', 0)
                    updated_restaurant['needs_review_update'] = False
                    updated_restaurants.append(updated_restaurant)
                    reviews_skipped += 1
            else:
                # New restaurant - use fresh data
                app_logger.info(f"   🆕 {reranked_restaurant.get('restaurant_name')}: New restaurant - will fetch reviews")
                updated_restaurants.append(reranked_restaurant)
                reviews_updated += 1
        
        app_logger.info(f"🔄 Re-ranking + incremental summary: {reviews_updated} need updates, {reviews_skipped} skipped")
        return updated_restaurants

    async def _rerank_restaurants_with_fresh_data(self, restaurants: List[Dict]) -> List[Dict]:
        """Re-rank restaurants using fresh quality scores and hybrid scoring."""
        app_logger.info(f"🔄 Re-ranking {len(restaurants)} restaurants with fresh data")
        
        # Recalculate quality scores with fresh data
        for restaurant in restaurants:
            # Use hybrid quality score if available, otherwise calculate original
            if 'hybrid_quality_score' in restaurant:
                restaurant['quality_score'] = restaurant['hybrid_quality_score']
                app_logger.info(f"   📊 {restaurant.get('restaurant_name')}: "
                               f"Hybrid score = {restaurant['hybrid_quality_score']:.2f}")
            else:
                # Fallback to original calculation for restaurants from single source
                rating = restaurant.get('rating', 0)
                review_count = restaurant.get('review_count', 0)
                import math
                quality_score = rating * math.log(review_count + 1)
                restaurant['quality_score'] = quality_score
                app_logger.info(f"   📊 {restaurant.get('restaurant_name')}: "
                               f"Original score = {quality_score:.2f} (rating={rating}, reviews={review_count})")
        
        # Sort by fresh quality scores
        reranked = sorted(restaurants, key=lambda x: x.get('quality_score', 0), reverse=True)
        
        # Log ranking changes
        app_logger.info(f"🔄 Re-ranking complete - top 5 restaurants:")
        for i, restaurant in enumerate(reranked[:5], 1):
            app_logger.info(f"   {i}. {restaurant.get('restaurant_name')} "
                           f"(Score: {restaurant.get('quality_score', 0):.2f}, "
                           f"Rating: {restaurant.get('rating', 0)}, "
                           f"Reviews: {restaurant.get('review_count', 0)})")
        
        return reranked

    def _track_ranking_changes(self, old_ranking: List[Dict], new_ranking: List[Dict]) -> Dict:
        """Track and log ranking changes between old and new rankings."""
        app_logger.info(f"📊 Analyzing ranking changes...")
        
        # Create lookup for old rankings
        old_lookup = {}
        for i, restaurant in enumerate(old_ranking):
            key = restaurant.get('restaurant_name', '').lower().strip()
            old_lookup[key] = i + 1  # 1-based ranking
        
        changes = {
            'moved_up': [],
            'moved_down': [],
            'new_entries': [],
            'dropped_entries': []
        }
        
        for i, restaurant in enumerate(new_ranking):
            key = restaurant.get('restaurant_name', '').lower().strip()
            new_rank = i + 1
            
            if key in old_lookup:
                old_rank = old_lookup[key]
                if new_rank < old_rank:
                    changes['moved_up'].append({
                        'name': restaurant.get('restaurant_name'),
                        'old_rank': old_rank,
                        'new_rank': new_rank,
                        'change': old_rank - new_rank
                    })
                elif new_rank > old_rank:
                    changes['moved_down'].append({
                        'name': restaurant.get('restaurant_name'),
                        'old_rank': old_rank,
                        'new_rank': new_rank,
                        'change': new_rank - old_rank
                    })
            else:
                changes['new_entries'].append({
                    'name': restaurant.get('restaurant_name'),
                    'new_rank': new_rank
                })
        
        # Find dropped entries
        new_names = {r.get('restaurant_name', '').lower().strip() for r in new_ranking}
        for restaurant in old_ranking:
            key = restaurant.get('restaurant_name', '').lower().strip()
            if key not in new_names:
                changes['dropped_entries'].append({
                    'name': restaurant.get('restaurant_name')
                })
        
        # Log significant changes
        if changes['moved_up']:
            app_logger.info(f"📈 Restaurants moved up: {len(changes['moved_up'])}")
            for change in changes['moved_up'][:3]:  # Show top 3
                app_logger.info(f"   📈 {change['name']}: #{change['old_rank']} → #{change['new_rank']} (+{change['change']})")
        
        if changes['moved_down']:
            app_logger.info(f"📉 Restaurants moved down: {len(changes['moved_down'])}")
            for change in changes['moved_down'][:3]:  # Show top 3
                app_logger.info(f"   📉 {change['name']}: #{change['old_rank']} → #{change['new_rank']} (-{change['change']})")
        
        if changes['new_entries']:
            app_logger.info(f"🆕 New restaurants: {len(changes['new_entries'])}")
            for entry in changes['new_entries'][:3]:  # Show top 3
                app_logger.info(f"   🆕 {entry['name']}: #{entry['new_rank']}")
        
        if changes['dropped_entries']:
            app_logger.info(f"❌ Dropped restaurants: {len(changes['dropped_entries'])}")
            for entry in changes['dropped_entries'][:3]:  # Show top 3
                app_logger.info(f"   ❌ {entry['name']}")
        
        return changes

    async def _should_rerank(self, new_restaurants: List[Dict], cached_restaurants: List[Dict], 
                           ranking_threshold: float) -> bool:
        """Determine if re-ranking is needed based on metadata changes."""
        if not cached_restaurants:
            app_logger.info(f"🔄 No cached data - re-ranking needed")
            return True
        
        app_logger.info(f"🔍 Analyzing metadata changes for re-ranking decision...")
        
        # Create lookup for cached restaurants
        cached_lookup = {}
        for restaurant in cached_restaurants:
            key = restaurant.get('restaurant_name', '').lower().strip()
            cached_lookup[key] = restaurant
        
        significant_changes = 0
        total_restaurants = 0
        
        for new_restaurant in new_restaurants:
            restaurant_name = new_restaurant.get('restaurant_name', '').lower().strip()
            total_restaurants += 1
            
            if restaurant_name in cached_lookup:
                cached_restaurant = cached_lookup[restaurant_name]
                
                # Check rating changes
                new_rating = new_restaurant.get('rating', 0)
                cached_rating = cached_restaurant.get('rating', 0)
                rating_diff = abs(new_rating - cached_rating)
                
                # Check review count changes
                new_reviews = new_restaurant.get('review_count', 0)
                cached_reviews = cached_restaurant.get('review_count', 0)
                review_diff = abs(new_reviews - cached_reviews)
                
                # Check quality score changes
                new_score = new_restaurant.get('quality_score', 0)
                cached_score = cached_restaurant.get('quality_score', 0)
                score_diff = abs(new_score - cached_score)
                
                # Determine if changes are significant
                is_significant = (
                    rating_diff > ranking_threshold or  # Rating changed significantly
                    review_diff > 100 or  # Review count changed by >100
                    score_diff > (ranking_threshold * 5)  # Quality score changed significantly
                )
                
                if is_significant:
                    significant_changes += 1
                    app_logger.debug(f"   📊 {new_restaurant.get('restaurant_name')}: "
                                   f"Rating {cached_rating:.1f}→{new_rating:.1f}, "
                                   f"Reviews {cached_reviews}→{new_reviews}, "
                                   f"Score {cached_score:.1f}→{new_score:.1f}")
            else:
                # New restaurant - always significant
                significant_changes += 1
                app_logger.debug(f"   🆕 {new_restaurant.get('restaurant_name')}: New restaurant")
        
        # Calculate percentage of significant changes
        change_percentage = (significant_changes / total_restaurants) * 100 if total_restaurants > 0 else 0
        
        app_logger.info(f"📊 Metadata analysis: {significant_changes}/{total_restaurants} restaurants "
                       f"({change_percentage:.1f}%) have significant changes")
        
        # Re-rank if >20% of restaurants have significant changes
        needs_reranking = change_percentage > 20 or significant_changes >= 3
        
        if needs_reranking:
            app_logger.info(f"🔄 Re-ranking needed: {change_percentage:.1f}% significant changes")
        else:
            app_logger.info(f"⏭️  Re-ranking skipped: only {change_percentage:.1f}% significant changes")
        
        return needs_reranking

    async def collect_all_restaurants(self) -> Dict[str, List[Dict]]:
        """Collect restaurants for all supported cities and cuisines."""
        all_restaurants = {}
        
        for city in self.settings.supported_cities:
            all_restaurants[city] = []
            
            for cuisine in self.settings.supported_cuisines:
                try:
                    restaurants = await self.search_restaurants(
                        city, cuisine, self.settings.max_restaurants_per_city
                    )
                    all_restaurants[city].extend(restaurants)
                    
                    # Rate limiting between cuisines
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    app_logger.error(f"Error collecting restaurants for {city}, {cuisine}: {e}")
                    continue
        
        return all_restaurants

    async def _search_yelp_restaurants(self, city: str, cuisine: str, limit: int = 30) -> List[Dict]:
        """Use SerpAPI's Yelp engine to fetch additional restaurants and normalize fields."""
        app_logger.info(f"Enriching with Yelp for {cuisine} in {city} (limit={limit})")
        try:
            params = {
                "engine": "yelp",
                "find_desc": f"{cuisine} restaurants",
                "find_loc": city,
                "api_key": self.settings.serpapi_key,
                "num": limit
            }
            search = GoogleSearch(params)
            yelp = search.get_dict()
            self._track_api_call()

            results = []
            items = yelp.get("organic_results") or yelp.get("results") or []
            for item in items[:limit]:
                normalized = self._parse_yelp_result(item, city, cuisine)
                if normalized and self._meets_criteria(normalized):
                    results.append(normalized)

            return results
        except Exception as e:
            app_logger.warning(f"Yelp search error: {e}")
            return []

    def _parse_yelp_result(self, item: Dict, city: str, cuisine: str) -> Optional[Dict]:
        """Normalize Yelp fields to our restaurant schema subset."""
        try:
            title = item.get("title") or item.get("name") or ""
            rating = item.get("rating")
            review_count = item.get("reviews_count") or item.get("reviews")

            # Parse review_count if it's a string like "1,234 reviews"
            if isinstance(review_count, str):
                import re
                m = re.search(r"(\d+(?:,\d+)*)", review_count)
                if m:
                    review_count = int(m.group(1).replace(",", ""))
                else:
                    review_count = None

            # Price to 1-4 scale if present like "$", "$$"
            price_text = item.get("price") or item.get("price_range") or ""
            price_map = {"$": 1, "$$": 2, "$$$": 3, "$$$$": 4}
            price_range = price_map.get(price_text, 2)

            # Location fields
            address = item.get("address") or item.get("location") or ""
            coordinates = item.get("coordinates") or {}
            lat = coordinates.get("latitude") or 0.0
            lng = coordinates.get("longitude") or 0.0

            # Quality score (same formula)
            import math
            qs = (float(rating) if rating is not None else 0.0) * math.log((int(review_count) if review_count else 0) + 1)

            # Generate a proper restaurant ID for Yelp results
            restaurant_id = item.get("listing_id") or item.get("business_id")
            if not restaurant_id or restaurant_id == "":
                # Generate a unique ID based on restaurant name and location
                import hashlib
                unique_string = f"{title}_{city}_{cuisine}_yelp"
                restaurant_id = hashlib.md5(unique_string.encode()).hexdigest()[:16]
            
            return {
                "restaurant_id": restaurant_id,
                "restaurant_name": title,
                "google_place_id": "",  # Not applicable for Yelp
                "full_address": address,
                "city": city,
                "neighborhood": "",  # Default empty neighborhood
                "latitude": float(lat) if isinstance(lat, (int, float, str)) else 0.0,
                "longitude": float(lng) if isinstance(lng, (int, float, str)) else 0.0,
                "cuisine_type": cuisine,
                "rating": float(rating) if rating is not None else None,
                "review_count": int(review_count) if isinstance(review_count, int) else (review_count or 0),
                "price_range": price_range,
                "operating_hours": {},
                "meal_types": ["lunch", "dinner"],
                "fallback_tier": self._calculate_fallback_tier(float(rating) if rating is not None else 0.0, int(review_count) if isinstance(review_count, int) else (review_count or 0)),
                "quality_score": qs,
                "source": "yelp"
            }
        except Exception as e:
            app_logger.warning(f"Failed to parse Yelp item: {e}")
            return None

    def calculate_hybrid_quality_score(self, google_data: Optional[Dict], yelp_data: Optional[Dict]) -> float:
        """Calculate hybrid quality score using weighted average of Google and Yelp data."""
        if not google_data and not yelp_data:
            return 0.0
        
        if google_data and yelp_data:
            # Both sources available - weighted average
            google_score = google_data.get('quality_score', 0.0)
            yelp_score = yelp_data.get('quality_score', 0.0)
            
            # Weight by review count (more reviews = more confidence)
            google_reviews = google_data.get('review_count', 0)
            yelp_reviews = yelp_data.get('review_count', 0)
            total_reviews = google_reviews + yelp_reviews
            
            if total_reviews > 0:
                google_weight = google_reviews / total_reviews
                yelp_weight = yelp_reviews / total_reviews
            else:
                # Fallback to equal weights if no review counts
                google_weight = 0.5
                yelp_weight = 0.5
            
            hybrid_score = (google_score * google_weight) + (yelp_score * yelp_weight)
            
            app_logger.debug(f"Hybrid score for {google_data.get('restaurant_name', 'Unknown')}: "
                           f"Google({google_score:.2f} * {google_weight:.2f}) + "
                           f"Yelp({yelp_score:.2f} * {yelp_weight:.2f}) = {hybrid_score:.2f}")
            
            return hybrid_score
        
        elif google_data:
            return google_data.get('quality_score', 0.0)
        else:
            return yelp_data.get('quality_score', 0.0)

    def _merge_dedupe_sources(self, google_items: List[Dict], yelp_items: List[Dict], max_total: int) -> List[Dict]:
        """Merge Google and Yelp lists, dedupe by name+city, use hybrid quality scores."""
        def key(item: Dict) -> str:
            name = (item.get("restaurant_name") or "").strip().lower()
            city = (item.get("city") or "").strip().lower()
            return f"{name}|{city}"

        # Group items by restaurant key
        google_by_key = {key(item): item for item in google_items}
        yelp_by_key = {key(item): item for item in yelp_items}
        
        merged: Dict[str, Dict] = {}
        
        # Process all unique restaurants
        all_keys = set(google_by_key.keys()) | set(yelp_by_key.keys())
        
        for restaurant_key in all_keys:
            google_data = google_by_key.get(restaurant_key)
            yelp_data = yelp_by_key.get(restaurant_key)
            
            # Calculate hybrid quality score
            hybrid_score = self.calculate_hybrid_quality_score(google_data, yelp_data)
            
            # Prefer Google data for the base record (has reviews_data_id)
            if google_data:
                base_record = google_data.copy()
                base_record['hybrid_quality_score'] = hybrid_score
                base_record['google_quality_score'] = google_data.get('quality_score', 0.0)
                base_record['yelp_quality_score'] = yelp_data.get('quality_score', 0.0) if yelp_data else None
                base_record['sources'] = ['google_maps'] + (['yelp'] if yelp_data else [])
            else:
                # Only Yelp data available
                base_record = yelp_data.copy()
                base_record['hybrid_quality_score'] = hybrid_score
                base_record['google_quality_score'] = None
                base_record['yelp_quality_score'] = yelp_data.get('quality_score', 0.0)
                base_record['sources'] = ['yelp']
            
            merged[restaurant_key] = base_record

        # Return top by hybrid_quality_score then rating, limited to max_total
        combined = list(merged.values())
        combined.sort(key=lambda x: (x.get('hybrid_quality_score', 0.0), x.get('rating', 0.0)), reverse=True)
        return combined[:max_total]

    async def get_city_tier(self, city: str) -> str:
        """Determine city tier based on known mapping or dynamic detection."""
        # Check known city mapping first
        if city in CITY_SIZE_MAP:
            return CITY_SIZE_MAP[city]
        
        # Dynamic detection based on restaurant count
        try:
            app_logger.info(f"🔍 Detecting city size for {city}...")
            restaurant_count = await self._estimate_restaurant_count(city)
            
            if restaurant_count > 5000:
                return "mega"
            elif restaurant_count > 2000:
                return "large"
            elif restaurant_count > 500:
                return "medium"
            else:
                return "small"
                
        except Exception as e:
            app_logger.warning(f"Could not detect city size for {city}, defaulting to medium: {e}")
            return "medium"
    
    async def _estimate_restaurant_count(self, city: str) -> int:
        """Estimate total restaurant count for a city using a broad search."""
        cache_key = f"city_restaurant_count:{city}"
        
        # Check cache first
        cached_count = await self.cache_manager.get(cache_key)
        if cached_count:
            return cached_count
        
        try:
            # Search for all restaurants (not cuisine-specific)
            search_params = {
                "engine": "google_maps",
                "q": f"restaurants in {city}",
                "type": "search",
                "api_key": self.settings.serpapi_key,
                "ll": CITY_COORDINATES_MAP.get(city, "@40.7178,-74.0431,14z")
            }
            
            search = GoogleSearch(search_params)
            results = search.get_dict()
            
            # Estimate based on available results and typical Google Maps behavior
            if "local_results" in results:
                # Google Maps typically shows 20 results per page
                # If we get 20 results, there are likely many more
                # If we get fewer, the city is smaller
                raw_count = len(results["local_results"])
                
                if raw_count >= 20:
                    # Large city - estimate 1000+ restaurants
                    estimated_count = 2000 + (raw_count - 20) * 100
                elif raw_count >= 15:
                    # Medium city - estimate 500-1000 restaurants
                    estimated_count = 500 + (raw_count - 15) * 50
                elif raw_count >= 10:
                    # Small city - estimate 200-500 restaurants
                    estimated_count = 200 + (raw_count - 10) * 30
                else:
                    # Very small city
                    estimated_count = raw_count * 10
                
                # Cache the estimate for 24 hours
                await self.cache_manager.set(cache_key, estimated_count, expire=86400)
                self._track_api_call()
                
                app_logger.info(f"🏙️ Estimated {estimated_count} restaurants in {city}")
                return estimated_count
            else:
                return 100  # Default fallback
                
        except Exception as e:
            app_logger.warning(f"Error estimating restaurant count for {city}: {e}")
            return 100  # Default fallback
    
    def get_dynamic_limits(self, city: str, cuisine: str) -> Dict[str, Any]:
        """Get dynamic limits based on city tier."""
        # Use asyncio.run for synchronous access to async method
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If we're already in an async context, we need to handle this differently
                # For now, use the known city mapping
                tier = CITY_SIZE_MAP.get(city, "medium")
            else:
                tier = asyncio.run(self.get_city_tier(city))
        except:
            tier = CITY_SIZE_MAP.get(city, "medium")
        
        limits = CITY_SIZE_TIERS[tier].copy()
        
        app_logger.info(f"🏙️ City tier for {city}: {tier}")
        app_logger.info(f"📊 Dynamic limits: {limits['max_restaurants']} restaurants, {limits['review_limit']} reviews per restaurant")
        
        return limits
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def search_restaurants_dynamic(self, city: str, cuisine: str) -> List[Dict]:
        """Search for restaurants with dynamic limits based on city size."""
        # Get dynamic limits for this city
        limits = self.get_dynamic_limits(city, cuisine)
        max_results = limits["max_restaurants"]
        
        app_logger.info(f"🎯 Dynamic search: {city} + {cuisine} (max {max_results} restaurants)")
        
        # Use the existing search method with dynamic limits
        return await self.search_restaurants(city, cuisine, max_results)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def search_restaurants_with_reviews_dynamic(self, city: str, cuisine: str, 
                                                    incremental=False, review_threshold=300, 
                                                    ranking_threshold=0.1) -> List[Dict]:
        """Search for restaurants and collect reviews with dynamic city scaling."""
        
        # Get dynamic limits for this city
        limits = self.get_dynamic_limits(city, cuisine)
        max_results = limits["max_restaurants"]
        review_limit = limits["review_limit"]
        min_rating = limits["min_rating"]
        min_reviews = limits["min_reviews"]
        
        cache_key = f"restaurants_with_reviews_dynamic:{city}:{cuisine}:{max_results}:{review_limit}"
        
        # Check cache first
        cached_result = await self.cache_manager.get(cache_key)
        if cached_result and not incremental:
            app_logger.info(f"Using cached dynamic restaurant data for {city}, {cuisine}")
            return cached_result
        
        app_logger.info(f" Dynamic collection: {city} + {cuisine}")
        app_logger.info(f"📊 Limits: {max_results} restaurants, {review_limit} reviews each")
        app_logger.info(f"⭐ Quality: min rating {min_rating}, min reviews {min_reviews}")
        
        # Step 1: Get restaurants with dynamic limits
        restaurants = await self.search_restaurants_dynamic(city, cuisine)
        
        if not restaurants:
            app_logger.warning(f"No restaurants found for {cuisine} in {city}")
            return []
        
        # Step 2: Apply quality filters
        quality_restaurants = []
        for restaurant in restaurants:
            rating = restaurant.get('rating', 0)
            review_count = restaurant.get('review_count', 0)
            
            if rating >= min_rating and review_count >= min_reviews:
                quality_restaurants.append(restaurant)
        
        app_logger.info(f"✅ Quality filtered: {len(quality_restaurants)}/{len(restaurants)} restaurants meet criteria")
        
        # Step 3: Re-rank restaurants with fresh data
        quality_restaurants = await self._rerank_restaurants_with_fresh_data(quality_restaurants)
        
        # Step 4: Select top restaurants for review collection
        candidates = [r for r in quality_restaurants if r.get('reviews_data_id')]
        if not candidates:
            candidates = [r for r in quality_restaurants if r.get('source') == 'google_maps'] or quality_restaurants
        
        top_restaurants = candidates[:review_limit]
        app_logger.info(f"🏆 Selected top {len(top_restaurants)} restaurants for review collection:")
        
        for i, restaurant in enumerate(top_restaurants, 1):
            app_logger.info(f"{i}. {restaurant['restaurant_name']} - "
                           f"Rating: {restaurant.get('rating')}, "
                           f"Reviews: {restaurant.get('review_count')}, "
                           f"Quality Score: {restaurant.get('quality_score', 0):.2f}")
        
        # Step 5: Collect reviews with increased limit (20 per restaurant)
        restaurants_with_reviews = []
        
        for restaurant in top_restaurants:
            app_logger.info(f" Collecting reviews for: {restaurant['restaurant_name']}")
            
            try:
                # Fetch reviews with increased limit
                reviews = await self.get_restaurant_reviews(restaurant, max_reviews=self.reviews_per_restaurant)
                
                # Add reviews to the restaurant data
                restaurant['reviews'] = reviews
                restaurant['reviews_collected'] = len(reviews)
                
                app_logger.info(f"✅ Collected {len(reviews)} reviews for {restaurant['restaurant_name']}")
                restaurants_with_reviews.append(restaurant)
                
                # Rate limiting between restaurants
                await asyncio.sleep(2)
                
            except Exception as e:
                app_logger.error(f"❌ Error collecting reviews for {restaurant['restaurant_name']}: {e}")
                # Still add the restaurant but mark review collection as failed
                restaurant['reviews'] = []
                restaurant['reviews_collected'] = 0
                restaurant['review_error'] = str(e)
                restaurants_with_reviews.append(restaurant)
        
        # Cache the results
        await self.cache_manager.set(cache_key, restaurants_with_reviews, expire=3600)
        self._track_api_call()
        
        app_logger.info(f" Dynamic collection completed: {len(restaurants_with_reviews)} restaurants with reviews")
        return restaurants_with_reviews