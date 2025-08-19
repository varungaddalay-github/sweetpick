"""
Yelp API integration for restaurant discovery with neighborhood data.
Uses the Yelp Search API to get detailed restaurant information including neighborhoods.
"""
import asyncio
import json
import time
from typing import List, Dict, Optional, Any
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential
from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.data_collection.cache_manager import CacheManager
from src.data_collection.neighborhood_coordinates import get_neighborhood_coordinates

class YelpCollector:
    """Collector for restaurant data using Yelp Search API with neighborhood support."""
    
    def __init__(self):
        self.settings = get_settings()
        self.cache_manager = CacheManager()
        self.api_calls = 0
        self.start_time = time.time()
        self.base_url = "https://api.yelp.com/v3"
        
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def search_restaurants(self, city: str, cuisine: str, max_results: int = 50, 
                               neighborhood: Optional[str] = None) -> List[Dict]:
        """Search for restaurants using Yelp Search API with neighborhood support."""
        cache_key = f"yelp_restaurants:{city}:{cuisine}:{max_results}:{neighborhood or 'all'}"
        
        # Check cache first
        cached_result = await self.cache_manager.get(cache_key)
        if cached_result:
            app_logger.info(f"Using cached Yelp data for {city}, {cuisine}, {neighborhood or 'all'}")
            return cached_result
        
        try:
            # Build search parameters
            search_params = {
                "term": f"{cuisine} restaurants",
                "location": self._build_location_string(city, neighborhood),
                "limit": min(max_results, 50),  # Yelp API limit is 50
                "sort_by": "rating",  # Sort by rating for better quality
                "price": "1,2,3,4",  # Include all price ranges
                "open_now": False,  # Don't filter by open status for broader results
                "radius": self._get_search_radius(neighborhood)
            }
            
            # Add category filter if cuisine maps to Yelp categories
            category_filter = self._get_yelp_category_filter(cuisine)
            if category_filter:
                search_params["categories"] = category_filter
            
            app_logger.info(f"🔍 Yelp API search: {search_params}")
            
            # Make API call
            restaurants = await self._make_yelp_api_call("/businesses/search", search_params)
            
            if not restaurants:
                app_logger.warning(f"No Yelp results for {cuisine} in {city} {neighborhood or ''}")
                return []
            
            # Process and enrich restaurant data
            processed_restaurants = []
            for restaurant in restaurants[:max_results]:
                processed = await self._process_restaurant_data(restaurant, city, cuisine, neighborhood)
                if processed and self._meets_criteria(processed):
                    processed_restaurants.append(processed)
            
            # Cache the results
            await self.cache_manager.set(cache_key, processed_restaurants, expire=3600)  # 1 hour
            self._track_api_call()
            
            app_logger.info(f"✅ Yelp API found {len(processed_restaurants)} restaurants for {cuisine} in {city} {neighborhood or ''}")
            return processed_restaurants
            
        except Exception as e:
            app_logger.error(f"Error in Yelp API search: {e}")
            return []
    
    async def get_restaurant_details(self, restaurant_id: str) -> Optional[Dict]:
        """Get detailed restaurant information including neighborhood data."""
        cache_key = f"yelp_restaurant_details:{restaurant_id}"
        
        # Check cache first
        cached_result = await self.cache_manager.get(cache_key)
        if cached_result:
            return cached_result
        
        try:
            restaurant_data = await self._make_yelp_api_call(f"/businesses/{restaurant_id}")
            
            if restaurant_data:
                # Cache the result
                await self.cache_manager.set(cache_key, restaurant_data, expire=7200)  # 2 hours
                self._track_api_call()
                
                return restaurant_data
            
        except Exception as e:
            app_logger.error(f"Error getting Yelp restaurant details: {e}")
        
        return None
    
    async def search_by_neighborhood(self, city: str, neighborhood: str, cuisine: Optional[str] = None, 
                                   max_results: int = 30) -> List[Dict]:
        """Search for restaurants in a specific neighborhood."""
        search_term = f"{cuisine} restaurants" if cuisine else "restaurants"
        
        try:
            # Get neighborhood coordinates for more precise search
            coords = get_neighborhood_coordinates(city, neighborhood)
            
            search_params = {
                "term": search_term,
                "latitude": coords.get("lat", 0),
                "longitude": coords.get("lng", 0),
                "radius": 1000,  # 1km radius for neighborhood search
                "limit": min(max_results, 50),
                "sort_by": "rating"
            }
            
            app_logger.info(f"🔍 Yelp neighborhood search: {neighborhood} in {city}")
            
            restaurants = await self._make_yelp_api_call("/businesses/search", search_params)
            
            if not restaurants:
                return []
            
            # Process restaurants with neighborhood context
            processed_restaurants = []
            for restaurant in restaurants[:max_results]:
                processed = await self._process_restaurant_data(restaurant, city, cuisine or "general", neighborhood)
                if processed and self._meets_criteria(processed):
                    processed_restaurants.append(processed)
            
            return processed_restaurants
            
        except Exception as e:
            app_logger.error(f"Error in Yelp neighborhood search: {e}")
            return []
    
    async def _make_yelp_api_call(self, endpoint: str, params: Optional[Dict] = None) -> Any:
        """Make a call to the Yelp API."""
        if not self.settings.yelp_api_key:
            app_logger.warning("Yelp API key not configured")
            return None
        
        headers = {
            "Authorization": f"Bearer {self.settings.yelp_api_key}",
            "Content-Type": "application/json"
        }
        
        url = f"{self.base_url}{endpoint}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("businesses", []) if endpoint.endswith("/search") else data
                    else:
                        app_logger.error(f"Yelp API error: {response.status} - {await response.text()}")
                        return None
                        
        except Exception as e:
            app_logger.error(f"Error making Yelp API call: {e}")
            return None
    
    async def _process_restaurant_data(self, restaurant: Dict, city: str, cuisine: str, 
                                     neighborhood: Optional[str] = None) -> Optional[Dict]:
        """Process raw Yelp restaurant data into our schema."""
        try:
            # Extract basic information
            restaurant_id = restaurant.get("id", "")
            name = restaurant.get("name", "")
            rating = restaurant.get("rating")
            review_count = restaurant.get("review_count", 0)
            price = restaurant.get("price", "")
            
            # Extract location information
            location = restaurant.get("location", {})
            address = location.get("address1", "")
            city_from_api = location.get("city", "")
            state = location.get("state", "")
            zip_code = location.get("zip_code", "")
            
            # Extract coordinates
            coordinates = restaurant.get("coordinates", {})
            latitude = coordinates.get("latitude", 0.0)
            longitude = coordinates.get("longitude", 0.0)
            
            # Extract categories
            categories = restaurant.get("categories", [])
            category_names = [cat.get("title", "") for cat in categories]
            
            # Determine neighborhood from Yelp data or use provided neighborhood
            detected_neighborhood = self._extract_neighborhood_from_yelp(restaurant, neighborhood)
            
            # Calculate quality score
            quality_score = self._calculate_quality_score(rating, review_count)
            
            # Determine cuisine type
            detected_cuisine = self._detect_cuisine_from_categories(category_names, cuisine)
            
            # Price range mapping
            price_range = len(price) if price else 2
            
            return {
                "restaurant_id": restaurant_id,
                "restaurant_name": name,
                "google_place_id": "",  # Not applicable for Yelp
                "full_address": f"{address}, {city_from_api}, {state} {zip_code}".strip(", "),
                "city": city_from_api or city,
                "neighborhood": detected_neighborhood,
                "latitude": float(latitude),
                "longitude": float(longitude),
                "cuisine_type": detected_cuisine,
                "rating": float(rating) if rating else None,
                "review_count": int(review_count),
                "price_range": price_range,
                "operating_hours": self._extract_hours(restaurant),
                "meal_types": self._determine_meal_types(restaurant),
                "fallback_tier": self._calculate_fallback_tier(rating or 0, review_count),
                "quality_score": quality_score,
                "source": "yelp",
                "yelp_url": restaurant.get("url", ""),
                "phone": restaurant.get("phone", ""),
                "categories": category_names,
                "is_closed": restaurant.get("is_closed", False),
                "distance": restaurant.get("distance", 0),
                "transactions": restaurant.get("transactions", [])
            }
            
        except Exception as e:
            app_logger.error(f"Error processing Yelp restaurant data: {e}")
            return None
    
    def _extract_neighborhood_from_yelp(self, restaurant: Dict, provided_neighborhood: Optional[str] = None) -> str:
        """Extract neighborhood information from Yelp data."""
        # First, check if we have a provided neighborhood
        if provided_neighborhood:
            return provided_neighborhood
        
        # Try to extract from Yelp location data
        location = restaurant.get("location", {})
        
        # Check for neighborhood in display_address
        display_address = location.get("display_address", [])
        for addr_part in display_address:
            # Look for common neighborhood indicators
            if any(keyword in addr_part.lower() for keyword in ["district", "heights", "square", "park", "village"]):
                return addr_part
        
        # Check for neighborhood in address
        address = location.get("address1", "")
        if address:
            # Extract potential neighborhood from address
            parts = address.split()
            for i, part in enumerate(parts):
                if part.lower() in ["street", "avenue", "boulevard", "drive"] and i > 0:
                    return parts[i-1]
        
        return ""
    
    def _detect_cuisine_from_categories(self, categories: List[str], fallback_cuisine: str) -> str:
        """Detect cuisine type from Yelp categories."""
        cuisine_mapping = {
            "italian": ["Italian", "Pizza", "Pasta"],
            "indian": ["Indian", "Pakistani", "Bangladeshi"],
            "chinese": ["Chinese", "Cantonese", "Szechuan", "Dim Sum"],
            "mexican": ["Mexican", "Tex-Mex", "Tacos"],
            "american": ["American", "Burgers", "Steakhouses", "BBQ"],
            "japanese": ["Japanese", "Sushi", "Ramen"],
            "thai": ["Thai"],
            "mediterranean": ["Mediterranean", "Greek", "Lebanese"],
            "french": ["French"],
            "spanish": ["Spanish", "Tapas"],
            "korean": ["Korean"],
            "vietnamese": ["Vietnamese"]
        }
        
        # Check if any category matches our cuisine mapping
        for cuisine, keywords in cuisine_mapping.items():
            if any(keyword.lower() in [cat.lower() for cat in categories] for keyword in keywords):
                return cuisine
        
        return fallback_cuisine
    
    def _calculate_quality_score(self, rating: Optional[float], review_count: int) -> float:
        """Calculate quality score based on rating and review count."""
        import math
        
        if not rating or review_count == 0:
            return 0.0
        
        # Quality score formula: rating * log(review_count + 1)
        return float(rating) * math.log(review_count + 1)
    
    def _calculate_fallback_tier(self, rating: float, review_count: int) -> int:
        """Calculate fallback tier based on rating and review count."""
        if rating >= 4.5 and review_count >= 1000:
            return 1
        elif rating >= 4.0 and review_count >= 500:
            return 2
        elif rating >= 3.5 and review_count >= 100:
            return 3
        else:
            return 4
    
    def _extract_hours(self, restaurant: Dict) -> Dict:
        """Extract operating hours from Yelp data."""
        hours = restaurant.get("hours", [])
        if hours:
            return hours[0].get("open", [])
        return {}
    
    def _determine_meal_types(self, restaurant: Dict) -> List[str]:
        """Determine meal types based on restaurant data."""
        meal_types = ["lunch", "dinner"]
        
        # Check if breakfast is available based on hours or categories
        categories = restaurant.get("categories", [])
        category_names = [cat.get("title", "").lower() for cat in categories]
        
        if any("breakfast" in cat or "brunch" in cat for cat in category_names):
            meal_types.insert(0, "breakfast")
        
        return meal_types
    
    def _build_location_string(self, city: str, neighborhood: Optional[str] = None) -> str:
        """Build location string for Yelp API search."""
        if neighborhood:
            return f"{neighborhood}, {city}"
        return city
    
    def _get_search_radius(self, neighborhood: Optional[str] = None) -> int:
        """Get appropriate search radius based on context."""
        if neighborhood:
            return 1000  # 1km for neighborhood searches
        return 5000  # 5km for city-wide searches
    
    def _get_yelp_category_filter(self, cuisine: str) -> Optional[str]:
        """Get Yelp category filter for cuisine type."""
        category_mapping = {
            "italian": "italian",
            "indian": "indpak",
            "chinese": "chinese",
            "mexican": "mexican",
            "japanese": "japanese",
            "thai": "thai",
            "american": "newamerican",
            "mediterranean": "mediterranean",
            "french": "french",
            "spanish": "spanish",
            "korean": "korean",
            "vietnamese": "vietnamese"
        }
        
        return category_mapping.get(cuisine.lower())
    
    def _meets_criteria(self, restaurant: Dict) -> bool:
        """Check if restaurant meets quality criteria."""
        rating = restaurant.get("rating", 0)
        review_count = restaurant.get("review_count", 0)
        
        # Basic quality filters
        if rating < 3.0:
            return False
        
        if review_count < 10:
            return False
        
        return True
    
    def _track_api_call(self):
        """Track API call for monitoring."""
        self.api_calls += 1
        if self.api_calls % 10 == 0:
            elapsed = time.time() - self.start_time
            rate = self.api_calls / elapsed if elapsed > 0 else 0
            app_logger.info(f"📊 Yelp API calls: {self.api_calls} (rate: {rate:.2f}/sec)")
