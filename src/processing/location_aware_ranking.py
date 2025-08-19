"""
Location-aware restaurant ranking system that considers location-specific factors.
"""
import asyncio
from typing import List, Dict, Optional, Any, Tuple
from src.utils.logger import app_logger
from src.vector_db.milvus_client import MilvusClient


class LocationAwareRanking:
    """Location-aware restaurant ranking and selection system."""
    
    def __init__(self):
        self.milvus_client = MilvusClient()
        from src.utils.neighborhood_mapper import neighborhood_mapper
        self.neighborhood_mapper = neighborhood_mapper
        
        # Location-specific ranking factors
        self.location_ranking_factors = {
            "Manhattan": {
                "cuisine_weights": {
                    "italian": 1.2,  # Manhattan is famous for Italian
                    "american": 1.3,  # NYC delis, bagels, street food
                    "chinese": 1.1,   # Chinatown
                    "japanese": 1.1,  # High-end sushi
                    "french": 1.2,    # Fine dining
                    "indian": 0.9     # Less prominent in Manhattan
                },
                "restaurant_types": {
                    "deli": 1.3,      # NYC delis are legendary
                    "pizzeria": 1.2,  # NY pizza is famous
                    "bagel_shop": 1.3, # NY bagels are world-famous
                    "street_food": 1.2, # NYC street food culture
                    "fine_dining": 1.1, # Manhattan has many
                    "fast_casual": 0.9  # Less emphasis
                },
                "neighborhood_weights": {
                    "chinatown": 1.2,    # For Chinese food
                    "little_italy": 1.3,  # For Italian food
                    "midtown": 1.1,      # Tourist area
                    "upper_east_side": 1.2, # Fine dining
                    "lower_east_side": 1.1, # Trendy
                    "chelsea": 1.1,      # Trendy
                    "west_village": 1.2   # Charming
                },
                "iconic_indicators": [
                    "new york", "nyc", "manhattan", "brooklyn",
                    "authentic", "original", "classic", "traditional",
                    "since", "established", "founded"
                ]
            },
            "Jersey City": {
                "cuisine_weights": {
                    "indian": 1.3,     # Large Indian community
                    "portuguese": 1.2,  # Portuguese community
                    "latin": 1.2,       # Latin American community
                    "italian": 1.0,     # Standard
                    "american": 1.0,    # Standard
                    "chinese": 1.0      # Standard
                },
                "restaurant_types": {
                    "ethnic_restaurant": 1.2,  # Diverse immigrant cuisines
                    "family_owned": 1.2,       # Many family businesses
                    "diner": 1.1,              # Classic diners
                    "pizzeria": 1.0,           # Standard
                    "fine_dining": 0.9         # Less emphasis
                },
                "neighborhood_weights": {
                    "journal_square": 1.2,     # Indian restaurants
                    "downtown_jc": 1.1,        # Upscale
                    "the_heights": 1.1,        # Diverse
                    "greenville": 1.0,         # Standard
                    "west_side": 1.0           # Standard
                },
                "iconic_indicators": [
                    "jersey city", "jc", "new jersey", "nj",
                    "authentic", "traditional", "homemade",
                    "family", "since", "established"
                ]
            },
            "Hoboken": {
                "cuisine_weights": {
                    "italian": 1.3,    # Strong Italian heritage
                    "american": 1.1,   # Bar food, college town
                    "seafood": 1.1,    # Waterfront
                    "indian": 0.9,     # Less prominent
                    "chinese": 0.9     # Less prominent
                },
                "restaurant_types": {
                    "italian_deli": 1.3,    # Hoboken is famous for Italian delis
                    "pizzeria": 1.2,        # Good pizza scene
                    "bar": 1.2,             # College town bars
                    "seafood": 1.1,         # Waterfront dining
                    "fine_dining": 1.0      # Standard
                },
                "neighborhood_weights": {
                    "washington_street": 1.2,  # Main commercial area
                    "waterfront": 1.1,         # Seafood, views
                    "uptown": 1.0,             # Standard
                    "downtown": 1.0            # Standard
                },
                "iconic_indicators": [
                    "hoboken", "hoboken nj", "new jersey",
                    "italian", "authentic", "traditional",
                    "since", "established", "family"
                ]
            }
        }
    
    def rank_restaurants_by_location(self, restaurants: List[Dict], location: str, 
                                   cuisine: str = None, dish_name: str = None) -> List[Dict]:
        """Rank restaurants considering location-specific factors."""
        
        if not restaurants:
            return []
        
        app_logger.info(f"🏙️ Location-aware ranking for {len(restaurants)} restaurants in {location}")
        
        # Get location-specific factors
        location_factors = self.location_ranking_factors.get(location, {})
        
        # Calculate location-aware scores
        ranked_restaurants = []
        for restaurant in restaurants:
            location_score = self._calculate_location_score(restaurant, location, location_factors, cuisine, dish_name)
            
            # Combine with existing quality score
            original_score = restaurant.get('quality_score', 0.5)
            combined_score = (original_score * 0.7) + (location_score * 0.3)
            
            ranked_restaurant = {
                **restaurant,
                'location_score': location_score,
                'combined_score': combined_score,
                'location_ranking_factors': self._get_ranking_factors(restaurant, location_factors)
            }
            ranked_restaurants.append(ranked_restaurant)
        
        # Sort by combined score
        ranked_restaurants.sort(key=lambda x: x['combined_score'], reverse=True)
        
        app_logger.info(f"🏙️ Location ranking completed. Top score: {ranked_restaurants[0]['combined_score']:.3f}")
        return ranked_restaurants
    
    def rank_restaurants_by_neighborhood(self, restaurants: List[Dict], neighborhood: str, 
                                       city: str, cuisine: str = None, dish_name: str = None) -> List[Dict]:
        """Rank restaurants considering neighborhood-specific factors."""
        
        # Get neighborhood data
        neighborhood_data = self.neighborhood_mapper.get_neighborhood_by_name(neighborhood, city)
        if not neighborhood_data:
            app_logger.warning(f"⚠️ Neighborhood {neighborhood} not found for {city}")
            return self.rank_restaurants_by_location(restaurants, city, cuisine, dish_name)
        
        app_logger.info(f"🏙️ Neighborhood-aware ranking for {len(restaurants)} restaurants in {neighborhood}, {city}")
        
        # Get neighborhood-specific ranking factors
        neighborhood_factors = self.neighborhood_mapper.get_neighborhood_ranking_factors(neighborhood_data)
        
        # Calculate neighborhood-aware scores
        ranked_restaurants = []
        for restaurant in restaurants:
            neighborhood_score = self._calculate_neighborhood_score(
                restaurant, neighborhood_data, neighborhood_factors, cuisine, dish_name
            )
            
            # Combine with existing quality score
            original_score = restaurant.get('quality_score', 0.5)
            combined_score = (original_score * 0.6) + (neighborhood_score * 0.4)
            
            ranked_restaurant = {
                **restaurant,
                'neighborhood_score': neighborhood_score,
                'combined_score': combined_score,
                'neighborhood': neighborhood,
                'neighborhood_context': self.neighborhood_mapper.get_neighborhood_context(neighborhood_data)
            }
            ranked_restaurants.append(ranked_restaurant)
        
        # Sort by combined score
        ranked_restaurants.sort(key=lambda x: x['combined_score'], reverse=True)
        
        app_logger.info(f"🏙️ Neighborhood ranking completed. Top score: {ranked_restaurants[0]['combined_score']:.3f}")
        return ranked_restaurants
    
    def _calculate_neighborhood_score(self, restaurant: Dict, neighborhood: 'Neighborhood', 
                                    neighborhood_factors: Dict, cuisine: str = None, dish_name: str = None) -> float:
        """Calculate neighborhood-specific score for a restaurant."""
        
        score = 0.5  # Base score
        
        # Factor 1: Cuisine relevance to neighborhood (35% weight)
        if cuisine and cuisine.lower() in [c.lower() for c in neighborhood.cuisine_focus]:
            cuisine_index = [c.lower() for c in neighborhood.cuisine_focus].index(cuisine.lower())
            cuisine_weight = 1.3 - (cuisine_index * 0.1)  # Primary cuisines get higher weight
            score += (cuisine_weight - 1.0) * 0.35
        
        # Factor 2: Restaurant type relevance (25% weight)
        restaurant_type = self._identify_restaurant_type(restaurant)
        if restaurant_type and restaurant_type in neighborhood.restaurant_types:
            score += 0.25
        
        # Factor 3: Iconic dish relevance (20% weight)
        if dish_name and any(dish_name.lower() in iconic.lower() for iconic in neighborhood.iconic_dishes):
            score += 0.20
        
        # Factor 4: Authenticity indicators (15% weight)
        iconic_indicators = neighborhood_factors.get('iconic_indicators', [])
        authenticity_score = self._calculate_iconic_score(restaurant, {'iconic_indicators': iconic_indicators})
        score += authenticity_score * 0.15
        
        # Factor 5: Price level match (5% weight)
        if self._matches_price_level(restaurant, neighborhood.price_level):
            score += 0.05
        
        return min(max(score, 0.0), 1.0)  # Clamp between 0 and 1
    
    def _matches_price_level(self, restaurant: Dict, neighborhood_price_level: str) -> bool:
        """Check if restaurant price level matches neighborhood expectation."""
        # This is a simplified check - in practice, you'd use actual price data
        restaurant_name = restaurant.get('restaurant_name', '').lower()
        
        price_indicators = {
            'budget': ['pizza', 'deli', 'diner', 'fast', 'chain'],
            'moderate': ['restaurant', 'bistro', 'cafe'],
            'upscale': ['fine', 'elegant', 'sophisticated', 'gourmet'],
            'luxury': ['luxury', 'exclusive', 'premium']
        }
        
        indicators = price_indicators.get(neighborhood_price_level, [])
        return any(indicator in restaurant_name for indicator in indicators)
    
    def _calculate_location_score(self, restaurant: Dict, location: str, 
                                location_factors: Dict, cuisine: str = None, dish_name: str = None) -> float:
        """Calculate location-specific score for a restaurant."""
        
        score = 0.5  # Base score
        
        # Factor 1: Cuisine relevance (30% weight)
        if cuisine and 'cuisine_weights' in location_factors:
            cuisine_weight = location_factors['cuisine_weights'].get(cuisine.lower(), 1.0)
            score += (cuisine_weight - 1.0) * 0.3
        
        # Factor 2: Restaurant type relevance (25% weight)
        restaurant_type = self._identify_restaurant_type(restaurant)
        if restaurant_type and 'restaurant_types' in location_factors:
            type_weight = location_factors['restaurant_types'].get(restaurant_type, 1.0)
            score += (type_weight - 1.0) * 0.25
        
        # Factor 3: Neighborhood relevance (20% weight)
        neighborhood = self._extract_neighborhood(restaurant, location)
        if neighborhood and 'neighborhood_weights' in location_factors:
            neighborhood_weight = location_factors['neighborhood_weights'].get(neighborhood.lower(), 1.0)
            score += (neighborhood_weight - 1.0) * 0.2
        
        # Factor 4: Iconic indicators (15% weight)
        iconic_score = self._calculate_iconic_score(restaurant, location_factors)
        score += iconic_score * 0.15
        
        # Factor 5: Dish-specific relevance (10% weight)
        if dish_name:
            dish_relevance = self._calculate_dish_relevance(restaurant, dish_name, location)
            score += dish_relevance * 0.1
        
        return min(max(score, 0.0), 1.0)  # Clamp between 0 and 1
    
    def _identify_restaurant_type(self, restaurant: Dict) -> Optional[str]:
        """Identify the type of restaurant based on name and description."""
        
        name = restaurant.get('restaurant_name', '').lower()
        description = restaurant.get('description', '').lower()
        text = f"{name} {description}"
        
        type_indicators = {
            'deli': ['deli', 'deli', 'sandwich', 'pastrami', 'corned beef'],
            'pizzeria': ['pizza', 'pizzeria', 'slice', 'pie'],
            'bagel_shop': ['bagel', 'bagels', 'lox', 'cream cheese'],
            'street_food': ['street', 'cart', 'food truck', 'halal'],
            'fine_dining': ['fine dining', 'upscale', 'elegant', 'sophisticated'],
            'ethnic_restaurant': ['authentic', 'traditional', 'homemade'],
            'family_owned': ['family', 'owned', 'since', 'established'],
            'bar': ['bar', 'pub', 'tavern', 'brewery'],
            'seafood': ['seafood', 'fish', 'lobster', 'crab'],
            'italian_deli': ['italian deli', 'sub', 'hero', 'italian sandwich']
        }
        
        for restaurant_type, indicators in type_indicators.items():
            if any(indicator in text for indicator in indicators):
                return restaurant_type
        
        return None
    
    def _extract_neighborhood(self, restaurant: Dict, location: str) -> Optional[str]:
        """Extract neighborhood information from restaurant data."""
        
        name = restaurant.get('restaurant_name', '').lower()
        address = restaurant.get('address', '').lower()
        description = restaurant.get('description', '').lower()
        
        # Manhattan neighborhoods
        manhattan_neighborhoods = [
            'chinatown', 'little italy', 'midtown', 'upper east side', 
            'upper west side', 'lower east side', 'chelsea', 'west village',
            'east village', 'soho', 'tribeca', 'financial district'
        ]
        
        # Jersey City neighborhoods
        jc_neighborhoods = [
            'journal square', 'downtown jc', 'the heights', 'greenville', 
            'west side', 'bergen-lafayette', 'mcginley square'
        ]
        
        # Hoboken neighborhoods
        hoboken_neighborhoods = [
            'washington street', 'waterfront', 'uptown', 'downtown',
            'hoboken', 'hoboken nj'
        ]
        
        all_neighborhoods = manhattan_neighborhoods + jc_neighborhoods + hoboken_neighborhoods
        
        for neighborhood in all_neighborhoods:
            if neighborhood in name or neighborhood in address or neighborhood in description:
                return neighborhood
        
        return None
    
    def _calculate_iconic_score(self, restaurant: Dict, location_factors: Dict) -> float:
        """Calculate score based on iconic indicators."""
        
        name = restaurant.get('restaurant_name', '').lower()
        description = restaurant.get('description', '').lower()
        text = f"{name} {description}"
        
        iconic_indicators = location_factors.get('iconic_indicators', [])
        
        score = 0.0
        for indicator in iconic_indicators:
            if indicator in text:
                score += 0.1
        
        return min(score, 1.0)
    
    def _calculate_dish_relevance(self, restaurant: Dict, dish_name: str, location: str) -> float:
        """Calculate relevance score for a specific dish."""
        
        # Check if restaurant has this dish
        dishes = restaurant.get('dishes', [])
        dish_names = [dish.get('dish_name', '').lower() for dish in dishes]
        
        dish_lower = dish_name.lower()
        
        # Exact match
        if dish_lower in dish_names:
            return 1.0
        
        # Partial match
        for dish in dish_names:
            if dish_lower in dish or dish in dish_lower:
                return 0.8
        
        # Location-specific dish matching
        location_dish_mapping = {
            "Manhattan": {
                "pizza": ["pizza", "slice", "pie", "margherita", "pepperoni"],
                "bagel": ["bagel", "lox", "cream cheese", "everything"],
                "deli": ["pastrami", "corned beef", "sandwich", "deli"]
            },
            "Jersey City": {
                "biryani": ["biryani", "chicken biryani", "mutton biryani"],
                "curry": ["curry", "chicken curry", "lamb curry"],
                "pizza": ["pizza", "jersey style", "thin crust"]
            },
            "Hoboken": {
                "italian sub": ["sub", "hero", "italian", "sandwich"],
                "pizza": ["pizza", "hoboken", "thin crust"],
                "seafood": ["seafood", "lobster", "fish", "clam"]
            }
        }
        
        location_dishes = location_dish_mapping.get(location, {})
        for category, related_dishes in location_dishes.items():
            if dish_lower in category or any(related in dish_lower for related in related_dishes):
                return 0.6
        
        return 0.0
    
    def _get_ranking_factors(self, restaurant: Dict, location_factors: Dict) -> Dict:
        """Get the ranking factors that influenced this restaurant's score."""
        
        factors = {}
        
        # Cuisine factor
        cuisine = restaurant.get('cuisine_type', '').lower()
        if cuisine and 'cuisine_weights' in location_factors:
            factors['cuisine_weight'] = location_factors['cuisine_weights'].get(cuisine, 1.0)
        
        # Restaurant type factor
        restaurant_type = self._identify_restaurant_type(restaurant)
        if restaurant_type and 'restaurant_types' in location_factors:
            factors['restaurant_type'] = restaurant_type
            factors['type_weight'] = location_factors['restaurant_types'].get(restaurant_type, 1.0)
        
        # Neighborhood factor
        neighborhood = self._extract_neighborhood(restaurant, restaurant.get('city', ''))
        if neighborhood and 'neighborhood_weights' in location_factors:
            factors['neighborhood'] = neighborhood
            factors['neighborhood_weight'] = location_factors['neighborhood_weights'].get(neighborhood.lower(), 1.0)
        
        return factors
    
    async def get_location_ranking_stats(self, location: str) -> Dict[str, Any]:
        """Get statistics about location-aware ranking for a location."""
        
        try:
            # Get restaurants in this location
            restaurants = await self.milvus_client.search_restaurants_with_filters({
                "city": location
            }, max_results=50)
            
            if not restaurants:
                return {"location": location, "error": "No restaurants found"}
            
            # Rank them
            ranked_restaurants = await self.rank_restaurants_by_location(restaurants, location)
            
            # Calculate statistics
            location_scores = [r['location_score'] for r in ranked_restaurants]
            combined_scores = [r['combined_score'] for r in ranked_restaurants]
            
            # Analyze ranking factors
            factor_analysis = {}
            for restaurant in ranked_restaurants[:10]:  # Top 10
                factors = restaurant.get('location_ranking_factors', {})
                for factor, value in factors.items():
                    if factor not in factor_analysis:
                        factor_analysis[factor] = []
                    factor_analysis[factor].append(value)
            
            return {
                "location": location,
                "total_restaurants": len(restaurants),
                "avg_location_score": sum(location_scores) / len(location_scores),
                "avg_combined_score": sum(combined_scores) / len(combined_scores),
                "top_location_score": max(location_scores),
                "factor_analysis": factor_analysis
            }
            
        except Exception as e:
            app_logger.error(f"Error getting location ranking stats for {location}: {e}")
            return {"location": location, "error": str(e)}
