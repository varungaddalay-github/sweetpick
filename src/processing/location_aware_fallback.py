"""
Location-aware fallback system that provides intelligent alternatives when famous dishes aren't available.
"""
import asyncio
from typing import List, Dict, Optional, Any, Tuple
from src.utils.logger import app_logger
from src.vector_db.milvus_client import MilvusClient


class LocationAwareFallback:
    """Provides intelligent fallbacks when location-specific dishes aren't available."""
    
    def __init__(self, retrieval_engine=None):
        self.milvus_client = MilvusClient()
        self.retrieval_engine = retrieval_engine
        
        # Location-specific fallback strategies
        self.fallback_strategies = {
            "Manhattan": {
                "pizza": {
                    "primary": ["New York Pizza", "Margherita Pizza", "Pepperoni Pizza"],
                    "fallback": ["Pizza", "Italian Food", "Pasta"],
                    "context": "Manhattan is famous for New York style pizza"
                },
                "bagel": {
                    "primary": ["Everything Bagel", "Lox Bagel", "Cream Cheese Bagel"],
                    "fallback": ["Bagel", "Breakfast", "Sandwich"],
                    "context": "Manhattan bagels are world-famous"
                },
                "deli": {
                    "primary": ["Pastrami Sandwich", "Corned Beef Sandwich"],
                    "fallback": ["Sandwich", "Deli Food", "American Food"],
                    "context": "Manhattan delis are legendary"
                }
            },
            "Jersey City": {
                "indian": {
                    "primary": ["Chicken Biryani", "Mutton Biryani", "Vegetable Biryani"],
                    "fallback": ["Biryani", "Curry", "Indian Food"],
                    "context": "Jersey City has excellent Indian cuisine"
                },
                "pizza": {
                    "primary": ["Jersey Style Pizza", "Thin Crust Pizza"],
                    "fallback": ["Pizza", "Italian Food"],
                    "context": "Jersey City has great local pizza"
                }
            },
            "Hoboken": {
                "italian": {
                    "primary": ["Italian Sub", "Hero Sandwich", "Meatball Sub"],
                    "fallback": ["Sub", "Sandwich", "Italian Food"],
                    "context": "Hoboken has excellent Italian delis"
                },
                "pizza": {
                    "primary": ["Hoboken Pizza", "Thin Crust Pizza"],
                    "fallback": ["Pizza", "Italian Food"],
                    "context": "Hoboken pizza is a local favorite"
                }
            }
        }
    
    async def get_available_location_dishes(self, dish: str, location: str, cuisine: str) -> List[str]:
        """Get location-specific dishes that are actually available in our data."""
        
        app_logger.info(f"🔍 Checking availability for '{dish}' in {location}")
        
        # Get location-specific expansions
        from src.processing.hybrid_dish_extractor import HybridDishExtractor
        hybrid_extractor = HybridDishExtractor()
        all_expansions = hybrid_extractor.get_location_aware_expansions(dish, location, cuisine)
        
        # Check which dishes are actually available
        available_dishes = []
        unavailable_dishes = []
        
        for expansion in all_expansions:
            try:
                # Search for this dish in our data using retrieval engine
                if self.retrieval_engine:
                    dishes = await self.retrieval_engine._search_dishes_by_name_and_location(
                        expansion, location, max_results=1
                    )
                else:
                    dishes = []
                
                if dishes:
                    available_dishes.append(expansion)
                    app_logger.debug(f"✅ Found: {expansion}")
                else:
                    unavailable_dishes.append(expansion)
                    app_logger.debug(f"❌ Not found: {expansion}")
                    
            except Exception as e:
                app_logger.error(f"Error checking availability for {expansion}: {e}")
                unavailable_dishes.append(expansion)
        
        app_logger.info(f"Available: {len(available_dishes)}, Unavailable: {len(unavailable_dishes)}")
        return available_dishes, unavailable_dishes
    
    async def get_intelligent_fallback(self, dish: str, location: str, cuisine: str) -> Dict[str, Any]:
        """Get intelligent fallback when location-specific dishes aren't available."""
        
        # Check what's available
        available_dishes, unavailable_dishes = await self.get_available_location_dishes(dish, location, cuisine)
        
        # Get fallback strategy for this location and dish type
        strategy = self._get_fallback_strategy(dish, location, cuisine)
        
        if available_dishes:
            # We have some location-specific dishes available
            return {
                "type": "location_specific_available",
                "available_dishes": available_dishes,
                "unavailable_dishes": unavailable_dishes,
                "recommendations": available_dishes[:3],  # Top 3 available
                "message": f"Found {len(available_dishes)} {location}-specific {dish} options",
                "fallback_needed": False
            }
        else:
            # No location-specific dishes available, use intelligent fallback
            return await self._generate_intelligent_fallback(dish, location, cuisine, strategy)
    
    async def _generate_intelligent_fallback(self, dish: str, location: str, cuisine: str, strategy: Dict) -> Dict[str, Any]:
        """Generate intelligent fallback when location-specific dishes aren't available."""
        
        app_logger.info(f"🔄 Generating intelligent fallback for '{dish}' in {location}")
        
        # Try fallback dish names
        fallback_dishes = strategy.get("fallback", [dish])
        available_fallbacks = []
        
        for fallback_dish in fallback_dishes:
            try:
                if self.retrieval_engine:
                    dishes = await self.retrieval_engine._search_dishes_by_name_and_location(
                        fallback_dish, location, max_results=5
                    )
                else:
                    dishes = []
                if dishes:
                    available_fallbacks.extend([d["dish_name"] for d in dishes])
            except Exception as e:
                app_logger.error(f"Error checking fallback {fallback_dish}: {e}")
        
        # If still no results, try cuisine-based search
        if not available_fallbacks:
            try:
                # Search for any dishes of this cuisine in the location
                restaurants = self.milvus_client.search_restaurants_with_filters({
                    "city": location,
                    "cuisine_type": cuisine
                }, limit=3)
                
                if restaurants:
                    # Get dishes from these restaurants
                    for restaurant in restaurants:
                        restaurant_dishes = self.retrieval_engine.get_restaurant_dishes(
                            restaurant["restaurant_id"], limit=2
                        )
                        available_fallbacks.extend([d["dish_name"] for d in restaurant_dishes])
            except Exception as e:
                app_logger.error(f"Error in cuisine-based fallback: {e}")
        
        # Remove duplicates and limit results
        unique_fallbacks = list(dict.fromkeys(available_fallbacks))[:5]
        
        return {
            "type": "intelligent_fallback",
            "original_dish": dish,
            "location": location,
            "cuisine": cuisine,
            "available_fallbacks": unique_fallbacks,
            "context": strategy.get("context", f"Great {cuisine} food in {location}"),
            "message": self._generate_fallback_message(dish, location, cuisine, unique_fallbacks),
            "fallback_needed": True
        }
    
    def _get_fallback_strategy(self, dish: str, location: str, cuisine: str) -> Dict:
        """Get fallback strategy for this location and dish type."""
        
        location_strategies = self.fallback_strategies.get(location, {})
        
        # Try exact dish match first
        if dish in location_strategies:
            return location_strategies[dish]
        
        # Try cuisine match
        if cuisine in location_strategies:
            return location_strategies[cuisine]
        
        # Default strategy
        return {
            "primary": [dish],
            "fallback": [dish, f"{cuisine} Food"],
            "context": f"Great {cuisine} food in {location}"
        }
    
    def _generate_fallback_message(self, dish: str, location: str, cuisine: str, fallbacks: List[str]) -> str:
        """Generate user-friendly fallback message."""
        
        if not fallbacks:
            return f"While we don't have specific {dish} data for {location}, we can help you find great {cuisine} restaurants in the area."
        
        if len(fallbacks) == 1:
            return f"While we don't have {dish} specifically, we found excellent {fallbacks[0]} in {location}."
        else:
            fallback_list = ", ".join(fallbacks[:-1]) + f" and {fallbacks[-1]}"
            return f"While we don't have {dish} specifically, we found great options like {fallback_list} in {location}."
    
    async def get_location_context_suggestions(self, location: str, cuisine: str) -> List[str]:
        """Get location-specific suggestions for a cuisine."""
        
        location_contexts = {
            "Manhattan": {
                "italian": ["New York Pizza", "Italian Deli", "Pasta", "Gelato"],
                "american": ["Bagel", "Deli Sandwich", "Street Food", "Cheesecake"],
                "chinese": ["Dim Sum", "Peking Duck", "Hot Pot", "Dumplings"],
                "indian": ["Biryani", "Curry", "Tandoori", "Naan"]
            },
            "Jersey City": {
                "italian": ["Pizza", "Pasta", "Italian Deli", "Gelato"],
                "indian": ["Biryani", "Curry", "Tandoori", "Naan"],
                "portuguese": ["Seafood", "Bacalhau", "Grilled Sardines"],
                "latin": ["Empanadas", "Arepas", "Ceviche", "Tacos"]
            },
            "Hoboken": {
                "italian": ["Italian Sub", "Pizza", "Pasta", "Gelato"],
                "american": ["Bar Food", "Burgers", "Wings", "Nachos"],
                "seafood": ["Lobster Roll", "Clam Chowder", "Fish and Chips"]
            }
        }
        
        return location_contexts.get(location, {}).get(cuisine, [])
    
    async def get_availability_stats(self, location: str) -> Dict[str, Any]:
        """Get statistics about dish availability in a location."""
        
        try:
            # Get total restaurants in location
            restaurants = await self.milvus_client.search_restaurants_with_filters({
                "city": location
            }, max_results=100)
            
            # Get total dishes in location
            total_dishes = 0
            cuisine_breakdown = {}
            
            for restaurant in restaurants[:10]:  # Sample first 10 restaurants
                dishes = self.retrieval_engine.get_restaurant_dishes(
                    restaurant["restaurant_id"], max_results=50
                )
                total_dishes += len(dishes)
                
                cuisine = restaurant.get("cuisine_type", "Unknown")
                cuisine_breakdown[cuisine] = cuisine_breakdown.get(cuisine, 0) + len(dishes)
            
            return {
                "location": location,
                "total_restaurants": len(restaurants),
                "total_dishes": total_dishes,
                "cuisine_breakdown": cuisine_breakdown,
                "sample_size": min(10, len(restaurants))
            }
            
        except Exception as e:
            app_logger.error(f"Error getting availability stats for {location}: {e}")
            return {
                "location": location,
                "error": str(e)
            }
