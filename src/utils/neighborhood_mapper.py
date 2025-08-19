"""
Neighborhood mapping system for large cities to improve location-aware recommendations.
"""
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from src.utils.logger import app_logger


@dataclass
class Neighborhood:
    """Represents a neighborhood within a city."""
    name: str
    city: str
    description: str
    cuisine_focus: List[str]  # What cuisines this neighborhood is known for
    restaurant_types: List[str]  # Types of restaurants common here
    iconic_dishes: List[str]  # Famous dishes from this area
    tourist_factor: float  # 0.0 to 1.0, how touristy the area is
    price_level: str  # "budget", "moderate", "upscale", "luxury"
    coordinates: Optional[Tuple[float, float]] = None  # lat, lng if available


class NeighborhoodMapper:
    """Maps large cities into smaller, manageable neighborhoods."""
    
    def __init__(self):
        self.neighborhoods = self._initialize_neighborhoods()
        self.city_neighborhoods = self._group_by_city()
    
    def _initialize_neighborhoods(self) -> List[Neighborhood]:
        """Initialize neighborhood data for supported cities."""
        
        return [
            # Manhattan Neighborhoods
            Neighborhood(
                name="Times Square",
                city="Manhattan",
                description="Tourist hub with Broadway theaters, bright lights, and chain restaurants",
                cuisine_focus=["american", "italian", "chinese", "fast_food"],
                restaurant_types=["chain_restaurant", "tourist_restaurant", "theater_district", "fast_casual"],
                iconic_dishes=["Broadway Burger", "Times Square Pizza", "Tourist Hot Dog"],
                tourist_factor=0.95,
                price_level="moderate"
            ),
            Neighborhood(
                name="Hell's Kitchen",
                city="Manhattan",
                description="Trendy neighborhood with diverse restaurants, popular with young professionals",
                cuisine_focus=["american", "italian", "thai", "mexican", "mediterranean"],
                restaurant_types=["trendy_restaurant", "gastropub", "wine_bar", "ethnic_restaurant"],
                iconic_dishes=["Hell's Kitchen Burger", "Artisan Pizza", "Craft Cocktails"],
                tourist_factor=0.3,
                price_level="moderate"
            ),
            Neighborhood(
                name="Little Italy",
                city="Manhattan",
                description="Historic Italian neighborhood with authentic restaurants and bakeries",
                cuisine_focus=["italian"],
                restaurant_types=["italian_restaurant", "pizzeria", "bakery", "deli", "family_owned"],
                iconic_dishes=["Margherita Pizza", "Cannoli", "Italian Sub", "Pasta Carbonara"],
                tourist_factor=0.7,
                price_level="moderate"
            ),
            Neighborhood(
                name="Chinatown",
                city="Manhattan",
                description="Authentic Chinese neighborhood with traditional restaurants and markets",
                cuisine_focus=["chinese", "dim_sum", "seafood"],
                restaurant_types=["chinese_restaurant", "dim_sum_house", "seafood_restaurant", "bubble_tea"],
                iconic_dishes=["Dim Sum", "Peking Duck", "Hot Pot", "Bubble Tea"],
                tourist_factor=0.6,
                price_level="budget"
            ),
            Neighborhood(
                name="Upper East Side",
                city="Manhattan",
                description="Affluent residential area with upscale restaurants and cafes",
                cuisine_focus=["french", "italian", "american", "japanese"],
                restaurant_types=["fine_dining", "cafe", "wine_bar", "upscale_restaurant"],
                iconic_dishes=["Upscale Burger", "French Cuisine", "Artisan Coffee"],
                tourist_factor=0.2,
                price_level="upscale"
            ),
            Neighborhood(
                name="Lower East Side",
                city="Manhattan",
                description="Historic Jewish neighborhood with delis and trendy new restaurants",
                cuisine_focus=["jewish", "american", "fusion", "asian"],
                restaurant_types=["deli", "jewish_restaurant", "trendy_restaurant", "fusion"],
                iconic_dishes=["Pastrami Sandwich", "Bagel with Lox", "Jewish Deli"],
                tourist_factor=0.4,
                price_level="moderate"
            ),
            Neighborhood(
                name="Chelsea",
                city="Manhattan",
                description="Art district with diverse restaurants and the High Line",
                cuisine_focus=["american", "italian", "asian", "mediterranean"],
                restaurant_types=["art_gallery_restaurant", "trendy_restaurant", "wine_bar"],
                iconic_dishes=["Chelsea Burger", "Art Gallery Dining", "High Line Food"],
                tourist_factor=0.5,
                price_level="moderate"
            ),
            Neighborhood(
                name="West Village",
                city="Manhattan",
                description="Charming neighborhood with cozy restaurants and historic streets",
                cuisine_focus=["italian", "french", "american", "mediterranean"],
                restaurant_types=["cozy_restaurant", "bistro", "wine_bar", "romantic"],
                iconic_dishes=["Village Pizza", "Cozy Bistro", "Romantic Dining"],
                tourist_factor=0.6,
                price_level="upscale"
            ),
            
            # Jersey City Neighborhoods
            Neighborhood(
                name="Journal Square",
                city="Jersey City",
                description="Transportation hub with diverse immigrant communities",
                cuisine_focus=["indian", "pakistani", "caribbean", "latin"],
                restaurant_types=["ethnic_restaurant", "family_owned", "immigrant_restaurant"],
                iconic_dishes=["Chicken Biryani", "Curry", "Caribbean Food"],
                tourist_factor=0.1,
                price_level="budget"
            ),
            Neighborhood(
                name="Downtown JC",
                city="Jersey City",
                description="Modern waterfront area with upscale restaurants and bars",
                cuisine_focus=["american", "seafood", "italian", "asian"],
                restaurant_types=["waterfront_restaurant", "upscale_restaurant", "bar", "seafood"],
                iconic_dishes=["Waterfront Seafood", "Downtown Burger", "Craft Beer"],
                tourist_factor=0.3,
                price_level="moderate"
            ),
            Neighborhood(
                name="The Heights",
                city="Jersey City",
                description="Residential area with local favorites and family restaurants",
                cuisine_focus=["italian", "american", "pizza", "diner"],
                restaurant_types=["family_restaurant", "pizzeria", "diner", "local_favorite"],
                iconic_dishes=["Heights Pizza", "Family Diner", "Local Italian"],
                tourist_factor=0.05,
                price_level="budget"
            ),
            
            # Hoboken Neighborhoods
            Neighborhood(
                name="Washington Street",
                city="Hoboken",
                description="Main commercial street with diverse restaurants and bars",
                cuisine_focus=["italian", "american", "pizza", "bar_food"],
                restaurant_types=["italian_restaurant", "pizzeria", "bar", "sub_shop"],
                iconic_dishes=["Italian Sub", "Hoboken Pizza", "Bar Food"],
                tourist_factor=0.4,
                price_level="moderate"
            ),
            Neighborhood(
                name="Waterfront",
                city="Hoboken",
                description="Scenic area with upscale restaurants and Manhattan views",
                cuisine_focus=["seafood", "american", "italian", "fine_dining"],
                restaurant_types=["waterfront_restaurant", "fine_dining", "seafood", "upscale"],
                iconic_dishes=["Waterfront Seafood", "Manhattan View Dining", "Upscale Italian"],
                tourist_factor=0.6,
                price_level="upscale"
            ),
            Neighborhood(
                name="Uptown Hoboken",
                city="Hoboken",
                description="Residential area with local favorites and college town vibe",
                cuisine_focus=["american", "pizza", "bar_food", "college_food"],
                restaurant_types=["college_bar", "pizzeria", "local_favorite", "casual"],
                iconic_dishes=["College Bar Food", "Local Pizza", "Casual Dining"],
                tourist_factor=0.2,
                price_level="budget"
            )
        ]
    
    def _group_by_city(self) -> Dict[str, List[Neighborhood]]:
        """Group neighborhoods by city."""
        grouped = {}
        for neighborhood in self.neighborhoods:
            if neighborhood.city not in grouped:
                grouped[neighborhood.city] = []
            grouped[neighborhood.city].append(neighborhood)
        return grouped
    
    def get_neighborhoods_for_city(self, city: str) -> List[Neighborhood]:
        """Get all neighborhoods for a specific city."""
        return self.city_neighborhoods.get(city, [])
    
    def get_neighborhood_by_name(self, name: str, city: str) -> Optional[Neighborhood]:
        """Get a specific neighborhood by name and city."""
        for neighborhood in self.neighborhoods:
            if neighborhood.name.lower() == name.lower() and neighborhood.city.lower() == city.lower():
                return neighborhood
        return None
    
    def find_best_neighborhood_for_cuisine(self, city: str, cuisine: str) -> Optional[Neighborhood]:
        """Find the best neighborhood for a specific cuisine in a city."""
        city_neighborhoods = self.get_neighborhoods_for_city(city)
        
        best_neighborhood = None
        best_score = 0
        
        for neighborhood in city_neighborhoods:
            if cuisine.lower() in [c.lower() for c in neighborhood.cuisine_focus]:
                # Score based on cuisine focus and tourist factor (lower is better for locals)
                score = 1.0
                if cuisine.lower() in [c.lower() for c in neighborhood.cuisine_focus[:2]]:
                    score += 0.5  # Bonus for primary cuisine focus
                score -= neighborhood.tourist_factor * 0.3  # Penalty for touristy areas
                
                if score > best_score:
                    best_score = score
                    best_neighborhood = neighborhood
        
        return best_neighborhood
    
    def get_neighborhood_ranking_factors(self, neighborhood: Neighborhood) -> Dict:
        """Get location-aware ranking factors for a specific neighborhood."""
        
        base_factors = {
            "cuisine_weights": {},
            "restaurant_types": {},
            "neighborhood_weights": {neighborhood.name.lower(): 1.3},
            "iconic_indicators": neighborhood.iconic_dishes + [neighborhood.name.lower()]
        }
        
        # Add cuisine weights based on neighborhood focus
        for i, cuisine in enumerate(neighborhood.cuisine_focus):
            weight = 1.3 - (i * 0.1)  # Primary cuisines get higher weight
            base_factors["cuisine_weights"][cuisine] = weight
        
        # Add restaurant type weights
        for restaurant_type in neighborhood.restaurant_types:
            base_factors["restaurant_types"][restaurant_type] = 1.2
        
        return base_factors
    
    def suggest_neighborhoods_for_query(self, city: str, cuisine: str = None, 
                                      dish: str = None, tourist_preference: float = 0.5) -> List[Neighborhood]:
        """Suggest neighborhoods for a specific query."""
        
        city_neighborhoods = self.get_neighborhoods_for_city(city)
        suggestions = []
        
        for neighborhood in city_neighborhoods:
            score = 0
            
            # Cuisine match
            if cuisine and cuisine.lower() in [c.lower() for c in neighborhood.cuisine_focus]:
                score += 2.0
                # Bonus for primary cuisine
                if cuisine.lower() == neighborhood.cuisine_focus[0].lower():
                    score += 1.0
            
            # Dish match
            if dish and any(dish.lower() in iconic.lower() for iconic in neighborhood.iconic_dishes):
                score += 1.5
            
            # Tourist preference match
            tourist_diff = abs(neighborhood.tourist_factor - tourist_preference)
            score += (1.0 - tourist_diff) * 0.5
            
            if score > 0:
                suggestions.append((neighborhood, score))
        
        # Sort by score and return top neighborhoods
        suggestions.sort(key=lambda x: x[1], reverse=True)
        return [neighborhood for neighborhood, score in suggestions[:3]]
    
    def get_neighborhood_context(self, neighborhood: Neighborhood) -> str:
        """Get context description for a neighborhood."""
        return f"{neighborhood.name} in {neighborhood.city}: {neighborhood.description}. " \
               f"Known for {', '.join(neighborhood.cuisine_focus)} cuisine and " \
               f"{', '.join(neighborhood.restaurant_types)} restaurants. " \
               f"Iconic dishes include {', '.join(neighborhood.iconic_dishes)}."
    
    def print_neighborhood_info(self, city: str):
        """Print information about all neighborhoods in a city."""
        neighborhoods = self.get_neighborhoods_for_city(city)
        
        print(f"\n🏙️ Neighborhoods in {city}:")
        print("=" * 60)
        
        for neighborhood in neighborhoods:
            print(f"\n📍 {neighborhood.name}")
            print(f"   Description: {neighborhood.description}")
            print(f"   Cuisine Focus: {', '.join(neighborhood.cuisine_focus)}")
            print(f"   Restaurant Types: {', '.join(neighborhood.restaurant_types)}")
            print(f"   Iconic Dishes: {', '.join(neighborhood.iconic_dishes)}")
            print(f"   Tourist Factor: {neighborhood.tourist_factor:.1%}")
            print(f"   Price Level: {neighborhood.price_level}")
            print("-" * 40)


# Global instance
neighborhood_mapper = NeighborhoodMapper()
