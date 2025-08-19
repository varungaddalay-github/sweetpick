#!/usr/bin/env python3
"""
Dynamic Neighborhood Discovery using Yelp Search API
Actually discovers neighborhoods by analyzing restaurant data from Yelp.
"""
import asyncio
import sys
import json
from pathlib import Path
from typing import List, Dict, Tuple, Set
from collections import defaultdict, Counter
from datetime import datetime

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.data_collection.yelp_collector import YelpCollector
from src.data_collection.serpapi_collector import SerpAPICollector
from src.utils.config import get_settings
from src.utils.logger import app_logger

class DynamicNeighborhoodDiscoverer:
    """Discovers neighborhoods dynamically using Yelp Search API."""
    
    def __init__(self):
        self.settings = get_settings()
        self.yelp_collector = YelpCollector() if self.settings.yelp_api_key else None
        self.serpapi_collector = SerpAPICollector()
        
        # Supported cuisines for discovery
        self.supported_cuisines = ["Italian", "Indian", "Chinese", "American", "Mexican"]
        
        # Manhattan center coordinates for broad search
        self.manhattan_center = {
            "lat": 40.7589,
            "lng": -73.9851
        }
        
        # Results storage
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "discovered_neighborhoods": [],
            "neighborhood_data": {},
            "summary": {}
        }
    
    async def discover_neighborhoods(self, top_n: int = 5) -> List[Tuple[str, int]]:
        """Discover neighborhoods by analyzing Yelp restaurant data."""
        print(f"🔍 Starting dynamic neighborhood discovery for Manhattan")
        print(f"🍽️  Using Yelp Search API to discover neighborhoods")
        print("=" * 80)
        
        if not self.yelp_collector:
            print("❌ Yelp API key not configured. Cannot perform dynamic discovery.")
            return []
        
        # Step 1: Collect restaurants from across Manhattan
        print("\n📊 STEP 1: Collecting restaurants from across Manhattan")
        print("-" * 60)
        
        all_restaurants = []
        neighborhood_counts = defaultdict(int)
        neighborhood_restaurants = defaultdict(list)
        
        for cuisine in self.supported_cuisines:
            print(f"\n🔍 Searching for {cuisine} restaurants across Manhattan...")
            
            try:
                # Search for restaurants in Manhattan with this cuisine
                restaurants = await self._search_manhattan_restaurants(cuisine)
                
                if restaurants:
                    print(f"  ✅ Found {len(restaurants)} {cuisine} restaurants")
                    all_restaurants.extend(restaurants)
                    
                    # Extract neighborhoods from restaurant data
                    for restaurant in restaurants:
                        neighborhood = self._extract_neighborhood_from_restaurant(restaurant)
                        if neighborhood:
                            neighborhood_counts[neighborhood] += 1
                            neighborhood_restaurants[neighborhood].append(restaurant)
                            print(f"    📍 {restaurant['restaurant_name']} -> {neighborhood}")
                else:
                    print(f"  ⚠️  No {cuisine} restaurants found")
                    
            except Exception as e:
                print(f"  ❌ Error searching {cuisine}: {e}")
        
        print(f"\n📊 Discovery Summary:")
        print(f"  🏢 Total restaurants found: {len(all_restaurants)}")
        print(f"  🏘️  Unique neighborhoods discovered: {len(neighborhood_counts)}")
        
        # Step 2: Analyze and rank neighborhoods
        print(f"\n📊 STEP 2: Analyzing discovered neighborhoods")
        print("-" * 60)
        
        # Sort neighborhoods by restaurant count
        sorted_neighborhoods = sorted(neighborhood_counts.items(), key=lambda x: x[1], reverse=True)
        
        print(f"🏘️  All discovered neighborhoods:")
        for i, (neighborhood, count) in enumerate(sorted_neighborhoods, 1):
            print(f"  {i:2d}. {neighborhood}: {count} restaurants")
        
        # Get top N neighborhoods
        top_neighborhoods = sorted_neighborhoods[:top_n]
        
        print(f"\n🎯 TOP {top_n} NEIGHBORHOODS DISCOVERED:")
        print("=" * 50)
        for i, (neighborhood, count) in enumerate(top_neighborhoods, 1):
            print(f"  {i}. {neighborhood}: {count} restaurants")
        
        # Store discovery results
        self.results["discovered_neighborhoods"] = top_neighborhoods
        self.results["all_neighborhoods"] = sorted_neighborhoods
        self.results["neighborhood_restaurants"] = {
            neighborhood: restaurants for neighborhood, restaurants in neighborhood_restaurants.items()
        }
        
        return top_neighborhoods
    
    async def _search_manhattan_restaurants(self, cuisine: str, max_results: int = 100) -> List[Dict]:
        """Search for restaurants across Manhattan using Yelp API."""
        try:
            # Use Yelp API to search across Manhattan
            search_params = {
                "term": f"{cuisine} restaurants",
                "latitude": self.manhattan_center["lat"],
                "longitude": self.manhattan_center["lng"],
                "radius": 8000,  # 8km radius to cover most of Manhattan
                "limit": min(max_results, 50),  # Yelp API limit
                "sort_by": "rating"
            }
            
            print(f"    🔍 Yelp API search: {cuisine} restaurants in Manhattan (8km radius)")
            
            restaurants = await self.yelp_collector._make_yelp_api_call("/businesses/search", search_params)
            
            if not restaurants:
                return []
            
            # Process and filter restaurants
            processed_restaurants = []
            for restaurant in restaurants:
                processed = await self.yelp_collector._process_restaurant_data(
                    restaurant, "Manhattan", cuisine
                )
                if processed and self._meets_criteria(processed):
                    processed_restaurants.append(processed)
            
            return processed_restaurants
            
        except Exception as e:
            print(f"    ❌ Error in Manhattan search: {e}")
            return []
    
    def _extract_neighborhood_from_restaurant(self, restaurant: Dict) -> str:
        """Extract neighborhood information from restaurant data."""
        # Try multiple sources for neighborhood information
        
        # 1. Check if neighborhood is already extracted
        if restaurant.get("neighborhood"):
            return restaurant["neighborhood"]
        
        # 2. Try to extract from address
        address = restaurant.get("full_address", "")
        if address:
            # Look for common Manhattan neighborhood patterns
            neighborhood_patterns = [
                "Times Square", "Hell's Kitchen", "Chelsea", "Greenwich Village", 
                "East Village", "Lower East Side", "Upper East Side", "Upper West Side",
                "Midtown", "Financial District", "Tribeca", "SoHo", "NoHo", "Harlem",
                "Washington Heights", "Inwood", "Morningside Heights", "Yorkville",
                "Chinatown", "Little Italy", "Nolita", "Meatpacking District"
            ]
            
            for pattern in neighborhood_patterns:
                if pattern.lower() in address.lower():
                    return pattern
        
        # 3. Try to extract from display address (Yelp specific)
        location = restaurant.get("location", {})
        display_address = location.get("display_address", [])
        
        for addr_part in display_address:
            # Look for neighborhood indicators
            if any(keyword in addr_part.lower() for keyword in [
                "district", "heights", "square", "park", "village", "side"
            ]):
                return addr_part
        
        # 4. Try to extract from coordinates (approximate neighborhood)
        lat = restaurant.get("latitude", 0)
        lng = restaurant.get("longitude", 0)
        
        if lat and lng:
            approximate_neighborhood = self._get_neighborhood_by_coordinates(lat, lng)
            if approximate_neighborhood:
                return approximate_neighborhood
        
        return "Unknown"
    
    def _get_neighborhood_by_coordinates(self, lat: float, lng: float) -> str:
        """Get approximate neighborhood based on coordinates."""
        # Manhattan neighborhood coordinate ranges (approximate)
        neighborhood_ranges = {
            "Times Square": {"lat": (40.75, 40.76), "lng": (-73.99, -73.98)},
            "Hell's Kitchen": {"lat": (40.76, 40.77), "lng": (-73.99, -73.98)},
            "Chelsea": {"lat": (40.74, 40.75), "lng": (-74.00, -73.99)},
            "Greenwich Village": {"lat": (40.73, 40.74), "lng": (-74.00, -73.99)},
            "East Village": {"lat": (40.72, 40.73), "lng": (-73.99, -73.98)},
            "Lower East Side": {"lat": (40.71, 40.72), "lng": (-73.99, -73.98)},
            "Upper East Side": {"lat": (40.77, 40.78), "lng": (-73.97, -73.96)},
            "Upper West Side": {"lat": (40.78, 40.79), "lng": (-73.98, -73.97)},
            "Midtown": {"lat": (40.75, 40.76), "lng": (-73.99, -73.98)},
            "Financial District": {"lat": (40.70, 40.71), "lng": (-74.01, -74.00)},
            "Tribeca": {"lat": (40.71, 40.72), "lng": (-74.01, -74.00)},
            "SoHo": {"lat": (40.72, 40.73), "lng": (-74.00, -73.99)},
            "NoHo": {"lat": (40.72, 40.73), "lng": (-74.00, -73.99)},
            "Harlem": {"lat": (40.81, 40.82), "lng": (-73.95, -73.94)},
            "Washington Heights": {"lat": (40.85, 40.86), "lng": (-73.94, -73.93)},
            "Inwood": {"lat": (40.86, 40.87), "lng": (-73.92, -73.91)},
            "Morningside Heights": {"lat": (40.80, 40.81), "lng": (-73.97, -73.96)},
            "Yorkville": {"lat": (40.75, 40.76), "lng": (-73.96, -73.95)},
            "Chinatown": {"lat": (40.71, 40.72), "lng": (-74.00, -73.99)},
            "Little Italy": {"lat": (40.71, 40.72), "lng": (-74.00, -73.99)},
            "Nolita": {"lat": (40.72, 40.73), "lng": (-74.00, -73.99)},
            "Meatpacking District": {"lat": (40.73, 40.74), "lng": (-74.01, -74.00)}
        }
        
        for neighborhood, ranges in neighborhood_ranges.items():
            lat_range = ranges["lat"]
            lng_range = ranges["lng"]
            
            if lat_range[0] <= lat <= lat_range[1] and lng_range[0] <= lng <= lng_range[1]:
                return neighborhood
        
        return "Other"
    
    def _meets_criteria(self, restaurant: Dict) -> bool:
        """Check if restaurant meets quality criteria."""
        rating = restaurant.get("rating", 0)
        review_count = restaurant.get("review_count", 0)
        
        # Basic quality filters
        if rating < 3.0:
            return False
        
        if review_count < 5:
            return False
        
        return True
    
    async def collect_neighborhood_data(self, neighborhood: str) -> Dict:
        """Collect detailed data for a discovered neighborhood."""
        print(f"\n🚀 Collecting detailed data for discovered neighborhood: {neighborhood}")
        print("-" * 60)
        
        neighborhood_data = {
            "neighborhood": neighborhood,
            "city": "Manhattan",
            "cuisines": {},
            "total_restaurants": 0,
            "discovery_method": "yelp_api"
        }
        
        # Get restaurants we already discovered for this neighborhood
        discovered_restaurants = self.results["neighborhood_restaurants"].get(neighborhood, [])
        
        # Group by cuisine
        cuisine_restaurants = defaultdict(list)
        for restaurant in discovered_restaurants:
            cuisine = restaurant.get("cuisine_type", "Unknown")
            cuisine_restaurants[cuisine].append(restaurant)
        
        # Process each cuisine
        for cuisine, restaurants in cuisine_restaurants.items():
            print(f"  🍽️  Processing {cuisine}: {len(restaurants)} restaurants")
            
            # Sort by quality score and get top 5
            restaurants.sort(key=lambda x: x.get("quality_score", 0), reverse=True)
            top_5_restaurants = restaurants[:5]
            
            print(f"    🏆 Top 5 {cuisine} restaurants:")
            for i, restaurant in enumerate(top_5_restaurants, 1):
                print(f"      {i}. {restaurant['restaurant_name']}")
                print(f"         Rating: {restaurant.get('rating', 'N/A')} | Score: {restaurant.get('quality_score', 0):.2f}")
                print(f"         Address: {restaurant.get('full_address', 'N/A')}")
            
            cuisine_data = {
                "restaurants": restaurants,
                "top_restaurants": top_5_restaurants,
                "total_restaurants": len(restaurants),
                "total_dishes": 0  # Will be updated when dish extraction is added
            }
            
            neighborhood_data["cuisines"][cuisine] = cuisine_data
            neighborhood_data["total_restaurants"] += len(restaurants)
        
        print(f"  ✅ Completed {neighborhood}: {neighborhood_data['total_restaurants']} restaurants")
        return neighborhood_data
    
    async def run_discovery(self):
        """Run the complete dynamic neighborhood discovery."""
        print("🚀 DYNAMIC NEIGHBORHOOD DISCOVERY USING YELP API")
        print("=" * 80)
        print(f"📅 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🍽️  Supported cuisines: {', '.join(self.supported_cuisines)}")
        print("=" * 80)
        
        try:
            # Step 1: Discover neighborhoods
            print("\n📊 STEP 1: DISCOVERING NEIGHBORHOODS VIA YELP API")
            print("-" * 60)
            
            top_neighborhoods = await self.discover_neighborhoods(top_n=5)
            
            if not top_neighborhoods:
                print("❌ No neighborhoods discovered. Exiting.")
                return
            
            # Step 2: Collect detailed data for discovered neighborhoods
            print("\n📊 STEP 2: COLLECTING DETAILED DATA FOR DISCOVERED NEIGHBORHOODS")
            print("-" * 60)
            
            for i, (neighborhood, count) in enumerate(top_neighborhoods, 1):
                print(f"\n🏆 DISCOVERED NEIGHBORHOOD {i}/5: {neighborhood}")
                print("=" * 50)
                
                neighborhood_data = await self.collect_neighborhood_data(neighborhood)
                self.results["neighborhood_data"][neighborhood] = neighborhood_data
                
                if i < len(top_neighborhoods):
                    print("\n" + "=" * 80)
            
            # Step 3: Generate summary
            print("\n📊 STEP 3: GENERATING DISCOVERY SUMMARY")
            print("-" * 60)
            
            await self._generate_summary()
            
            # Step 4: Save results
            print("\n📊 STEP 4: SAVING DISCOVERY RESULTS")
            print("-" * 60)
            
            await self._save_results()
            
            print("\n🎉 DYNAMIC NEIGHBORHOOD DISCOVERY COMPLETE!")
            print("=" * 80)
            
        except Exception as e:
            print(f"❌ Error in discovery: {e}")
            raise
    
    async def _generate_summary(self):
        """Generate discovery summary."""
        total_restaurants = 0
        total_cuisines = 0
        
        print("📋 DISCOVERY SUMMARY")
        print("=" * 40)
        print(f"🔍 Discovery Method: Yelp Search API")
        print(f"🏘️  Neighborhoods Discovered: {len(self.results['discovered_neighborhoods'])}")
        print(f"🍽️  Total Restaurants Analyzed: {sum(len(restaurants) for restaurants in self.results['neighborhood_restaurants'].values())}")
        
        for neighborhood, data in self.results["neighborhood_data"].items():
            print(f"\n🏘️  {neighborhood.upper()}")
            print(f"   📊 Total Restaurants: {data['total_restaurants']}")
            
            total_restaurants += data['total_restaurants']
            
            for cuisine, cuisine_data in data['cuisines'].items():
                restaurant_count = cuisine_data['total_restaurants']
                print(f"     🍽️  {cuisine}: {restaurant_count} restaurants")
                total_cuisines += restaurant_count
        
        print(f"\n📊 GRAND TOTALS")
        print(f"   🏢 Total Restaurants: {total_restaurants}")
        print(f"   🍽️  Total Cuisine Entries: {total_cuisines}")
        print(f"   🏘️  Neighborhoods: {len(self.results['neighborhood_data'])}")
        
        self.results["summary"] = {
            "discovery_method": "yelp_api",
            "total_restaurants": total_restaurants,
            "total_cuisines": total_cuisines,
            "neighborhoods_discovered": len(self.results['neighborhood_data'])
        }
    
    async def _save_results(self):
        """Save discovery results to JSON file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"dynamic_neighborhood_discovery_{timestamp}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(self.results, f, indent=2, default=str)
            
            print(f"✅ Discovery results saved to: {filename}")
            
        except Exception as e:
            print(f"❌ Error saving results: {e}")

async def main():
    """Main function."""
    discoverer = DynamicNeighborhoodDiscoverer()
    await discoverer.run_discovery()

if __name__ == "__main__":
    asyncio.run(main())
