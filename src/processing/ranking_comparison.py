"""
Comparison system for location-aware vs standard restaurant ranking.
"""
import asyncio
from typing import List, Dict, Optional, Any, Tuple
from src.utils.logger import app_logger
from src.vector_db.milvus_client import MilvusClient
from src.processing.location_aware_ranking import LocationAwareRanking


class RankingComparison:
    """Compare location-aware vs standard restaurant ranking."""
    
    def __init__(self):
        self.milvus_client = MilvusClient()
        self.location_ranker = LocationAwareRanking()
        
        # Comparison metrics
        self.comparison_metrics = {
            'location_specificity': 0.0,  # How location-specific are the results
            'cuisine_relevance': 0.0,     # How relevant to the cuisine
            'authenticity_score': 0.0,    # How authentic/local the restaurants are
            'user_satisfaction_prediction': 0.0  # Predicted user satisfaction
        }
    
    async def compare_rankings(self, location: str, cuisine: str = None, 
                             dish_name: str = None, max_results: int = 5) -> Dict[str, Any]:
        """Compare location-aware vs standard ranking for a location."""
        
        app_logger.info(f"🔍 Comparing rankings for {location} {cuisine}")
        
        # Get restaurants using standard ranking
        standard_restaurants = self._get_standard_ranked_restaurants(location, cuisine, max_results)
        
        # Get restaurants using location-aware ranking
        location_aware_restaurants = self._get_location_aware_ranked_restaurants(
            location, cuisine, dish_name, max_results
        )
        
        # Analyze and compare
        comparison = self._analyze_comparison(
            standard_restaurants, location_aware_restaurants, location, cuisine
        )
        
        return {
            "location": location,
            "cuisine": cuisine,
            "dish_name": dish_name,
            "standard_ranking": standard_restaurants,
            "location_aware_ranking": location_aware_restaurants,
            "comparison_metrics": comparison,
            "recommendation": self._generate_recommendation(comparison)
        }
    
    def _get_standard_ranked_restaurants(self, location: str, cuisine: str = None, 
                                       max_results: int = 5) -> List[Dict]:
        """Get restaurants using standard quality-based ranking."""
        
        filters = {"city": location}
        if cuisine:
            filters["cuisine_type"] = cuisine
        
        restaurants = self.milvus_client.search_restaurants_with_filters(filters, max_results * 2)
        
        # Sort by quality score (standard ranking)
        sorted_restaurants = sorted(restaurants, key=lambda x: x.get('quality_score', 0), reverse=True)
        
        return sorted_restaurants[:max_results]
    
    def _get_location_aware_ranked_restaurants(self, location: str, cuisine: str = None,
                                             dish_name: str = None, max_results: int = 5) -> List[Dict]:
        """Get restaurants using location-aware ranking."""
        
        filters = {"city": location}
        if cuisine:
            filters["cuisine_type"] = cuisine
        
        restaurants = self.milvus_client.search_restaurants_with_filters(filters, max_results * 2)
        
        # Apply location-aware ranking
        ranked_restaurants = self.location_ranker.rank_restaurants_by_location(
            restaurants, location, cuisine, dish_name
        )
        
        return ranked_restaurants[:max_results]
    
    def _analyze_comparison(self, standard_restaurants: List[Dict], 
                          location_aware_restaurants: List[Dict],
                          location: str, cuisine: str = None) -> Dict[str, float]:
        """Analyze the comparison between standard and location-aware rankings."""
        
        # Calculate location specificity
        standard_specificity = self._calculate_location_specificity(standard_restaurants, location)
        location_aware_specificity = self._calculate_location_specificity(location_aware_restaurants, location)
        
        # Calculate cuisine relevance
        standard_relevance = self._calculate_cuisine_relevance(standard_restaurants, cuisine)
        location_aware_relevance = self._calculate_cuisine_relevance(location_aware_restaurants, cuisine)
        
        # Calculate authenticity score
        standard_authenticity = self._calculate_authenticity_score(standard_restaurants, location)
        location_aware_authenticity = self._calculate_authenticity_score(location_aware_restaurants, location)
        
        # Calculate predicted user satisfaction
        standard_satisfaction = self._predict_user_satisfaction(standard_restaurants, location, cuisine)
        location_aware_satisfaction = self._predict_user_satisfaction(location_aware_restaurants, location, cuisine)
        
        return {
            "standard_metrics": {
                "location_specificity": standard_specificity,
                "cuisine_relevance": standard_relevance,
                "authenticity_score": standard_authenticity,
                "user_satisfaction_prediction": standard_satisfaction
            },
            "location_aware_metrics": {
                "location_specificity": location_aware_specificity,
                "cuisine_relevance": location_aware_relevance,
                "authenticity_score": location_aware_authenticity,
                "user_satisfaction_prediction": location_aware_satisfaction
            },
            "improvements": {
                "location_specificity": location_aware_specificity - standard_specificity,
                "cuisine_relevance": location_aware_relevance - standard_relevance,
                "authenticity_score": location_aware_authenticity - standard_authenticity,
                "user_satisfaction_prediction": location_aware_satisfaction - standard_satisfaction
            }
        }
    
    def _calculate_location_specificity(self, restaurants: List[Dict], location: str) -> float:
        """Calculate how location-specific the restaurants are."""
        
        if not restaurants:
            return 0.0
        
        specificity_scores = []
        location_indicators = self._get_location_indicators(location)
        
        for restaurant in restaurants:
            name = restaurant.get('restaurant_name', '').lower()
            description = restaurant.get('description', '').lower()
            text = f"{name} {description}"
            
            # Count location-specific indicators
            indicator_count = sum(1 for indicator in location_indicators if indicator in text)
            specificity_scores.append(min(indicator_count * 0.2, 1.0))
        
        return sum(specificity_scores) / len(specificity_scores)
    
    def _calculate_cuisine_relevance(self, restaurants: List[Dict], cuisine: str = None) -> float:
        """Calculate how relevant the restaurants are to the specified cuisine."""
        
        if not cuisine or not restaurants:
            return 0.5
        
        cuisine_matches = 0
        for restaurant in restaurants:
            restaurant_cuisine = restaurant.get('cuisine_type', '').lower()
            if cuisine.lower() in restaurant_cuisine or restaurant_cuisine in cuisine.lower():
                cuisine_matches += 1
        
        return cuisine_matches / len(restaurants)
    
    def _calculate_authenticity_score(self, restaurants: List[Dict], location: str) -> float:
        """Calculate authenticity score based on local indicators."""
        
        if not restaurants:
            return 0.0
        
        authenticity_scores = []
        authenticity_indicators = [
            'authentic', 'traditional', 'original', 'classic', 'since', 
            'established', 'founded', 'family', 'homemade', 'local'
        ]
        
        for restaurant in restaurants:
            name = restaurant.get('restaurant_name', '').lower()
            description = restaurant.get('description', '').lower()
            text = f"{name} {description}"
            
            # Count authenticity indicators
            indicator_count = sum(1 for indicator in authenticity_indicators if indicator in text)
            authenticity_scores.append(min(indicator_count * 0.15, 1.0))
        
        return sum(authenticity_scores) / len(authenticity_scores)
    
    def _predict_user_satisfaction(self, restaurants: List[Dict], location: str, cuisine: str = None) -> float:
        """Predict user satisfaction based on multiple factors."""
        
        if not restaurants:
            return 0.0
        
        satisfaction_scores = []
        
        for restaurant in restaurants:
            # Base satisfaction from quality score
            quality_score = restaurant.get('quality_score', 0.5)
            
            # Location relevance bonus
            location_relevance = self._calculate_location_specificity([restaurant], location)
            
            # Cuisine relevance bonus
            cuisine_relevance = self._calculate_cuisine_relevance([restaurant], cuisine)
            
            # Authenticity bonus
            authenticity = self._calculate_authenticity_score([restaurant], location)
            
            # Combined satisfaction score
            satisfaction = (quality_score * 0.5) + (location_relevance * 0.2) + (cuisine_relevance * 0.2) + (authenticity * 0.1)
            satisfaction_scores.append(satisfaction)
        
        return sum(satisfaction_scores) / len(satisfaction_scores)
    
    def _get_location_indicators(self, location: str) -> List[str]:
        """Get location-specific indicators for a location."""
        
        location_indicators = {
            "Manhattan": [
                "new york", "nyc", "manhattan", "brooklyn", "queens", "bronx",
                "chinatown", "little italy", "midtown", "upper east side", "upper west side",
                "lower east side", "chelsea", "west village", "east village", "soho", "tribeca"
            ],
            "Jersey City": [
                "jersey city", "jc", "new jersey", "nj", "journal square", "downtown jc",
                "the heights", "greenville", "west side", "bergen-lafayette"
            ],
            "Hoboken": [
                "hoboken", "hoboken nj", "new jersey", "washington street", "waterfront",
                "uptown", "downtown"
            ]
        }
        
        return location_indicators.get(location, [location.lower()])
    
    def _generate_recommendation(self, comparison: Dict[str, Any]) -> str:
        """Generate recommendation based on comparison results."""
        
        improvements = comparison['improvements']
        
        # Calculate overall improvement
        overall_improvement = sum(improvements.values()) / len(improvements)
        
        if overall_improvement > 0.1:
            return "Location-aware ranking significantly improves results"
        elif overall_improvement > 0.05:
            return "Location-aware ranking provides moderate improvements"
        elif overall_improvement > 0:
            return "Location-aware ranking provides slight improvements"
        else:
            return "Standard ranking performs similarly or better"
    
    async def run_comprehensive_comparison(self, locations: List[str], cuisines: List[str] = None) -> Dict[str, Any]:
        """Run comprehensive comparison across multiple locations and cuisines."""
        
        if not cuisines:
            cuisines = ["italian", "american", "indian", "chinese"]
        
        all_comparisons = []
        summary_stats = {
            "total_comparisons": 0,
            "location_aware_wins": 0,
            "standard_wins": 0,
            "ties": 0,
            "avg_improvement": 0.0
        }
        
        for location in locations:
            for cuisine in cuisines:
                try:
                    comparison = await self.compare_rankings(location, cuisine)
                    all_comparisons.append(comparison)
                    
                    # Update summary stats
                    summary_stats["total_comparisons"] += 1
                    improvements = comparison["comparison_metrics"]["improvements"]
                    avg_improvement = sum(improvements.values()) / len(improvements)
                    summary_stats["avg_improvement"] += avg_improvement
                    
                    if avg_improvement > 0.05:
                        summary_stats["location_aware_wins"] += 1
                    elif avg_improvement < -0.05:
                        summary_stats["standard_wins"] += 1
                    else:
                        summary_stats["ties"] += 1
                        
                except Exception as e:
                    app_logger.error(f"Error comparing {location} {cuisine}: {e}")
        
        # Calculate final averages
        if summary_stats["total_comparisons"] > 0:
            summary_stats["avg_improvement"] /= summary_stats["total_comparisons"]
        
        return {
            "summary": summary_stats,
            "detailed_comparisons": all_comparisons
        }
    
    def print_comparison_report(self, comparison: Dict[str, Any]):
        """Print a formatted comparison report."""
        
        print(f"\n{'='*80}")
        print(f"🏙️ RANKING COMPARISON REPORT")
        print(f"{'='*80}")
        print(f"📍 Location: {comparison['location']}")
        print(f"🍕 Cuisine: {comparison['cuisine']}")
        print(f"🍽️ Dish: {comparison['dish_name']}")
        
        # Standard Ranking
        print(f"\n📊 STANDARD RANKING (Top 5):")
        for i, restaurant in enumerate(comparison['standard_ranking'], 1):
            print(f"  {i}. {restaurant['restaurant_name']} (Score: {restaurant.get('quality_score', 0):.3f})")
        
        # Location-Aware Ranking
        print(f"\n🏙️ LOCATION-AWARE RANKING (Top 5):")
        for i, restaurant in enumerate(comparison['location_aware_ranking'], 1):
            print(f"  {i}. {restaurant['restaurant_name']} (Combined: {restaurant.get('combined_score', 0):.3f}, Location: {restaurant.get('location_score', 0):.3f})")
        
        # Metrics Comparison
        metrics = comparison['comparison_metrics']
        print(f"\n📈 METRICS COMPARISON:")
        print(f"  Location Specificity: {metrics['standard_metrics']['location_specificity']:.3f} → {metrics['location_aware_metrics']['location_specificity']:.3f} (Δ{metrics['improvements']['location_specificity']:+.3f})")
        print(f"  Cuisine Relevance: {metrics['standard_metrics']['cuisine_relevance']:.3f} → {metrics['location_aware_metrics']['cuisine_relevance']:.3f} (Δ{metrics['improvements']['cuisine_relevance']:+.3f})")
        print(f"  Authenticity Score: {metrics['standard_metrics']['authenticity_score']:.3f} → {metrics['location_aware_metrics']['authenticity_score']:.3f} (Δ{metrics['improvements']['authenticity_score']:+.3f})")
        print(f"  User Satisfaction: {metrics['standard_metrics']['user_satisfaction_prediction']:.3f} → {metrics['location_aware_metrics']['user_satisfaction_prediction']:.3f} (Δ{metrics['improvements']['user_satisfaction_prediction']:+.3f})")
        
        # Recommendation
        print(f"\n💡 RECOMMENDATION: {comparison['recommendation']}")
        print(f"{'='*80}")
