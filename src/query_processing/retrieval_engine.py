"""
Retrieval engine for vector similarity search and recommendation logic.
"""
import asyncio
from typing import List, Dict, Optional, Any, Tuple
from openai import AsyncOpenAI
from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.utils.location_resolver import location_resolver
from src.vector_db.milvus_client import MilvusClient

class RetrievalEngine:
    """Retrieval engine for restaurant and dish recommendations."""
    
    def __init__(self, milvus_client: MilvusClient):
        self.settings = get_settings()
        self.milvus_client = milvus_client
        self.client = AsyncOpenAI(api_key=self.settings.openai_api_key)
        self._embedding_cache = {}
    
    async def get_recommendations(self, parsed_query: Dict[str, Any], max_results: int = 10) -> Tuple[List[Dict], bool, Optional[str]]:
        """Get recommendations based on parsed query."""
        try:
            query_type = parsed_query.get("intent", "unknown")
            
            if query_type == "restaurant_specific":
                return await self._handle_restaurant_specific_query(parsed_query, max_results)
            elif query_type == "location_cuisine":
                return await self._handle_location_cuisine_query(parsed_query, max_results)
            elif query_type == "location_dish":
                return await self._handle_location_dish_query(parsed_query, max_results)
            elif query_type == "location_general":
                return await self._handle_location_general_query(parsed_query, max_results)
            elif query_type == "meal_type":
                return await self._handle_meal_type_query(parsed_query, max_results)
            else:
                return await self._handle_unknown_query(parsed_query, max_results)
                
        except Exception as e:
            app_logger.error(f"Error getting recommendations: {e}")
            return [], False, f"Error: {str(e)}"
    
    async def _handle_restaurant_specific_query(self, parsed_query: Dict[str, Any], max_results: int) -> Tuple[List[Dict], bool, Optional[str]]:
        """Handle restaurant-specific queries (e.g., "I am at Southern Spice, what should I order?")."""
        restaurant_name = parsed_query.get("restaurant_name")
        
        if not restaurant_name:
            return [], False, "No restaurant name provided"
        
        # Search for restaurant
        restaurants = await self._search_restaurants_by_name(restaurant_name)
        
        if not restaurants:
            return [], False, f"Restaurant '{restaurant_name}' not found"
        
        # Get top dishes for the restaurant
        restaurant_id = restaurants[0]["restaurant_id"]
        dishes = await self._get_restaurant_dishes(restaurant_id, limit=max_results)
        
        # Format recommendations with deduplication
        recommendations = []
        seen_dishes = set()  # Track (dish_name, restaurant_id) combinations
        
        for dish in dishes:
            dish_key = (dish["dish_name"], restaurant_id)
            if dish_key in seen_dishes:
                continue  # Skip duplicates
            seen_dishes.add(dish_key)
            
            recommendation = {
                "type": "dish",
                "dish_name": dish["dish_name"],
                "restaurant_name": restaurants[0]["restaurant_name"],
                "restaurant_id": restaurant_id,
                "location": restaurants[0].get("city", ""),  # Add restaurant location
                "neighborhood": restaurants[0].get("neighborhood", ""),
                "cuisine_type": restaurants[0].get("cuisine_type", ""),
                "sentiment_score": dish["sentiment_score"],
                "recommendation_score": dish["recommendation_score"],
                # Hybrid fields
                "topic_mentions": dish.get("topic_mentions", 0),
                "topic_score": dish.get("topic_score", 0.0),
                "final_score": dish.get("final_score", dish.get("recommendation_score", 0.0)),
                "source": dish.get("source", "sentiment"),
                "category": dish.get("category", "main"),
                "confidence": dish.get("confidence_score", 0.5)
            }
            recommendations.append(recommendation)
        
        return recommendations, False, None
    
    async def _handle_location_cuisine_query(self, parsed_query: Dict[str, Any], max_results: int) -> Tuple[List[Dict], bool, Optional[str]]:
        """Handle location + cuisine queries (e.g., "I am in Jersey City and in mood to eat Indian cuisine")."""
        location = parsed_query.get("location")
        cuisine_type = parsed_query.get("cuisine_type")
        neighborhood_context = parsed_query.get("neighborhood_context")
        
        if not location or not cuisine_type:
            return [], False, "Location and cuisine type required"
        
        # Handle neighborhood-specific queries (e.g., "Manhattan in Times Square")
        city = location
        neighborhood = None
        
        if " in " in location:
            parts = location.split(" in ")
            city = parts[0].strip()
            neighborhood = parts[1].strip()
            app_logger.info(f"🔍 Neighborhood query detected: {city} in {neighborhood}")
            
            # Use enhanced neighborhood search with Yelp API data
            from src.data_collection.yelp_collector import YelpCollector
            yelp_collector = YelpCollector()
            
            if yelp_collector.settings.yelp_api_key:
                try:
                    app_logger.info(f"🔍 Using Yelp API for neighborhood-specific search: {neighborhood}")
                    yelp_neighborhood_results = await yelp_collector.search_by_neighborhood(
                        city, neighborhood, cuisine_type, limit=max_results
                    )
                    
                    if yelp_neighborhood_results:
                        app_logger.info(f"✅ Found {len(yelp_neighborhood_results)} restaurants in {neighborhood} via Yelp API")
                        # Convert Yelp results to our recommendation format
                        recommendations = []
                        for restaurant in yelp_neighborhood_results[:3]:  # Top 3 restaurants
                            dishes = await self._get_restaurant_dishes(restaurant["restaurant_id"], 3)
                            
                            for dish in dishes:
                                recommendation = {
                                    "type": "dish",
                                    "dish_name": dish["dish_name"],
                                    "restaurant_name": restaurant["restaurant_name"],
                                    "restaurant_id": restaurant["restaurant_id"],
                                    "cuisine_type": cuisine_type,
                                    "location": location,
                                    "neighborhood": neighborhood,
                                    "sentiment_score": dish["sentiment_score"],
                                    "recommendation_score": dish["recommendation_score"],
                                    "restaurant_rating": restaurant["rating"],
                                    "confidence": dish.get("confidence_score", 0.5),
                                    "source": "yelp_neighborhood"
                                }
                                recommendations.append(recommendation)
                        
                        return recommendations, False, None
                        
                except Exception as e:
                    app_logger.warning(f"Yelp neighborhood search failed: {e}")
        
        # 1) Topics-first dish retrieval (hybrid)
        try:
            topic_dishes = self.milvus_client.search_dishes_with_topics(cuisine=cuisine_type, limit=max_results * 3)
            app_logger.info(f"🔍 Topics-first: found {len(topic_dishes)} {cuisine_type} dishes with topics")

            # Filter by city/neighborhood via restaurant details
            filtered_topic_dishes: List[Dict[str, Any]] = []
            for dish in topic_dishes:
                restaurant = await self._get_restaurant_details(dish.get("restaurant_id", ""))
                if not restaurant:
                    continue
                # City filter
                if restaurant.get("city") != city:
                    continue
                # Neighborhood filter if requested
                if neighborhood and restaurant.get("neighborhood") and neighborhood.lower() not in restaurant.get("neighborhood", "").lower():
                    continue

                # Build recommendation entry with hybrid fields
                filtered_topic_dishes.append({
                    "type": "dish",
                    "dish_name": dish.get("dish_name", "Unknown"),
                    "restaurant_name": restaurant.get("restaurant_name", "Unknown"),
                    "restaurant_id": restaurant.get("restaurant_id", ""),
                    "cuisine_type": cuisine_type,
                    "location": location,
                    "neighborhood": restaurant.get("neighborhood", ""),
                    "sentiment_score": float(dish.get("sentiment_score", 0.0) or 0.0),
                    "recommendation_score": float(dish.get("recommendation_score", 0.0) or 0.0),
                    # Hybrid fields
                    "topic_mentions": int(dish.get("topic_mentions", 0) or 0),
                    "topic_score": float(dish.get("topic_score", 0.0) or 0.0),
                    "final_score": float(dish.get("final_score", 0.0) or 0.0),
                    "source": dish.get("source", "hybrid"),
                    "restaurant_rating": float(restaurant.get("rating", 0.0) or 0.0),
                    "confidence": float(dish.get("confidence_score", 0.5) or 0.5)
                })

            # Sort by final_score desc and take top results
            if filtered_topic_dishes:
                filtered_topic_dishes.sort(key=lambda r: r.get("final_score", 0.0), reverse=True)
                top_topic_recs = filtered_topic_dishes[:max_results]
                # If we have enough, return immediately
                if len(top_topic_recs) >= max_results:
                    return top_topic_recs, False, None
                # Otherwise keep them and backfill using restaurant-based flow below
                topic_backfill = top_topic_recs
            else:
                topic_backfill = []
        except Exception as e:
            app_logger.warning(f"Topics-first retrieval failed: {e}")
            topic_backfill = []

        # 2) Backfill via restaurant-first flow
        # Search for restaurants in location with cuisine type
        filters = {
            "city": city,
            "cuisine_type": cuisine_type
        }
        
        # Add neighborhood filter if specified
        if neighborhood:
            filters["neighborhood"] = neighborhood
        
        # Try neighborhood-specific search first
        restaurants = await self._search_restaurants_with_filters(filters, max_results)
        
        # If no results and we have a neighborhood filter, try city-level search
        if not restaurants and neighborhood:
            app_logger.info(f"🔍 No neighborhood-specific results, trying city-level search for {city}")
            city_filters = {
                "city": city,
                "cuisine_type": cuisine_type
            }
            restaurants = await self._search_restaurants_with_filters(city_filters, max_results)
        
        if not restaurants:
            return [], False, f"No {cuisine_type} restaurants found in {location}"
        
        # Get top dishes for each restaurant with deduplication
        recommendations: List[Dict[str, Any]] = []
        seen_dishes = set()  # Track (dish_name, restaurant_id) combinations
        
        # Seed with any topic-based recs we already have
        if topic_backfill:
            for rec in topic_backfill:
                dish_key = (rec["dish_name"], rec["restaurant_id"])
                if dish_key not in seen_dishes:
                    recommendations.append(rec)
                    seen_dishes.add(dish_key)
        
        for restaurant in restaurants[:3]:  # Top 3 restaurants
            dishes = await self._get_restaurant_dishes(restaurant["restaurant_id"], 3)
            
            for dish in dishes:
                dish_key = (dish["dish_name"], restaurant["restaurant_id"])
                if dish_key in seen_dishes:
                    continue  # Skip duplicates
                seen_dishes.add(dish_key)
                
                recommendation = {
                    "type": "dish",
                    "dish_name": dish["dish_name"],
                    "restaurant_name": restaurant["restaurant_name"],
                    "restaurant_id": restaurant["restaurant_id"],
                    "cuisine_type": cuisine_type,
                    "location": location,
                    "neighborhood": restaurant.get("neighborhood", ""),
                    "sentiment_score": dish["sentiment_score"],
                    "recommendation_score": dish["recommendation_score"],
                    # Hybrid fields
                    "topic_mentions": dish.get("topic_mentions", 0),
                    "topic_score": dish.get("topic_score", 0.0),
                    "final_score": dish.get("final_score", dish.get("recommendation_score", 0.0)),
                    "source": dish.get("source", "sentiment"),
                    "restaurant_rating": restaurant["rating"],
                    "confidence": dish.get("confidence_score", 0.5)
                }
                recommendations.append(recommendation)
        
        # Prefer higher final_score when available
        recommendations.sort(key=lambda r: r.get("final_score", r.get("recommendation_score", 0.0)), reverse=True)
        recommendations = recommendations[:max_results]
        return recommendations, False, None
    
    async def _handle_location_dish_query(self, parsed_query: Dict[str, Any], max_results: int) -> Tuple[List[Dict], bool, Optional[str]]:
        """Handle location + dish queries (e.g., "I am in Jersey City and in mood to eat Chicken Biryani")."""
        location = parsed_query.get("location")
        dish_name = parsed_query.get("dish_name")
        cuisine_type = parsed_query.get("cuisine_type")
        
        if not location or not dish_name:
            return [], False, "Location and dish name required"
        
        # Extract city/neighborhood for filtering
        city = location
        neighborhood = None
        if " in " in location:
            parts = location.split(" in ")
            city = parts[0].strip()
            neighborhood = parts[1].strip()

        # 1) Topics-first: prefer hybrid topic dishes, biasing matches to the requested dish
        topic_recommendations: List[Dict[str, Any]] = []
        try:
            topic_dishes = self.milvus_client.search_dishes_with_topics(cuisine=cuisine_type, limit=max_results * 4)
            filtered: List[Dict[str, Any]] = []
            for dish in topic_dishes:
                restaurant = await self._get_restaurant_details(dish.get("restaurant_id", ""))
                if not restaurant:
                    continue
                if restaurant.get("city") != city:
                    continue
                if neighborhood and restaurant.get("neighborhood") and neighborhood.lower() not in restaurant.get("neighborhood", "").lower():
                    continue
                # Build rec
                rec = {
                    "type": "dish",
                    "dish_name": dish.get("dish_name", "Unknown"),
                    "restaurant_name": restaurant.get("restaurant_name", "Unknown"),
                    "restaurant_id": restaurant.get("restaurant_id", ""),
                    "location": location,
                    "neighborhood": restaurant.get("neighborhood", ""),
                    "cuisine_type": cuisine_type or restaurant.get("cuisine_type"),
                    "sentiment_score": float(dish.get("sentiment_score", 0.0) or 0.0),
                    "recommendation_score": float(dish.get("recommendation_score", 0.0) or 0.0),
                    # Hybrid fields
                    "topic_mentions": int(dish.get("topic_mentions", 0) or 0),
                    "topic_score": float(dish.get("topic_score", 0.0) or 0.0),
                    "final_score": float(dish.get("final_score", 0.0) or 0.0),
                    "source": dish.get("source", "hybrid"),
                    "restaurant_rating": float(restaurant.get("rating", 0.0) or 0.0),
                    "confidence": float(dish.get("confidence_score", 0.5) or 0.5)
                }
                # Add a simple match flag to bias exact/substring matches
                dn = rec["dish_name"] or ""
                match_flag = 1 if (dish_name.lower() in dn.lower() or dn.lower() in dish_name.lower()) else 0
                rec["_match_bias"] = match_flag
                filtered.append(rec)

            if filtered:
                # Sort: exact/substring matches first, then by final_score
                filtered.sort(key=lambda r: (r.get("_match_bias", 0), r.get("final_score", 0.0)), reverse=True)
                topic_recommendations = filtered[:max_results]
                if len(topic_recommendations) >= max_results:
                    # Remove helper key
                    for r in topic_recommendations:
                        r.pop("_match_bias", None)
                    return topic_recommendations, False, None
                for r in filtered:
                    r.pop("_match_bias", None)
                    if len(topic_recommendations) < max_results:
                        topic_recommendations.append(r)
        except Exception as e:
            app_logger.warning(f"Topics-first (location_dish) failed: {e}")
            topic_recommendations = []

        # Try location-aware dish expansion with availability checking
        from src.processing.location_aware_fallback import LocationAwareFallback
        location_fallback = LocationAwareFallback(self)
        
        # Check what's actually available
        fallback_result = await location_fallback.get_intelligent_fallback(
            dish_name, location, parsed_query.get("cuisine_type")
        )
        
        app_logger.info(f"Original dish: {dish_name}, Fallback result: {fallback_result['type']}")
        
        if fallback_result['type'] == 'location_specific_available':
            # We have location-specific dishes available
            available_dishes = fallback_result['available_dishes']
            app_logger.info(f"Found {len(available_dishes)} location-specific dishes: {available_dishes}")
            
            # Search for original dish first
            dishes = await self._search_dishes_by_name_and_location(dish_name, location, max_results)
            
            # If no results, try available location-specific variants
            if not dishes and available_dishes:
                for available_dish in available_dishes[:3]:  # Try top 3
                    dishes = await self._search_dishes_by_name_and_location(available_dish, location, max_results)
                    if dishes:
                        app_logger.info(f"Found results for available location dish: {available_dish}")
                        break
        else:
            # Use intelligent fallback
            app_logger.info(f"Using intelligent fallback: {fallback_result['message']}")
            
            # Search for original dish first
            dishes = await self._search_dishes_by_name_and_location(dish_name, location, max_results)
            
            # If no results, try fallback dishes
            if not dishes and fallback_result['available_fallbacks']:
                for fallback_dish in fallback_result['available_fallbacks'][:3]:
                    dishes = await self._search_dishes_by_name_and_location(fallback_dish, location, max_results)
                    if dishes:
                        app_logger.info(f"Found results for fallback dish: {fallback_dish}")
                        break
        
        if not dishes:
            # Try to find similar dishes from the same cuisine
            cuisine_type = parsed_query.get("cuisine_type")
            if cuisine_type:
                app_logger.info(f"No '{dish_name}' found, trying similar {cuisine_type} dishes")
                similar_dishes = await self._find_similar_dishes_by_cuisine(cuisine_type, location, max_results)
                if similar_dishes:
                    app_logger.info(f"Found {len(similar_dishes)} similar {cuisine_type} dishes")
                    return similar_dishes, False, f"No '{dish_name}' found, showing similar {cuisine_type} dishes"
            
            return [], False, f"No '{dish_name}' or related dishes found in {location}"
        
        # Get restaurant details for each dish and apply location-aware ranking
        recommendations: List[Dict[str, Any]] = []
        # Seed with topics-first results if any
        if topic_recommendations:
            recommendations.extend(topic_recommendations)
        restaurants_to_rank = []
        
        for dish in dishes:
            restaurant = await self._get_restaurant_details(dish["restaurant_id"])
            
            if restaurant:
                # Add dish info to restaurant for ranking
                restaurant_with_dish = {
                    **restaurant,
                    "dish_name": dish["dish_name"],
                    "sentiment_score": dish["sentiment_score"],
                    "recommendation_score": dish["recommendation_score"],
                    "confidence": dish.get("confidence_score", 0.5)
                }
                restaurants_to_rank.append(restaurant_with_dish)
        
        # Apply location-aware ranking
        if restaurants_to_rank:
            from src.processing.location_aware_ranking import LocationAwareRanking
            location_ranker = LocationAwareRanking()
            
            ranked_restaurants = location_ranker.rank_restaurants_by_location(
                restaurants_to_rank, 
                location, 
                cuisine=parsed_query.get("cuisine_type"),
                dish_name=dish_name
            )
            
            # Convert back to recommendations format
            for ranked_restaurant in ranked_restaurants[:max_results]:
                recommendation = {
                    "type": "dish",
                    "dish_name": ranked_restaurant["dish_name"],
                    "restaurant_name": ranked_restaurant["restaurant_name"],
                    "restaurant_id": ranked_restaurant["restaurant_id"],
                    "location": location,
                    "neighborhood": ranked_restaurant.get("neighborhood", ""),
                    "cuisine_type": ranked_restaurant.get("cuisine_type"),
                    "sentiment_score": ranked_restaurant["sentiment_score"],
                    "recommendation_score": ranked_restaurant["recommendation_score"],
                    "restaurant_rating": ranked_restaurant["rating"],
                    "confidence": ranked_restaurant["confidence"],
                    "location_score": ranked_restaurant.get("location_score", 0.5),
                    "combined_score": ranked_restaurant.get("combined_score", 0.5)
                }
                recommendations.append(recommendation)
        
        # Prefer higher final_score when available
        recommendations.sort(key=lambda r: r.get("final_score", r.get("recommendation_score", 0.0)), reverse=True)
        recommendations = recommendations[:max_results]
        return recommendations, False, None
    
    async def _handle_location_general_query(self, parsed_query: Dict[str, Any], max_results: int) -> Tuple[List[Dict], bool, Optional[str]]:
        """Handle general location queries (e.g., "I am in Jersey City and very hungry")."""
        location = parsed_query.get("location")
        
        if not location:
            return [], False, "Location required"
        
        # 1) Topics-first: popular dishes in this city
        topic_first_recs: List[Dict[str, Any]] = []
        try:
            topic_dishes = self.milvus_client.search_dishes_with_topics(limit=max_results * 4)
            filtered: List[Dict[str, Any]] = []
            for dish in topic_dishes:
                restaurant = await self._get_restaurant_details(dish.get("restaurant_id", ""))
                if not restaurant:
                    continue
                if restaurant.get("city") != location:
                    continue
                filtered.append({
                    "type": "dish",
                    "dish_name": dish.get("dish_name", "Unknown"),
                    "restaurant_name": restaurant.get("restaurant_name", "Unknown"),
                    "restaurant_id": restaurant.get("restaurant_id", ""),
                    "location": location,
                    "neighborhood": restaurant.get("neighborhood", ""),
                    "cuisine_type": restaurant.get("cuisine_type"),
                    "sentiment_score": float(dish.get("sentiment_score", 0.0) or 0.0),
                    "recommendation_score": float(dish.get("recommendation_score", 0.0) or 0.0),
                    # Hybrid fields
                    "topic_mentions": int(dish.get("topic_mentions", 0) or 0),
                    "topic_score": float(dish.get("topic_score", 0.0) or 0.0),
                    "final_score": float(dish.get("final_score", 0.0) or 0.0),
                    "source": dish.get("source", "hybrid"),
                    "restaurant_rating": float(restaurant.get("rating", 0.0) or 0.0),
                    "confidence": float(dish.get("confidence_score", 0.5) or 0.5)
                })
            if filtered:
                filtered.sort(key=lambda r: r.get("final_score", 0.0), reverse=True)
                topic_first_recs = filtered[:max_results]
                if len(topic_first_recs) >= max_results:
                    return topic_first_recs, False, None
        except Exception as e:
            app_logger.warning(f"Topics-first (location_general) failed: {e}")
            topic_first_recs = []

        # Check if we have too many restaurants (ask for cuisine selection)
        total_restaurants = await self._get_restaurant_count_by_location(location)
        app_logger.info(f"🔍 Found {total_restaurants} restaurants in {location}")
        
        if total_restaurants > 300:
            return [], False, f"Too many restaurants in {location}. Please specify a cuisine type."
        
        # Get top restaurants in location with location-aware ranking
        filters = self._get_location_filters(location)
        restaurants = await self._search_restaurants_with_filters(filters, max_results)
        app_logger.info(f"🔍 Retrieved {len(restaurants)} restaurants for {location}")
        
        if not restaurants:
            app_logger.warning(f"❌ No restaurants found in {location} with filters: {filters}")
            return [], False, f"No restaurants found in {location}"
        
        # Apply location-aware ranking to restaurants
        from src.processing.location_aware_ranking import LocationAwareRanking
        location_ranker = LocationAwareRanking()
        
        ranked_restaurants = location_ranker.rank_restaurants_by_location(
            restaurants, location
        )
        
        # Get top dishes for each ranked restaurant with deduplication
        recommendations: List[Dict[str, Any]] = []
        seen_dishes = set()  # Track (dish_name, restaurant_id) combinations
        
        # Seed with topics-first recs if any
        if topic_first_recs:
            for rec in topic_first_recs:
                dish_key = (rec["dish_name"], rec["restaurant_id"])
                if dish_key not in seen_dishes:
                    recommendations.append(rec)
                    seen_dishes.add(dish_key)
        
        for restaurant in ranked_restaurants[:3]:  # Top 3 restaurants
            dishes = await self._get_restaurant_dishes(restaurant["restaurant_id"], 2)
            
            for dish in dishes:
                dish_key = (dish["dish_name"], restaurant["restaurant_id"])
                if dish_key in seen_dishes:
                    continue  # Skip duplicates
                seen_dishes.add(dish_key)
                
                recommendation = {
                    "type": "dish",
                    "dish_name": dish["dish_name"],
                    "restaurant_name": restaurant["restaurant_name"],
                    "restaurant_id": restaurant["restaurant_id"],
                    "location": location,
                    "neighborhood": restaurant.get("neighborhood", ""),
                    "cuisine_type": restaurant["cuisine_type"],
                    "sentiment_score": dish["sentiment_score"],
                    "recommendation_score": dish["recommendation_score"],
                    # Hybrid fields
                    "topic_mentions": dish.get("topic_mentions", 0),
                    "topic_score": dish.get("topic_score", 0.0),
                    "final_score": dish.get("final_score", dish.get("recommendation_score", 0.0)),
                    "source": dish.get("source", "sentiment"),
                    "restaurant_rating": restaurant["rating"],
                    "confidence": dish.get("confidence_score", 0.5),
                    "location_score": restaurant.get("location_score", 0.5),
                    "combined_score": restaurant.get("combined_score", 0.5)
                }
                recommendations.append(recommendation)
        
        # Prefer higher final_score when available
        recommendations.sort(key=lambda r: r.get("final_score", r.get("recommendation_score", 0.0)), reverse=True)
        recommendations = recommendations[:max_results]
        return recommendations, False, None
    
    async def _handle_meal_type_query(self, parsed_query: Dict[str, Any], max_results: int) -> Tuple[List[Dict], bool, Optional[str]]:
        """Handle meal type queries (e.g., "I am in Hoboken and wanted to find place for Brunch")."""
        location = parsed_query.get("location")
        meal_type = parsed_query.get("meal_type")
        
        if not location or not meal_type:
            return [], False, "Location and meal type required"
        
        # Search for restaurants with meal type
        filters = {
            "city": location,
            "meal_types": [meal_type]
        }
        
        restaurants = await self._search_restaurants_with_filters(filters, max_results)
        
        if not restaurants:
            return [], False, f"No {meal_type} restaurants found in {location}"
        
        # Format recommendations
        recommendations = []
        for restaurant in restaurants[:3]:  # Top 3 restaurants
            recommendation = {
                "type": "restaurant",
                "restaurant_name": restaurant["restaurant_name"],
                "restaurant_id": restaurant["restaurant_id"],
                "location": location,
                "cuisine_type": restaurant["cuisine_type"],
                "meal_type": meal_type,
                "rating": restaurant["rating"],
                "review_count": restaurant["review_count"],
                "confidence": 0.7
            }
            recommendations.append(recommendation)
        
        return recommendations, False, None
    
    async def _handle_unknown_query(self, parsed_query: Dict[str, Any], max_results: int) -> Tuple[List[Dict], bool, Optional[str]]:
        """Handle unknown query types."""
        return [], False, "Unable to understand query. Please provide location and/or cuisine preference."
    
    async def _search_restaurants_by_name(self, restaurant_name: str) -> List[Dict]:
        """Search restaurants by name."""
        # Generate embedding for restaurant name
        query_vector = await self._generate_embedding(restaurant_name)
        
        # Search in Milvus
        restaurants = self.milvus_client.search_restaurants(
            query_vector, 
            filters={"restaurant_name": restaurant_name},
            limit=5
        )
        
        return restaurants
    
    async def _search_restaurants_with_filters(self, filters: Dict, max_results: int) -> List[Dict]:
        """Search restaurants with filters and rank by quality score."""
        # Use filter search directly (no vector search needed for filters)
        # Note: MilvusClient.search_restaurants_with_filters is NOT async, so don't await it
        restaurants = self.milvus_client.search_restaurants_with_filters(
            filters,
            limit=max_results
        )
        
        return restaurants  # Already sorted by quality_score in milvus_client
    
    async def _search_dishes_by_name_and_location(self, dish_name: str, location: str, max_results: int) -> List[Dict]:
        """Search dishes by name and location."""
        # Generate embedding for dish name
        query_vector = await self._generate_embedding(dish_name)
        
        # Search in Milvus
        dishes = self.milvus_client.search_dishes(
            query_vector,
            filters={"normalized_dish_name": dish_name},
            limit=max_results
        )
        
        # Filter by location with neighborhood support
        filtered_dishes = []
        for dish in dishes:
            restaurant = await self._get_restaurant_details(dish["restaurant_id"])
            if restaurant and self._is_location_match(restaurant, location):
                filtered_dishes.append(dish)
        
        return filtered_dishes
    

    async def _get_restaurant_dishes(self, restaurant_id: str, limit: int = 5) -> List[Dict]:
        """Get top dishes for a restaurant."""
        # Use search with filter to get dishes for this restaurant
        # We use a neutral query vector since we're filtering by restaurant_id
        neutral_query = "restaurant dishes menu"
        query_vector = [0.0] * self.settings.vector_dimension  # Use zero vector for simple filtering
        
        dishes = self.milvus_client.search_dishes(
            query_vector,
            filters={"restaurant_id": restaurant_id},
            limit=limit
        )
        
        # Sort by recommendation score (descending)
        dishes.sort(key=lambda x: x.get("recommendation_score", 0), reverse=True)
        
        return dishes
    
    async def _get_restaurant_details(self, restaurant_id: str) -> Optional[Dict]:
        """Get restaurant details by ID."""
        # Use search with filter to get specific restaurant
        # We use a neutral query vector since we're filtering by restaurant_id
        neutral_query = "restaurant details"
        query_vector = await self._generate_embedding(neutral_query)
        
        restaurants = self.milvus_client.search_restaurants(
            query_vector,
            filters={"restaurant_id": restaurant_id},
            limit=1
        )
        
        return restaurants[0] if restaurants else None
    
    async def _get_restaurant_count_by_location(self, location: str) -> int:
        """Get total restaurant count for a location."""
        # Use filter search to get restaurants in location
        filters = self._get_location_filters(location)
        restaurants = self.milvus_client.search_restaurants_with_filters(
            filters,
            limit=1000  # High limit to get approximate count
        )
        
        return len(restaurants)
    
    def _is_location_match(self, restaurant: Dict[str, Any], location: str) -> bool:
        """Check if restaurant matches the location query with neighborhood support."""
        restaurant_city = restaurant.get("city", "")
        restaurant_neighborhood = restaurant.get("neighborhood", "")
        
        # Resolve the query location
        location_info = location_resolver.resolve_location(location)
        
        # Direct city match
        if restaurant_city == location:
            return True
        
        # Check if location resolves to the restaurant's city
        if location_info.resolved_city and restaurant_city == location_info.resolved_city:
            return True
        
        # Check neighborhood match (if restaurant has neighborhood data)
        if (location_info.neighborhood and restaurant_neighborhood and 
            location_info.neighborhood.lower() in restaurant_neighborhood.lower()):
            return True
        
        # Fallback: check if query location is a neighborhood and restaurant is in that area
        if location_info.location_type == "neighborhood":
            # For Manhattan, all restaurants in Manhattan match Manhattan neighborhood queries
            if location_info.resolved_city == "Manhattan" and restaurant_city == "Manhattan":
                return True
        
        return False
    
    def _get_location_filters(self, location: str) -> Dict[str, str]:
        """Get appropriate filters for location search with neighborhood support."""
        location_info = location_resolver.resolve_location(location)
        
        if location_info.resolved_city:
            # Use resolved city for filtering
            return {"city": location_info.resolved_city}
        else:
            # Fallback to original location
            return {"city": location}
    
    async def _generate_embedding(self, text: str) -> List[float]:
        """Generate embedding for text using OpenAI."""

        # Add caching for common queries
        if text in self._embedding_cache:
            return self._embedding_cache[text]

        try:
            response = await self.client.embeddings.create(
                model=self.settings.embedding_model,
                input=text
            )
            
            embedding = response.data[0].embedding
            self._embedding_cache[text] = embedding
            return embedding
            
        except Exception as e:
            app_logger.error(f"Error generating embedding: {e}")
            # Return zero vector as fallback
            return [0.0] * self.settings.vector_dimension
    
    def calculate_confidence(self, recommendations: List[Dict], parsed_query: Dict[str, Any]) -> float:
        """Calculate confidence score for recommendations."""
        if not recommendations:
            return 0.0
        
        # Base confidence from query parsing - handle both dict and float formats
        confidence_data = parsed_query.get("confidence", 0.5)
        if isinstance(confidence_data, dict):
            base_confidence = confidence_data.get("overall", 0.5)
        else:
            base_confidence = float(confidence_data)
        
        # Adjust based on number of recommendations
        if len(recommendations) >= 3:
            quantity_factor = 1.0
        elif len(recommendations) >= 1:
            quantity_factor = 0.8
        else:
            quantity_factor = 0.5
        
        # Adjust based on recommendation quality
        avg_confidence = sum(r.get("confidence", 0.5) for r in recommendations) / len(recommendations)
        
        # Combine factors
        final_confidence = (base_confidence + avg_confidence + quantity_factor) / 3
        
        return min(1.0, max(0.0, final_confidence))
    
    async def get_restaurant_details(self, restaurant_id: str) -> Optional[Dict]:
        """Get detailed restaurant information."""
        return await self._get_restaurant_details(restaurant_id)
    
    async def get_restaurant_dishes(self, restaurant_id: str, limit: int = 5) -> List[Dict]:
        """Get top dishes for a restaurant."""
        return await self._get_restaurant_dishes(restaurant_id, limit)
    
    async def get_dish_details(self, dish_id: str) -> Optional[Dict]:
        """Get detailed dish information."""
        # Use real embedding like other methods
        neutral_query = "dish details"
        query_vector = await self._generate_embedding(neutral_query)
        
        dishes = self.milvus_client.search_dishes(
            query_vector,
            filters={"dish_id": dish_id},
            limit=1
        )
        
        return dishes[0] if dishes else None
    
    async def _find_similar_dishes_by_cuisine(self, cuisine_type: str, location: str, max_results: int) -> List[Dict]:
        """Find similar dishes from the same cuisine when specific dish not found."""
        try:
            # Search for restaurants of the specified cuisine in the location
            filters = {
                "city": location,
                "cuisine_type": cuisine_type
            }
            
            restaurants = await self._search_restaurants_with_filters(filters, 5)
            
            if not restaurants:
                return []
            
            # Get top dishes from these restaurants
            recommendations = []
            for restaurant in restaurants:
                dishes = await self._get_restaurant_dishes(restaurant["restaurant_id"], 2)
                
                for dish in dishes:
                    recommendation = {
                        "type": "similar_dish",
                        "dish_name": dish["dish_name"],
                        "restaurant_name": restaurant["restaurant_name"],
                        "restaurant_id": restaurant["restaurant_id"],
                        "location": location,
                        "neighborhood": dish.get("neighborhood", ""),
                        "cuisine_type": cuisine_type,
                        "sentiment_score": dish["sentiment_score"],
                        "recommendation_score": dish["recommendation_score"],
                        "restaurant_rating": restaurant["rating"],
                        "confidence": dish.get("confidence_score", 0.4),  # Lower confidence for similar dishes
                        "reason": f"Similar {cuisine_type} dish"
                    }
                    recommendations.append(recommendation)
                    
                    if len(recommendations) >= max_results:
                        break
                
                if len(recommendations) >= max_results:
                    break
            
            return recommendations
            
        except Exception as e:
            app_logger.error(f"Error finding similar dishes: {e}")
            return []