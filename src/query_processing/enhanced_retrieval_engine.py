"""
Enhanced Retrieval Engine for Discovery Collections and traditional collections.
Handles both the new AI-driven discovery data and existing restaurant/dish data.
"""
import asyncio
from typing import List, Dict, Optional, Any, Tuple
from openai import AsyncOpenAI
from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.vector_db.milvus_client import MilvusClient
from src.vector_db.discovery_collections import DiscoveryCollections
from src.query_processing.retrieval_engine import RetrievalEngine


class EnhancedRetrievalEngine(RetrievalEngine):
    """Enhanced retrieval engine with discovery collections support."""
    
    def __init__(self, milvus_client: MilvusClient):
        super().__init__(milvus_client)
        self.discovery_collections = DiscoveryCollections()
        
    async def get_recommendations(self, parsed_query: Dict[str, Any], max_results: int = 10) -> Tuple[List[Dict], bool, Optional[str]]:
        """Get recommendations using enhanced discovery collections + fallback to traditional."""
        try:
            # First try discovery collections for better AI-driven results
            discovery_results = await self._get_discovery_recommendations(parsed_query, max_results)
            
            if discovery_results and len(discovery_results) >= max_results // 2:
                # We have good discovery results, return them
                app_logger.info(f"✅ Using discovery collections: {len(discovery_results)} results")
                return discovery_results, False, None
            
            # Use OpenAI for intelligent fallback when discovery results are insufficient
            if discovery_results:
                app_logger.info("✅ Using discovery collections with OpenAI enhancement")
                return discovery_results, False, None
            else:
                app_logger.info("🤖 Using OpenAI fallback for intelligent recommendations")
                return await self._get_openai_fallback_recommendations(parsed_query, max_results)
            
        except Exception as e:
            app_logger.error(f"Error in enhanced retrieval: {e}")
            # Fallback to OpenAI for intelligent recommendations
            return await self._get_openai_fallback_recommendations(parsed_query, max_results)
    
    async def _get_discovery_recommendations(self, parsed_query: Dict[str, Any], max_results: int) -> List[Dict]:
        """Get recommendations from discovery collections."""
        query_type = parsed_query.get("intent", "unknown")
        
        if query_type == "location_cuisine":
            return await self._get_discovery_location_cuisine(parsed_query, max_results)
        elif query_type == "location_dish":
            return await self._get_discovery_location_dish(parsed_query, max_results)
        elif query_type == "location_general":
            return await self._get_discovery_location_general(parsed_query, max_results)
        elif query_type == "restaurant_specific":
            return await self._get_discovery_restaurant_specific(parsed_query, max_results)
        else:
            # For other query types, try famous restaurants
            return await self._get_discovery_famous_restaurants(parsed_query, max_results)
    
    async def _get_discovery_location_cuisine(self, parsed_query: Dict[str, Any], max_results: int) -> List[Dict]:
        """Get location + cuisine recommendations from discovery collections."""
        location = parsed_query.get("location")
        cuisine_type = parsed_query.get("cuisine_type")
        
        if not location or not cuisine_type:
            return []
        
        try:
            # Extract city and neighborhood
            city = location
            neighborhood = None
            if " in " in location:
                parts = location.split(" in ")
                city = parts[0].strip()
                neighborhood = parts[1].strip()
            
            recommendations = []
            
            # 1. Get neighborhood analysis results (Phase 2 data)
            collection_name = 'discovery_neighborhood_analysis'
            if self.milvus_client.has_collection(collection_name):
                # Build filter expression
                filter_expr = f'city == "{city}" and cuisine_type == "{cuisine_type}"'
                if neighborhood:
                    filter_expr += f' and neighborhood like "%{neighborhood}%"'
                
                # Search with vector similarity (generate embedding for location + cuisine)
                embedding_text = f"{location} {cuisine_type} restaurant recommendations"
                query_vector = await self._generate_embedding(embedding_text)
                
                # Search using Milvus client
                results = self.milvus_client.search_collection(
                    collection_name=collection_name,
                    query_vector=query_vector,
                    filter_expr=filter_expr,
                    limit=max_results,
                    output_fields=["*"]
                )
                
                if results and results[0]:
                    seen_restaurants = set()
                    
                    # Helper function to extract fields safely
                    def get_field(entity, field_name, default='Unknown'):
                        if hasattr(entity, field_name):
                            return getattr(entity, field_name)
                        elif hasattr(entity, 'get'):
                            return entity.get(field_name, default)
                        return default
                    
                    for hits in results:
                        for hit in hits:
                            # Extract data from hit - handle both Hit and dict types
                            if hasattr(hit.entity, 'get'):
                                # If entity is a dict-like object
                                entity = hit.entity
                            else:
                                # If entity is a Hit object, access fields directly
                                entity = hit.entity
                            
                            # Avoid duplicate restaurants
                            restaurant_name = get_field(entity, 'restaurant_name', 'Unknown')
                            restaurant_id = get_field(entity, 'restaurant_id', '')
                            restaurant_key = (restaurant_name, restaurant_id)
                            
                            if restaurant_key in seen_restaurants:
                                continue
                            seen_restaurants.add(restaurant_key)
                            
                            recommendation = {
                                "type": "discovery_dish",
                                "dish_name": get_field(entity, 'top_dish_name', 'Unknown'),
                                "restaurant_name": restaurant_name,
                                "restaurant_id": restaurant_id,
                                "location": location,
                                "neighborhood": get_field(entity, 'neighborhood', ''),
                                "cuisine_type": cuisine_type,
                                "sentiment_score": float(get_field(entity, 'top_dish_sentiment_score', 0.0)),
                                "recommendation_score": float(get_field(entity, 'top_dish_final_score', 0.0)),
                                "final_score": float(get_field(entity, 'top_dish_final_score', 0.0)),
                                "topic_mentions": int(get_field(entity, 'top_dish_topic_mentions', 0)),
                                "restaurant_rating": float(get_field(entity, 'rating', 0.0)),
                                "restaurant_rank": int(get_field(entity, 'restaurant_rank', 1)),
                                "hybrid_quality_score": float(get_field(entity, 'hybrid_quality_score', 0.0)),
                                "total_dishes": int(get_field(entity, 'total_dishes', 0)),
                                "confidence": float(get_field(entity, 'analysis_confidence', 0.8)),
                                "source": "ai_discovery",
                                "similarity_score": float(hit.score)
                            }
                            recommendations.append(recommendation)
            
            # 2. Add famous restaurants for this cuisine (Phase 1 data) - PRIORITY
            famous_restaurants = await self._get_famous_restaurants_by_cuisine(city, cuisine_type, max_results // 2)
            recommendations.extend(famous_restaurants)
            
            # 3. Add popular dishes for this cuisine (Phase 1 data)
            popular_dishes = await self._get_popular_dishes_by_cuisine(city, cuisine_type, max_results // 4)
            recommendations.extend(popular_dishes)
            
            # Remove duplicates and sort by quality
            seen_restaurants = set()
            unique_recommendations = []
            for rec in recommendations:
                restaurant_key = (rec.get('restaurant_name', ''), rec.get('restaurant_id', ''))
                if restaurant_key not in seen_restaurants:
                    seen_restaurants.add(restaurant_key)
                    unique_recommendations.append(rec)
            
            # Sort by source priority (famous restaurants first, then popular dishes, then others)
            def sort_key(r):
                source_priority = {
                    "famous_restaurant": 4,
                    "popular_dish": 3,
                    "ai_discovery": 2,
                    "ai_discovery_neighborhood": 2,
                    "ai_discovery_popular": 2,
                    "openai_fallback": 1
                }
                return (
                    source_priority.get(r.get("source", ""), 0),
                    r.get("fame_score", 0),
                    r.get("popularity_score", 0),
                    r.get("final_score", 0),
                    r.get("similarity_score", 0)
                )
            
            unique_recommendations.sort(key=sort_key, reverse=True)
            
            app_logger.info(f"🔍 Discovery location+cuisine: found {len(unique_recommendations)} results")
            return unique_recommendations[:max_results]
            
        except Exception as e:
            app_logger.error(f"Error in discovery location+cuisine search: {e}")
            return []
    
    async def _get_discovery_location_dish(self, parsed_query: Dict[str, Any], max_results: int) -> List[Dict]:
        """Get location + dish recommendations from discovery collections."""
        location = parsed_query.get("location")
        dish_name = parsed_query.get("dish_name")
        
        if not location or not dish_name:
            return []
        
        try:
            # Extract city
            city = location
            if " in " in location:
                city = location.split(" in ")[0].strip()
            
            recommendations = []
            
            # 1. Search neighborhood analysis for dishes (Phase 2 data)
            collection_name = 'discovery_neighborhood_analysis'
            if self.milvus_client.has_collection(collection_name):
                # Build filter for city and dish name (fuzzy match)
                filter_expr = f'city == "{city}" and top_dish_name like "%{dish_name}%"'
                
                # Generate embedding for dish search
                embedding_text = f"{location} {dish_name} restaurant"
                query_vector = await self._generate_embedding(embedding_text)
                
                # Search using Milvus client
                results = self.milvus_client.search_collection(
                    collection_name=collection_name,
                    query_vector=query_vector,
                    filter_expr=filter_expr,
                    limit=max_results,
                    output_fields=["*"]
                )
                
                if results and results[0]:
                    seen_restaurants = set()
                    
                    # Helper function to extract fields safely
                    def get_field(entity, field_name, default='Unknown'):
                        if hasattr(entity, field_name):
                            return getattr(entity, field_name)
                        elif hasattr(entity, 'get'):
                            return entity.get(field_name, default)
                        return default
                    
                    for hits in results:
                        for hit in hits:
                            entity = hit.entity
                            
                            restaurant_name = get_field(entity, 'restaurant_name', 'Unknown')
                            restaurant_id = get_field(entity, 'restaurant_id', '')
                            restaurant_key = (restaurant_name, restaurant_id)
                            if restaurant_key in seen_restaurants:
                                continue
                            seen_restaurants.add(restaurant_key)
                            
                            # Calculate match score for dish name
                            dish_in_data = get_field(entity, 'top_dish_name', '').lower()
                            dish_query = dish_name.lower()
                            match_score = 1.0 if dish_query in dish_in_data or dish_in_data in dish_query else 0.5
                            
                            recommendation = {
                                "type": "discovery_dish",
                                "dish_name": get_field(entity, 'top_dish_name', 'Unknown'),
                                "restaurant_name": restaurant_name,
                                "restaurant_id": restaurant_id,
                                "location": location,
                                "neighborhood": get_field(entity, 'neighborhood', ''),
                                "cuisine_type": get_field(entity, 'cuisine_type', ''),
                                "sentiment_score": float(get_field(entity, 'top_dish_sentiment_score', 0.0)),
                                "recommendation_score": float(get_field(entity, 'top_dish_final_score', 0.0)),
                                "final_score": float(get_field(entity, 'top_dish_final_score', 0.0)),
                                "topic_mentions": int(get_field(entity, 'top_dish_topic_mentions', 0)),
                                "restaurant_rating": float(get_field(entity, 'rating', 0.0)),
                                "restaurant_rank": int(get_field(entity, 'restaurant_rank', 1)),
                                "hybrid_quality_score": float(get_field(entity, 'hybrid_quality_score', 0.0)),
                                "match_score": match_score,
                                "confidence": float(get_field(entity, 'analysis_confidence', 0.8)),
                                "source": "ai_discovery",
                                "similarity_score": float(hit.score)
                            }
                            recommendations.append(recommendation)
            
            # 2. Add famous restaurants known for this dish (Phase 1 data) - PRIORITY
            famous_restaurants = await self._get_famous_restaurants_by_dish(city, dish_name, max_results // 2)
            recommendations.extend(famous_restaurants)
            
            # 3. Add popular dishes that match this dish (Phase 1 data)
            popular_dishes = await self._get_popular_dishes_by_dish(city, dish_name, max_results // 4)
            recommendations.extend(popular_dishes)
            
            # Remove duplicates and sort by match score and quality
            seen_restaurants = set()
            unique_recommendations = []
            for rec in recommendations:
                restaurant_key = (rec.get('restaurant_name', ''), rec.get('restaurant_id', ''))
                if restaurant_key not in seen_restaurants:
                    seen_restaurants.add(restaurant_key)
                    unique_recommendations.append(rec)
            
            # Sort by source priority and match score
            def sort_key(r):
                source_priority = {
                    "famous_restaurant": 4,
                    "popular_dish": 3,
                    "ai_discovery": 2,
                    "openai_fallback": 1
                }
                return (
                    source_priority.get(r.get("source", ""), 0),
                    r.get("match_score", 0),
                    r.get("fame_score", 0),
                    r.get("final_score", 0),
                    r.get("similarity_score", 0)
                )
            
            unique_recommendations.sort(key=sort_key, reverse=True)
            
            app_logger.info(f"🔍 Discovery location+dish: found {len(unique_recommendations)} results")
            return unique_recommendations[:max_results]
            
        except Exception as e:
            app_logger.error(f"Error in discovery location+dish search: {e}")
            return []
    
    async def _get_discovery_location_general(self, parsed_query: Dict[str, Any], max_results: int) -> List[Dict]:
        """Get general location recommendations from discovery collections."""
        location = parsed_query.get("location")
        
        if not location:
            return []
        
        try:
            # Extract city
            city = location
            if " in " in location:
                city = location.split(" in ")[0].strip()
            
            # Query discovery collections for popular dishes and restaurants using Milvus client
            recommendations = []
            
            # 1. Get top dishes from neighborhood analysis
            collection_name = 'discovery_neighborhood_analysis'
            if self.milvus_client.has_collection(collection_name):
                try:
                    filter_expr = f'city == "{city}"'
                    embedding_text = f"{location} best restaurants"
                    query_vector = await self._generate_embedding(embedding_text)
                    
                    results = self.milvus_client.search_collection(
                        collection_name=collection_name,
                        query_vector=query_vector,
                        filter_expr=filter_expr,
                        limit=max_results,
                        output_fields=["*"]
                    )
                    
                    if results and results[0]:
                        seen_restaurants = set()
                        
                        # Helper function to extract fields safely
                        def get_field(entity, field_name, default='Unknown'):
                            if hasattr(entity, field_name):
                                return getattr(entity, field_name)
                            elif hasattr(entity, 'get'):
                                return entity.get(field_name, default)
                            return default
                        
                        for hits in results:
                            for hit in hits:
                                entity = hit.entity
                                
                                restaurant_name = get_field(entity, 'restaurant_name', 'Unknown')
                                restaurant_id = get_field(entity, 'restaurant_id', '')
                                restaurant_key = (restaurant_name, restaurant_id)
                                if restaurant_key in seen_restaurants:
                                    continue
                                seen_restaurants.add(restaurant_key)
                                
                                recommendation = {
                                    "type": "discovery_dish",
                                    "dish_name": get_field(entity, 'top_dish_name', 'Unknown'),
                                    "restaurant_name": restaurant_name,
                                    "restaurant_id": restaurant_id,
                                    "location": location,
                                    "neighborhood": get_field(entity, 'neighborhood', ''),
                                    "cuisine_type": get_field(entity, 'cuisine_type', ''),
                                    "sentiment_score": float(get_field(entity, 'top_dish_sentiment_score', 0.0)),
                                    "recommendation_score": float(get_field(entity, 'top_dish_final_score', 0.0)),
                                    "final_score": float(get_field(entity, 'top_dish_final_score', 0.0)),
                                    "restaurant_rating": float(get_field(entity, 'rating', 0.0)),
                                    "restaurant_rank": int(get_field(entity, 'restaurant_rank', 1)),
                                    "hybrid_quality_score": float(get_field(entity, 'hybrid_quality_score', 0.0)),
                                    "confidence": float(get_field(entity, 'analysis_confidence', 0.8)),
                                    "source": "ai_discovery_neighborhood",
                                    "similarity_score": float(hit.score)
                                }
                                recommendations.append(recommendation)
                            
                except Exception as e:
                    app_logger.warning(f"Error querying neighborhood collection: {e}")
            
            # 2. Get popular dishes for the city
            collection_name = 'discovery_popular_dishes'
            if self.milvus_client.has_collection(collection_name) and len(recommendations) < max_results:
                try:
                    filter_expr = f'city == "{city}"'
                    embedding_text = f"{location} popular dishes"
                    query_vector = await self._generate_embedding(embedding_text)
                    
                    results = self.milvus_client.search_collection(
                        collection_name=collection_name,
                        query_vector=query_vector,
                        filter_expr=filter_expr,
                        limit=max_results // 3,
                        output_fields=["*"]
                    )
                    
                    if results and results[0]:
                        # Helper function to extract fields safely
                        def get_field(entity, field_name, default='Unknown'):
                            if hasattr(entity, field_name):
                                return getattr(entity, field_name)
                            elif hasattr(entity, 'get'):
                                return entity.get(field_name, default)
                            return default
                        
                        for hits in results:
                            for hit in hits:
                                entity = hit.entity
                                
                                recommendation = {
                                    "type": "discovery_popular_dish",
                                    "dish_name": get_field(entity, 'dish_name', 'Unknown'),
                                    "restaurant_name": "Multiple locations",  # Popular dishes span multiple restaurants
                                    "restaurant_id": "",
                                    "location": location,
                                    "neighborhood": "",
                                    "cuisine_type": get_field(entity, 'primary_cuisine', ''),
                                    "popularity_score": float(get_field(entity, 'popularity_score', 0.0)),
                                    "frequency": int(get_field(entity, 'frequency', 0)),
                                    "avg_sentiment": float(get_field(entity, 'avg_sentiment', 0.0)),
                                    "restaurant_count": int(get_field(entity, 'restaurant_count', 0)),
                                    "confidence": float(get_field(entity, 'confidence_score', 0.8)),
                                    "source": "ai_discovery_popular",
                                    "similarity_score": float(hit.score)
                                }
                                recommendations.append(recommendation)
                            
                except Exception as e:
                    app_logger.warning(f"Error querying popular dishes collection: {e}")
            
            # 3. Add famous restaurants for the city (Phase 1 data) - PRIORITY
            famous_restaurants = await self._get_famous_restaurants_by_location(city, max_results // 2)
            recommendations.extend(famous_restaurants)
            
            # Remove duplicates and sort by various scores
            seen_restaurants = set()
            unique_recommendations = []
            for rec in recommendations:
                restaurant_key = (rec.get('restaurant_name', ''), rec.get('restaurant_id', ''))
                if restaurant_key not in seen_restaurants:
                    seen_restaurants.add(restaurant_key)
                    unique_recommendations.append(rec)
            
            # Sort by source priority and various scores
            def sort_key(r):
                source_priority = {
                    "famous_restaurant": 4,
                    "popular_dish": 3,
                    "ai_discovery": 2,
                    "ai_discovery_neighborhood": 2,
                    "ai_discovery_popular": 2,
                    "openai_fallback": 1
                }
                return (
                    source_priority.get(r.get("source", ""), 0),
                    r.get("fame_score", 0),
                    r.get("popularity_score", 0),
                    r.get("final_score", 0),
                    r.get("hybrid_quality_score", 0),
                    r.get("similarity_score", 0)
                )
            
            unique_recommendations.sort(key=sort_key, reverse=True)
            
            app_logger.info(f"🔍 Discovery general location: found {len(unique_recommendations)} results")
            return unique_recommendations[:max_results]
            
        except Exception as e:
            app_logger.error(f"Error in discovery general location search: {e}")
            return []
    
    async def _get_discovery_restaurant_specific(self, parsed_query: Dict[str, Any], max_results: int) -> List[Dict]:
        """Get recommendations for restaurant-specific queries."""
        restaurant_name = parsed_query.get("restaurant_name")
        location = parsed_query.get("location")
        
        if not restaurant_name:
            return []
        
        try:
            # Extract city if location provided
            city = location if location else None
            if city and " in " in city:
                city = city.split(" in ")[0].strip()
            
            # Search famous restaurants by name
            collection_name = 'discovery_famous_restaurants'
            if not self.milvus_client.has_collection(collection_name):
                return []
            
            # Build filter expression
            filter_expr = f'restaurant_name like "%{restaurant_name}%"'
            if city:
                filter_expr += f' and city == "{city}"'
            
            # Generate embedding for search
            embedding_text = f"{restaurant_name} restaurant {city if city else ''}"
            query_vector = await self._generate_embedding(embedding_text)
            
            # Search using Milvus client
            results = self.milvus_client.search_collection(
                collection_name=collection_name,
                query_vector=query_vector,
                filter_expr=filter_expr,
                limit=max_results,
                output_fields=["*"]
            )
            
            if not results or not results[0]:
                return []
            
            # Convert to recommendations
            recommendations = []
            
            # Helper function to extract fields safely
            def get_field(entity, field_name, default='Unknown'):
                if hasattr(entity, field_name):
                    return getattr(entity, field_name)
                elif hasattr(entity, 'get'):
                    return entity.get(field_name, default)
                return default
            
            for hits in results:
                for hit in hits:
                    entity = hit.entity
                    
                    recommendation = {
                        "type": "famous_restaurant",
                        "dish_name": get_field(entity, 'famous_dish', 'Unknown'),
                        "restaurant_name": get_field(entity, 'restaurant_name', 'Unknown'),
                        "restaurant_id": get_field(entity, 'restaurant_id', ''),
                        "location": get_field(entity, 'city', location or ''),
                        "neighborhood": get_field(entity, 'neighborhood', ''),
                        "cuisine_type": get_field(entity, 'cuisine_type', ''),
                        "fame_score": float(get_field(entity, 'fame_score', 0.0)),
                        "dish_popularity": float(get_field(entity, 'dish_popularity', 0.0)),
                        "restaurant_rating": float(get_field(entity, 'rating', 0.0)),
                        "review_count": int(get_field(entity, 'review_count', 0)),
                        "quality_score": float(get_field(entity, 'quality_score', 0.0)),
                        "price_range": int(get_field(entity, 'price_range', 2)),
                        "cultural_significance": get_field(entity, 'cultural_significance', ''),
                        "confidence": 0.9,  # High confidence for exact restaurant match
                        "source": "famous_restaurant",
                        "similarity_score": float(hit.score)
                    }
                    recommendations.append(recommendation)
            
            # Sort by fame score and similarity
            recommendations.sort(key=lambda r: (r.get("fame_score", 0), r.get("similarity_score", 0)), reverse=True)
            
            app_logger.info(f"🔍 Discovery restaurant-specific: found {len(recommendations)} results")
            return recommendations[:max_results]
            
        except Exception as e:
            app_logger.error(f"Error in discovery restaurant-specific search: {e}")
            return []
    
    async def _get_discovery_famous_restaurants(self, parsed_query: Dict[str, Any], max_results: int) -> List[Dict]:
        """Get famous restaurants for general queries."""
        location = parsed_query.get("location")
        cuisine_type = parsed_query.get("cuisine_type")
        dish_name = parsed_query.get("dish_name")
        
        try:
            # Extract city if location provided
            city = location if location else None
            if city and " in " in city:
                city = city.split(" in ")[0].strip()
            
            # Build search based on available criteria
            if city and cuisine_type:
                return await self._get_famous_restaurants_by_cuisine(city, cuisine_type, max_results)
            elif city and dish_name:
                return await self._get_famous_restaurants_by_dish(city, dish_name, max_results)
            elif city:
                return await self._get_famous_restaurants_by_location(city, max_results)
            elif cuisine_type:
                return await self._get_famous_restaurants_by_cuisine(None, cuisine_type, max_results)
            elif dish_name:
                return await self._get_famous_restaurants_by_dish(None, dish_name, max_results)
            else:
                # General famous restaurants query
                return await self._get_all_famous_restaurants(max_results)
                
        except Exception as e:
            app_logger.error(f"Error in discovery famous restaurants search: {e}")
            return []
    
    async def _get_famous_restaurants_by_location(self, city: str, max_results: int) -> List[Dict]:
        """Get famous restaurants in a specific city."""
        try:
            collection_name = 'discovery_famous_restaurants'
            if not self.milvus_client.has_collection(collection_name):
                return []
            
            # Build filter for city
            filter_expr = f'city == "{city}"'
            
            # Generate embedding for search
            embedding_text = f"{city} famous restaurants"
            query_vector = await self._generate_embedding(embedding_text)
            
            # Search using Milvus client
            results = self.milvus_client.search_collection(
                collection_name=collection_name,
                query_vector=query_vector,
                filter_expr=filter_expr,
                limit=max_results,
                output_fields=["*"]
            )
            
            return self._convert_famous_restaurants_results(results, max_results)
            
        except Exception as e:
            app_logger.error(f"Error getting famous restaurants by location: {e}")
            return []
    
    async def _get_famous_restaurants_by_cuisine(self, city: Optional[str], cuisine_type: str, max_results: int) -> List[Dict]:
        """Get famous restaurants for a specific cuisine."""
        try:
            collection_name = 'discovery_famous_restaurants'
            if not self.milvus_client.has_collection(collection_name):
                return []
            
            # Build filter for cuisine and optionally city
            filter_expr = f'cuisine_type == "{cuisine_type}"'
            if city:
                filter_expr += f' and city == "{city}"'
            
            # Generate embedding for search
            embedding_text = f"{cuisine_type} famous restaurants {city if city else ''}"
            query_vector = await self._generate_embedding(embedding_text)
            
            # Search using Milvus client
            results = self.milvus_client.search_collection(
                collection_name=collection_name,
                query_vector=query_vector,
                filter_expr=filter_expr,
                limit=max_results,
                output_fields=["*"]
            )
            
            return self._convert_famous_restaurants_results(results, max_results)
            
        except Exception as e:
            app_logger.error(f"Error getting famous restaurants by cuisine: {e}")
            return []
    
    async def _get_famous_restaurants_by_dish(self, city: Optional[str], dish_name: str, max_results: int) -> List[Dict]:
        """Get famous restaurants known for a specific dish."""
        try:
            collection_name = 'discovery_famous_restaurants'
            if not self.milvus_client.has_collection(collection_name):
                return []
            
            # Build filter for dish and optionally city
            filter_expr = f'famous_dish like "%{dish_name}%"'
            if city:
                filter_expr += f' and city == "{city}"'
            
            # Generate embedding for search
            embedding_text = f"{dish_name} famous restaurants {city if city else ''}"
            query_vector = await self._generate_embedding(embedding_text)
            
            # Search using Milvus client
            results = self.milvus_client.search_collection(
                collection_name=collection_name,
                query_vector=query_vector,
                filter_expr=filter_expr,
                limit=max_results,
                output_fields=["*"]
            )
            
            return self._convert_famous_restaurants_results(results, max_results)
            
        except Exception as e:
            app_logger.error(f"Error getting famous restaurants by dish: {e}")
            return []
    
    async def _get_all_famous_restaurants(self, max_results: int) -> List[Dict]:
        """Get all famous restaurants (general query)."""
        try:
            collection_name = 'discovery_famous_restaurants'
            if not self.milvus_client.has_collection(collection_name):
                return []
            
            # Generate embedding for general search
            embedding_text = "famous restaurants recommendations"
            query_vector = await self._generate_embedding(embedding_text)
            
            # Search using Milvus client
            results = self.milvus_client.search_collection(
                collection_name=collection_name,
                query_vector=query_vector,
                filter_expr=None,
                limit=max_results,
                output_fields=["*"]
            )
            
            return self._convert_famous_restaurants_results(results, max_results)
            
        except Exception as e:
            app_logger.error(f"Error getting all famous restaurants: {e}")
            return []
    
    async def _get_popular_dishes_by_cuisine(self, city: str, cuisine_type: str, max_results: int) -> List[Dict]:
        """Get popular dishes for a specific cuisine in a city."""
        try:
            collection_name = 'discovery_popular_dishes'
            if not self.milvus_client.has_collection(collection_name):
                return []
            
            # Build filter for city and cuisine
            filter_expr = f'city == "{city}" and primary_cuisine == "{cuisine_type}"'
            
            # Generate embedding for search
            embedding_text = f"{city} {cuisine_type} popular dishes"
            query_vector = await self._generate_embedding(embedding_text)
            
            # Search using Milvus client
            results = self.milvus_client.search_collection(
                collection_name=collection_name,
                query_vector=query_vector,
                filter_expr=filter_expr,
                limit=max_results,
                output_fields=["*"]
            )
            
            return self._convert_popular_dishes_results(results, max_results)
            
        except Exception as e:
            app_logger.error(f"Error getting popular dishes by cuisine: {e}")
            return []
    
    async def _get_popular_dishes_by_dish(self, city: str, dish_name: str, max_results: int) -> List[Dict]:
        """Get popular dishes that match a specific dish name."""
        try:
            collection_name = 'discovery_popular_dishes'
            if not self.milvus_client.has_collection(collection_name):
                return []
            
            # Build filter for city and dish name (fuzzy match)
            filter_expr = f'city == "{city}" and dish_name like "%{dish_name}%"'
            
            # Generate embedding for search
            embedding_text = f"{city} {dish_name} popular dishes"
            query_vector = await self._generate_embedding(embedding_text)
            
            # Search using Milvus client
            results = self.milvus_client.search_collection(
                collection_name=collection_name,
                query_vector=query_vector,
                filter_expr=filter_expr,
                limit=max_results,
                output_fields=["*"]
            )
            
            return self._convert_popular_dishes_results(results, max_results)
            
        except Exception as e:
            app_logger.error(f"Error getting popular dishes by dish: {e}")
            return []
    
    def _convert_popular_dishes_results(self, results: List, max_results: int) -> List[Dict]:
        """Convert popular dishes search results to recommendation format."""
        if not results or not results[0]:
            return []
        
        # Helper function to extract fields safely
        def get_field(entity, field_name, default='Unknown'):
            if hasattr(entity, field_name):
                return getattr(entity, field_name)
            elif hasattr(entity, 'get'):
                return entity.get(field_name, default)
            return default
        
        recommendations = []
        for hits in results:
            for hit in hits:
                entity = hit.entity
                
                recommendation = {
                    "type": "popular_dish",
                    "dish_name": get_field(entity, 'dish_name', 'Unknown'),
                    "restaurant_name": "Multiple locations",  # Popular dishes span multiple restaurants
                    "restaurant_id": "",
                    "location": get_field(entity, 'city', ''),
                    "neighborhood": "",
                    "cuisine_type": get_field(entity, 'primary_cuisine', ''),
                    "popularity_score": float(get_field(entity, 'popularity_score', 0.0)),
                    "frequency": int(get_field(entity, 'frequency', 0)),
                    "avg_sentiment": float(get_field(entity, 'avg_sentiment', 0.0)),
                    "restaurant_count": int(get_field(entity, 'restaurant_count', 0)),
                    "cultural_significance": get_field(entity, 'cultural_significance', ''),
                    "reasoning": get_field(entity, 'reasoning', ''),
                    "confidence": float(get_field(entity, 'confidence_score', 0.8)),
                    "source": "popular_dish",
                    "similarity_score": float(hit.score)
                }
                recommendations.append(recommendation)
        
        # Sort by popularity score and similarity
        recommendations.sort(key=lambda r: (r.get("popularity_score", 0), r.get("similarity_score", 0)), reverse=True)
        
        return recommendations[:max_results]
    
    def _convert_famous_restaurants_results(self, results: List, max_results: int) -> List[Dict]:
        """Convert famous restaurants search results to recommendation format."""
        if not results or not results[0]:
            return []
        
        # Helper function to extract fields safely
        def get_field(entity, field_name, default='Unknown'):
            if hasattr(entity, field_name):
                return getattr(entity, field_name)
            elif hasattr(entity, 'get'):
                return entity.get(field_name, default)
            return default
        
        recommendations = []
        for hits in results:
            for hit in hits:
                entity = hit.entity
                
                # Calculate match score for dish name if applicable
                match_score = 1.0  # Default high match for famous restaurants
                
                recommendation = {
                    "type": "famous_restaurant",
                    "dish_name": get_field(entity, 'famous_dish', 'Unknown'),
                    "restaurant_name": get_field(entity, 'restaurant_name', 'Unknown'),
                    "restaurant_id": get_field(entity, 'restaurant_id', ''),
                    "location": get_field(entity, 'city', ''),
                    "neighborhood": get_field(entity, 'neighborhood', ''),
                    "cuisine_type": get_field(entity, 'cuisine_type', ''),
                    "fame_score": float(get_field(entity, 'fame_score', 0.0)),
                    "dish_popularity": float(get_field(entity, 'dish_popularity', 0.0)),
                    "restaurant_rating": float(get_field(entity, 'rating', 0.0)),
                    "review_count": int(get_field(entity, 'review_count', 0)),
                    "quality_score": float(get_field(entity, 'quality_score', 0.0)),
                    "price_range": int(get_field(entity, 'price_range', 2)),
                    "cultural_significance": get_field(entity, 'cultural_significance', ''),
                    "match_score": match_score,
                    "confidence": 0.85,  # High confidence for famous restaurants
                    "source": "famous_restaurant",
                    "similarity_score": float(hit.score)
                }
                recommendations.append(recommendation)
        
        # Sort by fame score and similarity
        recommendations.sort(key=lambda r: (r.get("fame_score", 0), r.get("similarity_score", 0)), reverse=True)
        
        return recommendations[:max_results]

    async def get_discovery_stats(self) -> Dict[str, Any]:
        """Get statistics about discovery collections."""
        try:
            stats = {}
            
            for collection_name, collection in self.discovery_collections.collections.items():
                if collection:
                    try:
                        collection.load()  # Ensure collection is loaded
                        stats[collection_name] = {
                            "num_entities": collection.num_entities,
                            "schema_fields": [field.name for field in collection.schema.fields]
                        }
                    except Exception as e:
                        stats[collection_name] = {"error": str(e)}
                        
            return stats
            
        except Exception as e:
            app_logger.error(f"Error getting discovery stats: {e}")
            return {"error": str(e)}
    
    async def _get_openai_fallback_recommendations(self, parsed_query: Dict[str, Any], max_results: int) -> tuple[List[Dict], bool, str]:
        """Generate intelligent recommendations using OpenAI when discovery collections are insufficient."""
        try:
            location = parsed_query.get("location", "")
            cuisine_type = parsed_query.get("cuisine_type", "")
            dish_name = parsed_query.get("dish_name", "")
            restaurant_name = parsed_query.get("restaurant_name", "")
            query_type = parsed_query.get("intent", "unknown")
            
            # Check if location or cuisine is outside supported areas
            supported_locations = ['Manhattan', 'Jersey City', 'Hoboken']
            supported_cuisines = ['Italian', 'Indian', 'Chinese', 'American', 'Mexican']
            
            location_unsupported = location and location not in supported_locations
            cuisine_unsupported = cuisine_type and cuisine_type not in supported_cuisines
            
            # Build context for OpenAI
            context_parts = []
            if location:
                context_parts.append(f"Location: {location}")
            if cuisine_type:
                context_parts.append(f"Cuisine: {cuisine_type}")
            if dish_name:
                context_parts.append(f"Dish: {dish_name}")
            if restaurant_name:
                context_parts.append(f"Restaurant: {restaurant_name}")
            
            context = ", ".join(context_parts) if context_parts else "general dining"
            
            # Create OpenAI prompt
            system_prompt = """You are a restaurant recommendation expert for SweetPick. Generate intelligent restaurant recommendations based on the user's query.

Your expertise:
- Deep knowledge of dining scenes across various cities and cuisines
- Understanding of what makes restaurants special and worth recommending
- Ability to provide specific dish recommendations
- Focus on quality, authenticity, and memorable dining experiences

When generating recommendations:
- Be specific about what makes each recommendation special
- Include signature dishes when relevant
- Consider the location and cuisine context
- Provide realistic, actionable recommendations
- Focus on restaurants that would actually exist in the specified area
- If the location/cuisine isn't in your primary knowledge base, provide general but helpful recommendations

Format your response as a JSON array of restaurant objects with these fields:
- restaurant_name: The name of the restaurant
- dish_name: A signature or recommended dish
- cuisine_type: The primary cuisine
- location: The city/area
- neighborhood: Specific neighborhood if known
- restaurant_rating: A realistic rating (4.0-4.8)
- price_range: 1-4 (1=budget, 2=moderate, 3=upscale, 4=very expensive)
- cultural_significance: What makes this place special
- reasoning: Why this restaurant fits the query

Keep recommendations realistic and helpful, even for areas outside your primary knowledge base."""

            user_prompt = f"""Generate {max_results} restaurant recommendations for this query: {context}

Query type: {query_type}
User is looking for: {context}

Provide realistic, specific recommendations that would actually exist in the specified area.

Note: If the location or cuisine type is outside your primary knowledge base, provide general recommendations based on your knowledge of similar areas and cuisines."""

            # Call OpenAI
            response = await self._call_openai_for_recommendations(system_prompt, user_prompt)
            
            if response and isinstance(response, list):
                # Convert OpenAI response to our recommendation format
                recommendations = []
                for item in response[:max_results]:
                    recommendation = {
                        "type": "openai_recommendation",
                        "restaurant_name": item.get("restaurant_name", "Unknown"),
                        "dish_name": item.get("dish_name", "Signature dish"),
                        "cuisine_type": item.get("cuisine_type", "Various"),
                        "location": item.get("location", location or "Unknown"),
                        "neighborhood": item.get("neighborhood", ""),
                        "restaurant_rating": float(item.get("restaurant_rating", 4.0)),
                        "price_range": int(item.get("price_range", 2)),
                        "cultural_significance": item.get("cultural_significance", ""),
                        "reasoning": item.get("reasoning", ""),
                        "confidence": 0.7,  # Moderate confidence for AI-generated recommendations
                        "source": "openai_fallback",
                        "similarity_score": 0.8
                    }
                    recommendations.append(recommendation)
                
                app_logger.info(f"🤖 OpenAI fallback generated {len(recommendations)} recommendations")
                return recommendations, True, "OpenAI fallback - discovery collections insufficient"
            else:
                app_logger.warning("OpenAI fallback failed to generate valid recommendations")
                return [], True, "OpenAI fallback failed"
                
        except Exception as e:
            app_logger.error(f"Error in OpenAI fallback: {e}")
            return [], True, f"OpenAI fallback error: {str(e)}"
    
    async def _call_openai_for_recommendations(self, system_prompt: str, user_prompt: str) -> List[Dict]:
        """Call OpenAI to generate restaurant recommendations."""
        try:
            from openai import AsyncOpenAI
            
            client = AsyncOpenAI()
            
            response = await client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=1000,
                temperature=0.7
            )
            
            content = response.choices[0].message.content.strip()
            
            # Try to extract JSON from the response
            import json
            import re
            
            # Look for JSON array in the response
            json_match = re.search(r'\[.*\]', content, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                return json.loads(json_str)
            
            # If no JSON found, try to parse the entire response
            try:
                return json.loads(content)
            except:
                app_logger.warning("Could not parse OpenAI response as JSON")
                return []
                
        except Exception as e:
            app_logger.error(f"OpenAI API call failed: {e}")
            return []
