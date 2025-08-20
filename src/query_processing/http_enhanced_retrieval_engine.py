"""
HTTP-Enhanced Retrieval Engine for Discovery Collections.
Compatible with MilvusHTTPClient instead of the old MilvusClient.
"""
import asyncio
from typing import List, Dict, Optional, Any, Tuple

# Try to import OpenAI with fallback
try:
    from openai import AsyncOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    AsyncOpenAI = None

from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.vector_db.milvus_http_client import MilvusHTTPClient
from src.vector_db.discovery_collections import DiscoveryCollections


class HTTPEnhancedRetrievalEngine:
    """Enhanced retrieval engine with discovery collections support using HTTP client."""
    
    def __init__(self, milvus_client: MilvusHTTPClient):
        self.milvus_client = milvus_client
        self.discovery_collections = DiscoveryCollections()
        self._embedding_cache = {}
        self.settings = get_settings()
        
    async def _generate_embedding(self, text: str) -> List[float]:
        """Generate embedding for text using OpenAI."""
        if not text:
            vector_dim = getattr(self.settings, 'vector_dimension', 1536)  # Default to OpenAI embedding dimension
            return [0.0] * vector_dim
        
        # Check cache
        cache_key = f"embedding:{hash(text)}"
        if cache_key in self._embedding_cache:
            app_logger.debug(f"Using cached embedding for text: {text[:50]}...")
            return self._embedding_cache[cache_key]
        
        try:
            app_logger.debug(f"Generating embedding for text: {text[:50]}...")
            client = AsyncOpenAI()
            response = await client.embeddings.create(
                model="text-embedding-3-small",
                input=text
            )
            
            embedding = response.data[0].embedding
            self._embedding_cache[cache_key] = embedding
            
            app_logger.debug(f"Successfully generated embedding with {len(embedding)} dimensions")
            return embedding
            
        except Exception as e:
            app_logger.error(f"Error generating embedding: {e}")
            vector_dim = getattr(self.settings, 'vector_dimension', 1536)  # Default to OpenAI embedding dimension
            return [0.0] * vector_dim
        
    async def get_recommendations(self, parsed_query: Dict[str, Any], max_results: int = 10) -> Tuple[List[Dict], bool, Optional[str]]:
        """Get recommendations using enhanced discovery collections + fallback to HTTP client."""
        try:
            # First try discovery collections for better AI-driven results
            discovery_results = await self._get_discovery_recommendations(parsed_query, max_results)
            
            if discovery_results and len(discovery_results) >= max_results // 2:
                # We have good discovery results, return them
                app_logger.info(f"✅ Using discovery collections: {len(discovery_results)} results")
                return discovery_results, False, None
            
            # Use HTTP client fallback when discovery results are insufficient
            if discovery_results:
                app_logger.info("✅ Using discovery collections with HTTP client enhancement")
                return discovery_results, False, None
            else:
                app_logger.info("🌐 Using HTTP client fallback for recommendations")
                return await self._get_http_client_fallback_recommendations(parsed_query, max_results)
            
        except Exception as e:
            app_logger.error(f"Error in enhanced retrieval: {e}")
            # Fallback to HTTP client for recommendations
            return await self._get_http_client_fallback_recommendations(parsed_query, max_results)
    
    async def _get_discovery_recommendations(self, parsed_query: Dict[str, Any], max_results: int) -> List[Dict]:
        """Get recommendations from discovery collections using HTTP client."""
        query_type = parsed_query.get("intent", "unknown")
        
        if query_type == "location_cuisine":
            return await self._get_discovery_location_cuisine(parsed_query, max_results)
        elif query_type == "cuisine_general":
            # Handle cuisine-only queries by treating them as location_cuisine with default location
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
        
        if not cuisine_type:
            return []
        
        # For cuisine_general queries, use default location if none provided
        if not location:
            location = "Manhattan"  # Default to Manhattan for cuisine-only queries
        
        try:
            # Extract city and neighborhood
            city = location
            neighborhood = None
            if " in " in location:
                parts = location.split(" in ")
                city = parts[0].strip()
                neighborhood = parts[1].strip()
            
            # Use HTTP client to search dishes with topics
            app_logger.info(f"🔍 Searching for {cuisine_type} cuisine in {location}")
            raw_results = await self.milvus_client.search_dishes_with_topics(
                cuisine=cuisine_type,
                neighborhood=neighborhood,
                limit=max_results
            )
            
            if not raw_results:
                app_logger.info("No results from discovery collections")
                return []
            
            # Format results for consistency
            recommendations = []
            for i, result in enumerate(raw_results):
                recommendation = {
                    "id": result.get('restaurant_id', f"rec_{i}"),
                    "restaurant_name": result.get('restaurant_name', 'Restaurant'),
                    "dish_name": result.get('top_dish_name', 'Dish'),  # Use correct field name
                    "cuisine_type": result.get('cuisine_type', cuisine_type),
                    "neighborhood": result.get('neighborhood', neighborhood or location),
                    "description": f"Try the {result.get('top_dish_name', 'dish')} at {result.get('restaurant_name', 'this restaurant')} in {result.get('neighborhood', location)}. Highly recommended!",
                    "final_score": float(result.get('top_dish_final_score', 0.8)),  # Use correct field name
                    "rating": float(result.get('rating', 4.5)),  # Use actual rating from data
                    "price_range": "$$",  # Default since data doesn't have this
                    "source": "discovery_collections",
                    "confidence": float(result.get('top_dish_final_score', 0.8)),  # Use correct field name
                    "topic_score": float(result.get('top_dish_topic_mentions', 0.0)),  # Use correct field name
                    "recommendation_score": float(result.get('hybrid_quality_score', 0.8))  # Use correct field name
                }
                recommendations.append(recommendation)
            
            app_logger.info(f"✅ Found {len(recommendations)} recommendations from discovery collections")
            return recommendations
            
        except Exception as e:
            app_logger.error(f"Error getting discovery location cuisine recommendations: {e}")
            return []
    
    async def _get_discovery_location_dish(self, parsed_query: Dict[str, Any], max_results: int) -> List[Dict]:
        """Get location + dish recommendations from discovery collections with quality-based routing."""
        location = parsed_query.get("location")
        dish_name = parsed_query.get("dish_name")
        original_query = parsed_query.get("original_query", "")
        
        if not location or not dish_name:
            return []
        
        # Check if query contains quality indicators - prioritize popular dishes collection
        quality_keywords = ["best", "popular", "famous", "legendary", "top", "amazing", "outstanding", "excellent"]
        is_quality_query = any(keyword in original_query.lower() for keyword in quality_keywords)
        app_logger.info(f"🔍 Dish search - Query contains quality keywords: {is_quality_query}")
        
        try:
            # Extract city and neighborhood
            city = location
            neighborhood = None
            if " in " in location:
                parts = location.split(" in ")
                city = parts[0].strip()
                neighborhood = parts[1].strip()
            
            cuisine_type = parsed_query.get("cuisine_type")
            
            # For quality queries, use enhanced search strategy
            if is_quality_query:
                app_logger.info(f"🎯 Quality query detected - using enhanced search strategy for {dish_name}")
                
                # Step 1: Try popular dishes collection first (best for quality queries)
                app_logger.info(f"🎯 Step 1: Searching popular dishes collection for {dish_name}")
                popular_results = await self._search_popular_dishes_collection(
                    dish_name, cuisine_type, neighborhood, max_results
                )
                
                if popular_results:
                    app_logger.info(f"✅ Found {len(popular_results)} results from popular dishes collection")
                    return popular_results
                
                # Step 2: If no results, try neighborhood analysis collection
                app_logger.info(f"🎯 Step 2: No popular dishes results, trying neighborhood analysis collection")
                neighborhood_results = await self._search_neighborhood_collection(
                    dish_name, cuisine_type, neighborhood, max_results
                )
                
                if neighborhood_results:
                    app_logger.info(f"✅ Found {len(neighborhood_results)} results from neighborhood collection")
                    return neighborhood_results
                
                # Step 3: If still no results, use OpenAI fallback for creative suggestions
                app_logger.info(f"🎯 Step 3: No results from either collection, using OpenAI fallback")
                try:
                    fallback_recommendations = await self._get_openai_dish_fallback(
                        dish_name, cuisine_type, location, max_results
                    )
                    if fallback_recommendations:
                        app_logger.info(f"✅ OpenAI fallback generated {len(fallback_recommendations)} recommendations")
                        return fallback_recommendations
                except Exception as e:
                    app_logger.warning(f"OpenAI fallback failed: {e}")
                
                # If all else fails, return empty list
                app_logger.warning(f"❌ No recommendations found for {dish_name} despite quality query")
                return []
                
            else:
                # Standard search for non-quality queries
                app_logger.info(f"📍 Standard search for {dish_name}")
                raw_results = await self.milvus_client.search_dishes_with_topics(
                    cuisine=cuisine_type,
                    neighborhood=neighborhood,
                    limit=max_results * 2
                )
                
                if not raw_results:
                    app_logger.info("No results from discovery collections")
                    return []
                
                # Filter results by dish name (case insensitive)
                dish_lower = dish_name.lower()
                filtered_results = [
                    result for result in raw_results 
                    if dish_lower in result.get('top_dish_name', '').lower()
                ]
                
                # Format results for consistency
                recommendations = []
                for i, result in enumerate(filtered_results):
                    recommendation = self._format_recommendation(result, i, neighborhood, location)
                    recommendations.append(recommendation)
                
                app_logger.info(f"✅ Found {len(recommendations)} recommendations for {dish_name} from discovery collections")
                return recommendations
            
        except Exception as e:
            app_logger.error(f"Error getting discovery location dish recommendations: {e}")
            return []
    
    async def _get_discovery_location_general(self, parsed_query: Dict[str, Any], max_results: int) -> List[Dict]:
        """Get general location recommendations from discovery collections."""
        location = parsed_query.get("location")
        
        if not location:
            return []
        
        try:
            # Extract city and neighborhood
            city = location
            neighborhood = None
            if " in " in location:
                parts = location.split(" in ")
                city = parts[0].strip()
                neighborhood = parts[1].strip()
            
            # Use HTTP client to search dishes with topics (no cuisine filter)
            app_logger.info(f"🔍 Searching for general recommendations in {location}")
            raw_results = await self.milvus_client.search_dishes_with_topics(
                cuisine=None,  # No cuisine filter for general search
                neighborhood=neighborhood,
                limit=max_results
            )
            
            if not raw_results:
                app_logger.info("No results from discovery collections")
                return []
            
            # Format results for consistency
            recommendations = []
            for i, result in enumerate(raw_results):
                recommendation = {
                    "id": result.get('restaurant_id', f"rec_{i}"),
                    "restaurant_name": result.get('restaurant_name', 'Restaurant'),
                    "dish_name": result.get('top_dish_name', 'Dish'),  # Use correct field name
                    "cuisine_type": result.get('cuisine_type', 'Unknown'),
                    "neighborhood": result.get('neighborhood', neighborhood or location),
                    "description": f"Try the {result.get('top_dish_name', 'dish')} at {result.get('restaurant_name', 'this restaurant')} in {result.get('neighborhood', location)}. Highly recommended!",
                    "final_score": float(result.get('top_dish_final_score', 0.8)),  # Use correct field name
                    "rating": float(result.get('rating', 4.5)),  # Use actual rating from data
                    "price_range": "$$",
                    "source": "discovery_collections",
                    "confidence": float(result.get('top_dish_final_score', 0.8)),  # Use correct field name
                    "topic_score": float(result.get('top_dish_topic_mentions', 0.0)),  # Use correct field name
                    "recommendation_score": float(result.get('hybrid_quality_score', 0.8))  # Use correct field name
                }
                recommendations.append(recommendation)
            
            app_logger.info(f"✅ Found {len(recommendations)} general recommendations from discovery collections")
            return recommendations
            
        except Exception as e:
            app_logger.error(f"Error getting discovery location general recommendations: {e}")
            return []
    
    async def _get_discovery_restaurant_specific(self, parsed_query: Dict[str, Any], max_results: int) -> List[Dict]:
        """Get restaurant-specific recommendations from discovery collections."""
        restaurant_name = parsed_query.get("restaurant_name")
        
        if not restaurant_name:
            return []
        
        try:
            # Use HTTP client to search dishes with topics (no filters)
            app_logger.info(f"🔍 Searching for dishes at {restaurant_name}")
            raw_results = await self.milvus_client.search_dishes_with_topics(
                cuisine=None,
                neighborhood=None,
                limit=max_results * 2  # Get more results to filter by restaurant
            )
            
            if not raw_results:
                app_logger.info("No results from discovery collections")
                return []
            
            # Filter results by restaurant name (case insensitive)
            restaurant_lower = restaurant_name.lower()
            filtered_results = [
                result for result in raw_results 
                if restaurant_lower in result.get('restaurant_name', '').lower()
            ]
            
            # Format results for consistency
            recommendations = []
            for i, result in enumerate(filtered_results[:max_results]):
                recommendation = {
                    "id": result.get('restaurant_id', f"rec_{i}"),
                    "restaurant_name": result.get('restaurant_name', 'Restaurant'),
                    "dish_name": result.get('top_dish_name', 'Dish'),  # Use correct field name
                    "cuisine_type": result.get('cuisine_type', 'Unknown'),
                    "neighborhood": result.get('neighborhood', 'Unknown'),
                    "description": f"Try the {result.get('top_dish_name', 'dish')} at {result.get('restaurant_name', 'this restaurant')}. Highly recommended!",
                    "final_score": float(result.get('top_dish_final_score', 0.8)),  # Use correct field name
                    "rating": float(result.get('rating', 4.5)),  # Use actual rating from data
                    "price_range": "$$",
                    "source": "discovery_collections",
                    "confidence": float(result.get('top_dish_final_score', 0.8)),  # Use correct field name
                    "topic_score": float(result.get('top_dish_topic_mentions', 0.0)),  # Use correct field name
                    "recommendation_score": float(result.get('hybrid_quality_score', 0.8))  # Use correct field name
                }
                recommendations.append(recommendation)
            
            app_logger.info(f"✅ Found {len(recommendations)} recommendations for {restaurant_name} from discovery collections")
            return recommendations
            
        except Exception as e:
            app_logger.error(f"Error getting discovery restaurant specific recommendations: {e}")
            return []
    
    async def _get_discovery_famous_restaurants(self, parsed_query: Dict[str, Any], max_results: int) -> List[Dict]:
        """Get famous restaurant recommendations from discovery collections."""
        try:
            # Use HTTP client to search dishes with topics (no filters)
            app_logger.info(f"🔍 Searching for famous restaurant recommendations")
            raw_results = await self.milvus_client.search_dishes_with_topics(
                cuisine=None,
                neighborhood=None,
                limit=max_results
            )
            
            if not raw_results:
                app_logger.info("No results from discovery collections")
                return []
            
            # Format results for consistency
            recommendations = []
            for i, result in enumerate(raw_results):
                recommendation = {
                    "id": result.get('restaurant_id', f"rec_{i}"),
                    "restaurant_name": result.get('restaurant_name', 'Restaurant'),
                    "dish_name": result.get('top_dish_name', 'Dish'),  # Use correct field name
                    "cuisine_type": result.get('cuisine_type', 'Unknown'),
                    "neighborhood": result.get('neighborhood', 'Unknown'),
                    "description": f"Try the {result.get('top_dish_name', 'dish')} at {result.get('restaurant_name', 'this restaurant')}. Highly recommended!",
                    "final_score": float(result.get('top_dish_final_score', 0.8)),  # Use correct field name
                    "rating": float(result.get('rating', 4.5)),  # Use actual rating from data
                    "price_range": "$$",
                    "source": "discovery_collections",
                    "confidence": float(result.get('top_dish_final_score', 0.8)),  # Use correct field name
                    "topic_score": float(result.get('top_dish_topic_mentions', 0.0)),  # Use correct field name
                    "recommendation_score": float(result.get('hybrid_quality_score', 0.8))  # Use correct field name
                }
                recommendations.append(recommendation)
            
            app_logger.info(f"✅ Found {len(recommendations)} famous restaurant recommendations from discovery collections")
            return recommendations
            
        except Exception as e:
            app_logger.error(f"Error getting discovery famous restaurant recommendations: {e}")
            return []
    
    async def _get_http_client_fallback_recommendations(self, parsed_query: Dict[str, Any], max_results: int) -> Tuple[List[Dict], bool, Optional[str]]:
        """Fallback to HTTP client for recommendations."""
        try:
            cuisine = parsed_query.get('cuisine_type', 'Italian')
            location = parsed_query.get('location', 'Manhattan')
            
            # Extract neighborhood from location if present
            neighborhood = None
            if location and " in " in location:
                parts = location.split(" in ")
                if len(parts) > 1:
                    neighborhood = parts[1].strip()
            
            # Get raw results from Milvus HTTP client
            app_logger.info(f"🔍 Calling HTTP client fallback with cuisine: {cuisine}, neighborhood: {neighborhood}")
            raw_recommendations = await self.milvus_client.search_dishes_with_topics(
                cuisine, 
                neighborhood, 
                max_results
            )
            app_logger.info(f"🔍 HTTP client fallback returned {len(raw_recommendations)} recommendations")
            
            # Format raw results for UI
            if raw_recommendations:
                formatted_recommendations = []
                for i, rec in enumerate(raw_recommendations):
                    formatted_rec = {
                        "id": rec.get('restaurant_id', f"rec_{i}"),
                        "restaurant_name": rec.get('restaurant_name', 'Restaurant'),
                        "dish_name": rec.get('top_dish_name', 'Dish'),  # Use correct field name
                        "cuisine_type": rec.get('cuisine_type', cuisine),
                        "neighborhood": rec.get('neighborhood', neighborhood or location),
                        "description": f"Try the {rec.get('top_dish_name', 'dish')} at {rec.get('restaurant_name', 'this restaurant')} in {rec.get('neighborhood', location)}. Highly recommended!",
                        "final_score": float(rec.get('top_dish_final_score', 0.8)),  # Use correct field name
                        "rating": float(rec.get('rating', 4.5)),  # Use actual rating from data
                        "price_range": "$$",  # Default since your data doesn't have this
                        "source": "http_client_fallback",
                        "confidence": float(rec.get('top_dish_final_score', 0.8)),  # Use correct field name
                        "topic_score": float(rec.get('top_dish_topic_mentions', 0.0)),  # Use correct field name
                        "recommendation_score": float(rec.get('hybrid_quality_score', 0.8))  # Use correct field name
                    }
                    formatted_recommendations.append(formatted_rec)
                
                app_logger.info(f"✅ HTTP client fallback successful: {len(formatted_recommendations)} recommendations")
                return formatted_recommendations, False, None
            else:
                app_logger.warning("No results from HTTP client fallback")
                return [], True, "No vector search results found"
                
        except Exception as e:
            app_logger.error(f"Error in HTTP client fallback: {e}")
            return [], True, f"HTTP client fallback error: {e}"
    
    def calculate_confidence(self, recommendations: List[Dict], parsed_query: Dict[str, Any]) -> float:
        """Calculate confidence score for recommendations."""
        if not recommendations:
            return 0.0
        
        # Base confidence on number of recommendations and their scores
        base_confidence = min(len(recommendations) / 10.0, 1.0)  # More recommendations = higher confidence
        
        # Average the final scores of recommendations
        if recommendations:
            avg_score = sum(float(rec.get('final_score', 0.5)) for rec in recommendations) / len(recommendations)
            return (base_confidence + avg_score) / 2.0
        
        return base_confidence
    
    async def _search_popular_dishes_collection(self, dish_name: str, cuisine_type: str, neighborhood: str, max_results: int) -> List[Dict]:
        """Search specifically in the popular dishes collection."""
        try:
            # Get all collections to find popular_dishes
            collections = await self.milvus_client.list_collections()
            popular_collection = None
            
            for col in collections:
                if "popular_dishes" in col.lower():
                    popular_collection = col
                    break
            
            if not popular_collection:
                app_logger.warning("No popular dishes collection found")
                return []
            
            app_logger.info(f"🎯 Searching popular dishes collection: {popular_collection}")
            
            # Use direct collection search for better control
            raw_results = await self.milvus_client._pure_vector_search(
                popular_collection, max_results * 2, cuisine_type
            )
            
            if not raw_results:
                return []
            
            # Filter by dish name and neighborhood
            dish_lower = dish_name.lower()
            filtered_results = []
            
            for result in raw_results:
                result_dish = result.get('top_dish_name', '').lower()
                result_neighborhood = result.get('neighborhood', '').lower()
                
                # Check if dish name contains the search term
                if dish_lower in result_dish:
                    # If neighborhood is specified, check for neighborhood match
                    if neighborhood and neighborhood.lower() in result_neighborhood:
                        filtered_results.append(result)
                    # If no neighborhood specified, include all matches
                    elif not neighborhood:
                        filtered_results.append(result)
            
            # Format results
            recommendations = []
            for i, result in enumerate(filtered_results[:max_results]):
                recommendation = self._format_recommendation(result, i, neighborhood, "popular_dishes")
                recommendations.append(recommendation)
            
            return recommendations
            
        except Exception as e:
            app_logger.error(f"Error searching popular dishes collection: {e}")
            return []
    
    async def _search_neighborhood_collection(self, dish_name: str, cuisine_type: str, neighborhood: str, max_results: int) -> List[Dict]:
        """Search in the neighborhood analysis collection as fallback."""
        try:
            # Get all collections to find neighborhood_analysis
            collections = await self.milvus_client.list_collections()
            neighborhood_collection = None
            
            for col in collections:
                if "neighborhood_analysis" in col.lower():
                    neighborhood_collection = col
                    break
            
            if not neighborhood_collection:
                app_logger.warning("No neighborhood analysis collection found")
                return []
            
            app_logger.info(f"📍 Searching neighborhood collection: {neighborhood_collection}")
            
            # Use direct collection search
            raw_results = await self.milvus_client._pure_vector_search(
                neighborhood_collection, max_results * 2, cuisine_type
            )
            
            if not raw_results:
                return []
            
            # Filter by dish name and neighborhood
            dish_lower = dish_name.lower()
            filtered_results = []
            
            for result in raw_results:
                result_dish = result.get('top_dish_name', '').lower()
                result_neighborhood = result.get('neighborhood', '').lower()
                
                # Check if dish name contains the search term
                if dish_lower in result_dish:
                    # If neighborhood is specified, check for neighborhood match
                    if neighborhood and neighborhood.lower() in result_neighborhood:
                        filtered_results.append(result)
                    # If no neighborhood specified, include all matches
                    elif not neighborhood:
                        filtered_results.append(result)
            
            # Format results
            recommendations = []
            for i, result in enumerate(filtered_results[:max_results]):
                recommendation = self._format_recommendation(result, i, neighborhood, "neighborhood_analysis")
                recommendations.append(recommendation)
            
            return recommendations
            
        except Exception as e:
            app_logger.error(f"Error searching neighborhood collection: {e}")
            return []
    
    def _format_recommendation(self, result: Dict, index: int, neighborhood: str, source: str) -> Dict:
        """Format a result into a consistent recommendation structure."""
        return {
            "id": result.get('restaurant_id', f"rec_{index}"),
            "restaurant_name": result.get('restaurant_name', 'Restaurant'),
            "dish_name": result.get('top_dish_name', 'Dish'),
            "cuisine_type": result.get('cuisine_type', 'Unknown'),
            "neighborhood": result.get('neighborhood', neighborhood),
            "description": f"Try the {result.get('top_dish_name', 'dish')} at {result.get('restaurant_name', 'this restaurant')} in {result.get('neighborhood', neighborhood)}. Highly recommended!",
            "final_score": float(result.get('top_dish_final_score', 0.8)),
            "rating": float(result.get('rating', 4.5)),
            "price_range": "$$",
            "source": source,
            "confidence": float(result.get('top_dish_final_score', 0.8)),
            "topic_score": float(result.get('top_dish_topic_mentions', 0.0)),
            "recommendation_score": float(result.get('hybrid_quality_score', 0.8))
        }
    
    async def _get_openai_dish_fallback(self, dish_name: str, cuisine_type: str, location: str, max_results: int) -> List[Dict]:
        """Generate creative dish recommendations using OpenAI when exact matches fail."""
        try:
            if not OPENAI_AVAILABLE:
                app_logger.warning("OpenAI not available for dish fallback")
                return []
            
            # Create a creative prompt for dish suggestions
            prompt = f"""I'm looking for the best {dish_name} in {location}. 
            Since '{dish_name}' isn't available in our database, suggest {max_results} alternative dishes that are:
            1. Similar to {dish_name} in taste, style, or concept
            2. Popular and well-loved in {location}
            3. From {cuisine_type} cuisine if specified
            4. Actually available at top restaurants in {location}
            
            Format as JSON:
            {{
                "recommendations": [
                    {{
                        "dish_name": "alternative dish name",
                        "description": "why this dish is great and similar to {dish_name}",
                        "similarity": "how it relates to {dish_name}",
                        "restaurant_suggestion": "type of restaurant to look for"
                    }}
                ]
            }}"""
            
            # Call OpenAI API
            client = AsyncOpenAI()
            response = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=800
            )
            
            # Parse response and create recommendations
            try:
                import json
                content = response.choices[0].message.content
                data = json.loads(content)
                
                recommendations = []
                for i, rec in enumerate(data.get("recommendations", [])):
                    recommendation = {
                        "id": f"openai_fallback_{i}",
                        "restaurant_name": f"Top {cuisine_type} Restaurant in {location}",
                        "dish_name": rec.get("dish_name", "Alternative Dish"),
                        "cuisine_type": cuisine_type or "Various",
                        "neighborhood": location,
                        "description": f"{rec.get('description', 'A great alternative option')} - {rec.get('similarity', 'Similar to what you were looking for')}",
                        "final_score": 0.9,  # High confidence for AI suggestions
                        "rating": 4.8,
                        "price_range": "$$",
                        "source": "openai_fallback",
                        "confidence": 0.9,
                        "topic_score": 0.0,
                        "recommendation_score": 0.9
                    }
                    recommendations.append(recommendation)
                
                return recommendations
                
            except (json.JSONDecodeError, KeyError) as e:
                app_logger.warning(f"Failed to parse OpenAI response: {e}")
                return []
                
        except Exception as e:
            app_logger.error(f"Error in OpenAI dish fallback: {e}")
            return []
