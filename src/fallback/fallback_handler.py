"""
Fallback handler for implementing tiered fallback strategies when primary queries fail.
Enhanced with OpenAI intelligence for smarter fallback recommendations.
"""
import json
from typing import List, Dict, Optional, Any, Tuple
from openai import AsyncOpenAI
from src.utils.config import get_settings
from src.utils.logger import app_logger


class FallbackHandler:
    """Handle fallback strategies when primary query returns no results."""
    
    def __init__(self, retrieval_engine=None, query_parser=None):
        self.settings = get_settings()
        self.client = AsyncOpenAI(api_key=self.settings.openai_api_key)
        self.retrieval_engine = retrieval_engine
        self.query_parser = query_parser
        
        # Define fallback tiers
        self.fallback_tiers = {
            1: {"min_rating": 4.2, "min_reviews": 500, "description": "Premium restaurants"},
            2: {"min_rating": 4.0, "min_reviews": 250, "description": "Good restaurants"},
            3: {"min_rating": 3.8, "min_reviews": 100, "description": "Acceptable restaurants"}
        }
        
        # Geographic expansion order
        self.geographic_expansion = {
            "Jersey City": ["Hoboken", "Manhattan"],
            "Hoboken": ["Jersey City", "Manhattan"],
            "Manhattan": ["Brooklyn", "Queens"],
            "Brooklyn": ["Manhattan", "Queens"],
            "Queens": ["Manhattan", "Brooklyn"]
        }
    
    async def execute_fallback_strategy(self, original_query: Dict[str, Any], empty_results: List[Dict]) -> Tuple[List[Dict], bool, Optional[str]]:
        """Execute fallback strategy when primary query returns no results."""
        app_logger.info("Executing enhanced fallback strategy with OpenAI")
        
        # Track which fallback was used
        fallback_used = False
        fallback_reason = None
        
        # Strategy 1: Traditional criteria relaxation (fast)
        results, fallback_used, fallback_reason = await self._relax_criteria_fallback(original_query)
        if results:
            return results, fallback_used, fallback_reason
        
        # Strategy 2: OpenAI-enhanced search (intelligent)
        if self.client and self.retrieval_engine:
            results, fallback_used, fallback_reason = await self._openai_enhanced_fallback(original_query)
            if results:
                return results, fallback_used, fallback_reason
        
        # Strategy 2.5: Web search for dish-specific queries (NEW)
        if self._is_dish_specific_query(original_query):
            # For dish-specific queries, skip to web search instead of generic recommendations
            app_logger.info("Dish-specific query detected, suggesting web search")
            return [], False, "Dish-specific query - recommend web search"
        
        # Strategy 3: Geographic expansion (structured)
        results, fallback_used, fallback_reason = await self._geographic_expansion_fallback(original_query)
        if results:
            return results, fallback_used, fallback_reason
        
        # Strategy 4: Cuisine type relaxation
        results, fallback_used, fallback_reason = await self._cuisine_relaxation_fallback(original_query)
        if results:
            return results, fallback_used, fallback_reason
        
        # Strategy 5: OpenAI creative reinterpretation (last resort)
        if self.client and self.query_parser and self.retrieval_engine:
            results, fallback_used, fallback_reason = await self._openai_creative_fallback(original_query)
            if results:
                return results, fallback_used, fallback_reason
        
        # Strategy 6: Generic recommendations (final fallback)
        results, fallback_used, fallback_reason = await self._generic_recommendations_fallback(original_query)
        if results:
            return results, fallback_used, fallback_reason
        
        return [], False, "All fallback strategies exhausted"
    
    async def _relax_criteria_fallback(self, query: Dict[str, Any]) -> Tuple[List[Dict], bool, Optional[str]]:
        """Relax rating and review count criteria."""
        location = query.get("location")
        cuisine_type = query.get("cuisine_type")
        
        if not location:
            return [], False, None
        
        # Try each tier from most restrictive to least
        for tier in [1, 2, 3]:
            tier_config = self.fallback_tiers[tier]
            
            # Create relaxed query with lower standards
            relaxed_query = query.copy()
            relaxed_query["min_rating"] = tier_config["min_rating"]
            relaxed_query["min_reviews"] = tier_config["min_reviews"]
            relaxed_query["fallback_tier"] = tier
            
            # Use retrieval engine if available, otherwise return placeholder
            if self.retrieval_engine:
                try:
                    results, _, _ = await self.retrieval_engine.get_recommendations(relaxed_query, max_results=5)
                    if results:
                        # Mark results as fallback
                        for result in results:
                            result["type"] = "fallback"
                            result["fallback_tier"] = tier
                            result["confidence"] = max(0.3, result.get("confidence", 0.5) * 0.8)  # Reduce confidence
                        
                        return results, True, f"Used {tier_config['description']} (Tier {tier})"
                except Exception as e:
                    app_logger.warning(f"Retrieval engine failed in fallback tier {tier}: {e}")
                    continue
            else:
                # Fallback to placeholder if no retrieval engine
                return [
                    {
                        "type": "fallback",
                        "restaurant_name": f"Quality Restaurant (Tier {tier})",
                        "cuisine_type": cuisine_type or "Mixed",
                        "location": location,
                        "rating": tier_config["min_rating"],
                        "review_count": tier_config["min_reviews"],
                        "fallback_tier": tier,
                        "confidence": 0.6
                    }
                ], True, f"Used {tier_config['description']} (Tier {tier})"
        
        return [], False, None
    
    async def _get_openai_suggestions(self, prompt: str) -> Dict[str, Any]:
        """Get suggestions from OpenAI for fallback strategies."""
        try:
            response = await self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {
                        "role": "system", 
                        "content": "You are a restaurant recommendation expert. When searches fail, provide intelligent alternatives that match user intent and location context. Always respond with valid JSON."
                    },
                    {
                        "role": "user", 
                        "content": prompt
                    }
                ],
                temperature=0.7,  # More creative than parsing
                max_tokens=400
            )
            
            content = response.choices[0].message.content.strip()
            app_logger.info(f"OpenAI fallback response: {content}")
            
            # Extract JSON from response
            json_start = content.find('{')
            json_end = content.rfind('}') + 1
            
            if json_start != -1 and json_end > json_start:
                json_str = content[json_start:json_end]
                return json.loads(json_str)
            else:
                # Handle array format
                array_start = content.find('[')
                array_end = content.rfind(']') + 1
                if array_start != -1 and array_end > array_start:
                    array_data = json.loads(content[array_start:array_end])
                    return {"suggestions": array_data}
                    
            app_logger.warning("No valid JSON found in OpenAI response")
            return {}
            
        except Exception as e:
            app_logger.error(f"OpenAI fallback error: {e}")
            return {}
    
    async def _openai_enhanced_fallback(self, query: Dict[str, Any]) -> Tuple[List[Dict], bool, Optional[str]]:
        """Use OpenAI to intelligently expand search criteria."""
        app_logger.info("Attempting OpenAI-enhanced fallback")
        
        location = query.get("location") or "the area"
        cuisine_type = query.get("cuisine_type") or "food"
        original_query = query.get("original_query", f"{cuisine_type} in {location}")
        
        prompt = f"""
The user's search for "{cuisine_type} in {location}" returned no results. 

Analyze this failed query and suggest smart alternatives:

Original query: "{original_query}"
Location: {location}
Desired cuisine: {cuisine_type}
Intent: {query.get('intent', 'restaurant_recommendation')}

Provide 3 alternative search strategies that would satisfy the same craving:

{{
    "alternative_cuisines": ["cuisine1", "cuisine2", "cuisine3"],
    "nearby_locations": ["{location}", "nearby1", "nearby2"], 
    "alternative_dishes": ["dish1", "dish2", "dish3"],
    "reasoning": "Brief explanation of why these alternatives work",
    "mood_keywords": ["keyword1", "keyword2"]
}}

Focus on:
- Similar flavor profiles and cooking styles
- Popular alternatives in this geographic area  
- What would satisfy the same craving/mood
"""
        
        suggestions = await self._get_openai_suggestions(prompt)
        if not suggestions:
            return [], False, None
        
        # Try alternative cuisines - ADD SAFETY CHECKS
        for alt_cuisine in suggestions.get("alternative_cuisines", []):
            if alt_cuisine and isinstance(alt_cuisine, str) and cuisine_type and isinstance(cuisine_type, str):
                if alt_cuisine.lower() != cuisine_type.lower():
                    modified_query = query.copy()
                    modified_query["cuisine_type"] = alt_cuisine
                    
                    try:
                        results, _, _ = await self.retrieval_engine.get_recommendations(modified_query, max_results=3)
                        if results:
                            # Mark as OpenAI fallback
                            for result in results:
                                result["type"] = "openai_fallback"
                                result["fallback_reason"] = f"OpenAI suggested {alt_cuisine} as alternative"
                                result["confidence"] = max(0.4, result.get("confidence", 0.5) * 0.9)
                            
                            reasoning = suggestions.get("reasoning", f"OpenAI suggested {alt_cuisine}")
                            return results, True, f"OpenAI alternative: {reasoning}"
                    except Exception as e:
                        app_logger.warning(f"Failed to try OpenAI cuisine alternative {alt_cuisine}: {e}")
                        continue
        
        # Try alternative locations - ADD SAFETY CHECKS
        for alt_location in suggestions.get("nearby_locations", []):
            if alt_location and isinstance(alt_location, str) and location and isinstance(location, str):
                if alt_location != location:
                    modified_query = query.copy()
                    modified_query["location"] = alt_location
                    
                    try:
                        results, _, _ = await self.retrieval_engine.get_recommendations(modified_query, max_results=3)
                        if results:
                            # Mark as OpenAI fallback
                            for result in results:
                                result["type"] = "openai_fallback"
                                result["fallback_reason"] = f"OpenAI expanded search to {alt_location}"
                                result["confidence"] = max(0.4, result.get("confidence", 0.5) * 0.85)
                            
                            return results, True, f"OpenAI expanded search to {alt_location}"
                    except Exception as e:
                        app_logger.warning(f"Failed to try OpenAI location alternative {alt_location}: {e}")
                        continue
        
        # Continue with rest of method...
        return [], False, "OpenAI fallback found no viable alternatives"
    
    async def _openai_creative_fallback(self, query: Dict[str, Any]) -> Tuple[List[Dict], bool, Optional[str]]:
        """Use OpenAI to creatively reinterpret the failed query."""
        app_logger.info("Attempting OpenAI creative fallback")
        
        original_query = query.get("original_query", "food recommendation")
        location = query.get("location", "")
        
        prompt = f"""
A user asked: "{original_query}"
Location: {location}
Intent: {query.get('intent', 'food_recommendation')}

The specific search failed completely. Generate 3 creative but relevant alternative search queries that would satisfy the same underlying need or craving.

Think about:
- What is the user really craving? (comfort food, specific flavors, dining experience)
- What similar foods/restaurants would satisfy this need?
- What's typically popular in {location}?
- Are there trending or seasonal alternatives?

Return as a JSON array of alternative search queries:
["alternative query 1", "alternative query 2", "alternative query 3"]

Each query should be a natural language restaurant/food search that someone might actually type.
"""
        
        suggestions = await self._get_openai_suggestions(prompt)
        if not suggestions:
            return [], False, None
        
        alternative_queries = suggestions.get("suggestions", suggestions if isinstance(suggestions, list) else [])
        
        # Execute each alternative query
        for alt_query in alternative_queries[:3]:  # Limit to 3 attempts
            if not alt_query or len(alt_query.strip()) < 5:
                continue
                
            try:
                app_logger.info(f"Trying OpenAI creative alternative: {alt_query}")
                
                # Parse the alternative query
                parsed_alt = await self.query_parser.parse_query(alt_query)
                if not parsed_alt:
                    continue
                
                # Execute the alternative query  
                results, _, _ = await self.retrieval_engine.get_recommendations(parsed_alt, max_results=3)
                if results:
                    # Mark as creative OpenAI fallback
                    for result in results:
                        result["type"] = "openai_creative_fallback"
                        result["fallback_reason"] = f"OpenAI creative reinterpretation: {alt_query}"
                        result["confidence"] = max(0.3, result.get("confidence", 0.5) * 0.7)
                    
                    return results, True, f"OpenAI creative alternative: {alt_query}"
                    
            except Exception as e:
                app_logger.warning(f"Failed to execute OpenAI creative alternative '{alt_query}': {e}")
                continue
        
        return [], False, None
    
    async def _geographic_expansion_fallback(self, query: Dict[str, Any]) -> Tuple[List[Dict], bool, Optional[str]]:
        """Expand search to nearby locations."""
        original_location = query.get("location")
        cuisine_type = query.get("cuisine_type")
        
        if not original_location or original_location not in self.geographic_expansion:
            return [], False, None
        
        # Try nearby locations
        for nearby_location in self.geographic_expansion[original_location]:
            expanded_query = query.copy()
            expanded_query["location"] = nearby_location
            
            # Use retrieval engine if available
            if self.retrieval_engine:
                try:
                    results, _, _ = await self.retrieval_engine.get_recommendations(expanded_query, max_results=4)
                    if results:
                        # Mark as geographic fallback
                        for result in results:
                            result["type"] = "geographic_fallback"
                            result["original_location"] = original_location
                            result["confidence"] = max(0.4, result.get("confidence", 0.5) * 0.85)
                        
                        return results, True, f"Expanded search to {nearby_location}"
                except Exception as e:
                    app_logger.warning(f"Geographic expansion to {nearby_location} failed: {e}")
                    continue
            else:
                # Fallback to placeholder
                return [
                    {
                        "type": "geographic_fallback",
                        "restaurant_name": f"Restaurant in {nearby_location}",
                        "cuisine_type": cuisine_type or "Mixed",
                        "location": nearby_location,
                        "original_location": original_location,
                        "rating": 4.0,
                        "review_count": 300,
                        "fallback_tier": 2,
                        "confidence": 0.5
                    }
                ], True, f"Expanded search to {nearby_location}"
        
        return [], False, None
    
    async def _cuisine_relaxation_fallback(self, query: Dict[str, Any]) -> Tuple[List[Dict], bool, Optional[str]]:
        """Relax cuisine type requirements."""
        location = query.get("location")
        original_cuisine = query.get("cuisine_type")
        
        if not location or not original_cuisine:
            return [], False, None
        
        # Try alternative cuisines
        alternative_cuisines = self._get_alternative_cuisines(original_cuisine)
        
        for cuisine in alternative_cuisines:
            relaxed_query = query.copy()
            relaxed_query["cuisine_type"] = cuisine
            
            # Use retrieval engine if available
            if self.retrieval_engine:
                try:
                    results, _, _ = await self.retrieval_engine.get_recommendations(relaxed_query, max_results=4)
                    if results:
                        # Mark as cuisine relaxation fallback
                        for result in results:
                            result["type"] = "cuisine_relaxation_fallback"
                            result["original_cuisine"] = original_cuisine
                            result["confidence"] = max(0.35, result.get("confidence", 0.5) * 0.8)
                        
                        return results, True, f"Relaxed cuisine from {original_cuisine} to {cuisine}"
                except Exception as e:
                    app_logger.warning(f"Cuisine relaxation to {cuisine} failed: {e}")
                    continue
            else:
                # Fallback to placeholder
                return [
                    {
                        "type": "cuisine_relaxation_fallback",
                        "restaurant_name": f"{cuisine} Restaurant",
                        "cuisine_type": cuisine,
                        "original_cuisine": original_cuisine,
                        "location": location,
                        "rating": 4.1,
                        "review_count": 250,
                        "fallback_tier": 2,
                        "confidence": 0.4
                    }
                ], True, f"Relaxed cuisine from {original_cuisine} to {cuisine}"
        
        return [], False, None
    
    async def _generic_recommendations_fallback(self, query: Dict[str, Any]) -> Tuple[List[Dict], bool, Optional[str]]:
        """Provide generic recommendations when all else fails."""
        location = query.get("location")
        
        if not location:
            return [], False, None
        
        # Return top restaurants in the area regardless of cuisine
        if self.retrieval_engine:
            try:
                # Create a very general query
                generic_query = {
                    "intent": "location_general",
                    "location": location,
                    "cuisine_type": None,
                    "original_query": f"restaurants in {location}"
                }
                
                results, _, _ = await self.retrieval_engine.get_recommendations(generic_query, max_results=5)
                if results:
                    # Mark as generic fallback
                    for result in results:
                        result["type"] = "generic_fallback"
                        result["confidence"] = max(0.25, result.get("confidence", 0.3) * 0.6)
                        result["reason"] = "Generic recommendation"
                    
                    return results, True, "Used generic recommendations"
            except Exception as e:
                app_logger.warning(f"Generic recommendations failed: {e}")
        
        # Final fallback to hardcoded recommendations
        return [
            {
                "type": "generic_fallback",
                "restaurant_name": "Popular Local Restaurant",
                "cuisine_type": "Mixed", 
                "location": location,
                "rating": 4.3,
                "review_count": 400,
                "fallback_tier": 1,
                "confidence": 0.3,
                "reason": "Generic recommendation"
            },
            {
                "type": "generic_fallback",
                "restaurant_name": "Well-Rated Restaurant", 
                "cuisine_type": "Mixed",
                "location": location,
                "rating": 4.1,
                "review_count": 300,
                "fallback_tier": 2,
                "confidence": 0.3,
                "reason": "Generic recommendation"
            }
        ], True, "Used generic recommendations"
    
    def _get_alternative_cuisines(self, original_cuisine: str) -> List[str]:
        """Get alternative cuisines for a given cuisine type."""
        cuisine_alternatives = {
            "Italian": ["American", "Mediterranean"],
            "Indian": ["Pakistani", "Middle Eastern"],
            "Chinese": ["Japanese", "Thai", "Vietnamese"],
            "American": ["Italian", "Mexican"],
            "Mexican": ["Spanish", "Latin American"]
        }
        
        return cuisine_alternatives.get(original_cuisine, ["American", "Mixed"])
    
    def get_fallback_tier_description(self, tier: int) -> str:
        """Get description for a fallback tier."""
        return self.fallback_tiers.get(tier, {}).get("description", "Unknown tier")
    
    def should_use_fallback(self, results: List[Dict], query: Dict[str, Any]) -> bool:
        """Determine if fallback should be used."""
        # No results
        if not results:
            return True
        
        # Results have low confidence
        avg_confidence = sum(r.get("confidence", 0) for r in results) / len(results)
        if avg_confidence < 0.3:
            return True
        
        # Results don't match query intent
        query_type = query.get("intent")
        if query_type == "restaurant_specific" and not any(r.get("restaurant_name") for r in results):
            return True
        
        return False
    
    def _is_dish_specific_query(self, query: Dict[str, Any]) -> bool:
        """Check if this is a dish-specific query that should trigger web search."""
        # Check if query has a specific dish
        dish = query.get("dish")
        intent = query.get("intent")
        
        # If it's a location_dish query with a specific dish, it's dish-specific
        if intent == "location_dish" and dish:
            return True
        
        # If it's a dish query (no location), it's dish-specific
        if intent == "dish" and dish:
            return True
        
        # Check original query text for dish keywords
        original_query = query.get("original_query", "").lower()
        dish_keywords = [
            "biryani", "pizza", "pasta", "sushi", "burger", "taco", "curry",
            "ramen", "pho", "pad thai", "enchilada", "lasagna", "risotto"
        ]
        
        return any(keyword in original_query for keyword in dish_keywords)
    
    def get_fallback_metadata(self, fallback_reason: str) -> Dict[str, Any]:
        """Get metadata about the fallback used."""
        return {
            "fallback_used": True,
            "fallback_reason": fallback_reason,
            "fallback_timestamp": "2024-01-01T00:00:00Z",  # Would be actual timestamp
            "original_criteria_relaxed": "rating_and_reviews" in fallback_reason,
            "geographic_expansion": "Expanded search to" in fallback_reason,
            "cuisine_relaxation": "Relaxed cuisine" in fallback_reason
        } 