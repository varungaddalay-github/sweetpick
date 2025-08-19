"""
Advanced dish extractor for complex cases requiring sophisticated reasoning.
"""
import json
import asyncio
from typing import List, Dict, Optional, Any
from openai import AsyncOpenAI
from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.processing.complexity_detector import ComplexityDetector


class AdvancedDishExtractor:
    """Advanced dish extractor for complex cases."""
    
    def __init__(self):
        self.settings = get_settings()
        self.client = AsyncOpenAI(api_key=self.settings.openai_api_key)
        self.complexity_detector = ComplexityDetector()
        self._embedding_cache = {}
    
    async def extract_dishes_from_reviews(self, reviews: List[Dict], location: str, cuisine: str) -> List[Dict]:
        """Extract dishes using advanced reasoning for complex cases."""
        if not reviews:
            return []
        
        app_logger.info(f"🔬 Advanced extraction for {location} {cuisine} from {len(reviews)} reviews")
        
        # Prepare review text
        review_texts = [review.get('text', '') for review in reviews if review.get('text')]
        if not review_texts:
            return []
        
        # Multi-step reasoning approach
        try:
            # Step 1: Location-aware dish identification
            location_dishes = await self._identify_location_dishes(review_texts, location, cuisine)
            
            # Step 2: Context-aware dish extraction
            context_dishes = await self._extract_with_context(review_texts, location, cuisine)
            
            # Step 3: Merge and prioritize results
            final_dishes = self._merge_and_prioritize(location_dishes, context_dishes, location, cuisine)
            
            app_logger.info(f"🔬 Advanced extraction found {len(final_dishes)} dishes")
            return final_dishes
            
        except Exception as e:
            app_logger.error(f"Advanced extraction failed: {e}")
            return []
    
    async def _identify_location_dishes(self, review_texts: List[str], location: str, cuisine: str) -> List[Dict]:
        """Step 1: Identify location-specific dishes."""
        combined_text = "\n\n".join(review_texts)
        
        prompt = f"""
You are an expert food analyst specializing in {location} cuisine. Your task is to identify location-specific dishes from restaurant reviews.

LOCATION: {location}
CUISINE: {cuisine}

LOCATION-SPECIFIC DISH PRIORITIES FOR {location.upper()}:
{self._get_location_priorities_text(location)}

INSTRUCTIONS:
1. Focus on dishes that are iconic or famous in {location}
2. Look for local restaurant names and specialties
3. Identify dishes that are unique to {location} or have local variations
4. Consider the cultural context of {location}

REVIEWS:
{combined_text}

Provide your analysis in this JSON format:
{{
    "location_dishes": [
        {{
            "dish_name": "string",
            "restaurant_name": "string (if mentioned)",
            "location_context": "string",
            "confidence": float,
            "reasoning": "string"
        }}
    ]
}}

Focus on dishes that are specifically associated with {location} or have local significance.
"""
        
        try:
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a food expert specializing in location-specific cuisine analysis. Always respond with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=1000
            )
            
            content = response.choices[0].message.content.strip()
            
            # Clean up the response to ensure valid JSON
            if content.startswith('```json'):
                content = content[7:]
            if content.endswith('```'):
                content = content[:-3]
            content = content.strip()
            
            data = json.loads(content)
            
            dishes = []
            for dish_data in data.get('location_dishes', []):
                dish = {
                    'dish_name': dish_data.get('dish_name', ''),
                    'restaurant_name': dish_data.get('restaurant_name', ''),
                    'location_context': dish_data.get('location_context', ''),
                    'confidence_score': dish_data.get('confidence', 0.5),
                    'reasoning': dish_data.get('reasoning', ''),
                    'category': 'main',
                    'cuisine_context': cuisine,
                    'dietary_tags': [],
                    'type': 'location_specific'
                }
                dishes.append(dish)
            
            return dishes
            
        except json.JSONDecodeError as e:
            app_logger.error(f"JSON parsing failed in location dish identification: {e}")
            app_logger.debug(f"Raw response: {content}")
            return []
        except Exception as e:
            app_logger.error(f"Location dish identification failed: {e}")
            return []
    
    async def _extract_with_context(self, review_texts: List[str], location: str, cuisine: str) -> List[Dict]:
        """Step 2: Extract dishes with full context awareness."""
        combined_text = "\n\n".join(review_texts)
        
        prompt = f"""
Extract all dish mentions from restaurant reviews with FULL CONTEXT AWARENESS.

LOCATION: {location}
CUISINE: {cuisine}

CONTEXT-AWARE EXTRACTION RULES:
1. Consider the cultural context of {location}
2. Understand local food terminology and slang
3. Recognize restaurant-specific dishes and specialties
4. Handle ambiguous terms based on context
5. Identify fusion dishes and modern variations
6. Consider seasonal and regional specialties

SPECIAL INSTRUCTIONS FOR {location.upper()}:
{self._get_location_context_text(location)}

REVIEWS:
{combined_text}

Provide detailed dish extraction in this JSON format:
{{
    "dishes": [
        {{
            "dish_name": "string",
            "normalized_name": "string",
            "category": "string",
            "cuisine_context": "string",
            "dietary_tags": ["string"],
            "confidence_score": float,
            "review_context": "string",
            "context_reasoning": "string"
        }}
    ]
}}

Be extremely thorough and consider all possible interpretations of the text.
"""
        
        try:
            response = await self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a sophisticated food extraction expert with deep cultural knowledge. Always respond with valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.4,
                max_tokens=1500
            )
            
            content = response.choices[0].message.content.strip()
            
            # Clean up the response to ensure valid JSON
            if content.startswith('```json'):
                content = content[7:]
            if content.endswith('```'):
                content = content[:-3]
            content = content.strip()
            
            data = json.loads(content)
            
            dishes = []
            for dish_data in data.get('dishes', []):
                dish = {
                    'dish_name': dish_data.get('dish_name', ''),
                    'normalized_dish_name': dish_data.get('normalized_name', ''),
                    'category': dish_data.get('category', 'main'),
                    'cuisine_context': dish_data.get('cuisine_context', cuisine),
                    'dietary_tags': dish_data.get('dietary_tags', []),
                    'confidence_score': dish_data.get('confidence_score', 0.5),
                    'review_context': dish_data.get('review_context', ''),
                    'context_reasoning': dish_data.get('context_reasoning', ''),
                    'type': 'context_aware'
                }
                dishes.append(dish)
            
            return dishes
            
        except json.JSONDecodeError as e:
            app_logger.error(f"JSON parsing failed in context-aware extraction: {e}")
            app_logger.debug(f"Raw response: {content}")
            return []
        except Exception as e:
            app_logger.error(f"Context-aware extraction failed: {e}")
            return []
    
    def _merge_and_prioritize(self, location_dishes: List[Dict], context_dishes: List[Dict], 
                            location: str, cuisine: str) -> List[Dict]:
        """Step 3: Merge and prioritize results from both approaches."""
        
        # Combine all dishes
        all_dishes = location_dishes + context_dishes
        
        # Remove duplicates based on dish name
        seen_dishes = set()
        unique_dishes = []
        
        for dish in all_dishes:
            dish_name = dish.get('dish_name', '').lower().strip()
            if dish_name and dish_name not in seen_dishes:
                seen_dishes.add(dish_name)
                unique_dishes.append(dish)
        
        # Prioritize location-specific dishes
        prioritized_dishes = []
        
        # First, add location-specific dishes
        for dish in unique_dishes:
            if dish.get('type') == 'location_specific':
                prioritized_dishes.append(dish)
        
        # Then, add context-aware dishes
        for dish in unique_dishes:
            if dish.get('type') == 'context_aware':
                prioritized_dishes.append(dish)
        
        # Apply location-specific enhancements
        enhanced_dishes = []
        for dish in prioritized_dishes:
            enhanced_dish = self._enhance_with_location_context(dish, location, cuisine)
            enhanced_dishes.append(enhanced_dish)
        
        return enhanced_dishes[:10]  # Limit to top 10
    
    def _enhance_with_location_context(self, dish: Dict, location: str, cuisine: str) -> Dict:
        """Enhance dish with location-specific context."""
        
        dish_name = dish.get('dish_name', '')
        
        # Get location-specific expansions
        location_expansions = self.complexity_detector.get_location_dish_expansions(dish_name, location)
        
        # If we have location expansions, use the most specific one
        if location_expansions:
            # Find the most specific expansion that matches
            for expansion in location_expansions:
                if expansion.lower() in dish_name.lower() or dish_name.lower() in expansion.lower():
                    dish['dish_name'] = expansion
                    dish['location_enhanced'] = True
                    break
        
        # Add location context
        dish['location'] = location
        dish['cuisine_type'] = cuisine
        
        return dish
    
    def _get_location_priorities_text(self, location: str) -> str:
        """Get location-specific priorities as text."""
        location_dishes = self.complexity_detector.location_dish_priorities.get(location, {})
        
        if not location_dishes:
            return f"No specific priorities defined for {location}"
        
        text = ""
        for dish_category, variants in location_dishes.items():
            text += f"- {dish_category.title()}: {', '.join(variants)}\n"
        
        return text
    
    def _get_location_context_text(self, location: str) -> str:
        """Get location-specific context instructions."""
        context_map = {
            "Manhattan": """
- Look for New York style pizza, bagels, deli sandwiches
- Consider famous restaurants: Joe's Pizza, Lombardi's, Russ & Daughters, Katz's Deli
- Recognize street food: hot dogs, pretzels, food trucks, halal carts
- Understand local terms: "slice", "pie", "deli", "bagel", "pretzel"
- Iconic dishes: New York Pizza, Everything Bagel, Pastrami Sandwich, Street Hot Dog
""",
            "Jersey City": """
- Look for local pizza joints, diners, ethnic restaurants
- Consider waterfront seafood, diverse immigrant cuisines (Indian, Portuguese, Latin)
- Recognize local specialties and family-owned places
- Understand local food culture and community favorites
- Iconic dishes: Jersey Style Pizza, Chicken Biryani, Portuguese Seafood, Diner Food
""",
            "Hoboken": """
- Look for Italian delis, pizza places, local favorites
- Consider waterfront dining, college town food, bar scene
- Recognize local institutions and neighborhood spots
- Understand the mix of Italian-American and modern cuisine
- Iconic dishes: Italian Sub, Hoboken Pizza, Bar Food, Seafood
""",
            "Brooklyn": """
- Look for Brooklyn-style pizza, Jewish delis, Caribbean food
- Consider famous spots: Di Fara, L&B Spumoni Gardens, Russ & Daughters
- Recognize diverse neighborhoods and ethnic cuisines
- Understand Brooklyn's unique food culture and history
- Iconic dishes: Brooklyn Pizza, Pastrami Sandwich, Jerk Chicken, Italian Ice
""",
            "Queens": """
- Look for authentic ethnic cuisines: Chinese, Indian, Greek, Thai, Korean
- Consider diverse immigrant communities and authentic restaurants
- Recognize regional specialties and traditional dishes
- Understand Queens as NYC's most diverse borough
- Iconic dishes: Dim Sum, Chicken Biryani, Gyro, Pad Thai, Korean BBQ
""",
            "Bronx": """
- Look for Italian delis, Caribbean food, Latin cuisine
- Consider Arthur Avenue Italian food, diverse immigrant communities
- Recognize local specialties and neighborhood favorites
- Understand Bronx's rich cultural food heritage
- Iconic dishes: Italian Sub, Jerk Chicken, Empanadas, Seafood
"""
        }
        
        return context_map.get(location, f"Consider local food culture and specialties of {location}")
    
    def get_extraction_stats(self) -> Dict:
        """Get extraction statistics."""
        return {
            'complexity_stats': self.complexity_detector.get_complexity_stats(),
            'cache_size': len(self._embedding_cache)
        }
