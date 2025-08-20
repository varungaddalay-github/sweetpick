"""
Natural language response generator using GPT-4o for conversational responses.
"""
import json
from typing import List, Dict, Any, Optional
from openai import AsyncOpenAI
from src.utils.config import get_settings
from src.utils.logger import app_logger


class ResponseGenerator:
    """Generate natural language responses from recommendation data."""
    
    def __init__(self):
        try:
            self.settings = get_settings()
            self.client = AsyncOpenAI(api_key=self.settings.openai_api_key)
            self.openai_available = True
        except Exception as e:
            # Fallback when settings are not available
            import os
            openai_key = os.getenv("OPENAI_API_KEY")
            if openai_key:
                try:
                    self.client = AsyncOpenAI(api_key=openai_key)
                    self.openai_available = True
                except Exception:
                    self.openai_available = False
            else:
                self.openai_available = False
            
            if not self.openai_available:
                print(f"Warning: OpenAI not available for response generation: {e}")
        
        # Response templates for consistency
        self.templates = {
            "greeting": "Based on your request for {cuisine} food in {location}, here's what I found:",
            "no_results": "I couldn't find exactly what you're looking for, but here are some great alternatives:",
            "fallback": "Since {original_query} wasn't available, I've found these excellent options:",
            "confidence_high": "I'm confident these recommendations will be perfect for you:",
            "confidence_medium": "Here are some good options that might interest you:",
            "confidence_low": "While not exactly what you asked for, these are popular choices:"
        }
    
    async def generate_conversational_response(
        self, 
        user_query: str,
        recommendations: List[Dict[str, Any]],
        query_metadata: Dict[str, Any]
    ) -> str:
        """Generate a natural language response using GPT-4o."""
        
        app_logger.info(f"🎯 ResponseGenerator.generate_conversational_response called")
        app_logger.info(f"📝 User query: '{user_query}'")
        app_logger.info(f"🍽️ Recommendations count: {len(recommendations)}")
        app_logger.info(f"📊 Query metadata: {query_metadata}")
        
        # Check if OpenAI is available
        if not hasattr(self, 'openai_available') or not self.openai_available:
            app_logger.warning("❌ OpenAI not available, using template response")
            template_response = self._generate_template_response(user_query, recommendations, query_metadata)
            app_logger.info(f"📋 Template response generated: '{template_response[:100]}...'")
            return template_response
        
        app_logger.info(f"✅ OpenAI is available, proceeding with GPT-4o generation")
        
        try:
            # Prepare context for GPT-4o
            context = self._prepare_context(user_query, recommendations, query_metadata)
            app_logger.info(f"📋 Context prepared (length: {len(context)})")
            
            system_prompt = """You are a restaurant recommendation expert for SweetPick.

Your expertise:
- Deep knowledge of restaurants and their signature dishes
- Understanding of what makes certain recommendations work for specific queries
- Ability to explain food quality indicators naturally
- Focus on helping users discover the best dining experiences

When providing recommendations:
- Be warm but authoritative in your knowledge
- Highlight specific dishes and what makes them special
- Mention quality indicators (ratings, specialties) naturally
- Keep responses brief and actionable (3-4 sentences max)
- End with confident encouragement
- If some recommendations are based on general knowledge, mention this naturally (e.g., "Based on my knowledge" or "From what I know about this cuisine")

Never mention:
- Technical details about the AI system
- Vector databases or similarity scores
- Internal processing methods
- API limitations
- Specific city specializations
"""
            
            user_prompt = f"""Generate a short, warm response for this restaurant recommendation:

User Query: "{user_query}"
Recommendations: {context}

Keep it brief, friendly, and highlight the best 1-2 dishes. Maximum 3-4 sentences total."""

            response = await self.client.chat.completions.create(
                model="gpt-4o",  # Use GPT-4o for better conversational responses
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=150,  # Reduced from 500 to 150
                temperature=0.7  # Slightly creative but consistent
            )
            
            generated_response = response.choices[0].message.content.strip()
            app_logger.info(f"✅ GPT-4o response generated: '{generated_response[:100]}...' (length: {len(generated_response)})")
            return generated_response
            
        except Exception as e:
            app_logger.error(f"❌ Error generating conversational response: {e}")
            app_logger.error(f"📋 Error type: {type(e).__name__}")
            app_logger.error(f"📋 Error details: {str(e)}")
            # Fallback to template-based response
            app_logger.info(f"🔄 Falling back to template response")
            template_response = self._generate_template_response(user_query, recommendations, query_metadata)
            app_logger.info(f"📋 Template fallback response: '{template_response[:100]}...'")
            return template_response
    
    def _prepare_context(
        self, 
        user_query: str, 
        recommendations: List[Dict[str, Any]], 
        query_metadata: Dict[str, Any]
    ) -> str:
        """Prepare context for GPT-4o."""
        
        context_parts = []
        
        # Query information
        location = query_metadata.get('location', 'the area')
        cuisine = query_metadata.get('cuisine_type', 'cuisine')
        fallback_used = query_metadata.get('fallback_used', False)
        confidence = query_metadata.get('confidence_score', 0.5)
        
        context_parts.append(f"Location: {location}")
        context_parts.append(f"Cuisine: {cuisine}")
        context_parts.append(f"Fallback used: {fallback_used}")
        context_parts.append(f"Confidence: {confidence:.2f}")
        
        # Check if recommendations are from OpenAI fallback
        openai_fallback_count = sum(1 for rec in recommendations if rec.get('source') == 'openai_fallback')
        if openai_fallback_count > 0:
            context_parts.append(f"Note: {openai_fallback_count} recommendations are based on general knowledge")
        
        # Recommendations
        context_parts.append(f"\nRecommendations ({len(recommendations)} total):")
        
        for i, rec in enumerate(recommendations[:5], 1):  # Top 5 for context
            dish_name = self._format_dish_name(rec.get('dish_name', 'Dish'))
            restaurant_name = rec.get('restaurant_name', 'Restaurant')
            rating = rec.get('restaurant_rating', 0)
            rec_score = rec.get('recommendation_score', 0)
            
            context_parts.append(
                f"{i}. {dish_name} at {restaurant_name} "
                f"(Rating: {rating:.1f}, Score: {rec_score:.2f})"
            )
        
        return "\n".join(context_parts)
    
    def _generate_template_response(
        self, 
        user_query: str, 
        recommendations: List[Dict[str, Any]], 
        query_metadata: Dict[str, Any]
    ) -> str:
        """Generate response using templates as fallback."""
        
        location = query_metadata.get('location', 'the area')
        cuisine = query_metadata.get('cuisine_type', 'cuisine')
        fallback_used = query_metadata.get('fallback_used', False)
        confidence = query_metadata.get('confidence_score', 0.5)
        
        # Choose template based on context
        if not recommendations:
            response = "I couldn't find any recommendations for your request at the moment. Please try a different query."
        elif fallback_used:
            response = self.templates["fallback"].format(original_query=user_query)
        elif confidence > 0.8:
            response = self.templates["confidence_high"]
        elif confidence > 0.5:
            response = self.templates["confidence_medium"]
        else:
            response = self.templates["confidence_low"]
        
        # Add recommendation details
        if recommendations:
            response += f"\n\n{self.templates['greeting'].format(cuisine=cuisine, location=location)}"
            
            for i, rec in enumerate(recommendations[:3], 1):
                dish_name = self._format_dish_name(rec.get('dish_name', 'Dish'))
                restaurant_name = rec.get('restaurant_name', 'Restaurant')
                rating = rec.get('restaurant_rating', 0)
                
                response += f"\n\n{i}. **{dish_name}** at {restaurant_name}"
                if rating > 0:
                    response += f" (⭐ {rating:.1f})"
        
        return response
    
    def generate_quick_response(
        self, 
        recommendations: List[Dict[str, Any]], 
        query_metadata: Dict[str, Any]
    ) -> str:
        """Generate a quick, template-based response for fast responses."""
        
        if not recommendations:
            return "No recommendations found for your query."
        
        location = query_metadata.get('location', 'your area')
        count = len(recommendations)
        top_dish = self._format_dish_name(recommendations[0].get('dish_name', 'dish'))
        top_restaurant = recommendations[0].get('restaurant_name', 'restaurant')
        
        return f"Found {count} great options in {location}! Top recommendation: {top_dish} at {top_restaurant}."
    
    def _format_dish_name(self, dish_name: str) -> str:
        """Format dish name with proper title case, handling special cases."""
        if not dish_name:
            return dish_name
        
        # Split into words
        words = dish_name.split()
        
        # Define words that should remain lowercase (unless first word)
        lowercase_words = {
            'a', 'an', 'and', 'as', 'at', 'but', 'by', 'for', 'if', 'in', 'is', 'it', 'no', 'not', 'of', 'on', 'or', 'so', 'the', 'to', 'up', 'yet',
            'with', 'without', 'from', 'into', 'during', 'including', 'until', 'against', 'among', 'throughout', 'despite', 'towards', 'upon'
        }
        
        # Define words that should always be capitalized
        always_capitalize = {
            'chicken', 'beef', 'lamb', 'pork', 'fish', 'shrimp', 'salmon', 'tuna', 'vegetable', 'vegetarian', 'vegan',
            'pizza', 'pasta', 'curry', 'biryani', 'taco', 'burrito', 'sushi', 'ramen', 'pho', 'burger', 'sandwich',
            'margherita', 'alfredo', 'marinara', 'pesto', 'carbonara', 'lasagna', 'ravioli', 'gnocchi', 'risotto',
            'paella', 'pad', 'thai', 'kung', 'pao', 'teriyaki'
        }
        
        formatted_words = []
        for i, word in enumerate(words):
            # First word is always capitalized
            if i == 0:
                formatted_words.append(word.title())
            # Always capitalize specific food terms
            elif word in always_capitalize:
                formatted_words.append(word.title())
            # Keep prepositions and articles lowercase (unless they're food terms)
            elif word in lowercase_words and word not in always_capitalize:
                formatted_words.append(word.lower())
            # Default: capitalize other words
            else:
                formatted_words.append(word.title())
        
        return ' '.join(formatted_words) 