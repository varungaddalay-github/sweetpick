"""
Dish extraction using GPT-4o-mini for identifying dish mentions in reviews.
"""
import asyncio
import json
from typing import List, Dict, Optional, Any
from openai import AsyncOpenAI
from tenacity import retry, stop_after_attempt, wait_exponential
from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.data_collection.cache_manager import CacheManager
import re


class DishExtractor:
    """Extract dish mentions from review text using GPT-4o-mini."""
    
    def __init__(self):
        self.settings = get_settings()
        self.client = AsyncOpenAI(api_key=self.settings.openai_api_key)
        self.cache_manager = CacheManager()
        self.api_calls = 0
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def extract_dishes_from_reviews(self, reviews: List[Dict]) -> List[Dict]:
        """Extract dishes from a batch of reviews."""
        if not reviews:
            return []
        
        # Create cache key for this batch
        review_texts = [review.get('text', '') for review in reviews]
        cache_key = f"dish_extraction:{hash(''.join(review_texts))}"
        
        # Check cache first
        cached_result = await self.cache_manager.get(cache_key)
        if cached_result:
            app_logger.info("Using cached dish extraction results")
            return cached_result
        
        app_logger.info(f"Extracting dishes from {len(reviews)} reviews")
        
        # Process reviews in batches
        batch_size = self.settings.batch_size
        all_dishes = []
        
        for i in range(0, len(reviews), batch_size):
            batch = reviews[i:i + batch_size]
            batch_dishes = await self._extract_dishes_batch(batch)
            all_dishes.extend(batch_dishes)
            
            # Rate limiting between batches
            if i + batch_size < len(reviews):
                await asyncio.sleep(1)
        
        # Cache the results
        await self.cache_manager.set(cache_key, all_dishes, expire=7200)  # 2 hours
        
        app_logger.info(f"Extracted {len(all_dishes)} dish mentions")
        return all_dishes
    
    async def _extract_dishes_batch(self, reviews: List[Dict]) -> List[Dict]:
        """Extract dishes from a batch of reviews."""
        if not reviews:
            return []
        
        # Prepare review text for GPT
        review_texts = []
        for review in reviews:
            text = review.get('text', '')
            if text and len(text) > 10:  # Only process meaningful reviews
                review_texts.append(text)
        
        if not review_texts:
            return []
        
        # Create prompt for dish extraction
        prompt = self._create_dish_extraction_prompt(review_texts)
        
        try:
            response = await self.client.chat.completions.create(
                model=self.settings.openai_model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a food expert who extracts dish mentions from restaurant reviews. Extract specific dishes, their variations, and any relevant details."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=self.settings.temperature,
                max_tokens=self.settings.max_tokens
            )
            
            self._track_api_call()
            
            # Parse the response
            content = response.choices[0].message.content
            dishes = self._parse_dish_extraction_response(content, reviews)
            
            return dishes
            
        except Exception as e:
            app_logger.error(f"Error extracting dishes from batch: {e}")
            return []
    
    def _create_dish_extraction_prompt(self, review_texts: List[str]) -> str:
        """Create prompt for dish extraction."""
        combined_text = "\n\n".join(review_texts)
        
        prompt = f"""
Extract all dish mentions from the following restaurant reviews. Be INTELLIGENT about mapping single words to specific dish variants.

IMPORTANT: When you see a single word like "pizza", "biryani", "pasta", etc., try to extract the MOST SPECIFIC version mentioned in the reviews. If no specific variant is mentioned, extract the single word as a valid dish.

CUISINE-SPECIFIC INTELLIGENT MAPPING:

ITALIAN CUISINE:
- "pizza" → Look for: "Margherita Pizza", "Pepperoni Pizza", "Neapolitan Pizza", "New York Pizza", "Sicilian Pizza", "Bufalina Pizza"
- "pasta" → Look for: "Spaghetti Carbonara", "Fettuccine Alfredo", "Penne Arrabbiata", "Lasagna", "Ravioli"
- "bread" → Look for: "Garlic Bread", "Focaccia", "Bruschetta"
- Accept single words: "Pizza", "Pasta", "Bread", "Salad", "Tiramisu", "Gelato"

AMERICAN CUISINE:
- "burger" → Look for: "Cheeseburger", "Bacon Burger", "Veggie Burger", "Turkey Burger"
- "wings" → Look for: "Buffalo Wings", "BBQ Wings", "Garlic Wings"
- "salad" → Look for: "Caesar Salad", "Greek Salad", "Cobb Salad"
- Accept single words: "Burger", "Wings", "Salad", "Pie", "Steak"

MEXICAN CUISINE:
- "tacos" → Look for: "Beef Tacos", "Chicken Tacos", "Fish Tacos", "Veggie Tacos"
- "burrito" → Look for: "Beef Burrito", "Chicken Burrito", "Bean Burrito"
- Accept single words: "Tacos", "Burrito", "Quesadilla", "Guacamole", "Enchilada"

CHINESE CUISINE:
- "rice" → Look for: "Fried Rice", "Steamed Rice", "Yangzhou Fried Rice"
- "noodles" → Look for: "Lo Mein", "Chow Mein", "Dan Dan Noodles"
- "soup" → Look for: "Hot and Sour Soup", "Wonton Soup", "Egg Drop Soup"
- Accept single words: "Rice", "Noodles", "Soup", "Dumplings", "Dim Sum"

INDIAN CUISINE:
- "biryani" → Look for: "Chicken Biryani", "Mutton Biryani", "Vegetable Biryani", "Hyderabadi Biryani"
- "curry" → Look for: "Chicken Curry", "Lamb Curry", "Vegetable Curry", "Butter Chicken"
- "tandoori" → Look for: "Tandoori Chicken", "Tandoori Fish", "Tandoori Vegetables"
- Accept single words: "Samosa", "Pakora", "Naan", "Roti", "Lassi", "Chai"

JAPANESE CUISINE:
- "sushi" → Look for: "California Roll", "Salmon Nigiri", "Spicy Tuna Roll"
- "ramen" → Look for: "Tonkotsu Ramen", "Miso Ramen", "Shoyu Ramen"
- Accept single words: "Sushi", "Ramen", "Tempura", "Teriyaki", "Udon"

THAI CUISINE:
- "pad thai" → Look for: "Chicken Pad Thai", "Shrimp Pad Thai", "Tofu Pad Thai"
- Accept single words: "Pad Thai", "Tom Yum", "Green Curry", "Red Curry"

VIETNAMESE CUISINE:
- "pho" → Look for: "Beef Pho", "Chicken Pho", "Vegetable Pho"
- Accept single words: "Pho", "Banh Mi", "Spring Rolls"

INTELLIGENT EXTRACTION RULES:
1. ALWAYS try to find the MOST SPECIFIC dish name mentioned in the reviews
2. If a specific variant is mentioned (e.g., "Margherita Pizza"), extract that
3. If only the general dish is mentioned (e.g., "pizza"), extract the single word
4. Look for context clues: "their pizza", "the biryani", "this pasta"
5. Consider restaurant context: Italian restaurant → likely Italian dishes
6. Reject generic ingredients: "chicken", "beef", "rice", "bread" (alone)
7. Reject generic terms: "food", "meal", "dish", "cuisine"

Format the response as a JSON array:
{{
    "dishes": [
        {{
            "dish_name": "string",
            "normalized_name": "string", 
            "category": "string",
            "cuisine_context": "string",
            "dietary_tags": ["string"],
            "confidence_score": float,
            "review_context": "string"
        }}
    ]
}}

Reviews:
{combined_text}

EXAMPLES OF INTELLIGENT EXTRACTION:
- Review: "The pizza here is amazing" → Extract: "Pizza"
- Review: "Their margherita pizza is the best" → Extract: "Margherita Pizza"
- Review: "I love the biryani" → Extract: "Biryani"
- Review: "The chicken biryani is perfect" → Extract: "Chicken Biryani"
- Review: "Great pasta dishes" → Extract: "Pasta"
- Review: "The carbonara pasta is creamy" → Extract: "Pasta Carbonara"

Be intelligent and extract the most specific dish names possible while accepting valid single-word dishes.
"""
        return prompt
    
    def _parse_dish_extraction_response(self, response: str, reviews: List[Dict]) -> List[Dict]:
        """Parse the GPT response for dish extraction."""
        dishes = []
        
        try:
            # Try to extract JSON from response
            json_start = response.find('{')
            json_end = response.rfind('}') + 1
            
            if json_start != -1 and json_end > json_start:
                json_str = response[json_start:json_end]
                data = json.loads(json_str)
                
                if 'dishes' in data:
                    for dish_data in data['dishes']:
                        dish = self._normalize_dish_data(dish_data, reviews)
                        if dish:
                            dishes.append(dish)
            
        except json.JSONDecodeError as e:
            app_logger.error(f"Error parsing dish extraction response: {e}")
            # Fallback: try to extract dishes using regex patterns
            dishes = self._fallback_dish_extraction(response, reviews)
        
        return dishes
    
    def _normalize_dish_data(self, dish_data: Dict, reviews: List[Dict]) -> Optional[Dict]:
        """Normalize and validate dish data."""
        try:
            dish_name = dish_data.get('dish_name', '').strip()
            if not dish_name:
                return None
            
            # REJECT single ingredients - these are not dishes
            single_ingredients = {
                'chicken', 'beef', 'lamb', 'pork', 'fish', 'shrimp', 'salmon', 'tuna',
                'rice', 'bread', 'noodles', 'pasta', 'sauce', 'gravy', 'soup', 'salad',
                'vegetables', 'meat', 'seafood', 'paneer', 'tofu', 'mushroom', 'cheese',
                'tomato', 'onion', 'garlic', 'ginger', 'potato', 'carrot', 'pepper'
            }
            
            if dish_name.lower() in single_ingredients:
                app_logger.warning(f"Rejecting single ingredient as dish: {dish_name}")
                return None
            
            # REJECT generic terms (but allow common dishes)
            generic_terms = {
                'curry', 'biryani', 'tikka', 'tandoori', 'naan', 'roti', 'samosa', 'pakora',
                'lassi', 'chai', 'dessert', 'appetizer', 'main', 'side', 'drink', 'food',
                'meal', 'dish', 'cuisine', 'entree', 'course'
            }
            
            # Allow common single-word dishes for all cuisines
            allowed_single_dishes = {
                # Indian
                'samosa', 'pakora', 'lassi', 'chai', 'naan', 'roti', 'paratha', 'kulcha',
                'gulab', 'jamun', 'rasmalai', 'kheer', 'jalebi',
                
                # Italian
                'pizza', 'pasta', 'risotto', 'tiramisu', 'gelato', 'bruschetta',
                'calzone', 'focaccia', 'gnocchi', 'ravioli', 'lasagna',
                
                # American
                'burger', 'sandwich', 'wings', 'ribs', 'steak', 'pie', 'cake',
                'fries', 'chips', 'salad', 'soup', 'chowder',
                
                # Mexican
                'tacos', 'burrito', 'quesadilla', 'guacamole', 'salsa', 'churros',
                'enchilada', 'tamale', 'fajita', 'mole', 'pozole',
                
                # Chinese
                'rice', 'noodles', 'soup', 'dumplings', 'dim', 'sum', 'wonton',
                'spring', 'rolls', 'egg', 'rolls', 'chow', 'mein', 'lo', 'mein',
                
                # Japanese
                'sushi', 'ramen', 'tempura', 'teriyaki', 'udon', 'soba', 'miso',
                'bento', 'onigiri', 'takoyaki', 'okonomiyaki',
                
                # Thai
                'pad', 'thai', 'tom', 'yum', 'green', 'curry', 'red', 'curry',
                'massaman', 'curry', 'som', 'tam', 'larb', 'satay',
                
                # Vietnamese
                'pho', 'banh', 'mi', 'spring', 'rolls', 'bun', 'bo', 'nam', 'bo',
                
                # Korean
                'bibimbap', 'bulgogi', 'kimchi', 'japchae', 'tteokbokki', 'samgyeopsal',
                
                # Mediterranean
                'hummus', 'falafel', 'shawarma', 'kebab', 'gyro', 'tabbouleh',
                'baba', 'ganoush', 'dolma', 'baklava'
            }
            
            if dish_name.lower() in generic_terms and dish_name.lower() not in allowed_single_dishes:
                app_logger.warning(f"Rejecting generic term as dish: {dish_name}")
                return None
            
            # Normalize dish name
            normalized_name = self._normalize_dish_name(dish_name)
            
            # Validate category
            category = dish_data.get('category', 'main').lower()
            valid_categories = ['appetizer', 'main', 'dessert', 'drink', 'side']
            if category not in valid_categories:
                category = 'main'
            
            # Extract dietary tags
            dietary_tags = dish_data.get('dietary_tags', [])
            if isinstance(dietary_tags, str):
                dietary_tags = [tag.strip() for tag in dietary_tags.split(',')]
            
            # Validate confidence score
            confidence = dish_data.get('confidence_score', 0.5)
            if not isinstance(confidence, (int, float)) or confidence < 0 or confidence > 1:
                confidence = 0.5
            
            return {
                'dish_name': dish_name,
                'normalized_dish_name': normalized_name,
                'category': category,
                'cuisine_context': dish_data.get('cuisine_context', ''),
                'dietary_tags': dietary_tags,
                'confidence_score': confidence,
                'review_context': dish_data.get('review_context', ''),
                'restaurant_id': reviews[0].get('restaurant_id') if reviews else None,
                'review_ids': [review.get('review_id') for review in reviews if review.get('review_id')]
            }
            
        except Exception as e:
            app_logger.error(f"Error normalizing dish data: {e}")
            return None
    
    def _normalize_dish_name(self, dish_name: str) -> str:
        """Normalize dish name for consistency and proper formatting."""
        # Convert to lowercase for processing
        normalized = dish_name.lower().strip()
        
        # Remove common prefixes/suffixes
        prefixes_to_remove = ['the ', 'a ', 'an ']
        for prefix in prefixes_to_remove:
            if normalized.startswith(prefix):
                normalized = normalized[len(prefix):]
        
        # Normalize common variations for Indian cuisine
        indian_variations = {
            # Curry variations
            'chicken curry': 'chicken curry',
            'lamb curry': 'lamb curry',
            'beef curry': 'beef curry',
            'fish curry': 'fish curry',
            'vegetable curry': 'vegetable curry',
            'paneer curry': 'paneer curry',
            
            # Biryani variations
            'chicken biryani': 'chicken biryani',
            'lamb biryani': 'lamb biryani',
            'beef biryani': 'beef biryani',
            'vegetable biryani': 'vegetable biryani',
            'mushroom biryani': 'mushroom biryani',
            
            # Tikka variations
            'chicken tikka': 'chicken tikka',
            'paneer tikka': 'paneer tikka',
            'fish tikka': 'fish tikka',
            'chicken tikka masala': 'chicken tikka masala',
            'paneer tikka masala': 'paneer tikka masala',
            
            # Tandoori variations
            'tandoori chicken': 'tandoori chicken',
            'tandoori fish': 'tandoori fish',
            'tandoori lamb': 'tandoori lamb',
            
            # Butter variations
            'butter chicken': 'butter chicken',
            'butter paneer': 'butter paneer',
            
            # Bread variations
            'garlic naan': 'garlic naan',
            'plain naan': 'naan',
            'butter naan': 'butter naan',
            'cheese naan': 'cheese naan',
            
            # Other common variations
            'chicken 65': 'chicken 65',
            'gulab jamun': 'gulab jamun',
            'rasmalai': 'rasmalai',
            'mango lassi': 'mango lassi',
            'sweet lassi': 'lassi',
            'masala chai': 'masala chai',
            'jeera rice': 'jeera rice',
            'pulao': 'pulao',
            'samosa': 'samosa',
            'pakora': 'pakora'
        }
        
        # Check for Indian variations first
        if normalized in indian_variations:
            normalized = indian_variations[normalized]
        else:
            # Fallback to other cuisine variations
            other_variations = {
                'pizza margherita': 'margherita pizza',
                'pizza margarita': 'margherita pizza',
                'chicken taco': 'taco chicken',
                'beef taco': 'taco beef',
                'fish taco': 'taco fish',
                'chicken burrito': 'burrito chicken',
                'beef burrito': 'burrito beef',
                'vegetarian burrito': 'burrito vegetarian'
            }
            if normalized in other_variations:
                normalized = other_variations[normalized]
        
        # Apply proper title case formatting
        formatted = self._format_dish_name_title_case(normalized)
        
        return formatted
    
    def _format_dish_name_title_case(self, dish_name: str) -> str:
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
            # Meat and protein
            'chicken', 'beef', 'lamb', 'pork', 'fish', 'shrimp', 'salmon', 'tuna', 'paneer', 'tofu',
            'vegetable', 'vegetarian', 'vegan', 'mushroom',
            
            # International dishes
            'pizza', 'pasta', 'taco', 'burrito', 'sushi', 'ramen', 'pho', 'burger', 'sandwich',
            'margherita', 'alfredo', 'marinara', 'pesto', 'carbonara', 'lasagna', 'ravioli', 'gnocchi', 'risotto',
            'paella', 'pad', 'thai', 'kung', 'pao', 'teriyaki',
            
            # Indian cuisine specific
            'curry', 'biryani', 'tikka', 'tandoori', 'butter', 'masala', 'naan', 'roti', 'paratha', 'poori',
            'kulcha', 'samosa', 'pakora', 'lassi', 'chai', 'pulao', 'jeera', 'gulab', 'jamun', 'rasmalai',
            'kheer', 'jalebi', 'dal', 'sabzi', 'raita', 'chutney', 'papadum', 'vindaloo', 'korma', 'dosa',
            'idli', 'vada', 'sambar', 'rasam', 'upma', 'pongal', 'bhel', 'pani', 'puri', 'bhatura'
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
    
    def _fallback_dish_extraction(self, response: str, reviews: List[Dict]) -> List[Dict]:
        """Fallback dish extraction using regex patterns."""
        dishes = []
        
        # Common dish patterns with comprehensive Indian cuisine support
        dish_patterns = [
            # Indian cuisine - Curries and main dishes
            r'\b(chicken|lamb|beef|fish|vegetable|paneer|mushroom)\s+(curry|biryani|tikka|tandoori|korma|vindaloo|dopiaza|jalfrezi|madras|rogan|josh)\b',
            r'\b(butter\s+chicken|butter\s+paneer|chicken\s+65|chicken\s+tikka\s+masala|paneer\s+tikka\s+masala)\b',
            r'\b(biryani|pulao|jeera\s+rice|basmati\s+rice|fried\s+rice)\b',
            
            # Indian breads
            r'\b(naan|roti|paratha|poori|kulcha|bhatura|chapati|phulka)\b',
            r'\b(garlic\s+naan|butter\s+naan|cheese\s+naan|plain\s+naan)\b',
            
            # Indian appetizers and snacks
            r'\b(samosa|pakora|paneer\s+tikka|chicken\s+tikka|fish\s+tikka|aloo\s+tikki|dahi\s+puré)\b',
            r'\b(bhel\s+puré|pani\s+puré|sev\s+puré|papdi\s+chaat|dahi\s+chaat)\b',
            
            # Indian sides and accompaniments
            r'\b(dal|sabzi|raita|chutney|papadum|pickle|achari)\b',
            r'\b(mint\s+chutney|tamarind\s+chutney|coconut\s+chutney|onion\s+chutney)\b',
            
            # Indian desserts
            r'\b(gulab\s+jamun|rasmalai|kheer|jalebi|barfi|laddu|halwa|kulfi)\b',
            r'\b(mango\s+kulfi|pista\s+kulfi|kesar\s+kulfi|badam\s+milk)\b',
            
            # Indian drinks
            r'\b(lassi|mango\s+lassi|sweet\s+lassi|salted\s+lassi|masala\s+chai|chai|tea)\b',
            r'\b(masala\s+chai|ginger\s+chai|cardamom\s+chai|saffron\s+chai)\b',
            
            # South Indian dishes
            r'\b(dosa|idli|vada|sambar|rasam|upma|pongal|uttapam|medu\s+vada)\b',
            r'\b(masala\s+dosa|plain\s+dosa|onion\s+dosa|cheese\s+dosa)\b',
            
            # Other international cuisines
            r'\b(pizza|pasta|burger|sandwich|salad|soup|steak)\b',
            r'\b(tacos?|burrito|sushi|ramen|pho)\b',
            r'\b(pad\s+thai|kung\s+pao|teriyaki|alfredo|marinara|pesto)\b',
            r'\b(carbonara|lasagna|ravioli|gnocchi|risotto|paella)\b',
            r'\b(cake|ice\s+cream|tiramisu|cheesecake|brownie)\b',
            r'\b(coffee|juice|soda|wine|beer|cocktail)\b'
        ]
        
        for pattern in dish_patterns:
            matches = re.findall(pattern, response.lower())
            for match in matches:
                # Handle tuple matches (for patterns with groups)
                if isinstance(match, tuple):
                    # Join the tuple elements to form the full dish name
                    dish_name = ' '.join(match).strip()
                else:
                    dish_name = match.strip()
                
                # Skip if dish name is too short or just a single ingredient
                single_ingredients = {
                    'chicken', 'beef', 'lamb', 'pork', 'fish', 'shrimp', 'salmon', 'tuna',
                    'rice', 'bread', 'noodles', 'pasta', 'sauce', 'gravy', 'soup', 'salad',
                    'vegetables', 'meat', 'seafood', 'paneer', 'tofu', 'mushroom'
                }
                
                generic_terms = {
                    'curry', 'biryani', 'tikka', 'tandoori', 'naan', 'roti', 'samosa', 'pakora',
                    'lassi', 'chai', 'dessert', 'appetizer', 'main', 'side', 'drink'
                }
                
                if (len(dish_name) < 3 or 
                    dish_name in single_ingredients or 
                    dish_name in generic_terms):
                    continue
                
                # Format dish name properly
                formatted_name = self._format_dish_name_title_case(dish_name)
                
                # Determine category based on dish type
                category = self._determine_dish_category(dish_name)
                
                dish = {
                    'dish_name': formatted_name,
                    'normalized_dish_name': formatted_name,  # Use formatted name for consistency
                    'category': category,
                    'cuisine_context': 'indian' if self._is_indian_dish(dish_name) else '',
                    'dietary_tags': self._extract_dietary_tags(dish_name),
                    'confidence_score': 0.3,  # Lower confidence for fallback
                    'review_context': '',
                    'restaurant_id': reviews[0].get('restaurant_id') if reviews else None,
                    'review_ids': [review.get('review_id') for review in reviews if review.get('review_id')]
                }
                dishes.append(dish)
        
        return dishes
    
    def _determine_dish_category(self, dish_name: str) -> str:
        """Determine dish category based on dish name."""
        dish_lower = dish_name.lower()
        
        # Appetizers and snacks
        appetizer_keywords = ['samosa', 'pakora', 'tikki', 'chaat', 'papdi', 'dahi', 'bhel', 'pani', 'sev']
        if any(keyword in dish_lower for keyword in appetizer_keywords):
            return 'appetizer'
        
        # Breads
        bread_keywords = ['naan', 'roti', 'paratha', 'poori', 'kulcha', 'bhatura', 'chapati', 'phulka']
        if any(keyword in dish_lower for keyword in bread_keywords):
            return 'side'
        
        # Desserts
        dessert_keywords = ['gulab', 'jamun', 'rasmalai', 'kheer', 'jalebi', 'barfi', 'laddu', 'halwa', 'kulfi']
        if any(keyword in dish_lower for keyword in dessert_keywords):
            return 'dessert'
        
        # Drinks
        drink_keywords = ['lassi', 'chai', 'tea', 'milk']
        if any(keyword in dish_lower for keyword in drink_keywords):
            return 'drink'
        
        # Sides and accompaniments
        side_keywords = ['dal', 'sabzi', 'raita', 'chutney', 'papadum', 'pickle', 'achari', 'sambar', 'rasam']
        if any(keyword in dish_lower for keyword in side_keywords):
            return 'side'
        
        # Default to main course
        return 'main'
    
    def _is_indian_dish(self, dish_name: str) -> bool:
        """Check if dish is Indian cuisine."""
        dish_lower = dish_name.lower()
        
        indian_keywords = [
            'curry', 'biryani', 'tikka', 'tandoori', 'masala', 'naan', 'roti', 'paratha', 'poori',
            'samosa', 'pakora', 'lassi', 'chai', 'pulao', 'jeera', 'gulab', 'jamun', 'rasmalai',
            'kheer', 'jalebi', 'dal', 'sabzi', 'raita', 'chutney', 'papadum', 'vindaloo', 'korma',
            'dosa', 'idli', 'vada', 'sambar', 'rasam', 'upma', 'pongal', 'bhel', 'pani', 'puri',
            'bhatura', 'dopiaza', 'jalfrezi', 'madras', 'rogan', 'josh', 'butter'
        ]
        
        return any(keyword in dish_lower for keyword in indian_keywords)
    
    def _extract_dietary_tags(self, dish_name: str) -> List[str]:
        """Extract dietary tags from dish name."""
        dish_lower = dish_name.lower()
        tags = []
        
        # Vegetarian dishes
        veg_keywords = ['paneer', 'vegetable', 'dal', 'sabzi', 'samosa', 'pakora', 'gulab', 'jamun', 'rasmalai', 'kheer']
        if any(keyword in dish_lower for keyword in veg_keywords):
            tags.append('vegetarian')
        
        # Spicy dishes
        spicy_keywords = ['vindaloo', 'madras', 'jalfrezi', 'chicken 65', 'tandoori']
        if any(keyword in dish_lower for keyword in spicy_keywords):
            tags.append('spicy')
        
        # Gluten-free options
        gluten_free_keywords = ['rice', 'dal', 'sabzi', 'lassi', 'chai']
        if any(keyword in dish_lower for keyword in gluten_free_keywords) and 'naan' not in dish_lower and 'roti' not in dish_lower:
            tags.append('gluten-free')
        
        return tags
    
    def _track_api_call(self):
        """Track API calls for cost monitoring."""
        self.api_calls += 1
        if self.api_calls % 10 == 0:
            app_logger.info(f"Dish extraction API calls: {self.api_calls}")
    
    async def get_extraction_stats(self) -> Dict[str, Any]:
        """Get statistics about dish extraction."""
        return {
            'total_api_calls': self.api_calls,
            'estimated_cost': self.api_calls * 0.0001,  # Rough estimate
            'cache_hits': 0,  # Would need to track this
            'cache_misses': 0  # Would need to track this
        } 