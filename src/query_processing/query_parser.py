"""
Query parser for extracting entities and intent from user queries.
"""
import re
import json
from typing import Dict, List, Optional, Any
from openai import AsyncOpenAI
from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.utils.location_resolver import location_resolver
from src.data_collection.cache_manager import CacheManager


class QueryParser:
    """Parse user queries to extract entities and intent."""
    
    def __init__(self):
        self.settings = get_settings()
        self.client = AsyncOpenAI(api_key=self.settings.openai_api_key)
        self.cache = CacheManager()
        
        # Enhanced system prompt
        self.system_prompt = """You are an expert query parser for a comprehensive restaurant recommendation system. Your role is to accurately extract structured information from natural language restaurant queries while maintaining high precision and appropriate confidence scoring.

## Core Instructions
1. **Accuracy over confidence** - Only assign high confidence when you're genuinely certain
2. **Handle ambiguity gracefully** - Use null values when information isn't clearly present
3. **Be comprehensive** - Consider all possible entity types that might be relevant
4. **Context awareness** - Use contextual clues to disambiguate (e.g., "curry" suggests Indian cuisine)

## Entity Extraction Guidelines

### Location
- Extract cities, neighborhoods, addresses, or landmarks
- Include both explicit ("in downtown") and implicit location references
- **Supported areas**: 
  - Manhattan and its neighborhoods (Times Square, SoHo, Chelsea, etc.)
  - Jersey City and its neighborhoods (Journal Square, Downtown JC, etc.)
  - Hoboken and its neighborhoods (Washington Street, etc.)
- **Note**: Extract ALL mentioned locations - the system will handle unsupported areas gracefully

### Restaurant Name
- Only extract if a specific restaurant is mentioned by name
- Don't confuse chain names with cuisine types

### Cuisine Type
**Supported cuisines**: Italian, Indian, Chinese, American, Mexican
**Note**: Extract these 5 supported cuisines when mentioned. If user mentions other cuisines, set cuisine_type to null.
**Important**: Always extract cuisine type when clearly mentioned, even if location is missing.
**Mapping rules**:
- "Tacos" → Mexican  
- "Pasta" → Italian
- "Curry" → Indian (unless context suggests otherwise)
- "Dim sum" → Chinese

### Dish Name
- Extract specific dish names, not general categories
- Include modifiers if mentioned

### Meal Type
**Options**: breakfast, lunch, dinner, brunch, late-night, snacks, drinks/happy hour
- Consider time-based context clues

### Price Range
**Scale**: 
- 1 = Budget-friendly ($, under $15 per person)
- 2 = Moderate ($$, $15-30 per person)  
- 3 = Upscale ($$$, $30-60 per person)
- 4 = Fine dining ($$$$, $60+ per person)

**Keywords mapping**:
- "cheap", "affordable", "budget" → 1
- "reasonable", "mid-range" → 2  
- "upscale", "nice", "fancy" → 3
- "fine dining", "expensive", "high-end" → 4

### Dietary Restrictions
**Options**: vegetarian, vegan, gluten-free, halal, kosher, keto, low-carb, dairy-free, nut-free

### Restaurant Features  
**Options**: outdoor_seating, delivery, takeout, reservations, parking, live_music, bar, kid_friendly, romantic, business_dinner, casual, formal, pet_friendly

### Time Preference
- Extract specific times, time ranges, or relative time references

### Party Size
- Extract number of people if mentioned

### Query Intent Classification
**Primary intents**:
- **restaurant_specific**: Looking for a particular restaurant (e.g., "What are the top dishes at Razza", "Show me the menu at Southern Spice")
- **location_cuisine**: Want specific cuisine in an area  
- **location_dish**: Want specific dish in an area
- **location_general**: General dining in an area
- **cuisine_general**: General cuisine preference, any location
- **dish_search**: Looking for specific dish, any location/cuisine
- **meal_planning**: Planning for specific meal/time
- **dietary_focused**: Primary concern is dietary restrictions
- **ambiance_focused**: Primary concern is restaurant atmosphere/features
- **delivery_takeout**: Specifically wants delivery/takeout options

**Important**: When a restaurant name is mentioned (like "Razza", "Southern Spice"), the intent should be "restaurant_specific" regardless of other context.

## Confidence Scoring Guidelines
- **0.9-1.0**: Explicitly mentioned, unambiguous
- **0.7-0.8**: Strongly implied by context or common associations  
- **0.5-0.6**: Reasonably inferred but could be interpreted differently
- **0.3-0.4**: Weak inference, multiple interpretations possible
- **0.1-0.2**: Very uncertain, mostly guessing

## Error Handling
- For ambiguous queries, prefer null values over low-confidence guesses
- If multiple cuisines are mentioned, choose the most specific or emphasized one
- If query is completely unclear, set intent to "unclear" and overall confidence below 0.3
- Always return valid JSON even for malformed or nonsensical queries

Return valid JSON with this exact structure:
{
    "location": "string or null",
    "restaurant_name": "string or null", 
    "cuisine_type": "string or null",
    "dish_name": "string or null",
    "meal_type": "string or null",
    "price_range": "number (1-4) or null",
    "dietary_restrictions": ["array of strings or empty array"],
    "restaurant_features": ["array of strings or empty array"],
    "time_preference": "string or null",
    "party_size": "number or null",
    "intent": "string (required)",
    "confidence": {
        "location": "number 0-1 or null",
        "restaurant_name": "number 0-1 or null",
        "cuisine_type": "number 0-1 or null", 
        "dish_name": "number 0-1 or null",
        "meal_type": "number 0-1 or null",
        "price_range": "number 0-1 or null",
        "dietary_restrictions": "number 0-1 or null",
        "restaurant_features": "number 0-1 or null",
        "time_preference": "number 0-1 or null",
        "party_size": "number 0-1 or null",
        "overall": "number 0-1"
    }
}"""
        
        # Query type patterns (updated to match new intents)
        self.query_patterns = {
            'restaurant_specific': [
                r'i am at (.+)',
                r'i\'m at (.+)',
                r'at (.+) restaurant',
                r'in (.+) restaurant',
                r'(.+) restaurant',
                r'restaurant (.+)'
            ],
            'location_cuisine': [
                r'in (.+) and.*(?:mood|want|looking).*?(?:eat|try|find).*?(italian|indian|chinese|american|mexican)',
                r'in (.+) for (.+) food',
                r'in (.+) craving (.+)',
                r'(.+) cuisine in (.+)'
            ],
            'location_dish': [
                r'in (.+) and.*?(?:mood|want|looking).*?(?:eat|try|find).*?([a-zA-Z\s]+(?:chicken biryani|vegetable biryani|chicken curry|pizza|pasta|burger|taco|sushi|pad thai|pho|ramen))',
                r'in (.+) for (.+)',
                r'in (.+) craving (.+)',
                r'best (.+) in (.+)',
                r'top (.+) in (.+)',
                r'show me the best (.+) in (.+)',
                r'(.+) in (.+)'
            ],
            'location_general': [
                r'in (.+) and.*?(?:hungry|want|looking).*?(?:eat|food|restaurant)',
                r'in (.+) what.*?(?:eat|order)',
                r'in (.+) recommend'
            ],
            'meal_planning': [
                r'for (.+) in (.+)',
                r'(.+) time in (.+)',
                r'looking for (.+) in (.+)'
            ],
            'delivery_takeout': [
                r'delivery.*in (.+)',
                r'takeout.*in (.+)',
                r'order.*from (.+)'
            ]
        }
    
    async def parse_query(self, query: str) -> Dict[str, Any]:
        """Parse user query to extract entities and intent."""
        try:
            # Redis cache check (keyed by raw query)
            cache_key = f"parsed_query:{(query or '').strip().lower()}"
            cached = await self.cache.get_json(cache_key)
            if cached:
                app_logger.info("🧠 Parsed query cache hit")
                return cached
            app_logger.info("🧠 Parsed query cache miss")
            
            # Try OpenAI parsing first
            parsed = await self._parse_with_openai(query)
            if parsed:
                # Resolve location using our location resolver
                parsed = self._resolve_location_in_parsed_query(parsed)
                # Store in cache for 6 hours
                await self.cache.set_json(cache_key, parsed, expire=6*3600)
                return parsed
            
            # Fallback to regex parsing
            parsed = self._parse_with_regex(query)
            # Resolve location for regex parsing too
            parsed = self._resolve_location_in_parsed_query(parsed)
            # Store in cache for 2 hours
            await self.cache.set_json(cache_key, parsed, expire=2*3600)
            return parsed
            
        except Exception as e:
            app_logger.error(f"Error parsing query: {e}")
            return self._get_default_parsed_query(query)
    
    async def _parse_with_openai(self, query: str) -> Optional[Dict[str, Any]]:
        """Parse query using OpenAI with enhanced prompts."""
        try:
            user_prompt = f"""Parse the following restaurant query and extract all relevant entities:

Query: "{query}"

Extract the following information based on the system guidelines:
1. Location (city/neighborhood/landmark)
2. Restaurant name (if specifically mentioned)
3. Cuisine type (from the defined list)
4. Dish name (specific dishes only)
5. Meal type (breakfast/lunch/dinner/brunch/late-night/snacks/drinks)
6. Price range preference (1-4 scale as defined)
7. Dietary restrictions (if any mentioned)
8. Restaurant features (delivery, outdoor seating, etc.)
9. Time preference (specific times or relative references)
10. Party size (number of people)
11. Query intent (primary purpose of the search)
12. Confidence scores for each extracted entity

Analyze the query context carefully and return the structured JSON response with appropriate confidence scores for each field."""
            
            response = await self.client.chat.completions.create(
                model=self.settings.openai_model,
                messages=[
                    {
                        "role": "system",
                        "content": self.system_prompt
                    },
                    {
                        "role": "user",
                        "content": user_prompt
                    }
                ],
                temperature=0.1,
                max_tokens=800  # Increased for expanded response
            )
            
            content = response.choices[0].message.content.strip()
            app_logger.info(f"🤖 OpenAI raw response for '{query}': {content}")
            
            # Handle markdown-wrapped JSON responses
            if content.startswith('```json'):
                # Extract JSON from markdown code blocks
                json_start = content.find('{')
                json_end = content.rfind('}') + 1
                if json_start != -1 and json_end != 0:
                    content = content[json_start:json_end]
            elif content.startswith('```'):
                # Handle other markdown code blocks
                lines = content.split('\n')
                json_lines = []
                in_json = False
                for line in lines:
                    if line.strip().startswith('```'):
                        if not in_json:
                            in_json = True
                        else:
                            break
                    elif in_json:
                        json_lines.append(line)
                content = '\n'.join(json_lines)
            
            # Parse JSON response
            parsed = json.loads(content)
            app_logger.info(f"🔍 OpenAI parsed JSON: {parsed}")
            
            # Validate and normalize the response
            validated = self._validate_parsed_query(parsed, query)
            app_logger.info(f"✅ After validation: {validated}")
            
            return validated
            
        except Exception as e:
            app_logger.error(f"Error in OpenAI parsing: {e}")
            return None
    
    def _parse_with_regex(self, query: str) -> Dict[str, Any]:
        """Parse query using regex patterns (fallback method)."""
        query_lower = query.lower()
        
        # Initialize result with new structure
        result = {
            "location": None,
            "restaurant_name": None,
            "cuisine_type": None,
            "dish_name": None,
            "meal_type": None,
            "price_range": None,
            "dietary_restrictions": [],
            "restaurant_features": [],
            "time_preference": None,
            "party_size": None,
            "intent": "unknown",
            "confidence": {
                "location": None,
                "restaurant_name": None,
                "cuisine_type": None,
                "dish_name": None,
                "meal_type": None,
                "price_range": None,
                "dietary_restrictions": None,
                "restaurant_features": None,
                "time_preference": None,
                "party_size": None,
                "overall": 0.5
            }
        }
        
        # Extract location
        location = self._extract_location(query_lower)
        if location:
            result["location"] = location
            result["confidence"]["location"] = 0.8
        
        # Check query patterns (stop after first matched intent to avoid overwriting)
        intent_matched = False
        for intent, patterns in self.query_patterns.items():
            if intent_matched:
                break
            for pattern in patterns:
                match = re.search(pattern, query_lower)
                if match:
                    result["intent"] = intent
                    
                    if intent == "restaurant_specific":
                        result["restaurant_name"] = match.group(1).strip()
                        result["confidence"]["restaurant_name"] = 0.7
                    elif intent == "location_cuisine":
                        if not result["location"]:
                            result["location"] = match.group(1).strip()
                            result["confidence"]["location"] = 0.7
                        if len(match.groups()) > 1:
                            result["cuisine_type"] = match.group(2).strip().title()
                            result["confidence"]["cuisine_type"] = 0.8
                    elif intent == "location_dish":
                        if not result["location"]:
                            result["location"] = match.group(1).strip()
                            result["confidence"]["location"] = 0.7
                        if len(match.groups()) > 1:
                            result["dish_name"] = match.group(2).strip()
                            result["confidence"]["dish_name"] = 0.7
                    elif intent == "meal_planning":
                        if not result["location"]:
                            result["location"] = match.group(2).strip() if len(match.groups()) > 1 else match.group(1).strip()
                            result["confidence"]["location"] = 0.7
                        result["meal_type"] = match.group(1).strip() if len(match.groups()) > 1 else None
                        if result["meal_type"]:
                            result["confidence"]["meal_type"] = 0.7
                    
                    intent_matched = True
                    break
        
        # Extract additional entities
        if not result["cuisine_type"]:
            cuisine = self._extract_cuisine(query_lower)
            if cuisine:
                result["cuisine_type"] = cuisine
                result["confidence"]["cuisine_type"] = 0.8
        
        if not result["dish_name"]:
            dish = self._extract_dish(query_lower)
            if dish:
                result["dish_name"] = dish
                result["confidence"]["dish_name"] = 0.7
        
        if not result["meal_type"]:
            meal = self._extract_meal_type(query_lower)
            if meal:
                result["meal_type"] = meal
                result["confidence"]["meal_type"] = 0.8
        
        price_range = self._extract_price_range(query_lower)
        if price_range:
            result["price_range"] = price_range
            result["confidence"]["price_range"] = 0.6
        
        # Extract new entities
        dietary = self._extract_dietary_restrictions(query_lower)
        if dietary:
            result["dietary_restrictions"] = dietary
            result["confidence"]["dietary_restrictions"] = 0.8
        
        features = self._extract_restaurant_features(query_lower)
        if features:
            result["restaurant_features"] = features
            result["confidence"]["restaurant_features"] = 0.7
        
        time_pref = self._extract_time_preference(query_lower)
        if time_pref:
            result["time_preference"] = time_pref
            result["confidence"]["time_preference"] = 0.7
        
        party_size = self._extract_party_size(query_lower)
        if party_size:
            result["party_size"] = party_size
            result["confidence"]["party_size"] = 0.8
        
        # Calculate overall confidence
        confidences = [v for v in result["confidence"].values() if v is not None and v != result["confidence"]["overall"]]
        if confidences:
            result["confidence"]["overall"] = sum(confidences) / len(confidences)
        
        return result
    
    def _extract_location(self, query: str) -> Optional[str]:
        """Extract location from query."""
        # Expanded location list
        locations = [
            "jersey city", "hoboken", "manhattan", "new york", "nyc"
        ]
        
        for location in locations:
            if location in query:
                return self._normalize_city_name(location)
        
        return None
    
    def _normalize_city_name(self, city: str) -> str:
        """Normalize city names to standard format."""
        city_mapping = {
            "jersey city": "Jersey City",
            "hoboken": "Hoboken", 
            "manhattan": "Manhattan",
            "new york": "Manhattan",  # Map New York to Manhattan for our system
            "nyc": "Manhattan"        # Map NYC to Manhattan for our system
        }
        
        return city_mapping.get(city.lower(), city.title())
    
    def _extract_cuisine(self, query: str) -> Optional[str]:
        """Extract cuisine type from query (expanded list)."""
        cuisines = {
                "italian": "Italian", "indian": "Indian", "chinese": "Chinese",
                "american": "American", "mexican": "Mexican"
        }
        
        for cuisine_key, cuisine_name in cuisines.items():
            if cuisine_key in query:
                return cuisine_name
        
        return None
    
    def _extract_dish(self, query: str) -> Optional[str]:
        """Extract dish name from query (expanded patterns, Indian multi-word)."""
        q = query.lower()

        # High-priority: adjective + dish patterns (no beef)
        combo = re.search(
            r'\b(chicken|mutton|lamb|paneer|vegetable|veg|egg)\s+'
            r'(biryani|curry|korma|tikka masala|butter chicken|butter masala|saag|kebab|keema|karahi|bhuna|tikka)\b',
            q, re.IGNORECASE
        )
        if combo:
            return combo.group(0).title()

        # Named multi-word dishes
        named_patterns = [
            r'\b(chana masala|masala dosa|palak paneer|paneer tikka|chole bhature|dal makhani|malai kofta|aloo gobi)\b',
            r'\b(veg biryani|vegetable biryani|mutton biryani|chicken biryani|paneer biryani)\b',
            r'\b(tandoori chicken|chicken tikka)\b'
        ]
        for pat in named_patterns:
            m = re.search(pat, q, re.IGNORECASE)
            if m:
                return m.group(0).title()

        # Existing generic patterns
        generic = [
            r'\b(chicken curry|fish curry|mutton curry|dal|paneer|butter chicken|tikka masala)\b',
            r'\b(pizza|pasta|burger|sandwich|salad|soup|steak|ribs)\b',
            r'\b(tacos?|burrito|quesadilla|nachos)\b',
            r'\b(kung pao|sweet and sour|general tso|chow mein|lo mein|fried rice|chicken fried rice|egg fried rice)\b'
        ]
        for pat in generic:
            m = re.search(pat, q, re.IGNORECASE)
            if m:
                return m.group(0).title()

        return None
    
    def _extract_meal_type(self, query: str) -> Optional[str]:
        """Extract meal type from query (expanded options)."""
        meal_types = {
            "breakfast": "breakfast", "lunch": "lunch", "dinner": "dinner",
            "brunch": "brunch", "late night": "late-night", "late-night": "late-night",
            "snack": "snacks", "snacks": "snacks", "drinks": "drinks",
            "happy hour": "drinks", "cocktails": "drinks"
        }
        
        for meal_key, meal_name in meal_types.items():
            if meal_key in query:
                return meal_name
        
        return None
    
    def _extract_price_range(self, query: str) -> Optional[int]:
        """Extract price range from query (improved patterns)."""
        price_patterns = [
            (r'\$(\$+)', lambda m: len(m.group(1)) + 1),
            (r'(\d+)\s*dollars?', lambda m: min(4, max(1, int(m.group(1)) // 15))),
            (r'\b(cheap|budget|affordable)\b', lambda m: 1),
            (r'\b(reasonable|moderate|mid-range)\b', lambda m: 2),
            (r'\b(upscale|nice|fancy)\b', lambda m: 3),
            (r'\b(expensive|fine dining|high-end|luxury)\b', lambda m: 4)
        ]
        
        for pattern, extractor in price_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                return extractor(match)
        
        return None
    
    def _extract_dietary_restrictions(self, query: str) -> List[str]:
        """Extract dietary restrictions from query."""
        restrictions = []
        dietary_patterns = {
            r'\b(vegetarian|veggie)\b': 'vegetarian',
            r'\b(vegan)\b': 'vegan',
            r'\b(gluten.free|gluten free)\b': 'gluten-free',
            r'\b(halal)\b': 'halal',
            r'\b(kosher)\b': 'kosher',
            r'\b(keto|ketogenic)\b': 'keto',
            r'\b(low.carb|low carb)\b': 'low-carb',
            r'\b(dairy.free|dairy free|lactose.free)\b': 'dairy-free',
            r'\b(nut.free|nut free)\b': 'nut-free'
        }
        
        for pattern, restriction in dietary_patterns.items():
            if re.search(pattern, query, re.IGNORECASE):
                restrictions.append(restriction)
        
        return restrictions
    
    def _extract_restaurant_features(self, query: str) -> List[str]:
        """Extract restaurant features from query."""
        features = []
        feature_patterns = {
            r'\b(outdoor|outside|patio|terrace)\b': 'outdoor_seating',
            r'\b(delivery|deliver)\b': 'delivery',
            r'\b(takeout|take.out|pickup)\b': 'takeout',
            r'\b(reservation|book|booking)\b': 'reservations',
            r'\b(parking|park)\b': 'parking',
            r'\b(live music|music|band)\b': 'live_music',
            r'\b(bar|drinks|cocktails)\b': 'bar',
            r'\b(kid.friendly|kids|family|children)\b': 'kid_friendly',
            r'\b(romantic|date|intimate)\b': 'romantic',
            r'\b(business|meeting|corporate)\b': 'business_dinner',
            r'\b(casual|relaxed|laid.back)\b': 'casual',
            r'\b(formal|upscale|elegant)\b': 'formal',
            r'\b(pet.friendly|dog.friendly|pets)\b': 'pet_friendly'
        }
        
        for pattern, feature in feature_patterns.items():
            if re.search(pattern, query, re.IGNORECASE):
                features.append(feature)
        
        return features
    
    def _extract_time_preference(self, query: str) -> Optional[str]:
        """Extract time preference from query."""
        time_patterns = [
            r'\b(\d{1,2}:\d{2}\s*(?:am|pm)?)\b',
            r'\b(\d{1,2}\s*(?:am|pm))\b',
            r'\b(now|asap|immediately)\b',
            r'\b(tonight|today|tomorrow)\b',
            r'\b(lunch time|dinner time|breakfast time)\b',
            r'\b(early|late|around \d+)\b'
        ]
        
        for pattern in time_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                return match.group(1).lower()
        
        return None
    
    def _extract_party_size(self, query: str) -> Optional[int]:
        """Extract party size from query."""
        party_patterns = [
            r'\b(?:table for|party of|group of)\s*(\d+)\b',
            r'\b(\d+)\s*(?:people|person|ppl)\b',
            r'\b(two|three|four|five|six|seven|eight)\b'
        ]
        
        number_words = {
            'two': 2, 'three': 3, 'four': 4, 'five': 5,
            'six': 6, 'seven': 7, 'eight': 8
        }
        
        for pattern in party_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                try:
                    return int(match.group(1))
                except ValueError:
                    word = match.group(1).lower()
                    if word in number_words:
                        return number_words[word]
        
        # Special cases
        if re.search(r'\b(date|couple|romantic)\b', query, re.IGNORECASE):
            return 2
        if re.search(r'\b(family|kids)\b', query, re.IGNORECASE):
            return 4  # Estimated family size
        
        return None
    
    def _validate_parsed_query(self, parsed: Dict[str, Any], original_query: str) -> Dict[str, Any]:
        """Validate and normalize parsed query (updated for new structure)."""
        app_logger.info(f"🔍 _validate_parsed_query input: {parsed}")
        
        # Ensure all required fields exist
        required_fields = [
            "location", "restaurant_name", "cuisine_type", "dish_name", 
            "meal_type", "price_range", "dietary_restrictions", "restaurant_features",
            "time_preference", "party_size", "intent", "confidence"
        ]
        
        for field in required_fields:
            if field not in parsed:
                if field in ["dietary_restrictions", "restaurant_features"]:
                    parsed[field] = []
                elif field == "confidence":
                    parsed[field] = {"overall": 0.5}
                else:
                    parsed[field] = None
        
        # Validate location (expanded list)
        if parsed["location"]:
            valid_locations = [
                "Jersey City", "Hoboken"
            ]
            if parsed["location"] not in valid_locations:
                # Try to match partial names
                location_lower = parsed["location"].lower()
                for valid_loc in valid_locations:
                    if location_lower in valid_loc.lower() or valid_loc.lower() in location_lower:
                        parsed["location"] = valid_loc
                        break
                # IMPORTANT: do NOT set unsupported locations to None here.
                # Leave as-is so API validation can detect unsupported_location
        
        # Validate cuisine type (expanded list)
        if parsed["cuisine_type"]:
            valid_cuisines = [
                 "Italian", "Indian", "Chinese", "American", "Mexican"
            ]
            if parsed["cuisine_type"] not in valid_cuisines:
                parsed["cuisine_type"] = None
        
        # Validate price range
        if parsed["price_range"]:
            try:
                price_range = int(parsed["price_range"])
                if price_range < 1 or price_range > 4:
                    parsed["price_range"] = None
                else:
                    parsed["price_range"] = price_range
            except (ValueError, TypeError):
                parsed["price_range"] = None
        
        # Validate confidence structure
        if not isinstance(parsed["confidence"], dict):
            parsed["confidence"] = {"overall": 0.5}
        
        if "overall" not in parsed["confidence"]:
            # Calculate overall confidence from individual scores
            individual_scores = [v for k, v in parsed["confidence"].items() 
                               if k != "overall" and v is not None]
            if individual_scores:
                parsed["confidence"]["overall"] = sum(individual_scores) / len(individual_scores)
            else:
                parsed["confidence"]["overall"] = 0.5
        
        # Validate overall confidence range
        try:
            overall_conf = float(parsed["confidence"]["overall"])
            if overall_conf < 0 or overall_conf > 1:
                parsed["confidence"]["overall"] = 0.5
            else:
                parsed["confidence"]["overall"] = overall_conf
        except (ValueError, TypeError):
            parsed["confidence"]["overall"] = 0.5
        
        app_logger.info(f"✅ _validate_parsed_query output: {parsed}")
        return parsed
    
    def _get_default_parsed_query(self, query: str) -> Dict[str, Any]:
        """Get default parsed query when parsing fails (updated structure)."""
        return {
            "location": None,
            "restaurant_name": None,
            "cuisine_type": None,
            "dish_name": None,
            "meal_type": None,
            "price_range": None,
            "dietary_restrictions": [],
            "restaurant_features": [],
            "time_preference": None,
            "party_size": None,
            "intent": "unknown",
            "confidence": {
                "location": None,
                "restaurant_name": None,
                "cuisine_type": None,
                "dish_name": None,
                "meal_type": None,
                "price_range": None,
                "dietary_restrictions": None,
                "restaurant_features": None,
                "time_preference": None,
                "party_size": None,
                "overall": 0.0
            },
            "original_query": query
        }
    
    def classify_query_type(self, parsed_query: Dict[str, Any]) -> str:
        """Classify the query type based on parsed entities (updated intents)."""
        intent = parsed_query.get("intent", "unknown")
        
        if intent != "unknown":
            return intent
        
        # Fallback classification based on entities
        if parsed_query.get("restaurant_name"):
            return "restaurant_specific"
        elif parsed_query.get("dietary_restrictions"):
            return "dietary_focused"
        elif parsed_query.get("restaurant_features") and any(f in parsed_query["restaurant_features"] for f in ["delivery", "takeout"]):
            return "delivery_takeout"
        elif parsed_query.get("location") and parsed_query.get("cuisine_type"):
            return "location_cuisine"
        elif parsed_query.get("location") and parsed_query.get("dish_name"):
            return "location_dish"
        elif parsed_query.get("location") and parsed_query.get("meal_type"):
            return "meal_planning"
        elif parsed_query.get("location"):
            return "location_general"
        elif parsed_query.get("cuisine_type"):
            return "cuisine_general"
        elif parsed_query.get("dish_name"):
            return "dish_search"
        else:
            return "unknown"
    
    def expand_dish_name(self, dish: str, cuisine_type: Optional[str] = None) -> List[str]:
        """Expand short dish names into specific variants."""
        if not dish:
            return []
        
        dish_lower = dish.lower().strip()
        
        # Dish expansion mappings
        dish_expansions = {
            # Indian dishes
            'biryani': ['Chicken Biryani', 'Mutton Biryani', 'Vegetable Biryani', 'Hyderabadi Biryani'],
            'curry': ['Chicken Curry', 'Lamb Curry', 'Vegetable Curry', 'Butter Chicken'],
            'tandoori': ['Tandoori Chicken', 'Tandoori Fish', 'Tandoori Vegetables'],
            'naan': ['Butter Naan', 'Garlic Naan', 'Plain Naan'],
            'dal': ['Dal Makhani', 'Dal Tadka', 'Yellow Dal'],
            'samosa': ['Vegetable Samosa', 'Chicken Samosa'],
            'kebab': ['Chicken Kebab', 'Lamb Kebab', 'Seekh Kebab'],
            
            # Italian dishes
            'pizza': ['Margherita Pizza', 'Pepperoni Pizza', 'Marinara Pizza', 'Quattro Stagioni', 'Bufalina Pizza', 'New York Pizza', 'Neapolitan Pizza', 'Sicilian Pizza'],
            'pasta': ['Spaghetti Carbonara', 'Fettuccine Alfredo', 'Penne Arrabbiata', 'Lasagna'],
            'risotto': ['Mushroom Risotto', 'Seafood Risotto', 'Truffle Risotto'],
            'gnocchi': ['Potato Gnocchi', 'Spinach Gnocchi'],
            'ravioli': ['Cheese Ravioli', 'Spinach Ravioli', 'Mushroom Ravioli'],
            
            # Chinese dishes
            'dim sum': ['Har Gow', 'Siu Mai', 'Char Siu Bao', 'Xiao Long Bao'],
            'noodles': ['Lo Mein', 'Chow Mein', 'Dan Dan Noodles'],
            'rice': ['Fried Rice', 'Steamed Rice', 'Yangzhou Fried Rice'],
            'soup': ['Hot and Sour Soup', 'Wonton Soup', 'Egg Drop Soup'],
            
            # American dishes
            'burger': ['Cheeseburger', 'Bacon Burger', 'Veggie Burger'],
            'sandwich': ['Club Sandwich', 'BLT', 'Turkey Sandwich'],
            'steak': ['Ribeye Steak', 'Filet Mignon', 'Sirloin Steak'],
            'salad': ['Caesar Salad', 'Greek Salad', 'Cobb Salad'],
            
            # Mexican dishes
            'taco': ['Beef Taco', 'Chicken Taco', 'Fish Taco', 'Veggie Taco'],
            'burrito': ['Beef Burrito', 'Chicken Burrito', 'Bean Burrito'],
            'enchilada': ['Chicken Enchilada', 'Beef Enchilada', 'Cheese Enchilada'],
            'quesadilla': ['Chicken Quesadilla', 'Cheese Quesadilla'],
            
            # Generic dishes
            'sushi': ['California Roll', 'Salmon Nigiri', 'Spicy Tuna Roll'],
            'ramen': ['Tonkotsu Ramen', 'Miso Ramen', 'Shoyu Ramen'],
            'pho': ['Beef Pho', 'Chicken Pho', 'Vegetable Pho'],
            'pad thai': ['Chicken Pad Thai', 'Shrimp Pad Thai', 'Tofu Pad Thai']
        }
        
        # Return expanded variants if found
        if dish_lower in dish_expansions:
            return dish_expansions[dish_lower]
        
        # If not found, return the original dish name
        return [dish]
    
    def get_query_entities(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        """Extract key entities from parsed query (updated with new fields)."""
        return {
            "location": parsed_query.get("location"),
            "restaurant_name": parsed_query.get("restaurant_name"),
            "cuisine_type": parsed_query.get("cuisine_type"),
            "dish_name": parsed_query.get("dish_name"),
            "meal_type": parsed_query.get("meal_type"),
            "price_range": parsed_query.get("price_range"),
            "dietary_restrictions": parsed_query.get("dietary_restrictions", []),
            "restaurant_features": parsed_query.get("restaurant_features", []),
            "time_preference": parsed_query.get("time_preference"),
            "party_size": parsed_query.get("party_size")
        }
    
    def is_valid_query(self, parsed_query: Dict[str, Any]) -> bool:
        """Check if parsed query is valid (updated validation logic)."""
        # Require a strong anchor: location OR restaurant OR specific dish
        if not any([
            parsed_query.get("location"),
            parsed_query.get("restaurant_name"),
            parsed_query.get("dish_name"),
        ]):
            return False
        
        # Must have some intent (not unknown)
        if parsed_query.get("intent") in ["unknown", "unclear"]:
            # Check if overall confidence is too low
            confidence = parsed_query.get("confidence", {})
            overall_conf = confidence.get("overall", 0)
            if overall_conf < 0.3:
                return False
        
        return True
    
    def get_confidence_score(self, parsed_query: Dict[str, Any]) -> float:
        """Get overall confidence score for the parsed query."""
        confidence = parsed_query.get("confidence", {})
        return confidence.get("overall", 0.0)
    
    def has_location_context(self, parsed_query: Dict[str, Any]) -> bool:
        """Check if query has location context."""
        return bool(parsed_query.get("location"))
    
    def has_cuisine_preference(self, parsed_query: Dict[str, Any]) -> bool:
        """Check if query has cuisine preference."""
        return bool(parsed_query.get("cuisine_type"))
    
    def has_dietary_requirements(self, parsed_query: Dict[str, Any]) -> bool:
        """Check if query has dietary requirements."""
        dietary = parsed_query.get("dietary_restrictions", [])
        return bool(dietary)
    
    def get_search_filters(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        """Extract search filters for restaurant recommendation system."""
        filters = {}
        
        if parsed_query.get("location"):
            filters["location"] = parsed_query["location"]
        
        if parsed_query.get("cuisine_type"):
            filters["cuisine"] = parsed_query["cuisine_type"]
        
        if parsed_query.get("price_range"):
            filters["price_range"] = parsed_query["price_range"]
        
        if parsed_query.get("meal_type"):
            filters["meal_type"] = parsed_query["meal_type"]
        
        if parsed_query.get("dietary_restrictions"):
            filters["dietary_restrictions"] = parsed_query["dietary_restrictions"]
        
        if parsed_query.get("restaurant_features"):
            filters["features"] = parsed_query["restaurant_features"]
        
        if parsed_query.get("party_size"):
            filters["party_size"] = parsed_query["party_size"]
        
        return filters
    
    def get_query_summary(self, parsed_query: Dict[str, Any]) -> str:
        """Generate a human-readable summary of the parsed query."""
        parts = []
        
        if parsed_query.get("restaurant_name"):
            parts.append(f"Restaurant: {parsed_query['restaurant_name']}")
        
        if parsed_query.get("cuisine_type"):
            parts.append(f"Cuisine: {parsed_query['cuisine_type']}")
        
        if parsed_query.get("dish_name"):
            parts.append(f"Dish: {parsed_query['dish_name']}")
        
        if parsed_query.get("location"):
            parts.append(f"Location: {parsed_query['location']}")
        
        if parsed_query.get("meal_type"):
            parts.append(f"Meal: {parsed_query['meal_type']}")
        
        if parsed_query.get("price_range"):
            price_labels = {1: "Budget", 2: "Moderate", 3: "Upscale", 4: "Fine Dining"}
            parts.append(f"Price: {price_labels.get(parsed_query['price_range'], 'Unknown')}")
        
        if parsed_query.get("dietary_restrictions"):
            parts.append(f"Dietary: {', '.join(parsed_query['dietary_restrictions'])}")
        
        if parsed_query.get("restaurant_features"):
            parts.append(f"Features: {', '.join(parsed_query['restaurant_features'])}")
        
        if parsed_query.get("party_size"):
            parts.append(f"Party size: {parsed_query['party_size']}")
        
        if parsed_query.get("time_preference"):
            parts.append(f"Time: {parsed_query['time_preference']}")
        
        summary = " | ".join(parts) if parts else "General restaurant search"
        intent = parsed_query.get("intent", "unknown")
        confidence = parsed_query.get("confidence", {}).get("overall", 0)
        
        return f"{summary} (Intent: {intent}, Confidence: {confidence:.2f})"
    
    def _resolve_location_in_parsed_query(self, parsed_query: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve location in parsed query using location resolver."""
        if not parsed_query.get("location"):
            return parsed_query
        
        original_location = parsed_query["location"]
        location_info = location_resolver.resolve_location(original_location)
        
        app_logger.info(f"🔍 Location resolution: '{original_location}' -> {location_info}")
        
        # Update parsed query with resolved location info
        if location_info.location_type == "unsupported":
            # Mark as unsupported for later handling
            parsed_query["location_status"] = "unsupported"
            parsed_query["original_location"] = original_location
            parsed_query["location"] = None  # Clear location to trigger fallback
        elif location_info.location_type == "unknown":
            # Keep original but mark as uncertain
            parsed_query["location_status"] = "unknown"
            parsed_query["original_location"] = original_location
            # Keep the original location for potential partial matching
        else:
            # Successfully resolved
            parsed_query["location_status"] = "supported"
            parsed_query["original_location"] = original_location
            parsed_query["resolved_city"] = location_info.resolved_city
            parsed_query["neighborhood"] = location_info.neighborhood
            parsed_query["location"] = location_info.resolved_city  # Use resolved city for searches
            
            # Update location confidence
            if "confidence" in parsed_query and isinstance(parsed_query["confidence"], dict):
                parsed_query["confidence"]["location"] = location_info.confidence
        
        return parsed_query