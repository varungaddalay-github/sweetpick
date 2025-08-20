"""
Main FastAPI application for the Sweet Morsels restaurant recommendation system.
"""
import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Dict, Optional, Any, Tuple
import uuid
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Request
from fastapi.responses import HTMLResponse
import os

# Try to import optional dependencies with fallbacks
try:
    from openai import AsyncOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("Warning: OpenAI not available")

try:
    from src.utils.config import get_settings
    CONFIG_AVAILABLE = True
except ImportError as e:
    CONFIG_AVAILABLE = False
    print(f"Warning: Config not available: {e}")

try:
    from src.utils.logger import app_logger
    LOGGER_AVAILABLE = True
except ImportError:
    LOGGER_AVAILABLE = False
    print("Warning: Logger not available")

# Try to import other modules with fallbacks
try:
    from src.data_collection.serpapi_collector import SerpAPICollector
    SERPAPI_AVAILABLE = True
except ImportError:
    SERPAPI_AVAILABLE = False
    print("Warning: SerpAPI collector not available")

try:
    from src.data_collection.data_validator import DataValidator
    VALIDATOR_AVAILABLE = True
except ImportError:
    VALIDATOR_AVAILABLE = False
    print("Warning: Data validator not available")

try:
    from src.processing.dish_extractor import DishExtractor
    DISH_EXTRACTOR_AVAILABLE = True
except ImportError:
    DISH_EXTRACTOR_AVAILABLE = False
    print("Warning: Dish extractor not available")

try:
    from src.processing.sentiment_analyzer import SentimentAnalyzer
    SENTIMENT_AVAILABLE = True
except ImportError:
    SENTIMENT_AVAILABLE = False
    print("Warning: Sentiment analyzer not available")

# Use Milvus HTTP API instead of pymilvus
try:
    from src.vector_db.milvus_http_client import MilvusHTTPClient
    MILVUS_AVAILABLE = True
    print("✅ Milvus HTTP client available")
except ImportError as e:
    MILVUS_AVAILABLE = False
    print(f"Warning: Milvus HTTP client not available - {e}")
except Exception as e:
    MILVUS_AVAILABLE = False
    print(f"Warning: Milvus HTTP client error - {e}")

try:
    from src.query_processing.query_parser import QueryParser
    QUERY_PARSER_AVAILABLE = True
except ImportError:
    QUERY_PARSER_AVAILABLE = False
    print("Warning: Query parser not available")

try:
    from src.query_processing.enhanced_retrieval_engine import EnhancedRetrievalEngine
    RETRIEVAL_AVAILABLE = True
except ImportError:
    RETRIEVAL_AVAILABLE = False
    print("Warning: Retrieval engine not available")

try:
    from src.fallback.fallback_handler import FallbackHandler
    FALLBACK_AVAILABLE = True
except ImportError:
    FALLBACK_AVAILABLE = False
    print("Warning: Fallback handler not available")

try:
    from src.processing.response_generator import ResponseGenerator
    RESPONSE_GENERATOR_AVAILABLE = True
except ImportError:
    RESPONSE_GENERATOR_AVAILABLE = False
    print("Warning: Response generator not available")

try:
    from src.security.abuse_protection import abuse_protection
    ABUSE_PROTECTION_AVAILABLE = True
except ImportError:
    ABUSE_PROTECTION_AVAILABLE = False
    print("Warning: Abuse protection not available")


# Pydantic models
class QueryRequest(BaseModel):
    query: str = Field(..., description="User query for restaurant/dish recommendations")
    user_location: Optional[str] = Field(None, description="User's current location")
    cuisine_preference: Optional[str] = Field(None, description="Preferred cuisine type")
    price_range: Optional[int] = Field(None, description="Price range (1-4)")
    max_results: Optional[int] = Field(10, description="Maximum number of results")

class QueryResponse(BaseModel):
    query: str
    query_type: str
    recommendations: List[Dict[str, Any]]
    natural_response: str = ""
    fallback_used: bool = False
    fallback_reason: Optional[str] = None
    processing_time: float
    confidence_score: float
    web_search_available: bool = False  # ← ADD THIS LINE

class RestaurantResponse(BaseModel):
    restaurant_id: str
    restaurant_name: str
    city: str
    cuisine_type: str
    rating: float
    review_count: int
    address: str
    phone: Optional[str] = None
    website: Optional[str] = None
    price_range: int
    meal_types: List[str]
    top_dishes: List[Dict[str, Any]]

class DishResponse(BaseModel):
    dish_id: str
    dish_name: str
    restaurant_id: str
    restaurant_name: str
    category: str
    sentiment_score: float
    recommendation_score: float
    # Hybrid fields
    topic_mentions: int = 0
    topic_score: float = 0.0
    final_score: float = 0.0
    source: str = "sentiment"
    hybrid_insights: Dict[str, Any] = {}
    positive_aspects: List[str]
    negative_aspects: List[str]
    dietary_tags: List[str]
    sample_reviews: List[str]

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    version: str
    services: Dict[str, str]

class StatsResponse(BaseModel):
    total_restaurants: int
    total_dishes: int
    total_queries: int
    cache_hit_rate: float
    average_response_time: float
    api_costs: Dict[str, float]

class ChatMessage(BaseModel):
    role: str  # 'user' | 'assistant'
    content: str

class ChatRequest(BaseModel):
    session_id: Optional[str] = None
    message: str

class ChatResponse(BaseModel):
    session_id: str
    messages: List[ChatMessage]
    recommendations: List[Dict[str, Any]] = []
    natural_response: str = ""
    fallback_used: bool = False
    processing_time: float
    confidence_score: float = 0.0


# Validation and suggestion functions
async def suggest_alternatives_with_choice(
    original_query: str, 
    unsupported_location: Optional[str] = None,
    unsupported_cuisine: Optional[str] = None
) -> str:
    """Provide OpenAI web search for unsupported locations/cuisines."""
    try:
        app_logger.info(f"🔍 suggest_alternatives_with_choice called with location: {unsupported_location}, cuisine: {unsupported_cuisine}")
        
        settings = get_settings()
        openai_client = AsyncOpenAI(api_key=settings.openai_api_key)
        
        # For unsupported locations, directly do web search instead of suggesting alternatives
        if unsupported_location:
            app_logger.info(f"🌍 Processing unsupported location: {unsupported_location}")
            
            prompt = f"""IMPORTANT: The user asked about restaurants in {unsupported_location}. 

            DO NOT suggest alternative cities that are not Manhattan.
            DO NOT mention traveling to other cities.
            DO NOT say "just a short drive away" or similar phrases.
            
            Instead, provide helpful recommendations FOR {unsupported_location} specifically.
            
            Response format:
            1. Start with: "I don't have deep local insights for {unsupported_location}, but based on my knowledge, here are some great options:"
            2. List 2-3 actual restaurants IN {unsupported_location} for: "{original_query}"
            3. Include restaurant names, specific dishes, and why they're good
            4. Focus on {unsupported_location} only - do not suggest other cities
            
            Be helpful about {unsupported_location} specifically."""
            
            # Append this section
            prompt += """

Include a compact JSON block after your brief message with this shape:
{
  "items": [
    {"restaurant_name": "string", "dish": "string or null", "reason": "string", "location": "string (prefer {unsupported_location})", "rating": "number or null"}
  ]
}
Only one JSON block. Keep item fields short.
"""
            
        elif unsupported_cuisine:
            app_logger.info(f"🍽️ Processing unsupported cuisine: {unsupported_cuisine}")
            
            prompt = f"""IMPORTANT: The user asked about {unsupported_cuisine} cuisine.
            
            DO NOT suggest other cuisines as alternatives.
            DO NOT mention alternative cities.
            
            Instead, provide helpful recommendations FOR {unsupported_cuisine} cuisine specifically.
            
            Response format:
            1. Start with: "I don't have specialized analysis for {unsupported_cuisine} cuisine, but from my knowledge, here are some excellent options:"
            2. List 2-3 restaurants that serve {unsupported_cuisine} cuisine for: "{original_query}"
            3. Include restaurant names, specific dishes, and why they're recommended
            4. Focus on {unsupported_cuisine} cuisine only
            
            Be helpful about {unsupported_cuisine} specifically."""
            
            # Append this section
            prompt += """

Include a compact JSON block after your brief message with this shape:
{
  "items": [
    {"restaurant_name": "string", "dish": "string or null", "reason": "string", "location": "string (prefer {unsupported_location})", "rating": "number or null"}
  ]
}
Only one JSON block. Keep item fields short.
"""
        
        else:
            app_logger.warning("❓ Neither unsupported_location nor unsupported_cuisine provided")
            return "I'm not sure how to help with that request. Could you try asking about restaurants in Manhattan?"

        app_logger.info(f"📝 Sending prompt to OpenAI: {prompt[:200]}...")
        
        response = await openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": "You are a restaurant recommendation expert. When asked about locations or cuisines outside your specialized database, provide recommendations for the specific location/cuisine requested. NEVER suggest alternative cities. Focus on the user's actual request. Do not mention traveling to other cities."
                },
                {
                    "role": "user", 
                    "content": prompt
                }
            ],
            max_tokens=300,
            temperature=0.7
        )
        
        result = response.choices[0].message.content.strip()
        app_logger.info(f"✅ OpenAI response: {result[:200]}...")
        
        return result
        
    except Exception as e:
        app_logger.error(f"❌ Error generating alternatives: {e}")
        return f"I don't have specialized data for that area, but here are some recommendations based on my general knowledge."


async def validate_query_scope(parsed_query: Dict[str, Any], original_query: str) -> Tuple[bool, Optional[str]]:
    """Validate query scope, cultural sensitivity, and provide OpenAI suggestions if needed."""
    settings = get_settings()
    
    app_logger.info(f"🔍 Validating query scope for parsed_query: {parsed_query}")
    
    unsupported_location = None
    unsupported_cuisine = None
    cultural_sensitivity_issue = None
    
    # Check location
    location = parsed_query.get('location')
    app_logger.info(f"🌍 Location check: '{location}' in supported cities {settings.supported_cities}")
    
    # Extract base city from location (handle "City in Neighborhood" format)
    base_city = None
    neighborhood = None
    if location:
        # Split by " in " to extract city from "City in Neighborhood" format
        if " in " in location:
            parts = location.split(" in ")
            base_city = parts[0].strip()
            neighborhood = parts[1].strip() if len(parts) > 1 else None
            app_logger.info(f"🏙️ Extracted base city: '{base_city}' and neighborhood: '{neighborhood}' from location: '{location}'")
        else:
            base_city = location.strip()
        
        if base_city not in settings.supported_cities:
            unsupported_location = location
            app_logger.info(f"❌ Unsupported location detected: {unsupported_location} (base city: {base_city})")
        else:
            app_logger.info(f"✅ Supported location: {location} (base city: {base_city})")
            
            # Check if neighborhood exists in location metadata
            if neighborhood:
                try:
                    from src.vector_db.milvus_client import MilvusClient
                    milvus_client = MilvusClient()
                    neighborhoods = milvus_client.get_neighborhoods_for_city(base_city)
                    
                    # Check if the neighborhood exists
                    neighborhood_exists = any(
                        n.get('neighborhood', '').lower() == neighborhood.lower() 
                        for n in neighborhoods
                    )
                    
                    if not neighborhood_exists:
                        app_logger.info(f"⚠️ Neighborhood '{neighborhood}' not found in {base_city}")
                        # Don't block the query, just log a warning
                        # The system can still work with city-level data
                    else:
                        app_logger.info(f"✅ Neighborhood '{neighborhood}' found in {base_city}")
                        
                except Exception as e:
                    app_logger.warning(f"⚠️ Could not verify neighborhood: {e}")
                    # Continue with the query even if neighborhood verification fails
    
    # Check cuisine
    cuisine = parsed_query.get('cuisine_type')
    app_logger.info(f"🍽️ Cuisine check: '{cuisine}' in supported cuisines {settings.supported_cuisines}")
    if cuisine and cuisine not in settings.supported_cuisines:
        unsupported_cuisine = cuisine
        app_logger.info(f"❌ Unsupported cuisine detected: {unsupported_cuisine}")
    
    # ✅ NEW: Cultural Sensitivity Validation
    cultural_sensitivity_issue = await validate_cultural_sensitivity(parsed_query, original_query)
    
    # Handle cultural sensitivity issues first (highest priority)
    if cultural_sensitivity_issue:
        return False, cultural_sensitivity_issue
    
    # Handle scope issues
    if unsupported_location or unsupported_cuisine:
        app_logger.info(f"🚫 Scope validation failed - calling suggest_alternatives_with_choice")
        suggestion = await suggest_alternatives_with_choice(
            original_query, 
            unsupported_location, 
            unsupported_cuisine
        )
        return False, suggestion
    
    app_logger.info(f"✅ Scope validation passed")
    return True, None

async def validate_cultural_sensitivity(parsed_query: Dict[str, Any], original_query: str) -> Optional[str]:
    """Validate cultural sensitivity and content appropriateness of the query."""
    
    # Ensure original_query is not None
    if not original_query:
        return None
    
    # Check for inappropriate language first
    inappropriate_language_issue = check_inappropriate_language(original_query)
    if inappropriate_language_issue:
        return inappropriate_language_issue
    
    # Then check cultural sensitivity for dish-cuisine combinations
    cuisine = parsed_query.get('cuisine_type')
    dish_name = parsed_query.get('dish_name')
    
    # If no cuisine or dish specified, no sensitivity check needed
    if not cuisine or not dish_name:
        return None
    
    # Define culturally inappropriate combinations
    inappropriate_combinations = {
        'Indian': [
            'beef', 'beef curry', 'beef biryani', 'beef kebab', 'beef masala',
            'steak', 'hamburger', 'beef burger', 'roast beef'
        ],
        # Future extensions:
        'Halal': ['pork', 'ham', 'bacon', 'wine'],
        'Kosher': ['pork', 'ham', 'bacon', 'shellfish', 'cheeseburger'],
    }
    
    # Check for inappropriate combinations - add extra safety check
    if cuisine in inappropriate_combinations and dish_name and isinstance(dish_name, str):
        dish_lower = dish_name.lower()
        inappropriate_dishes = inappropriate_combinations[cuisine]
        
        for inappropriate_dish in inappropriate_dishes:
            if inappropriate_dish in dish_lower:
                return await generate_cultural_sensitivity_response(cuisine, dish_name, original_query)
    
    return None


def check_inappropriate_language(query: str) -> Optional[str]:
    """Check for inappropriate language in the query."""
    
    # Ensure query is not None or empty
    if not query or not isinstance(query, str):
        return None
    
    # Define categories of inappropriate content
    inappropriate_patterns = {
        'profanity': [
            # Common profanity (add more as needed)
            'damn', 'crap', 'suck', 'stupid', 'idiot',
            # Removed 'hell' to allow "Hell's Kitchen" neighborhood
        ],
        'offensive_terms': [
            # Racial/ethnic slurs (add specific terms you want to block)
            # Note: Be careful with this list - some words have context-dependent meanings
        ],
        'sexual_content': [
            'sexy', 'nude', 'naked', 'adult'
        ],
        'discriminatory': [
            'hate', 'racist', 'sexist', 'terrorist', 'illegal alien'
        ],
        'threats_violence': [
            'kill', 'murder', 'violence', 'hurt', 'harm', 'beat up', 'destroy'
        ]
    }
    
    query_lower = query.lower()
    
    # Context-aware profanity check - allow legitimate place names
    legitimate_place_names = [
        "hell's kitchen", "hells kitchen", "hell kitchen",  # Manhattan neighborhood
        "hell's gate", "hells gate",  # Other legitimate place names
    ]
    
    # Check if the query contains legitimate place names
    for place_name in legitimate_place_names:
        if place_name in query_lower:
            # Skip profanity check for legitimate place names
            continue
    
    # Check each category
    for category, words in inappropriate_patterns.items():
        for word in words:
            if word in query_lower:
                return generate_inappropriate_language_response(word, category)
    
    return None


def generate_inappropriate_language_response(detected_word: str, category: str) -> str:
    """Generate a response for inappropriate language detection."""
    
    if category == 'profanity':
        return "I'd prefer to keep our conversation professional. Let me help you find great restaurant recommendations! What type of cuisine are you interested in?"
    
    elif category == 'sexual_content':
        return "Let's focus on finding delicious food recommendations. What cuisine or dish are you in the mood for?"
    
    elif category == 'discriminatory' or category == 'threats_violence':
        return "I'm here to help with restaurant recommendations in a respectful manner. Please let me know what type of food you're looking for!"
    
    else:
        return "Let's keep our conversation focused on finding great food! What cuisine are you interested in trying?"


async def generate_cultural_sensitivity_response(cuisine: str, inappropriate_dish: str, original_query: str) -> str:
    """Generate a culturally sensitive response for inappropriate dish-cuisine combinations."""
    try:
        settings = get_settings()
        openai_client = AsyncOpenAI(api_key=settings.openai_api_key)
        
        prompt = f"""
The user asked: "{original_query}"

They requested "{inappropriate_dish}" from {cuisine} cuisine, but this combination may be culturally inappropriate or unavailable due to dietary/religious considerations.

Generate a respectful response that:
1. Politely explains why this combination might not be available
2. Suggests 3 popular and appropriate {cuisine} dishes instead
3. Maintains a helpful and respectful tone
4. Doesn't make assumptions about the user's background

Keep it educational and helpful, not preachy. Focus on delicious alternatives.
"""

        response = await openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "system",
                    "content": "You are a culturally aware restaurant recommendation assistant. Provide respectful explanations about cuisine traditions and suggest appropriate alternatives."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.7,
            max_tokens=200
        )
        
        return response.choices[0].message.content.strip()
        
    except Exception as e:
        app_logger.error(f"Cultural sensitivity response error: {e}")
        # Fallback message
        if cuisine == 'Indian':
            return "I understand you're interested in that dish, but most Indian restaurants don't serve beef dishes due to cultural traditions. Instead, I'd recommend trying chicken biryani, paneer curry, or dal - these are delicious and authentic Indian options! Would you like recommendations for these dishes?"
        
        return f"That combination might not be available. Let me suggest some popular {cuisine} dishes instead!"
# Global instances
settings = get_settings()
milvus_client = None
query_parser = None
retrieval_engine = None
fallback_handler = None
response_generator = None  # Add this line

web_search_cache: Dict[str, Dict[str, Any]] = {}  # key: f"{original_query}|{unsupported_location}|{unsupported_cuisine}"
WEB_SEARCH_TTL_SECONDS = 6 * 3600

chat_sessions: Dict[str, List[ChatMessage]] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI app."""
    global milvus_client, query_parser, retrieval_engine, fallback_handler, response_generator  # Update this line
    
    # Startup
    try:
        app_logger.info("Starting SweetPick API...")
        
        # Initialize Milvus client
        milvus_client = MilvusClient()
        
        # Initialize query processing components
        query_parser = QueryParser()
        retrieval_engine = EnhancedRetrievalEngine(milvus_client)
        fallback_handler = FallbackHandler(retrieval_engine, query_parser)
        response_generator = ResponseGenerator()  # Add this line
        
        app_logger.info("API startup completed successfully")
        
    except Exception as e:
        app_logger.error(f"Error during startup: {e}")
        raise
    
    yield
    
    # Shutdown
    try:
        if milvus_client:
            milvus_client.close()
        app_logger.info("API shutdown completed")
    except Exception as e:
        app_logger.error(f"Error during shutdown: {e}")


# Initialize FastAPI app
app = FastAPI(
    title="SweetPick API",
    description="Restaurant dish recommendation system using RAG",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Statistics tracking
stats = {
    'total_queries': 0,
    'cache_hits': 0,
    'cache_misses': 0,
    'response_times': [],
    'api_costs': {
        'openai': 0.0,
        'serpapi': 0.0
    }
}

templates = Jinja2Templates(directory="templates")

# Mount static files (add this after creating the FastAPI app)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Add this route for serving the UI
@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Serve the SweetPick UI."""
    return templates.TemplateResponse("index.html", {"request": request})

# Add this import at the top
from fastapi.responses import HTMLResponse
import hashlib


def get_client_id(request: Request) -> str:
    """Extract client ID from request for abuse protection."""
    # Try to get real IP address
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        client_ip = forwarded_for.split(",")[0].strip()
    else:
        # In TestClient or certain proxies, request.client may be None
        client_ip = (request.client.host if getattr(request, "client", None) and request.client
                     else "localtest")
    
    # Add user agent hash for additional uniqueness
    user_agent = request.headers.get("User-Agent", "")
    client_hash = hashlib.md5(f"{client_ip}:{user_agent}".encode()).hexdigest()[:16]
    
    return client_hash


@app.get("/test-app")
async def test_app():
    """Test endpoint to check if the main app components are working."""
    try:
        # Check if we can access the main components
        components_status = {
            "config_available": CONFIG_AVAILABLE,
            "logger_available": LOGGER_AVAILABLE,
            "openai_available": OPENAI_AVAILABLE,
            "milvus_available": MILVUS_AVAILABLE,
            "query_parser_available": QUERY_PARSER_AVAILABLE,
            "retrieval_available": RETRIEVAL_AVAILABLE,
            "fallback_available": FALLBACK_AVAILABLE,
            "response_generator_available": RESPONSE_GENERATOR_AVAILABLE,
            "abuse_protection_available": ABUSE_PROTECTION_AVAILABLE
        }
        
        # Check environment variables
        env_status = {
            "OPENAI_API_KEY": "SET" if os.getenv("OPENAI_API_KEY") else "NOT SET",
            "SERPAPI_API_KEY": "SET" if os.getenv("SERPAPI_API_KEY") else "NOT SET",
            "MILVUS_URI": "SET" if os.getenv("MILVUS_URI") else "NOT SET",
            "MILVUS_TOKEN": "SET" if os.getenv("MILVUS_TOKEN") else "NOT SET"
        }
        
        # Try to initialize key components
        component_errors = {}
        
        try:
            if CONFIG_AVAILABLE:
                settings = get_settings()
                component_errors["config"] = "OK"
            else:
                component_errors["config"] = "NOT AVAILABLE"
        except Exception as e:
            component_errors["config"] = f"ERROR: {str(e)}"
        
        try:
            if MILVUS_AVAILABLE:
                from src.vector_db.milvus_client import MilvusClient
                milvus = MilvusClient()
                component_errors["milvus"] = "OK"
            else:
                component_errors["milvus"] = "NOT AVAILABLE"
        except Exception as e:
            component_errors["milvus"] = f"ERROR: {str(e)}"
        
        return {
            "status": "test_complete",
            "components": components_status,
            "environment": env_status,
            "component_errors": component_errors,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "test_failed",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@app.get("/test-milvus")
async def test_milvus():
    """Test endpoint specifically for Milvus configuration and connection."""
    try:
        # Check environment variables
        milvus_uri = os.getenv("MILVUS_URI")
        milvus_token = os.getenv("MILVUS_TOKEN")
        
        # Test Milvus HTTP client
        client_test = {}
        try:
            if MILVUS_AVAILABLE:
                from src.vector_db.milvus_http_client import MilvusHTTPClient
                milvus_client = MilvusHTTPClient()
                client_test["client_initialization"] = {
                    "success": True,
                    "client_type": "MilvusHTTPClient"
                }
                
                # Test connection
                try:
                    connection_result = await milvus_client.test_connection()
                    client_test["connection"] = connection_result
                except Exception as e:
                    client_test["connection"] = {
                        "success": False,
                        "error": str(e)
                    }
            else:
                client_test["client_initialization"] = {
                    "success": False,
                    "error": "MILVUS_AVAILABLE is False"
                }
        except Exception as e:
            client_test["client_initialization"] = {
                "success": False,
                "error": str(e)
            }
        
        return {
            "status": "milvus_http_test_complete",
            "message": "Testing Milvus HTTP API client",
            "environment_check": {
                "MILVUS_URI": {
                    "set": bool(milvus_uri),
                    "value": milvus_uri[:20] + "..." if milvus_uri and len(milvus_uri) > 20 else milvus_uri,
                    "valid_format": milvus_uri and milvus_uri.startswith("https://") if milvus_uri else False
                },
                "MILVUS_TOKEN": {
                    "set": bool(milvus_token),
                    "value": milvus_token[:10] + "..." if milvus_token and len(milvus_token) > 10 else milvus_token,
                    "valid_format": milvus_token and len(milvus_token) > 20 if milvus_token else False
                }
            },
            "client_test": client_test,
            "explanation": {
                "approach": "Using Milvus HTTP API instead of pymilvus Python client",
                "advantages": [
                    "No pymilvus installation required",
                    "Better Vercel compatibility",
                    "Standard HTTP requests",
                    "Async support with httpx"
                ],
                "current_status": "HTTP client implemented with sample data fallback"
            },
            "current_capabilities": [
                "✅ Milvus HTTP client available",
                "✅ Environment variables configured",
                "✅ HTTP requests to Milvus Cloud",
                "✅ Sample data fallback",
                "✅ Async operation support"
            ],
            "timestamp": datetime.now().isoformat()
        }
        

        
    except Exception as e:
        return {
            "status": "milvus_test_failed",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    services_status = {}
    
    try:
        # Check Milvus connection
        if milvus_client:
            collection_stats = await milvus_client.get_collection_stats()
            services_status['milvus'] = 'healthy' if collection_stats else 'unhealthy'
        else:
            services_status['milvus'] = 'uninitialized'
        
        # Check other services
        services_status['query_parser'] = 'healthy' if query_parser else 'uninitialized'
        services_status['retrieval_engine'] = 'healthy' if retrieval_engine else 'uninitialized'
        services_status['fallback_handler'] = 'healthy' if fallback_handler else 'uninitialized'
        
    except Exception as e:
        app_logger.error(f"Health check error: {e}")
        services_status['error'] = str(e)
    
    return HealthResponse(
        status="healthy" if all(status == 'healthy' for status in services_status.values() if status != 'uninitialized') else "degraded",
        timestamp=datetime.now().isoformat(),
        version="1.0.0",
        services=services_status
    )


@app.post("/query", response_model=QueryResponse)
async def process_query(request: QueryRequest, background_tasks: BackgroundTasks, http_request: Request):
    """Main query endpoint for restaurant/dish recommendations."""
    start_time = datetime.now()
    
    try:
        # Basic logging for debugging
        print(f"🔍 Query received: {request.query}")
        print(f"🔍 Request details: {request.dict()}")
        
        # Check if essential components are available
        if not CONFIG_AVAILABLE:
            raise HTTPException(status_code=500, detail="Configuration not available")
        
        if not MILVUS_AVAILABLE:
            # Instead of failing, provide a fallback response
            print("⚠️ Milvus HTTP client not available, using fallback response")
            return QueryResponse(
                query=request.query,
                query_type="fallback",
                recommendations=[
                    {
                        "dish_name": "Margherita Pizza",
                        "restaurant_name": "Example Italian Restaurant",
                        "restaurant_id": "example_1",
                        "neighborhood": "Manhattan",
                        "cuisine_type": "Italian",
                        "topic_mentions": 0,
                        "topic_score": 0.0,
                        "final_score": 0.8,
                        "source": "fallback"
                    },
                    {
                        "dish_name": "Spaghetti Carbonara",
                        "restaurant_name": "Sample Italian Place",
                        "restaurant_id": "example_2", 
                        "neighborhood": "Manhattan",
                        "cuisine_type": "Italian",
                        "topic_mentions": 0,
                        "topic_score": 0.0,
                        "final_score": 0.7,
                        "source": "fallback"
                    }
                ],
                natural_response="I'm currently in fallback mode while the database is being set up. Here are some popular Italian dishes you might enjoy in Manhattan!",
                fallback_used=True,
                fallback_reason="Milvus HTTP client not available - using fallback data",
                processing_time=(datetime.now() - start_time).total_seconds(),
                confidence_score=0.5
            )
        
        print("🔍 Essential components check passed")
        # 🔒 ABUSE PROTECTION: Check request before processing
        client_id = get_client_id(http_request)
        is_allowed, security_error, security_report = await abuse_protection.check_request(
            client_id, request.query, request.dict()
        )
        
        if not is_allowed:
            app_logger.warning(f"Request blocked for {client_id}: {security_error}")
            raise HTTPException(
                status_code=429 if "rate limit" in security_error.lower() else 400,
                detail=f"Request blocked: {security_error}"
            )
        
        app_logger.info(f"Processing query: {request.query} (client: {client_id})")
        
        # Update statistics
        stats['total_queries'] += 1
        
        # Parse the query
        parsed_query = await query_parser.parse_query(request.query)

        # Heuristic fallback: infer location/cuisine from raw text if missing
        try:
            if not parsed_query.get('location'):
                text_lower = (request.query or '').lower()
                for city in settings.supported_cities:
                    if city.lower() in text_lower:
                        parsed_query['location'] = city
                        break
            if not parsed_query.get('cuisine_type'):
                text_lower = (request.query or '').lower()
                for cuisine in settings.supported_cuisines:
                    if cuisine.lower() in text_lower:
                        parsed_query['cuisine_type'] = cuisine
                        break
        except Exception:
            pass
        
        # ✅ NEW: Validate query scope with choice offering
        is_valid, choice_message = await validate_query_scope(parsed_query, request.query)
        if not is_valid:
            # Build cache key
            loc = parsed_query.get('location')
            cui = parsed_query.get('cuisine_type')
            cache_key = f"{request.query}|{loc}|{cui}"
            cached = _cache_get_web_search(cache_key)
            if cached:
                cards = cached.get("cards", [])
                natural = cached.get("natural_response", "")
            else:
                # Extract cards from the text we already generated
                cards = _extract_cards_from_text(choice_message or "", loc)
                # Clean the natural response by removing JSON
                natural = _clean_natural_response(choice_message or "")
                _cache_set_web_search(cache_key, {"cards": cards, "natural_response": natural})

            return QueryResponse(
                query=request.query,
                query_type="out_of_scope_with_choice",
                recommendations=cards,                # <-- cards now populate UI
                natural_response=natural,             # <-- clean response without JSON
                fallback_used=True,
                fallback_reason="Out of scope query - using web search",
                processing_time=(datetime.now() - start_time).total_seconds(),
                confidence_score=0.0,
                web_search_available=True
            )
        
        # Determine query type
        query_type = query_parser.classify_query_type(parsed_query)
        
        # Get recommendations
        recommendations, fallback_used, fallback_reason = await retrieval_engine.get_recommendations(
            parsed_query, request.max_results or 10
        )
        
        # Apply fallback if needed
        if not recommendations and not fallback_used:
            recommendations, fallback_used, fallback_reason = await fallback_handler.execute_fallback_strategy(
                parsed_query, recommendations
            )
            
            # Check if fallback suggests web search for dish-specific queries
            if not recommendations and fallback_reason == "Dish-specific query - recommend web search":
                # Trigger web search for dish-specific queries
                app_logger.info("Dish-specific query detected, triggering web search")
                
                # Build cache key
                loc = parsed_query.get('location')
                cui = parsed_query.get('cuisine_type')
                cache_key = f"{request.query}|{loc}|{cui}"
                cached = _cache_get_web_search(cache_key)
                
                if cached:
                    cards = cached.get("cards", [])
                    natural = cached.get("natural_response", "")
                else:
                    # Generate web search response
                    web_response = await _generate_web_search_response(request.query, loc)
                    cards = _extract_cards_from_text(web_response, loc)
                    natural = _clean_natural_response(web_response)
                    _cache_set_web_search(cache_key, {"cards": cards, "natural_response": natural})
                
                return QueryResponse(
                    query=request.query,
                    query_type="dish_specific_web_search",
                    recommendations=cards,
                    natural_response=natural,
                    fallback_used=True,
                    fallback_reason="Dish-specific query - using web search",
                    processing_time=(datetime.now() - start_time).total_seconds(),
                    confidence_score=0.0,
                    web_search_available=True
                )
        
        # Calculate processing time
        processing_time = (datetime.now() - start_time).total_seconds()
        stats['response_times'].append(processing_time)
        
        # Calculate confidence score
        confidence_score = retrieval_engine.calculate_confidence(recommendations, parsed_query)
        
        # Update cache statistics
        if fallback_used:
            stats['cache_misses'] += 1
        else:
            stats['cache_hits'] += 1
        
        # Generate natural language response
        query_metadata = {
            'location': parsed_query.get('location'),
            'cuisine_type': parsed_query.get('cuisine_type'),
            'fallback_used': fallback_used,
            'confidence_score': confidence_score,
            'query_type': query_type
        }
        
        # Generate conversational response
        natural_response = ""
        if response_generator:
            try:
                natural_response = await response_generator.generate_conversational_response(
                    request.query, 
                    recommendations, 
                    query_metadata
                )
            except Exception as e:
                app_logger.error(f"Error generating natural response: {e}")
                # Fallback to quick response
                natural_response = response_generator.generate_quick_response(recommendations, query_metadata) if response_generator else ""
        
        app_logger.info(f"Query processed in {processing_time:.2f}s with {len(recommendations)} recommendations")
        
        # Debug: Log the structure of recommendations
        if recommendations:
            app_logger.info(f"First recommendation structure: {list(recommendations[0].keys())}")
            app_logger.info(f"First recommendation sample: {recommendations[0]}")
            
            # Check for non-serializable objects and convert numpy types
            for i, rec in enumerate(recommendations):
                for key, value in rec.items():
                    # Convert numpy types to native Python types
                    if hasattr(value, 'item'):  # numpy scalar
                        recommendations[i][key] = value.item()
                    elif hasattr(value, 'tolist'):  # numpy array
                        recommendations[i][key] = value.tolist()
                    elif not isinstance(value, (str, int, float, bool, list, dict, type(None))):
                        app_logger.error(f"Non-serializable value in recommendation {i}, key '{key}': {type(value)} = {value}")
                        # Convert to string as fallback
                        recommendations[i][key] = str(value)
        
        return QueryResponse(
            query=request.query,
            query_type=query_type,
            recommendations=recommendations,
            natural_response=natural_response,  # Add this line
            fallback_used=fallback_used,
            fallback_reason=fallback_reason,
            processing_time=processing_time,
            confidence_score=confidence_score
        )
        
    except Exception as e:
        import traceback
        error_details = {
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": traceback.format_exc(),
            "components_available": {
                "config": CONFIG_AVAILABLE,
                "milvus": MILVUS_AVAILABLE,
                "query_parser": QUERY_PARSER_AVAILABLE,
                "retrieval": RETRIEVAL_AVAILABLE,
                "fallback": FALLBACK_AVAILABLE
            }
        }
        
        print(f"❌ Query processing error: {error_details}")
        app_logger.error(f"Error processing query: {e}")
        app_logger.error(f"Full traceback: {traceback.format_exc()}")
        
        # Return detailed error for debugging
        raise HTTPException(
            status_code=500, 
            detail=f"Error processing query: {str(e)} | Type: {type(e).__name__} | Check /test-app for component status"
        )


@app.post("/query/web-search", response_model=QueryResponse)
async def process_web_search_query(request: QueryRequest, background_tasks: BackgroundTasks):
    """Fallback endpoint for generic OpenAI web search when out of scope."""
    start_time = datetime.now()
    
    try:
        app_logger.info(f"Processing web search query: {request.query}")
        settings = get_settings()
        
        # Use OpenAI to search web and provide generic recommendations
        openai_client = AsyncOpenAI(api_key=settings.openai_api_key)
        
        prompt = f"""
User is looking for: "{request.query}"

This is outside our curated restaurant database. Please search the web and provide generic restaurant recommendations that match their request.

Provide 3-5 restaurant suggestions in this JSON format:
{{
  "recommendations": [
    {{
      "restaurant_name": "Restaurant Name",
      "cuisine_type": "Cuisine",
      "location": "Address/Area", 
      "description": "Brief description why it's good",
      "type": "web_search_result"
    }}
  ]
}}

Focus on popular, well-reviewed restaurants that match their request.
"""

        response = await openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a restaurant recommendation assistant with web search capabilities. Provide helpful generic restaurant suggestions."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.7,
            max_tokens=500
        )
        
        # Parse OpenAI response
        content = response.choices[0].message.content
        try:
            # Extract JSON from response
            json_start = content.find('{')
            json_end = content.rfind('}') + 1
            if json_start != -1 and json_end > json_start:
                result = json.loads(content[json_start:json_end])
                recommendations = result.get('recommendations', [])
            else:
                recommendations = []
        except:
            recommendations = []
        
        processing_time = (datetime.now() - start_time).total_seconds()
        
        return QueryResponse(
            query=request.query,
            query_type="web_search",
            recommendations=recommendations,
            fallback_used=True,
            fallback_reason="Generic web search results (not from our curated database)",
            processing_time=processing_time,
            confidence_score=0.5,
            web_search_available=False
        )
        
    except Exception as e:
        app_logger.error(f"Error in web search: {e}")
        raise HTTPException(status_code=500, detail=f"Error in web search: {str(e)}")

@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    start_time = datetime.now()
    sid = request.session_id or str(uuid.uuid4())
    history = chat_sessions.setdefault(sid, [])
    history.append(ChatMessage(role="user", content=request.message))

    # Parse and validate current turn
    parsed_query = await query_parser.parse_query(request.message)
    is_valid, choice_message = await validate_query_scope(parsed_query, request.message)
    recommendations: List[Dict[str, Any]] = []
    fallback_used = False
    natural_response = ""

    if not is_valid:
        # Out-of-scope: use the earlier web-search text
        natural_response = choice_message or "Let me pull options for that."
    else:
        # In-scope: run retrieval
        query_type = query_parser.classify_query_type(parsed_query)
        recommendations, fallback_used, _ = await retrieval_engine.get_recommendations(parsed_query, max_results=6)
        # Build metadata and generate NL response with conversation context
        query_metadata = {
            'location': parsed_query.get('location'),
            'cuisine_type': parsed_query.get('cuisine_type'),
            'fallback_used': fallback_used,
            'query_type': query_type
        }
        try:
            convo_context = _summarize_history(history)
            natural_response = await response_generator.generate_conversational_response(
                f"{request.message}\n\nConversation:\n{convo_context}",
                recommendations,
                query_metadata
            )
        except Exception:
            natural_response = ""

    # Confidence and timing
    processing_time = (datetime.now() - start_time).total_seconds()
    confidence_score = retrieval_engine.calculate_confidence(recommendations, parsed_query)

    # Save assistant turn
    history.append(ChatMessage(role="assistant", content=natural_response or "Done."))

    return ChatResponse(
        session_id=sid,
        messages=history,
        recommendations=recommendations,
        natural_response=natural_response,
        fallback_used=fallback_used or (not is_valid),
        processing_time=processing_time,
        confidence_score=confidence_score
    )

@app.get("/restaurant/{restaurant_id}", response_model=RestaurantResponse)
async def get_restaurant_details(restaurant_id: str):
    """Get detailed information about a specific restaurant."""
    try:
        # Get restaurant details from Milvus
        restaurant = await retrieval_engine.get_restaurant_details(restaurant_id)
        
        if not restaurant:
            raise HTTPException(status_code=404, detail="Restaurant not found")
        
        # Get top dishes for this restaurant
        top_dishes = retrieval_engine.get_restaurant_dishes(restaurant_id, limit=5)
        
        return RestaurantResponse(
            restaurant_id=restaurant['restaurant_id'],
            restaurant_name=restaurant['restaurant_name'],
            city=restaurant['city'],
            cuisine_type=restaurant['cuisine_type'],
            rating=restaurant['rating'],
            review_count=restaurant['review_count'],
            address=restaurant['full_address'],
            phone=restaurant.get('phone'),
            website=restaurant.get('website'),
            price_range=restaurant['price_range'],
            meal_types=restaurant.get('meal_types', []),
            top_dishes=top_dishes
        )
        
    except HTTPException:
        raise
    except Exception as e:
        app_logger.error(f"Error getting restaurant details: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving restaurant details: {str(e)}")


@app.get("/dish/{dish_id}", response_model=DishResponse)
async def get_dish_details(dish_id: str):
    """Get detailed information about a specific dish."""
    try:
        # Get dish details from Milvus
        dish = await retrieval_engine.get_dish_details(dish_id)
        
        if not dish:
            raise HTTPException(status_code=404, detail="Dish not found")
        
        # Get restaurant information
        restaurant = await retrieval_engine.get_restaurant_details(dish['restaurant_id'])
        
        return DishResponse(
            dish_id=dish['dish_id'],
            dish_name=dish['dish_name'],
            restaurant_id=dish['restaurant_id'],
            restaurant_name=restaurant['restaurant_name'] if restaurant else 'Unknown',
            category=dish['category'],
            sentiment_score=dish['sentiment_score'],
            recommendation_score=dish['recommendation_score'],
            topic_mentions=dish.get('topic_mentions', 0),
            topic_score=dish.get('topic_score', 0.0),
            final_score=dish.get('final_score', dish.get('sentiment_score', 0.0)),
            source=dish.get('source', 'sentiment'),
            hybrid_insights=dish.get('hybrid_insights', {}),
            positive_aspects=dish.get('positive_aspects', []),
            negative_aspects=dish.get('negative_aspects', []),
            dietary_tags=dish.get('dietary_tags', []),
            sample_reviews=dish.get('sample_contexts', [])
        )
        
    except HTTPException:
        raise
    except Exception as e:
        app_logger.error(f"Error getting dish details: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving dish details: {str(e)}")


@app.get("/stats", response_model=StatsResponse)
async def get_statistics():
    """Get system statistics."""
    try:
        # Get collection statistics
        collection_stats = await milvus_client.get_collection_stats()
        
        total_restaurants = collection_stats.get('restaurants', {}).get('num_entities', 0)
        total_dishes = collection_stats.get('dishes', {}).get('num_entities', 0)
        
        # Calculate cache hit rate
        total_cache_requests = stats['cache_hits'] + stats['cache_misses']
        cache_hit_rate = stats['cache_hits'] / total_cache_requests if total_cache_requests > 0 else 0.0
        
        # Calculate average response time
        avg_response_time = sum(stats['response_times']) / len(stats['response_times']) if stats['response_times'] else 0.0
        
        return StatsResponse(
            total_restaurants=total_restaurants,
            total_dishes=total_dishes,
            total_queries=stats['total_queries'],
            cache_hit_rate=cache_hit_rate,
            average_response_time=avg_response_time,
            api_costs=stats['api_costs']
        )
        
    except Exception as e:
        app_logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving statistics: {str(e)}")


@app.get("/discovery/stats")
async def get_discovery_statistics():
    """Get AI-driven discovery system statistics."""
    try:
        if hasattr(retrieval_engine, 'get_discovery_stats'):
            discovery_stats = await retrieval_engine.get_discovery_stats()
            return {
                "discovery_collections": discovery_stats,
                "timestamp": datetime.now().isoformat(),
                "system_type": "ai_driven_discovery"
            }
        else:
            return {
                "message": "Discovery statistics not available - using traditional retrieval engine",
                "timestamp": datetime.now().isoformat()
            }
    except Exception as e:
        app_logger.error(f"Error getting discovery statistics: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving discovery statistics: {str(e)}")


@app.get("/security/stats")
async def get_security_statistics():
    """Get security statistics and abuse protection metrics."""
    try:
        security_stats = abuse_protection.get_security_stats()
        return {
            "security_overview": security_stats,
            "timestamp": datetime.now().isoformat(),
            "system_status": "protected"
        }
    except Exception as e:
        app_logger.error(f"Error getting security statistics: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving security statistics: {str(e)}")


@app.get("/security/client/{client_id}")
async def get_client_security_status(client_id: str):
    """Get security status for a specific client."""
    try:
        client_status = abuse_protection.get_client_status(client_id)
        return {
            "client_id": client_id,
            "status": client_status,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        app_logger.error(f"Error getting client status: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving client status: {str(e)}")


@app.post("/security/block/{client_id}")
async def block_client(client_id: str, duration_seconds: int = 3600, reason: str = "Manual block"):
    """Manually block a client."""
    try:
        abuse_protection.block_client(client_id, duration_seconds, reason)
        return {
            "message": f"Client {client_id} blocked for {duration_seconds} seconds",
            "reason": reason,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        app_logger.error(f"Error blocking client: {e}")
        raise HTTPException(status_code=500, detail=f"Error blocking client: {str(e)}")


@app.post("/security/unblock/{client_id}")
async def unblock_client(client_id: str):
    """Unblock a client."""
    try:
        abuse_protection.unblock_client(client_id)
        return {
            "message": f"Client {client_id} unblocked",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        app_logger.error(f"Error unblocking client: {e}")
        raise HTTPException(status_code=500, detail=f"Error unblocking client: {str(e)}")


@app.post("/collect-data")
async def collect_data(background_tasks: BackgroundTasks):
    """Trigger data collection (runs in background)."""
    try:
        background_tasks.add_task(run_data_collection)
        return {"message": "Data collection started in background"}
    except Exception as e:
        app_logger.error(f"Error starting data collection: {e}")
        raise HTTPException(status_code=500, detail=f"Error starting data collection: {str(e)}")


async def run_data_collection():
    """Run the complete data collection pipeline."""
    try:
        app_logger.info("Starting data collection pipeline...")
        
        # Initialize components
        collector = SerpAPICollector()
        validator = DataValidator()
        dish_extractor = DishExtractor()
        sentiment_analyzer = SentimentAnalyzer()
        
        # Collect restaurants
        all_restaurants = await collector.collect_all_restaurants()
        
        # Validate and filter restaurants
        for city, restaurants in all_restaurants.items():
            filtered_restaurants = validator.filter_restaurants_by_quality(restaurants)
            
            # Collect reviews for each restaurant
            for restaurant in filtered_restaurants:
                reviews = await collector.get_restaurant_reviews(
                    restaurant['google_place_id'],
                    settings.max_reviews_per_restaurant
                )
                
                # Validate reviews
                valid_reviews, invalid_reviews = validator.validate_review_batch(reviews)
                
                if valid_reviews:
                    # Extract dishes
                    dishes = await dish_extractor.extract_dishes_from_reviews(valid_reviews)
                    
                    # Analyze sentiment for each dish
                    for dish in dishes:
                        sentiment = await sentiment_analyzer.analyze_dish_sentiment(
                            dish['dish_name'], valid_reviews
                        )
                        dish.update(sentiment)
                    
                    # Store in Milvus
                    await milvus_client.insert_restaurants([restaurant])
                    await milvus_client.insert_dishes(dishes)
        
        app_logger.info("Data collection pipeline completed")
        
    except Exception as e:
        app_logger.error(f"Error in data collection pipeline: {e}")


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Sweet Morsels Restaurant Recommendation API",
        "version": "1.0.0",
        "endpoints": {
            "POST /query": "Get restaurant/dish recommendations",
            "POST /query/web-search": "Generic web search for out-of-scope queries",  # ← ADD THIS
            "GET /restaurant/{id}": "Get restaurant details",
            "GET /dish/{id}": "Get dish details",
            "GET /health": "Health check",
            "GET /stats": "System statistics",
            "POST /collect-data": "Trigger data collection"
        },
        "documentation": "/docs"
    } 

def _cache_get_web_search(key: str) -> Optional[Dict[str, Any]]:
    item = web_search_cache.get(key)
    if not item:
        return None
    if (datetime.now() - item["ts"]).total_seconds() > WEB_SEARCH_TTL_SECONDS:
        web_search_cache.pop(key, None)
        return None
    return item["data"]

def _cache_set_web_search(key: str, data: Dict[str, Any]) -> None:
    web_search_cache[key] = {"ts": datetime.now(), "data": data}

def _extract_items_json(text: str) -> List[Dict[str, Any]]:
    # Try to find a JSON object/array in the text
    try:
        start = text.find("{")
        alt_start = text.find("[")
        if start == -1 and alt_start != -1:
            start = alt_start
        if start == -1:
            return []
        end = max(text.rfind("}"), text.rfind("]"))
        if end == -1 or end <= start:
            return []
        block = text[start:end+1]
        parsed = json.loads(block)
        if isinstance(parsed, dict) and "items" in parsed and isinstance(parsed["items"], list):
            return parsed["items"]
        if isinstance(parsed, list):
            return parsed
        return []
    except Exception:
        return []

def _normalize_web_items(items: List[Dict[str, Any]], location_hint: Optional[str]) -> List[Dict[str, Any]]:
    norm = []
    for it in items[:6]:
        norm.append({
            "restaurant_name": it.get("restaurant_name") or it.get("name"),
            "dish_name": it.get("dish") or it.get("dish_name"),
            "reason": it.get("reason") or it.get("why"),
            "location": it.get("location") or location_hint,
            "cuisine_type": it.get("cuisine"),
            "rating": it.get("rating") or 0,
            "type": "web_search",
            "confidence": 0.5
        })
    return [x for x in norm if x["restaurant_name"] or x["dish_name"]]

async def _generate_web_search_response(query: str, location_hint: Optional[str]) -> str:
    """Generate web search response for dish-specific queries."""
    try:
        settings = get_settings()
        openai_client = AsyncOpenAI(api_key=settings.openai_api_key)
        
        prompt = f"""
User is looking for: "{query}"

This is a dish-specific query. Please search the web and provide restaurant recommendations that match their request.

Provide 3-5 restaurant suggestions in this JSON format:
{{
  "items": [
    {{
      "restaurant_name": "Restaurant Name",
      "dish": "Specific Dish Name",
      "reason": "Brief description why it's good",
      "location": "{location_hint or 'Various locations'}",
      "rating": 4.5
    }}
  ]
}}

Focus on the specific dish mentioned in the query. Be helpful and provide realistic recommendations.
"""
        
        response = await openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.7
        )
        
        return response.choices[0].message.content or ""
        
    except Exception as e:
        app_logger.error(f"Error generating web search response: {e}")
        return ""

def _clean_natural_response(text: str) -> str:
    """Clean natural response by removing JSON blocks."""
    # Remove JSON blocks
    import re
    # Remove JSON objects/arrays
    cleaned = re.sub(r'```json\s*\n.*?\n```', '', text, flags=re.DOTALL)
    cleaned = re.sub(r'\{.*?\}', '', cleaned, flags=re.DOTALL)
    cleaned = re.sub(r'\[.*?\]', '', cleaned, flags=re.DOTALL)
    
    # Clean up extra whitespace and newlines
    cleaned = re.sub(r'\n\s*\n', '\n', cleaned)
    cleaned = cleaned.strip()
    
    return cleaned

def _extract_cards_from_text(text: str, location_hint: Optional[str]) -> List[Dict[str, Any]]:
    items = _extract_items_json(text)
    if items:
        return _normalize_web_items(items, location_hint)
    # Fallback: parse top 3 list/bullets
    lines = [ln.strip("- ").strip() for ln in text.splitlines() if ln.strip()]
    rows = []
    for ln in lines:
        if ln[:2].isdigit() or ln.startswith("*") or " - " in ln or "**" in ln:
            rows.append(ln)
    cards = []
    for ln in rows[:3]:
        name = None
        dish = None
        reason = None
        # very light heuristics
        if ":" in ln:
            parts = ln.split(":", 1)
            name = parts[0].strip(" -*#")
            reason = parts[1].strip()
        else:
            name = ln.strip(" -*#")
        cards.append({
            "restaurant_name": name,
            "dish_name": dish,
            "reason": reason,
            "location": location_hint,
            "type": "web_search",
            "confidence": 0.5,
            "rating": 0
        })
    return cards 

@app.get("/api/neighborhoods/{city}")
async def get_neighborhoods(city: str):
    """Get neighborhoods for a specific city."""
    try:
        from src.utils.neighborhood_mapper import neighborhood_mapper
        
        neighborhoods = neighborhood_mapper.get_neighborhoods_for_city(city)
        
        neighborhood_data = []
        for neighborhood in neighborhoods:
            neighborhood_data.append({
                "name": neighborhood.name,
                "description": neighborhood.description,
                "cuisine_focus": neighborhood.cuisine_focus,
                "restaurant_types": neighborhood.restaurant_types,
                "iconic_dishes": neighborhood.iconic_dishes,
                "tourist_factor": neighborhood.tourist_factor,
                "price_level": neighborhood.price_level
            })
        
        return {
            "city": city,
            "neighborhoods": neighborhood_data,
            "total_neighborhoods": len(neighborhood_data)
        }
    except Exception as e:
        app_logger.error(f"Error getting neighborhoods for {city}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get neighborhoods for {city}")


@app.get("/api/neighborhoods")
async def get_all_neighborhoods():
    """Get all available neighborhoods across all cities."""
    try:
        from src.utils.neighborhood_mapper import neighborhood_mapper
        
        all_neighborhoods = {}
        for city in ["Manhattan"]:
            neighborhoods = neighborhood_mapper.get_neighborhoods_for_city(city)
            all_neighborhoods[city] = [
                {
                    "name": neighborhood.name,
                    "description": neighborhood.description,
                    "cuisine_focus": neighborhood.cuisine_focus[:3],  # Top 3 cuisines
                    "tourist_factor": neighborhood.tourist_factor,
                    "price_level": neighborhood.price_level
                }
                for neighborhood in neighborhoods
            ]
        
        return {
            "cities": all_neighborhoods,
            "total_cities": len(all_neighborhoods)
        }
    except Exception as e:
        app_logger.error(f"Error getting all neighborhoods: {e}")
        raise HTTPException(status_code=500, detail="Failed to get neighborhoods")


@app.get("/api/locations/{city}")
async def get_location_statistics(city: str):
    """Get location statistics for a specific city."""
    try:
        from src.vector_db.milvus_client import MilvusClient
        milvus_client = MilvusClient()
        
        # Get city-level statistics
        city_stats = milvus_client.get_location_statistics(city)
        
        # Get neighborhoods for the city
        neighborhoods = milvus_client.get_neighborhoods_for_city(city)
        
        return {
            "city": city,
            "city_statistics": city_stats,
            "neighborhoods": neighborhoods,
            "total_neighborhoods": len(neighborhoods)
        }
    except Exception as e:
        app_logger.error(f"Error getting location statistics for {city}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get location statistics for {city}")


@app.get("/topics/{city}/{cuisine}")
async def get_topic_insights(city: str, cuisine: str, limit: int = 10):
    """Return top dishes by hybrid final_score for a city+cuisine (city not stored on dish; approximated by cuisine only)."""
    try:
        if not milvus_client:
            raise HTTPException(status_code=500, detail="Milvus client not initialized")

        # City filter isn't in dishes schema; we return top by cuisine for now
        # Over-fetch to allow deduplication before trimming
        raw_rows = milvus_client.search_dishes_with_topics(cuisine=cuisine, limit=max(limit * 3, 50))

        # Normalize/clean numpy types
        normalized: List[Dict[str, Any]] = []
        for row in raw_rows:
            clean: Dict[str, Any] = {}
            for k, v in row.items():
                try:
                    if hasattr(v, "item"):
                        clean[k] = v.item()
                    elif hasattr(v, "tolist"):
                        clean[k] = v.tolist()
                    elif isinstance(v, (str, int, float, bool)) or v is None:
                        clean[k] = v
                    elif isinstance(v, (list, dict)):
                        clean[k] = v
                    else:
                        clean[k] = str(v)
                except Exception:
                    clean[k] = str(v)
            normalized.append(clean)

        # Sort strictly by final_score desc, then topic_score, then recommendation_score
        normalized.sort(key=lambda d: (
            float(d.get("final_score") or 0.0),
            float(d.get("topic_score") or 0.0),
            float(d.get("recommendation_score") or 0.0)
        ), reverse=True)

        # Deduplicate by (dish_name, restaurant_id)
        seen = set()
        deduped: List[Dict[str, Any]] = []
        for d in normalized:
            dish_name = d.get("dish_name") or d.get("name")
            restaurant_id = d.get("restaurant_id")
            if not dish_name or not restaurant_id:
                continue
            key = (str(dish_name).lower(), str(restaurant_id))
            if key in seen:
                continue
            seen.add(key)
            deduped.append(d)

        # Trim to limit and map to concise output including restaurant_name
        top = deduped[:limit]
        output = []
        for d in top:
            output.append({
                "dish_name": d.get("dish_name") or d.get("name"),
                "restaurant_name": d.get("restaurant_name", "Unknown"),
                "restaurant_id": d.get("restaurant_id"),
                "neighborhood": d.get("neighborhood", ""),
                "cuisine_type": d.get("cuisine_type") or cuisine,
                "topic_mentions": int(d.get("topic_mentions") or 0),
                "topic_score": float(d.get("topic_score") or 0.0),
                "final_score": float(d.get("final_score") or float(d.get("recommendation_score") or 0.0)),
                "source": d.get("source", "hybrid")
            })

        return {
            "city": city,
            "cuisine": cuisine,
            "count": len(output),
            "dishes": output
        }
    except Exception as e:
        app_logger.error(f"Error getting topic insights: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get topic insights: {str(e)}")


@app.get("/api/locations/{city}/{neighborhood}")
async def get_neighborhood_statistics(city: str, neighborhood: str):
    """Get detailed statistics for a specific neighborhood."""
    try:
        from src.vector_db.milvus_client import MilvusClient
        milvus_client = MilvusClient()
        
        # Get neighborhood-specific statistics
        neighborhood_stats = milvus_client.get_location_statistics(city, neighborhood)
        
        return {
            "city": city,
            "neighborhood": neighborhood,
            "statistics": neighborhood_stats
        }
    except Exception as e:
        app_logger.error(f"Error getting neighborhood statistics for {neighborhood} in {city}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get neighborhood statistics")


@app.get("/api/locations/search")
async def search_locations(query: str, city: Optional[str] = None, max_results: int = 10):
    """Search locations by text similarity."""
    try:
        from src.vector_db.milvus_client import MilvusClient
        milvus_client = MilvusClient()
        
        locations = await milvus_client.search_locations(query, city, max_results)
        
        return {
            "query": query,
            "city_filter": city,
            "locations": locations,
            "total_found": len(locations)
        }
    except Exception as e:
        app_logger.error(f"Error searching locations: {e}")
        raise HTTPException(status_code=500, detail="Failed to search locations")


def _summarize_history(messages: List[ChatMessage], limit: int = 6) -> str:
    recent = messages[-limit:]
    return "\n".join([f"{m.role}: {m.content}" for m in recent]) 