"""
Optimized FastAPI application for the SweetPick restaurant recommendation system.
Integrates parallel data collection, optimized vector operations, and comprehensive monitoring.
"""
import asyncio
import json
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Dict, Optional, Any, Tuple
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from openai import AsyncOpenAI
from src.utils.config import get_settings
from src.utils.logger import app_logger
from src.data_collection.parallel_collector import ParallelDataCollector, ProcessingConfig
from src.vector_db.optimized_milvus_client import OptimizedMilvusClient, ConnectionConfig, BulkConfig
from src.monitoring.metrics_collector import monitoring
from src.query_processing.query_parser import QueryParser
from src.query_processing.retrieval_engine import RetrievalEngine
from src.fallback.fallback_handler import FallbackHandler
from src.processing.response_generator import ResponseGenerator
from src.security.abuse_protection import abuse_protection
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
import hashlib


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
    web_search_available: bool = False
    trace_id: Optional[str] = None

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    version: str
    services: Dict[str, str]
    monitoring: Dict[str, Any]

class StatsResponse(BaseModel):
    total_restaurants: int
    total_dishes: int
    total_queries: int
    cache_hit_rate: float
    average_response_time: float
    api_costs: Dict[str, float]
    monitoring_stats: Dict[str, Any]

class MonitoringResponse(BaseModel):
    metrics: Dict[str, Any]
    active_alerts: List[Dict[str, Any]]
    recent_logs: List[Dict[str, Any]]
    active_spans: Dict[str, Any]
    statistics: Dict[str, Any]
    uptime: float


# Global instances
settings = get_settings()
optimized_milvus_client = None
query_parser = None
retrieval_engine = None
fallback_handler = None
response_generator = None
parallel_collector = None

# Web search cache
web_search_cache: Dict[str, Dict[str, Any]] = {}
WEB_SEARCH_TTL_SECONDS = 6 * 3600

# Chat sessions
chat_sessions: Dict[str, List[Dict]] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for optimized FastAPI app."""
    global optimized_milvus_client, query_parser, retrieval_engine, fallback_handler, response_generator, parallel_collector
    
    # Startup
    try:
        app_logger.info("🚀 Starting optimized SweetPick API...")
        
        # Initialize optimized Milvus client
        connection_config = ConnectionConfig(
            max_connections=20,
            min_connections=5,
            connection_timeout=30,
            idle_timeout=300
        )
        bulk_config = BulkConfig(
            batch_size=1000,
            max_concurrent_batches=5,
            embedding_batch_size=50,
            insert_timeout=60
        )
        
        optimized_milvus_client = OptimizedMilvusClient(connection_config, bulk_config)
        await optimized_milvus_client.initialize()
        
        # Initialize query processing components
        query_parser = QueryParser()
        retrieval_engine = RetrievalEngine(optimized_milvus_client)
        fallback_handler = FallbackHandler(retrieval_engine, query_parser)
        response_generator = ResponseGenerator()
        
        # Initialize parallel data collector
        processing_config = ProcessingConfig(
            max_concurrent_restaurants=10,
            max_concurrent_reviews=20,
            max_concurrent_sentiment=30,
            batch_size=50
        )
        parallel_collector = ParallelDataCollector(processing_config)
        
        # Start monitoring loop
        asyncio.create_task(monitoring.start_monitoring_loop())
        
        app_logger.info("✅ Optimized API startup completed successfully")
        
    except Exception as e:
        app_logger.error(f"❌ Error during startup: {e}")
        raise
    
    yield
    
    # Shutdown
    try:
        if optimized_milvus_client:
            await optimized_milvus_client.close()
        app_logger.info("✅ Optimized API shutdown completed")
    except Exception as e:
        app_logger.error(f"❌ Error during shutdown: {e}")


# Initialize FastAPI app
app = FastAPI(
    title="SweetPick Optimized API",
    description="Optimized restaurant dish recommendation system using RAG",
    version="2.0.0",
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

# Templates
templates = Jinja2Templates(directory="templates")

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")


def get_client_id(request: Request) -> str:
    """Extract client ID from request for abuse protection."""
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        client_ip = forwarded_for.split(",")[0].strip()
    else:
        client_ip = request.client.host
    
    user_agent = request.headers.get("User-Agent", "")
    client_hash = hashlib.md5(f"{client_ip}:{user_agent}".encode()).hexdigest()[:16]
    
    return client_hash


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Serve the SweetPick UI."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Enhanced health check with monitoring data."""
    services_status = {}
    
    try:
        # Check optimized Milvus connection
        if optimized_milvus_client:
            stats = optimized_milvus_client.get_statistics()
            services_status['milvus'] = 'healthy' if stats.get('bulk_inserts', 0) >= 0 else 'unhealthy'
        else:
            services_status['milvus'] = 'uninitialized'
        
        # Check other services
        services_status['query_parser'] = 'healthy' if query_parser else 'uninitialized'
        services_status['retrieval_engine'] = 'healthy' if retrieval_engine else 'uninitialized'
        services_status['fallback_handler'] = 'healthy' if fallback_handler else 'uninitialized'
        services_status['parallel_collector'] = 'healthy' if parallel_collector else 'uninitialized'
        
        # Get monitoring data
        monitoring_data = monitoring.get_monitoring_data()
        
    except Exception as e:
        app_logger.error(f"Health check error: {e}")
        services_status['error'] = str(e)
        monitoring_data = {}
    
    return HealthResponse(
        status="healthy" if all(status == 'healthy' for status in services_status.values() if status != 'uninitialized') else "degraded",
        timestamp=datetime.now().isoformat(),
        version="2.0.0",
        services=services_status,
        monitoring=monitoring_data
    )


@app.post("/query", response_model=QueryResponse)
async def process_query_optimized(request: QueryRequest, background_tasks: BackgroundTasks, http_request: Request):
    """Optimized query endpoint with comprehensive monitoring."""
    start_time = time.time()
    trace_id = str(uuid.uuid4())
    correlation_id = str(uuid.uuid4())
    
    async with monitoring.trace_operation("process_query", trace_id) as span:
        try:
            # Add span tags
            await monitoring.tracing.add_span_tag(span["span_id"], "query", request.query)
            await monitoring.tracing.add_span_tag(span["span_id"], "correlation_id", correlation_id)
            
            # Log structured message
            await monitoring.log_structured("info", f"Processing query: {request.query}", 
                                          correlation_id, {"query_type": "restaurant_recommendation"}, trace_id)
            
            # 🔒 ABUSE PROTECTION: Check request before processing
            client_id = get_client_id(http_request)
            is_allowed, security_error, security_report = await abuse_protection.check_request(
                client_id, request.query, request.dict()
            )
            
            if not is_allowed:
                await monitoring.log_structured("warning", f"Request blocked for {client_id}: {security_error}", 
                                              correlation_id, {"client_id": client_id}, trace_id)
                raise HTTPException(
                    status_code=429 if "rate limit" in security_error.lower() else 400,
                    detail=f"Request blocked: {security_error}"
                )
            
            # Parse the query with tracing
            async with monitoring.trace_operation("parse_query", trace_id, span["span_id"]) as parse_span:
                parsed_query = await query_parser.parse_query(request.query)
                await monitoring.tracing.add_span_tag(parse_span["span_id"], "parsed_intent", parsed_query.get("intent", "unknown"))
            
            # Validate query scope
            async with monitoring.trace_operation("validate_scope", trace_id, span["span_id"]) as validate_span:
                is_valid, choice_message = await validate_query_scope(parsed_query, request.query)
                await monitoring.tracing.add_span_tag(validate_span["span_id"], "is_valid", str(is_valid))
            
            if not is_valid:
                # Handle out-of-scope queries
                cards, natural = await handle_out_of_scope_query(request.query, parsed_query, choice_message)
                
                processing_time = time.time() - start_time
                
                # Record metrics
                await monitoring.record_query_metrics(
                    query_type="out_of_scope",
                    response_time=processing_time,
                    success=True,
                    result_count=len(cards)
                )
                
                return QueryResponse(
                    query=request.query,
                    query_type="out_of_scope_with_choice",
                    recommendations=cards,
                    natural_response=natural,
                    fallback_used=True,
                    fallback_reason="Out of scope query - using web search",
                    processing_time=processing_time,
                    confidence_score=0.0,
                    web_search_available=True,
                    trace_id=trace_id
                )
            
            # Get recommendations with tracing
            async with monitoring.trace_operation("get_recommendations", trace_id, span["span_id"]) as retrieval_span:
                query_type = query_parser.classify_query_type(parsed_query)
                recommendations, fallback_used, fallback_reason = await retrieval_engine.get_recommendations(
                    parsed_query, request.max_results or 10
                )
                
                await monitoring.tracing.add_span_tag(retrieval_span["span_id"], "query_type", query_type)
                await monitoring.tracing.add_span_tag(retrieval_span["span_id"], "result_count", str(len(recommendations)))
                await monitoring.tracing.add_span_tag(retrieval_span["span_id"], "fallback_used", str(fallback_used))
            
            # Apply fallback if needed
            if not recommendations and not fallback_used:
                async with monitoring.trace_operation("apply_fallback", trace_id, span["span_id"]) as fallback_span:
                    recommendations, fallback_used, fallback_reason = await fallback_handler.execute_fallback_strategy(
                        parsed_query, recommendations
                    )
                    await monitoring.tracing.add_span_tag(fallback_span["span_id"], "fallback_strategy", fallback_reason or "none")
            
            # Generate natural language response
            async with monitoring.trace_operation("generate_response", trace_id, span["span_id"]) as response_span:
                natural_response = ""
                if response_generator:
                    try:
                        query_metadata = {
                            'location': parsed_query.get('location'),
                            'cuisine_type': parsed_query.get('cuisine_type'),
                            'fallback_used': fallback_used,
                            'query_type': query_type
                        }
                        
                        natural_response = await response_generator.generate_conversational_response(
                            request.query, 
                            recommendations, 
                            query_metadata
                        )
                    except Exception as e:
                        await monitoring.log_structured("error", f"Error generating response: {e}", 
                                                      correlation_id, {}, trace_id)
                        natural_response = response_generator.generate_quick_response(recommendations, query_metadata) if response_generator else ""
            
            # Calculate processing time and confidence
            processing_time = time.time() - start_time
            confidence_score = retrieval_engine.calculate_confidence(recommendations, parsed_query)
            
            # Record comprehensive metrics
            await monitoring.record_query_metrics(
                query_type=query_type,
                response_time=processing_time,
                success=True,
                result_count=len(recommendations)
            )
            
            await monitoring.record_business_metrics(
                recommendations_generated=len(recommendations)
            )
            
            # Log success
            await monitoring.log_structured("info", f"Query processed successfully in {processing_time:.2f}s", 
                                          correlation_id, {
                                              "result_count": len(recommendations),
                                              "confidence_score": confidence_score,
                                              "fallback_used": fallback_used
                                          }, trace_id)
            
            return QueryResponse(
                query=request.query,
                query_type=query_type,
                recommendations=recommendations,
                natural_response=natural_response,
                fallback_used=fallback_used,
                fallback_reason=fallback_reason,
                processing_time=processing_time,
                confidence_score=confidence_score,
                trace_id=trace_id
            )
            
        except Exception as e:
            processing_time = time.time() - start_time
            
            # Record error metrics
            await monitoring.record_query_metrics(
                query_type="error",
                response_time=processing_time,
                success=False
            )
            
            # Log error
            await monitoring.log_structured("error", f"Error processing query: {e}", 
                                          correlation_id, {"error": str(e)}, trace_id)
            
            app_logger.error(f"Error processing query: {e}")
            raise HTTPException(status_code=500, detail=f"Error processing query: {str(e)}")


async def validate_query_scope(parsed_query: Dict[str, Any], original_query: str) -> Tuple[bool, Optional[str]]:
    """Validate query scope with monitoring."""
    async with monitoring.trace_operation("validate_query_scope") as span:
        try:
            # Check location
            location = parsed_query.get('location')
            if location and location not in settings.supported_cities:
                await monitoring.tracing.add_span_tag(span["span_id"], "unsupported_location", location)
                return False, f"Location '{location}' not supported. Try Jersey City or Hoboken."
            
            # Check cuisine
            cuisine = parsed_query.get('cuisine_type')
            if cuisine and cuisine not in settings.supported_cuisines:
                await monitoring.tracing.add_span_tag(span["span_id"], "unsupported_cuisine", cuisine)
                return False, f"Cuisine '{cuisine}' not supported. Try Italian, Indian, Chinese, American, or Mexican."
            
            await monitoring.tracing.add_span_tag(span["span_id"], "validation_result", "passed")
            return True, None
            
        except Exception as e:
            await monitoring.log_structured("error", f"Error in query validation: {e}")
            return False, "Validation error occurred"


async def handle_out_of_scope_query(query: str, parsed_query: Dict[str, Any], choice_message: str) -> Tuple[List[Dict], str]:
    """Handle out-of-scope queries with caching."""
    async with monitoring.trace_operation("handle_out_of_scope") as span:
        # Build cache key
        loc = parsed_query.get('location')
        cui = parsed_query.get('cuisine_type')
        cache_key = f"{query}|{loc}|{cui}"
        
        # Check cache
        cached = web_search_cache.get(cache_key)
        if cached and (datetime.now() - cached["timestamp"]).total_seconds() < WEB_SEARCH_TTL_SECONDS:
            await monitoring.tracing.add_span_tag(span["span_id"], "cache_hit", "true")
            return cached["cards"], cached["natural_response"]
        
        # Generate new response
        await monitoring.tracing.add_span_tag(span["span_id"], "cache_hit", "false")
        
        # Extract cards from choice message
        cards = extract_cards_from_text(choice_message or "", loc)
        natural = clean_natural_response(choice_message or "")
        
        # Cache result
        web_search_cache[cache_key] = {
            "cards": cards,
            "natural_response": natural,
            "timestamp": datetime.now()
        }
        
        return cards, natural


def extract_cards_from_text(text: str, location_hint: Optional[str]) -> List[Dict[str, Any]]:
    """Extract recommendation cards from text."""
    # Simple extraction logic - can be enhanced
    lines = [ln.strip("- ").strip() for ln in text.splitlines() if ln.strip()]
    cards = []
    
    for line in lines[:3]:  # Top 3 recommendations
        if ":" in line:
            parts = line.split(":", 1)
            name = parts[0].strip(" -*#")
            reason = parts[1].strip()
        else:
            name = line.strip(" -*#")
            reason = ""
        
        cards.append({
            "restaurant_name": name,
            "reason": reason,
            "location": location_hint,
            "type": "web_search",
            "confidence": 0.5,
            "rating": 0
        })
    
    return cards


def clean_natural_response(text: str) -> str:
    """Clean natural response by removing JSON blocks."""
    import re
    # Remove JSON objects/arrays
    cleaned = re.sub(r'```json\s*\n.*?\n```', '', text, flags=re.DOTALL)
    cleaned = re.sub(r'\{.*?\}', '', cleaned, flags=re.DOTALL)
    cleaned = re.sub(r'\[.*?\]', '', cleaned, flags=re.DOTALL)
    
    # Clean up extra whitespace and newlines
    cleaned = re.sub(r'\n\s*\n', '\n', cleaned)
    cleaned = cleaned.strip()
    
    return cleaned


@app.post("/collect-data/parallel")
async def collect_data_parallel(background_tasks: BackgroundTasks):
    """Trigger parallel data collection."""
    try:
        if not parallel_collector:
            raise HTTPException(status_code=503, detail="Parallel collector not initialized")
        
        # Start collection in background
        background_tasks.add_task(run_parallel_data_collection)
        
        await monitoring.log_structured("info", "Parallel data collection started")
        
        return {
            "message": "Parallel data collection started in background",
            "status": "started"
        }
        
    except Exception as e:
        await monitoring.log_structured("error", f"Error starting parallel data collection: {e}")
        raise HTTPException(status_code=500, detail=f"Error starting data collection: {str(e)}")


async def run_parallel_data_collection():
    """Run parallel data collection for all supported cities and cuisines."""
    async with monitoring.trace_operation("parallel_data_collection") as span:
        try:
            await monitoring.log_structured("info", "Starting parallel data collection pipeline")
            
            total_restaurants = 0
            total_dishes = 0
            
            for city in settings.supported_cities:
                for cuisine in settings.supported_cuisines:
                    try:
                        await monitoring.tracing.add_span_tag(span["span_id"], f"collecting_{city}_{cuisine}", "started")
                        
                        result = await parallel_collector.collect_data_parallel(city, cuisine)
                        
                        if result["success"]:
                            total_restaurants += result["restaurants_processed"]
                            total_dishes += result["dishes_extracted"]
                            
                            await monitoring.log_structured("info", 
                                f"Collected data for {city}, {cuisine}: {result['restaurants_processed']} restaurants, {result['dishes_extracted']} dishes",
                                extra_data=result
                            )
                        else:
                            await monitoring.log_structured("warning", 
                                f"Failed to collect data for {city}, {cuisine}: {result.get('error', 'Unknown error')}"
                            )
                    
                    except Exception as e:
                        await monitoring.log_structured("error", 
                            f"Error collecting data for {city}, {cuisine}: {e}"
                        )
            
            # Record business metrics
            await monitoring.record_business_metrics(
                recommendations_generated=total_dishes
            )
            
            await monitoring.log_structured("info", 
                f"Parallel data collection completed: {total_restaurants} restaurants, {total_dishes} dishes"
            )
            
        except Exception as e:
            await monitoring.log_structured("error", f"Error in parallel data collection: {e}")


@app.get("/monitoring", response_model=MonitoringResponse)
async def get_monitoring_data():
    """Get comprehensive monitoring data."""
    try:
        monitoring_data = monitoring.get_monitoring_data()
        
        # Add system metrics
        import psutil
        monitoring_data["system_metrics"] = {
            "memory_usage": psutil.virtual_memory().percent / 100.0,
            "cpu_usage": psutil.cpu_percent() / 100.0,
            "disk_usage": psutil.disk_usage('/').percent / 100.0
        }
        
        # Record system metrics
        await monitoring.record_system_metrics(
            memory_usage=monitoring_data["system_metrics"]["memory_usage"],
            cpu_usage=monitoring_data["system_metrics"]["cpu_usage"],
            active_connections=len(monitoring.tracing.get_active_spans())
        )
        
        return MonitoringResponse(**monitoring_data)
        
    except Exception as e:
        app_logger.error(f"Error getting monitoring data: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving monitoring data: {str(e)}")


@app.get("/stats", response_model=StatsResponse)
async def get_statistics():
    """Get enhanced system statistics with monitoring."""
    try:
        # Get collection statistics
        if optimized_milvus_client:
            collection_stats = await optimized_milvus_client.get_collection_stats()
            total_restaurants = collection_stats.get('restaurants', {}).get('num_entities', 0)
            total_dishes = collection_stats.get('dishes', {}).get('num_entities', 0)
        else:
            total_restaurants = 0
            total_dishes = 0
        
        # Get monitoring data
        monitoring_data = monitoring.get_monitoring_data()
        
        # Calculate metrics
        query_metrics = monitoring_data["metrics"]["metrics"].get("query_response_time", {})
        avg_response_time = query_metrics.get("avg", 0.0) if query_metrics else 0.0
        
        cache_metrics = monitoring_data["metrics"]["gauges"].get("cache_hit_rate", 0.0)
        cache_hit_rate = cache_metrics if isinstance(cache_metrics, float) else 0.0
        
        # Get total queries
        total_queries = sum(
            monitoring_data["metrics"]["counters"].get(key, 0) 
            for key in monitoring_data["metrics"]["counters"] 
            if "query_total" in key
        )
        
        return StatsResponse(
            total_restaurants=total_restaurants,
            total_dishes=total_dishes,
            total_queries=total_queries,
            cache_hit_rate=cache_hit_rate,
            average_response_time=avg_response_time,
            api_costs={},  # TODO: Implement cost tracking
            monitoring_stats=monitoring_data
        )
        
    except Exception as e:
        app_logger.error(f"Error getting statistics: {e}")
        raise HTTPException(status_code=500, detail=f"Error retrieving statistics: {str(e)}")


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
        await monitoring.log_structured("error", f"Error getting neighborhoods for {city}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get neighborhoods for {city}")


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "SweetPick Optimized Restaurant Recommendation API",
        "version": "2.0.0",
        "features": [
            "Parallel data collection",
            "Optimized vector operations", 
            "Comprehensive monitoring",
            "Enhanced performance"
        ],
        "endpoints": {
            "POST /query": "Get restaurant/dish recommendations (optimized)",
            "POST /collect-data/parallel": "Trigger parallel data collection",
            "GET /monitoring": "Get comprehensive monitoring data",
            "GET /health": "Enhanced health check with monitoring",
            "GET /stats": "Enhanced system statistics"
        },
        "documentation": "/docs"
    }
