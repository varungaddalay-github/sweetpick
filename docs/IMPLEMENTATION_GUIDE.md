# Sweet Morsels - Phase 1 Implementation Guide

## Overview

This document provides a comprehensive guide to the Phase 1 implementation of the Sweet Morsels RAG (Retrieval-Augmented Generation) application for restaurant dish recommendations.

## Architecture Overview

The application follows a modular architecture with the following key components:

```
sweet_morsels/
├── src/
│   ├── data_collection/     # SerpAPI integration and data collection
│   ├── processing/          # AI processing pipeline
│   ├── vector_db/          # Milvus integration
│   ├── query_processing/   # Query parsing and retrieval
│   ├── fallback/           # Fallback strategies
│   ├── api/                # FastAPI backend
│   └── utils/              # Shared utilities
├── tests/                  # Test suite
├── config/                 # Configuration files
└── docs/                   # Documentation
```

## Key Components

### 1. Data Collection Pipeline

**Files:**
- `src/data_collection/serpapi_collector.py`
- `src/data_collection/cache_manager.py`
- `src/data_collection/data_validator.py`

**Features:**
- Restaurant discovery using SerpAPI
- Review collection with rate limiting
- Redis caching for API responses
- Data validation and quality filtering
- Cost tracking and budget management

**Usage:**
```python
from src.data_collection.serpapi_collector import SerpAPICollector

collector = SerpAPICollector()
restaurants = await collector.search_restaurants("Jersey City", "Indian")
reviews = await collector.get_restaurant_reviews(place_id, max_reviews=40)
```

### 2. AI Processing Pipeline

**Files:**
- `src/processing/text_processor.py`
- `src/processing/dish_extractor.py`
- `src/processing/sentiment_analyzer.py`

**Features:**
- Text cleaning and normalization
- Dish extraction using GPT-4o-mini
- Sentiment analysis for dishes
- Batch processing for cost optimization
- Fallback mechanisms for API failures

**Usage:**
```python
from src.processing.dish_extractor import DishExtractor
from src.processing.sentiment_analyzer import SentimentAnalyzer

extractor = DishExtractor()
dishes = await extractor.extract_dishes_from_reviews(reviews)

analyzer = SentimentAnalyzer()
sentiment = await analyzer.analyze_dish_sentiment("Chicken Biryani", reviews)
```

### 3. Vector Database Integration

**Files:**
- `src/vector_db/milvus_client.py`

**Features:**
- Milvus Cloud integration
- Three collections: restaurants, dishes, locations
- Vector similarity search
- Filtering and ranking
- Automatic indexing

**Collections Schema:**

**restaurants_enhanced:**
- restaurant_id (primary key)
- restaurant_name, city, cuisine_type
- rating, review_count, price_range
- vector_embedding (1536 dimensions)
- fallback_tier (1-4)

**dishes_detailed:**
- dish_id (primary key)
- restaurant_id, dish_name, normalized_dish_name
- sentiment_score, recommendation_score
- vector_embedding (1536 dimensions)
- sample_contexts, dietary_tags

**locations_metadata:**
- location_id (primary key)
- city, neighborhood, restaurant_count
- cuisine_distribution, price_distribution
- vector_embedding (1536 dimensions)

### 4. Query Processing System

**Files:**
- `src/query_processing/query_parser.py`
- `src/query_processing/retrieval_engine.py`

**Features:**
- Natural language query parsing
- Entity extraction (location, cuisine, dish, restaurant)
- Query type classification
- Vector similarity search
- Multi-modal retrieval strategies

**Supported Query Types:**
1. Restaurant-specific: "I am at Southern Spice, what should I order?"
2. Location + Cuisine: "I am in Jersey City and in mood to eat Indian cuisine"
3. Location + Dish: "I am in Jersey City and in mood to eat Chicken Biryani"
4. Location General: "I am in Jersey City and very hungry"
5. Meal Type: "I am in Hoboken and wanted to find place for Brunch"

### 5. Fallback System

**Files:**
- `src/fallback/fallback_handler.py`

**Features:**
- Tiered fallback strategies
- Geographic expansion
- Cuisine type relaxation
- Criteria relaxation (rating, reviews)
- Generic recommendations

**Fallback Tiers:**
- Tier 1: 500+ reviews, 4.2+ rating (Premium)
- Tier 2: 250+ reviews, 4.0+ rating (Good)
- Tier 3: 100+ reviews, 3.8+ rating (Acceptable)

### 6. FastAPI Backend

**Files:**
- `src/api/main.py`

**Endpoints:**
- `POST /query` - Main recommendation endpoint
- `GET /restaurant/{id}` - Restaurant details
- `GET /dish/{id}` - Dish details
- `GET /health` - Health check
- `GET /stats` - System statistics
- `POST /collect-data` - Trigger data collection

## Configuration

### Environment Variables

Copy `config.env.example` to `.env` and configure:

```bash
# API Keys
OPENAI_API_KEY=your_openai_api_key
SERPAPI_KEY=your_serpapi_key
MILVUS_URI=your_milvus_uri
MILVUS_TOKEN=your_milvus_token

# Database
DATABASE_URL=postgresql://user:password@localhost/sweet_morsels

# Redis
REDIS_URL=redis://localhost:6379

# Application Settings
ENVIRONMENT=development
LOG_LEVEL=INFO
```

### Cost Optimization Settings

```bash
# Processing
BATCH_SIZE=8                    # OpenAI API calls per batch
MAX_RETRIES=3                   # Retry attempts
MONTHLY_BUDGET=90.0            # Monthly budget limit
COST_ALERT_THRESHOLD=0.8       # Alert at 80% of budget

# Data Collection
MAX_RESTAURANTS_PER_CITY=50    # Limit restaurants per city
MAX_REVIEWS_PER_RESTAURANT=40  # Limit reviews per restaurant
MIN_RATING=4.0                 # Minimum restaurant rating
MIN_REVIEWS=400                # Minimum review count
```

## Usage Examples

### 1. Starting the Application

```bash
# Ensure Python 3.12.2+ is installed
python --version

# Install dependencies
pip install -r requirements.txt

# Set up environment
cp config.env.example .env
# Edit .env with your API keys

# Run the application
python run.py
# or
uvicorn src.api.main:app --reload
```

### 2. Making API Calls

```python
import requests

# Restaurant-specific query
response = requests.post("http://localhost:8000/query", json={
    "query": "I am at Southern Spice, Jersey City. What should I order?",
    "max_results": 5
})

# Location + cuisine query
response = requests.post("http://localhost:8000/query", json={
    "query": "I am in Jersey City and in mood to eat Indian cuisine",
    "max_results": 10
})

# Location + dish query
response = requests.post("http://localhost:8000/query", json={
    "query": "I am in Jersey City and in mood to eat Chicken Biryani",
    "max_results": 5
})
```

### 3. Data Collection

```python
# Trigger data collection
response = requests.post("http://localhost:8000/collect-data")

# Check collection status
response = requests.get("http://localhost:8000/stats")
```

### 4. Health Monitoring

```python
# Health check
response = requests.get("http://localhost:8000/health")

# System statistics
response = requests.get("http://localhost:8000/stats")
```

## Testing

### Running Tests

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/test_query_parser.py

# Run with coverage
pytest --cov=src tests/
```

### Test Structure

```
tests/
├── test_query_parser.py      # Query parsing tests
├── test_data_validator.py    # Data validation tests
├── test_cache_manager.py     # Cache management tests
└── test_api_endpoints.py     # API endpoint tests
```

## Performance Optimization

### 1. Caching Strategy

- **Redis Caching**: All API responses cached with TTL
- **Embedding Caching**: Vector embeddings cached to avoid regeneration
- **Query Result Caching**: Similar queries cached for faster responses

### 2. Batch Processing

- **OpenAI API**: Process 8 reviews per batch
- **SerpAPI**: Rate limiting with exponential backoff
- **Vector Operations**: Batch insertions to Milvus

### 3. Cost Management

- **API Call Tracking**: Monitor usage across all services
- **Budget Alerts**: Notifications at 80% of monthly budget
- **Fallback Strategies**: Reduce API calls when possible

## Monitoring and Logging

### Logging Configuration

```python
from src.utils.logger import app_logger

app_logger.info("Application started")
app_logger.error("Error occurred", exc_info=True)
app_logger.debug("Debug information")
```

### Metrics Tracking

- API response times
- Cache hit rates
- Error rates
- Cost tracking
- Query success rates

## Deployment Considerations

### 1. Environment Setup

- **Redis**: Required for caching
- **PostgreSQL**: Optional for additional data storage
- **Milvus Cloud**: Vector database service

### 2. Scaling

- **Horizontal Scaling**: Multiple FastAPI instances
- **Load Balancing**: Nginx or similar
- **Database Scaling**: Milvus Cloud handles vector scaling

### 3. Security

- **API Key Management**: Secure environment variables
- **Rate Limiting**: Implement at API gateway level
- **Input Validation**: Pydantic models for request validation

## Troubleshooting

### Common Issues

1. **Milvus Connection Errors**
   - Check MILVUS_URI and MILVUS_TOKEN
   - Verify network connectivity
   - Check collection existence

2. **OpenAI API Errors**
   - Verify OPENAI_API_KEY
   - Check rate limits
   - Monitor API usage

3. **SerpAPI Errors**
   - Verify SERPAPI_KEY
   - Check quota limits
   - Review rate limiting

4. **Redis Connection Issues**
   - Check REDIS_URL
   - Verify Redis server is running
   - Check network connectivity

### Debug Mode

```bash
# Enable debug logging
LOG_LEVEL=DEBUG

# Run with debug output
python run.py --debug
```

## Future Enhancements

### Phase 2 Considerations

1. **User Preferences**: Personalized recommendations
2. **Real-time Updates**: Live data collection
3. **Mobile App**: Native mobile application
4. **Advanced Analytics**: User behavior analysis
5. **Multi-language Support**: International expansion

### Performance Improvements

1. **Model Optimization**: Fine-tuned models for specific tasks
2. **Caching Enhancement**: Multi-level caching strategy
3. **Database Optimization**: Query optimization and indexing
4. **API Optimization**: GraphQL for flexible queries

## Support and Documentation

- **API Documentation**: Available at `/docs` when running
- **Code Documentation**: Inline docstrings and type hints
- **Issue Tracking**: GitHub issues for bug reports
- **Contributing**: Follow PEP 8 and add tests for new features

## License

This project is licensed under the MIT License. See LICENSE file for details. 