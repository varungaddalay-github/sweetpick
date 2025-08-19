# Yelp API Neighborhood Integration

This document explains how to use the enhanced neighborhood functionality with Yelp API integration in the Sweet Morsels system.

## Overview

The system now includes direct Yelp API integration to provide better neighborhood-specific restaurant recommendations. This enhancement allows users to get more precise, location-aware restaurant suggestions by leveraging Yelp's comprehensive neighborhood data.

## Features

### 1. Direct Yelp API Integration
- **Direct API Access**: Uses the official Yelp Search API v3 for real-time data
- **Neighborhood-Specific Searches**: Precise searches within specific neighborhoods
- **Enhanced Data Quality**: Better restaurant information including neighborhoods, categories, and detailed metadata
- **Caching**: Intelligent caching to reduce API calls and improve performance

### 2. Enhanced Neighborhood Support
- **Precise Coordinates**: Uses neighborhood-specific coordinates for accurate searches
- **Multiple Search Methods**: Supports both location-based and coordinate-based searches
- **Fallback Mechanisms**: Graceful fallback to city-wide searches when neighborhood data is limited

### 3. Improved Data Processing
- **Neighborhood Extraction**: Automatically extracts neighborhood information from Yelp data
- **Cuisine Detection**: Intelligent cuisine type detection from Yelp categories
- **Quality Scoring**: Enhanced quality scoring based on Yelp ratings and review counts

## Setup

### 1. Get Yelp API Key
1. Visit [Yelp Developers](https://www.yelp.com/developers)
2. Create a new app
3. Get your API key from the app dashboard

### 2. Configure Environment
Add your Yelp API key to your `.env` file:

```bash
# Yelp API Configuration (Optional - for enhanced neighborhood data)
YELP_API_KEY=your_yelp_api_key_here
```

### 3. Install Dependencies
The system uses `aiohttp` for async HTTP requests. Ensure it's installed:

```bash
pip install aiohttp
```

## Usage

### Basic Neighborhood Search

```python
from src.data_collection.yelp_collector import YelpCollector

# Initialize collector
yelp_collector = YelpCollector()

# Search for restaurants in a specific neighborhood
results = await yelp_collector.search_by_neighborhood(
    city="Manhattan",
    neighborhood="Times Square",
    cuisine="Italian",
    max_results=10
)
```

### Enhanced SerpAPI Integration

The system automatically enhances SerpAPI searches with Yelp data when neighborhood context is detected:

```python
from src.data_collection.serpapi_collector import SerpAPICollector

# Initialize collector
serpapi_collector = SerpAPICollector()

# Search with neighborhood context (automatically uses Yelp API)
results = await serpapi_collector.search_restaurants(
    city="Manhattan",
    cuisine="Italian",
    location="Manhattan in Times Square",  # Triggers Yelp enhancement
    max_results=20
)
```

### Retrieval Engine Integration

The retrieval engine automatically uses Yelp API for neighborhood-specific queries:

```python
from src.query_processing.retrieval_engine import RetrievalEngine

# Initialize retrieval engine
retrieval_engine = RetrievalEngine(milvus_client)

# Query with neighborhood context
query = {
    "intent": "location_cuisine",
    "location": "Manhattan in Times Square",
    "cuisine_type": "Italian",
    "confidence": 0.9
}

# Get recommendations (automatically uses Yelp API for neighborhood data)
recommendations, use_fallback, error = await retrieval_engine.get_recommendations(query)
```

## Supported Neighborhoods

### Manhattan
- Times Square
- Hell's Kitchen
- Chelsea
- Greenwich Village
- East Village
- Lower East Side
- Upper East Side
- Upper West Side
- Midtown
- Financial District
- Tribeca
- SoHo
- NoHo
- Harlem
- Washington Heights
- Inwood
- Morningside Heights
- Yorkville
- Chinatown
- Little Italy
- Nolita
- Meatpacking District

### Jersey City
- Downtown
- Journal Square
- Grove Street
- Exchange Place
- Newport
- Harborside
- Paulus Hook
- Van Vorst Park
- Hamilton Park
- Bergen-Lafayette
- Greenville
- West Side
- The Heights
- McGinley Square
- Five Corners

### Hoboken
- Downtown
- Uptown
- Midtown
- Waterfront
- Washington Street
- Sinatra Drive
- Hudson Street
- Willow Avenue
- Garden Street
- Monroe Street
- Madison Street
- Bloomfield Street

## API Methods

### YelpCollector Class

#### `search_restaurants(city, cuisine, max_results=50, neighborhood=None)`
Search for restaurants in a city with optional neighborhood filtering.

**Parameters:**
- `city` (str): City name
- `cuisine` (str): Cuisine type
- `max_results` (int): Maximum number of results (default: 50)
- `neighborhood` (str, optional): Specific neighborhood

**Returns:**
- `List[Dict]`: List of restaurant dictionaries

#### `search_by_neighborhood(city, neighborhood, cuisine=None, max_results=30)`
Search for restaurants in a specific neighborhood using coordinates.

**Parameters:**
- `city` (str): City name
- `neighborhood` (str): Neighborhood name
- `cuisine` (str, optional): Cuisine type filter
- `max_results` (int): Maximum number of results (default: 30)

**Returns:**
- `List[Dict]`: List of restaurant dictionaries

#### `get_restaurant_details(restaurant_id)`
Get detailed information for a specific restaurant.

**Parameters:**
- `restaurant_id` (str): Yelp restaurant ID

**Returns:**
- `Dict`: Detailed restaurant information

## Data Schema

Restaurants returned by the Yelp API integration include the following fields:

```python
{
    "restaurant_id": "yelp_restaurant_id",
    "restaurant_name": "Restaurant Name",
    "full_address": "123 Main St, City, State ZIP",
    "city": "City Name",
    "neighborhood": "Neighborhood Name",  # Enhanced neighborhood data
    "latitude": 40.7580,
    "longitude": -73.9855,
    "cuisine_type": "italian",
    "rating": 4.5,
    "review_count": 1234,
    "price_range": 2,
    "quality_score": 15.67,
    "source": "yelp",
    "yelp_url": "https://www.yelp.com/biz/...",
    "phone": "+1-555-123-4567",
    "categories": ["Italian", "Pizza", "Restaurants"],
    "is_closed": False,
    "distance": 500,  # Distance in meters
    "transactions": ["delivery", "pickup"]
}
```

## Error Handling

The system includes comprehensive error handling:

- **API Key Missing**: Graceful fallback to SerpAPI-only mode
- **API Rate Limits**: Automatic retry with exponential backoff
- **Network Errors**: Retry logic with configurable attempts
- **Invalid Neighborhoods**: Fallback to city-wide searches
- **Data Processing Errors**: Logging and graceful degradation

## Performance Considerations

### Caching
- **Restaurant Searches**: Cached for 1 hour
- **Restaurant Details**: Cached for 2 hours
- **Neighborhood Coordinates**: Static data, no caching needed

### Rate Limiting
- **Yelp API**: 5000 requests per day (free tier)
- **Automatic Throttling**: Built-in rate limiting and monitoring
- **Cost Management**: API call tracking and alerts

### Optimization
- **Async Operations**: All API calls are asynchronous
- **Batch Processing**: Efficient handling of multiple requests
- **Connection Pooling**: Reuses HTTP connections

## Testing

### Run Yelp API Tests
```bash
python test_yelp_neighborhood.py
```

### Run Integration Tests
```bash
python test_neighborhood_integration.py
```

### Test Specific Functionality
```bash
# Test neighborhood search only
python -c "
import asyncio
from src.data_collection.yelp_collector import YelpCollector

async def test():
    yelp = YelpCollector()
    results = await yelp.search_by_neighborhood('Manhattan', 'Times Square', 'Italian', 5)
    print(f'Found {len(results)} restaurants')

asyncio.run(test())
"
```

## Troubleshooting

### Common Issues

1. **No Yelp API Key**
   - Error: "Yelp API key not configured"
   - Solution: Add `YELP_API_KEY` to your `.env` file

2. **Rate Limit Exceeded**
   - Error: "Yelp API error: 429"
   - Solution: Wait for rate limit reset or upgrade API plan

3. **Invalid Neighborhood**
   - Error: "No coordinates found for neighborhood"
   - Solution: Check supported neighborhoods list

4. **Network Issues**
   - Error: "Error making Yelp API call"
   - Solution: Check internet connection and firewall settings

### Debug Mode

Enable debug logging to see detailed API interactions:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Future Enhancements

- **More Neighborhoods**: Expand to additional cities and neighborhoods
- **Real-time Data**: WebSocket integration for live updates
- **Advanced Filtering**: Price range, dietary restrictions, etc.
- **Machine Learning**: Predictive neighborhood recommendations
- **Multi-language Support**: International neighborhood data

## Support

For issues or questions about the Yelp API integration:

1. Check the troubleshooting section above
2. Review the test scripts for examples
3. Check the logs for detailed error information
4. Verify your API key and permissions
