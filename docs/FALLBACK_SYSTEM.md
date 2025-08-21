# Fallback System Documentation

## Overview

The Sweet Morsels system now includes an intelligent fallback mechanism that handles queries outside the supported scope (locations and cuisines) by leveraging OpenAI's GPT-4o to generate relevant restaurant recommendations.

## Supported vs. Unsupported Scope

### Supported Locations
- Manhattan (and neighborhoods)
- Jersey City (and neighborhoods)  
- Hoboken (and neighborhoods)

### Supported Cuisines
- Italian
- Indian
- Chinese
- American
- Mexican

### Unsupported Examples
- **Locations**: Chicago, Los Angeles, Miami, etc.
- **Cuisines**: Thai, Japanese, Korean, French, Vietnamese, etc.

## Fallback System Architecture

### 1. Query Parsing & Location Resolution
```python
# Location resolution preserves original location for scope validation
if location_info.location_type == "unsupported":
    parsed_query["location_status"] = "unsupported"
    parsed_query["original_location"] = original_location
    parsed_query["location"] = original_location  # ✅ Preserved for validation
```

### 2. Scope Validation
```python
# Check location and cuisine support
if unsupported_location or unsupported_cuisine:
    suggestion = await suggest_alternatives_with_choice(
        original_query, 
        unsupported_location, 
        unsupported_cuisine
    )
    return False, suggestion  # Trigger fallback
```

### 3. OpenAI Fallback Generation
The system calls `suggest_alternatives_with_choice()` which:
- Generates location-specific recommendations for unsupported locations
- Generates cuisine-specific recommendations for unsupported cuisines
- Combines both for queries with multiple unsupported elements

## Fallback Scenarios

### Scenario 1: Unsupported Location Only
**Query**: "I am in Chicago and mood to eat indian"
**Flow**:
1. Parser extracts: `location: 'Chicago'`, `cuisine_type: 'Indian'`
2. Location resolution: Chicago marked as unsupported
3. Scope validation: `unsupported_location = 'Chicago'`
4. Fallback triggered: OpenAI generates Chicago Indian restaurant recommendations

**Response**:
```json
{
  "fallback_used": true,
  "query_type": "out_of_scope_with_choice",
  "fallback_reason": "Out of scope query - using web search",
  "recommendations": [
    {
      "restaurant_name": "The Spice Room",
      "location": "Chicago",
      "reason": "Known for authentic Indian flavors..."
    }
  ]
}
```

### Scenario 2: Unsupported Cuisine Only
**Query**: "Thai food in Manhattan"
**Flow**:
1. Parser extracts: `location: 'Manhattan'`, `cuisine_type: 'Thai'`
2. Location resolution: Manhattan marked as supported
3. Scope validation: `unsupported_cuisine = 'Thai'`
4. Fallback triggered: OpenAI generates Manhattan Thai restaurant recommendations

### Scenario 3: Both Unsupported
**Query**: "Japanese sushi in Chicago"
**Flow**:
1. Parser extracts: `location: 'Chicago'`, `cuisine_type: 'Japanese'`
2. Location resolution: Chicago marked as unsupported
3. Scope validation: Both location and cuisine unsupported
4. Fallback triggered: OpenAI generates comprehensive Chicago Japanese recommendations

## Implementation Details

### Query Parser Enhancements
- **Regex Parser**: Extended to recognize 20+ cuisines (Thai, Korean, Japanese, French, etc.)
- **Location Resolution**: Preserves original location data for scope validation
- **Validation**: No longer sets unsupported cuisines to `None`

### Scope Validation Logic
```python
# Handle scope issues
if unsupported_location or unsupported_cuisine:
    app_logger.info(f"🚫 Scope validation failed - calling suggest_alternatives_with_choice")
    suggestion = await suggest_alternatives_with_choice(
        original_query, 
        unsupported_location, 
        unsupported_cuisine
    )
    return False, suggestion
```

### OpenAI Fallback Prompts
The system uses specialized prompts for different fallback scenarios:
- **Location Fallback**: Focuses on generating recommendations for specific cities
- **Cuisine Fallback**: Focuses on generating recommendations for specific cuisines
- **Combined Fallback**: Handles both unsupported location and cuisine

## Response Format

### Fallback Response Structure
```json
{
  "query": "Thai food in Manhattan",
  "query_type": "out_of_scope_with_choice",
  "recommendations": [
    {
      "restaurant_name": "Pure Thai Cookhouse",
      "dish_name": null,
      "reason": "Known for its vibrant flavors...",
      "location": "Manhattan",
      "type": "web_search",
      "confidence": 0.5,
      "rating": 0
    }
  ],
  "natural_response": "For authentic Thai food in Manhattan...",
  "fallback_used": true,
  "fallback_reason": "Out of scope query - using web search",
  "processing_time": 3.81,
  "confidence_score": 0.0,
  "web_search_available": true
}
```

## Benefits

### 1. Expanded Coverage
- Users can query any location or cuisine
- System gracefully handles unsupported scope
- No more "location not supported" errors

### 2. Intelligent Fallbacks
- Location-specific recommendations
- Cuisine-specific recommendations
- Natural language responses

### 3. Seamless User Experience
- Same API endpoint for all queries
- Consistent response format
- Automatic fallback detection

## Configuration

### Supported Locations
```python
# src/utils/config.py
supported_cities: List[str] = ["Manhattan", "Jersey City", "Hoboken"]
```

### Supported Cuisines
```python
# src/query_processing/query_parser.py
cuisines = {
    "italian": "Italian", "indian": "Indian", "chinese": "Chinese",
    "american": "American", "mexican": "Mexican",
    "thai": "Thai", "japanese": "Japanese", "korean": "Korean",
    # ... additional cuisines
}
```

## Monitoring & Debugging

### Logging
The system provides comprehensive logging for fallback scenarios:
```
🚫 Scope validation failed - calling suggest_alternatives_with_choice
🌍 Processing unsupported location: Chicago
🍽️ Processing unsupported cuisine: Thai
✅ OpenAI response: [fallback content]
```

### Metrics
- Fallback usage statistics
- OpenAI API response times
- User satisfaction with fallback responses

## Future Enhancements

### 1. Enhanced Cuisine Coverage
- Add more global cuisines (Mediterranean, Middle Eastern, African)
- Improve cuisine detection accuracy
- Support for fusion cuisines

### 2. Location Expansion
- Add support for more cities and regions
- Neighborhood-level fallback handling
- Geographic clustering for better recommendations

### 3. Fallback Quality
- Response validation and quality scoring
- User feedback integration
- A/B testing for fallback prompts

### 4. Hybrid Recommendations
- Combine Milvus data with OpenAI fallback
- Cross-reference multiple sources
- Intelligent ranking of fallback results

## Troubleshooting

### Common Issues

1. **Fallback Not Triggering**
   - Check if location/cuisine is being parsed correctly
   - Verify scope validation logic
   - Ensure OpenAI API is accessible

2. **Poor Fallback Quality**
   - Review OpenAI prompts
   - Check response parsing
   - Validate fallback response structure

3. **Performance Issues**
   - Monitor OpenAI API response times
   - Check caching configuration
   - Optimize fallback prompts

### Debug Commands
```bash
# Test location fallback
curl -X POST "http://localhost:8000/query" \
  -H "Content-Type: application/json" \
  -d '{"query": "Italian food in Chicago"}'

# Test cuisine fallback  
curl -X POST "http://localhost:8000/query" \
  -H "Content-Type: application/json" \
  -d '{"query": "Thai food in Manhattan"}'

# Test both unsupported
curl -X POST "http://localhost:8000/query" \
  -H "Content-Type: application/json" \
  -d '{"query": "Japanese sushi in Chicago"}'
```

## Conclusion

The fallback system significantly expands Sweet Morsels' capabilities by providing intelligent, location-aware restaurant recommendations for queries outside the supported scope. This ensures users always receive relevant, helpful responses regardless of their location or cuisine preferences.
