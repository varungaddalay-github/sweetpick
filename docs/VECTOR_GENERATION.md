# Vector Generation and Storage in SweetPick RAG System

## Overview

The SweetPick RAG system uses OpenAI's `text-embedding-ada-002` model to generate vector embeddings for semantic search. This document explains how vectors are generated, stored, and used throughout the system.

## 🔄 Complete Vector Pipeline

### **1. Data Collection Phase (Fixed)**

#### **Embedding Text Generation**
For each restaurant and dish, the system creates semantic text representations:

**Restaurant Embedding Text:**
```python
def _create_restaurant_embedding_text(self, restaurant: Dict) -> str:
    parts = [
        restaurant.get('restaurant_name', ''),      # "Porta Pizza"
        restaurant.get('cuisine_type', ''),         # "Italian"
        restaurant.get('city', ''),                 # "Jersey City"
        ' '.join(restaurant.get('meal_types', [])), # "lunch dinner"
        ' '.join(restaurant.get('sub_cuisines', [])) # "pizza pasta"
    ]
    return ' '.join(filter(None, parts))
    # Result: "Porta Pizza Italian Jersey City lunch dinner pizza pasta"
```

**Dish Embedding Text:**
```python
def _create_dish_embedding_text(self, dish: Dict) -> str:
    parts = [
        dish.get('dish_name', ''),                  # "Margherita Pizza"
        dish.get('normalized_dish_name', ''),       # "Margherita Pizza"
        dish.get('dish_category', ''),              # "main"
        dish.get('cuisine_context', ''),            # "Italian"
        ' '.join(dish.get('dietary_tags', []))      # "vegetarian"
    ]
    return ' '.join(filter(None, parts))
    # Result: "Margherita Pizza Margherita Pizza main Italian vegetarian"
```

#### **Vector Generation (Now Fixed)**
```python
async def _generate_embedding(self, text: str) -> List[float]:
    """Generate embedding for text using OpenAI."""
    
    # Add caching for common queries
    if text in self._embedding_cache:
        return self._embedding_cache[text]

    try:
        response = await self.openai_client.embeddings.create(
            model=self.settings.embedding_model,  # "text-embedding-ada-002"
            input=text
        )
        
        embedding = response.data[0].embedding  # 1536-dimensional vector
        self._embedding_cache[text] = embedding
        return embedding
        
    except Exception as e:
        app_logger.error(f"Error generating embedding: {e}")
        # Return zero vector as fallback
        return [0.0] * self.settings.vector_dimension
```

#### **Storage in Milvus**
```python
# Restaurant insertion
embedding_text = self._create_restaurant_embedding_text(restaurant)
vector_embedding = await self._generate_embedding(embedding_text)

# Store in Milvus with vector
entities = [{
    'restaurant_id': restaurant_id,
    'restaurant_name': restaurant_name,
    # ... other fields ...
    'embedding_text': embedding_text,      # Text used for embedding
    'vector_embedding': vector_embedding,  # 1536-dimensional vector
}]
```

### **2. Search Phase (Working)**

#### **Query Processing**
When a user searches, the system:

1. **Parses the query** to understand intent
2. **Generates embedding** for the search query
3. **Performs vector similarity search** in Milvus

```python
async def _handle_location_cuisine_query(self, parsed_query: Dict, max_results: int):
    location = parsed_query.get("location")      # "Jersey City"
    cuisine_type = parsed_query.get("cuisine_type")  # "Italian"
    
    # Create search query
    query_text = f"restaurants in {location} serving {cuisine_type} cuisine"
    
    # Generate embedding for search
    query_vector = await self._generate_embedding(query_text)
    
    # Search with filters
    restaurants = self.milvus_client.search_restaurants(
        query_vector,
        filters={"city": location, "cuisine_type": cuisine_type},
        limit=max_results
    )
```

#### **Vector Similarity Search**
```python
def search_restaurants(self, query_vector: List[float], filters: Dict = None, limit: int = 10):
    collection = self.collections.get('restaurants')
    
    search_params = {
        "metric_type": "COSINE",
        "params": {"nprobe": 10}
    }
    
    results = collection.search(
        data=[query_vector],
        anns_field="vector_embedding",
        param=search_params,
        limit=limit,
        expr=self._build_filter_expression(filters) if filters else None,
        output_fields=["restaurant_id", "restaurant_name", "rating", "cuisine_type"]
    )
    
    return results
```

## 📊 Vector Schema in Milvus

### **Restaurants Collection**
```python
fields = [
    # ... other fields ...
    FieldSchema(name="embedding_text", dtype=DataType.VARCHAR, max_length=4000,
               description="Text used for embedding generation"),
    FieldSchema(name="vector_embedding", dtype=DataType.FLOAT_VECTOR,
               dim=1536, description="OpenAI embedding vector"),
]
```

### **Dishes Collection**
```python
fields = [
    # ... other fields ...
    FieldSchema(name="embedding_text", dtype=DataType.VARCHAR, max_length=4000,
               description="Text used for embedding generation"),
    FieldSchema(name="vector_embedding", dtype=DataType.FLOAT_VECTOR,
               dim=1536, description="OpenAI embedding vector"),
]
```

## 🎯 Vector Search Strategies

### **1. Semantic Similarity**
- **Restaurant Search**: "Italian restaurants in Jersey City" → finds restaurants with similar semantic meaning
- **Dish Search**: "best pizza" → finds dishes with similar descriptions

### **2. Hybrid Search (Vector + Filters)**
```python
# Vector similarity + exact filters
restaurants = milvus_client.search_restaurants(
    query_vector=embedding,
    filters={
        "city": "Jersey City",
        "cuisine_type": "Italian",
        "rating": {"min": 4.0}
    },
    limit=10
)
```

### **3. Multi-Modal Search**
- **Location + Cuisine**: Vector similarity for semantic matching
- **Rating + Price**: Exact filters for precise requirements
- **Meal Types**: Array filters for specific dining times

## 💾 Caching Strategy

### **Embedding Cache**
```python
self._embedding_cache = {}  # In-memory cache

# Cache hit for repeated queries
if text in self._embedding_cache:
    return self._embedding_cache[text]

# Cache miss - generate and store
embedding = await self.openai_client.embeddings.create(...)
self._embedding_cache[text] = embedding
```

### **Benefits**
- **Cost Reduction**: Avoids repeated API calls for same text
- **Performance**: Instant retrieval for cached embeddings
- **Rate Limiting**: Reduces OpenAI API usage

## 🔧 Configuration

### **Vector Settings**
```python
# In config.py
vector_dimension: int = Field(1536, description="OpenAI embedding dimension")
embedding_model: str = Field("text-embedding-ada-002", description="Embedding model")
```

### **Milvus Index Configuration**
```python
index_params = {
    "metric_type": "COSINE",
    "index_type": "IVF_FLAT",
    "params": {"nlist": 1024}
}
```

## 📈 Performance Considerations

### **Vector Generation**
- **API Cost**: ~$0.0001 per embedding (text-embedding-ada-002)
- **Latency**: ~100-500ms per embedding generation
- **Caching**: Reduces cost and latency for repeated text

### **Vector Storage**
- **Dimension**: 1536 floats per vector (6KB per vector)
- **Index Type**: IVF_FLAT for good accuracy/speed balance
- **Search Speed**: ~1-10ms for similarity search

### **Memory Usage**
- **Cache**: In-memory embedding cache (configurable size)
- **Milvus**: Optimized for large-scale vector storage
- **Scaling**: Supports millions of vectors

## 🚀 Usage Examples

### **Restaurant Search**
```python
# User query: "I want Italian food in Jersey City"
query_text = "Italian restaurants in Jersey City"
query_vector = await generate_embedding(query_text)

# Vector similarity search
results = milvus_client.search_restaurants(
    query_vector,
    filters={"city": "Jersey City", "cuisine_type": "Italian"}
)
```

### **Dish Search**
```python
# User query: "best pizza in Hoboken"
query_text = "best pizza restaurants in Hoboken"
query_vector = await generate_embedding(query_text)

# Search dishes with restaurant context
results = milvus_client.search_dishes(
    query_vector,
    filters={"city": "Hoboken"}
)
```

## 🔍 Debugging and Monitoring

### **Embedding Quality**
- **Text Quality**: Ensure embedding text is descriptive and relevant
- **Vector Similarity**: Monitor search result relevance
- **Cache Hit Rate**: Track embedding cache efficiency

### **Performance Monitoring**
- **API Latency**: Monitor OpenAI embedding generation time
- **Search Speed**: Track Milvus search performance
- **Memory Usage**: Monitor embedding cache size

## 🛠️ Troubleshooting

### **Common Issues**

1. **Zero Vectors**: Ensure OpenAI API key is valid
2. **Poor Search Results**: Check embedding text quality
3. **High Latency**: Implement embedding caching
4. **Memory Issues**: Limit embedding cache size

### **Debug Commands**
```python
# Check embedding generation
embedding = await milvus_client._generate_embedding("test text")
print(f"Embedding dimension: {len(embedding)}")

# Check cache status
print(f"Cache size: {len(milvus_client._embedding_cache)}")

# Test vector search
results = milvus_client.search_restaurants(query_vector, limit=5)
print(f"Search results: {len(results)}")
```

## 🎯 Best Practices

### **Embedding Text Design**
- **Include Key Information**: Name, cuisine, location, features
- **Consistent Format**: Use same structure across similar entities
- **Relevant Keywords**: Include terms users might search for

### **Caching Strategy**
- **Cache Common Queries**: Frequently searched terms
- **Monitor Cache Size**: Prevent memory issues
- **Cache Invalidation**: Clear cache periodically

### **Search Optimization**
- **Use Filters**: Combine vector search with exact filters
- **Limit Results**: Set appropriate result limits
- **Index Tuning**: Optimize Milvus index parameters

This vector generation and storage system enables powerful semantic search capabilities in the SweetPick RAG system, allowing users to find restaurants and dishes using natural language queries.
