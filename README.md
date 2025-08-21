# 🍽️ SweetPick - AI-Powered Restaurant Discovery

> **Live App**: [https://sweetpick.vercel.app/](https://sweetpick.vercel.app/)

Your AI-powered guide to the tastiest dishes at the hottest restaurants. SweetPick combines vector search, sentiment analysis, and intelligent fallbacks to deliver personalized restaurant recommendations.

## 🎯 **Business Problem**

**SweetPick solves the restaurant discovery problem by providing dish-level recommendations instead of just restaurant suggestions.** Our AI-powered platform reduces decision fatigue from 10+ minutes to under 2 seconds while expanding coverage to any location and cuisine through intelligent fallbacks.

## ✨ Features

### 🎯 **Core Capabilities**
- **Smart Dish Discovery**: Find the best dishes at top local restaurants using review intelligence
- **Hybrid Ranking**: Combines topic analysis, sentiment scoring, and neighborhood relevance
- **Location-Aware**: Supports Manhattan, Jersey City, and Hoboken with neighborhood precision
- **Cuisine Coverage**: Italian, Indian, Chinese, American, and Mexican cuisines

### 🚀 **Intelligent Fallback System**
- **Unsupported Locations**: Get AI-generated recommendations for any city (e.g., "Italian food in Chicago")
- **Unsupported Cuisines**: Discover restaurants for global cuisines (e.g., "Thai food in Manhattan")
- **Seamless Experience**: Automatic fallback detection with consistent response format

### 🧠 **AI-Powered Features**
- **Natural Language Queries**: Ask for recommendations in plain English
- **Sentiment Analysis**: Reviews analyzed for authentic dish quality assessment
- **Contextual Understanding**: Handles complex queries with multiple preferences
- **Conversational Responses**: Human-like recommendations with reasoning

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Frontend      │    │   FastAPI        │    │   Milvus Cloud  │
│   (Vercel)      │◄──►│   Backend        │◄──►│   Vector DB     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │   OpenAI GPT-4o  │
                       │   Fallback       │
                       └──────────────────┘
```

## 🚀 Quick Start

### Live Demo
Visit [https://sweetpick.vercel.app/](https://sweetpick.vercel.app/) to try SweetPick right now!

### Local Development
```bash
# Clone the repository
git clone <repository-url>
cd sweet_morsels

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp env.example .env
# Edit .env with your API keys

# Run the development server
python -m uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000
```

## 📱 Usage Examples

### Supported Scope Queries
```
✅ "Italian food in Manhattan"
✅ "Best Indian restaurants in Jersey City"
✅ "Chinese food in Hoboken"
✅ "Show me the top dishes at Razza"
```

### Fallback Queries (AI-Generated)
```
🔄 "Thai food in Manhattan" → AI generates Manhattan Thai recommendations
🔄 "Italian food in Chicago" → AI generates Chicago Italian recommendations
🔄 "Japanese sushi in Miami" → AI generates comprehensive Miami Japanese recommendations
```

## 🛠️ Technology Stack

### **Backend**
- **FastAPI**: High-performance async API framework
- **Python 3.12**: Modern Python with async/await support
- **Uvicorn**: Lightning-fast ASGI server

### **AI & ML**
- **OpenAI GPT-4o**: Natural language processing and fallback generation
- **Vector Search**: Semantic similarity using text embeddings
- **Sentiment Analysis**: Review quality assessment

### **Data & Storage**
- **Milvus Cloud**: Vector database for similarity search
- **Redis**: Caching layer for performance optimization
- **SerpAPI**: Google Places data collection

### **Deployment**
- **Vercel**: Frontend hosting and edge functions
- **Zilliz Cloud**: Managed Milvus vector database
- **Redis Cloud**: Managed Redis caching

## 📊 System Components

### **Query Processing**
- **Query Parser**: Extracts entities (location, cuisine, dish, etc.)
- **Scope Validation**: Determines supported vs. unsupported queries
- **Location Resolution**: Maps locations to supported areas

### **Retrieval Engine**
- **Vector Search**: Semantic similarity matching
- **Hybrid Ranking**: Combines multiple scoring factors
- **Filtering**: Location, cuisine, and quality-based filtering

### **Fallback Handler**
- **OpenAI Integration**: GPT-4o for unsupported scope
- **Response Generation**: Natural language recommendations
- **Quality Assurance**: Structured output validation

## 🔧 Configuration

### Environment Variables
```bash
# OpenAI
OPENAI_API_KEY=your_openai_api_key

# Milvus Cloud
MILVUS_CLOUD_URI=your_milvus_cloud_uri
MILVUS_CLOUD_TOKEN=your_milvus_cloud_token

# Redis
REDIS_URL=your_redis_url

# SerpAPI (optional)
SERPAPI_KEY=your_serpapi_key
```

### Supported Locations
```python
supported_cities = ["Manhattan", "Jersey City", "Hoboken"]
```

### Supported Cuisines
```python
supported_cuisines = ["Italian", "Indian", "Chinese", "American", "Mexican"]
```

## 📈 Performance

- **Response Time**: < 2 seconds for supported queries
- **Fallback Time**: < 4 seconds for AI-generated recommendations
- **Cache Hit Rate**: > 80% for repeated queries
- **Uptime**: 99.9% on Vercel infrastructure

## 🔍 API Endpoints

### Query Processing
```http
POST /query
Content-Type: application/json

{
  "query": "Italian food in Manhattan",
  "max_results": 10
}
```

### Response Format
```json
{
  "query": "Italian food in Manhattan",
  "query_type": "location_cuisine",
  "recommendations": [
    {
      "restaurant_name": "Tony's Di Napoli",
      "dish_name": "vodka pasta",
      "location": "Times Square",
      "rating": 4.6,
      "confidence": 0.8
    }
  ],
  "natural_response": "For a delightful Italian dining experience...",
  "fallback_used": false,
  "processing_time": 1.39
}
```

## 🧪 Testing

### Test Queries
```bash
# Test supported scope
curl -X POST "http://localhost:8000/query" \
  -H "Content-Type: application/json" \
  -d '{"query": "Italian food in Manhattan"}'

# Test location fallback
curl -X POST "http://localhost:8000/query" \
  -H "Content-Type: application/json" \
  -d '{"query": "Italian food in Chicago"}'

# Test cuisine fallback
curl -X POST "http://localhost:8000/query" \
  -H "Content-Type: application/json" \
  -d '{"query": "Thai food in Manhattan"}'
```

## 📚 Documentation

- **[Project Writeup](docs/PROJECT_WRITEUP.md)**: Comprehensive project overview
- **[System Design](docs/SYSTEM_DESIGN.md)**: Architecture and component details
- **[Fallback System](docs/FALLBACK_SYSTEM.md)**: AI fallback implementation guide
- **[Implementation Guide](docs/IMPLEMENTATION_GUIDE.md)**: Development and deployment instructions
- **[Vercel Deployment](vercel-deployment-guide.md)**: Production deployment guide

## 🚀 Deployment

### Vercel (Recommended)
```bash
# Install Vercel CLI
npm i -g vercel

# Deploy
vercel --prod
```

### Local Production
```bash
# Build and run
python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **OpenAI** for GPT-4o language model
- **Zilliz** for Milvus Cloud vector database
- **Vercel** for hosting and deployment
- **FastAPI** for the excellent web framework

## 📞 Support

- **Live App**: [https://sweetpick.vercel.app/](https://sweetpick.vercel.app/)
- **Issues**: [GitHub Issues](https://github.com/yourusername/sweet_morsels/issues)
- **Documentation**: [docs/](docs/) directory

---

**Made with ❤️ for food lovers everywhere**

*SweetPick - Your AI-powered guide to the tastiest dishes at the hottest restaurants*
