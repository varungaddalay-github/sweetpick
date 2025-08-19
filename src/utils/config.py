"""
Configuration management for the Sweet Morsels RAG application.
"""
import os
from typing import Optional, List

# Try to import pydantic_settings, fallback to basic config if not available
try:
    from pydantic import BaseModel, Field
    from pydantic_settings import BaseSettings
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False
    print("Warning: pydantic_settings not available, using basic config")


if PYDANTIC_AVAILABLE:
    class Settings(BaseSettings):
        """Application settings loaded from environment variables."""
        
        # API Keys
        openai_api_key: str = Field(..., description="OpenAI API key")
        serpapi_key: str = Field(..., description="SerpAPI key")
        yelp_api_key: Optional[str] = Field(None, description="Yelp API key for direct Yelp Search API access")
        
        # Milvus Cloud Configuration
        milvus_uri: str = Field(..., description="Milvus Cloud URI (e.g., https://your-cluster.zillizcloud.com)")
        milvus_token: str = Field(..., description="Milvus Cloud API token")
        milvus_username: Optional[str] = Field(None, description="Milvus Cloud username (if using username/password)")
        milvus_password: Optional[str] = Field(None, description="Milvus Cloud password (if using username/password)")
        milvus_database: str = Field("default", description="Milvus database name")
        
        # Database
        database_url: str = Field("sqlite:///./sweet_morsels.db", description="Database URL")
        
        # Redis
        redis_url: str = Field("redis://localhost:6379", description="Redis URL")
        
        # Application
        environment: str = Field("development", description="Environment")
        log_level: str = Field("INFO", description="Log level")
        
        # OpenAI Configuration
        openai_model: str = Field("gpt-4o-mini", description="OpenAI model")
        embedding_model: str = Field("text-embedding-ada-002", description="Embedding model")
        max_tokens: int = Field(1000, description="Max tokens")
        temperature: float = Field(0.1, description="Temperature")
        
        # SerpAPI Configuration
        max_restaurants_per_city: int = Field(50, description="Max restaurants per city")
        max_reviews_per_restaurant: int = Field(40, description="Max reviews per restaurant")
        min_rating: float = Field(4.0, description="Minimum rating")
        min_reviews: int = Field(400, description="Minimum reviews")
        serpapi_enable_yelp: bool = Field(True, description="Enable Yelp engine via SerpAPI as an additional source")
        serpapi_yelp_limit: int = Field(30, description="Max Yelp results to fetch per query")
        
        # Processing Configuration
        batch_size: int = Field(8, description="Batch size")
        max_retries: int = Field(3, description="Max retries")
        retry_delay: float = Field(1.0, description="Retry delay")
        
        # Vector Database Configuration
        vector_dimension: int = Field(1536, description="Vector dimension")
        similarity_threshold: float = Field(0.7, description="Similarity threshold")
        
        # Cost Management
        monthly_budget: float = Field(90.0, description="Monthly budget")
        cost_alert_threshold: float = Field(0.8, description="Cost alert threshold")
        
        # Supported Locations and Cuisines
        supported_cities: List[str] = Field(
            default=["Manhattan"], 
            description="Supported cities"
        )
        supported_cuisines: List[str] = Field(
            default=["Italian", "Indian", "Chinese", "American", "Mexican"], 
            description="Supported cuisines"
        )
        
        model_config = {
            "case_sensitive": False,
            "extra": "ignore"
        }

else:
    # Fallback basic settings class
    class Settings:
        """Basic settings class when pydantic is not available."""
        
        def __init__(self):
            self.openai_api_key = os.getenv("OPENAI_API_KEY", "")
            self.serpapi_key = os.getenv("SERPAPI_API_KEY", "")
            self.yelp_api_key = os.getenv("YELP_API_KEY")
            self.milvus_uri = os.getenv("MILVUS_URI", "")
            self.milvus_token = os.getenv("MILVUS_TOKEN", "")
            self.milvus_username = os.getenv("MILVUS_USERNAME")
            self.milvus_password = os.getenv("MILVUS_PASSWORD")
            self.milvus_database = os.getenv("MILVUS_DATABASE", "default")
            self.database_url = os.getenv("DATABASE_URL", "sqlite:///./sweet_morsels.db")
            self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
            self.environment = os.getenv("ENVIRONMENT", "production")
            self.log_level = os.getenv("LOG_LEVEL", "INFO")
            self.openai_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
            self.embedding_model = os.getenv("EMBEDDING_MODEL", "text-embedding-ada-002")
            self.max_tokens = int(os.getenv("MAX_TOKENS", "1000"))
            self.temperature = float(os.getenv("TEMPERATURE", "0.1"))
            self.max_restaurants_per_city = int(os.getenv("MAX_RESTAURANTS_PER_CITY", "50"))
            self.max_reviews_per_restaurant = int(os.getenv("MAX_REVIEWS_PER_RESTAURANT", "40"))
            self.min_rating = float(os.getenv("MIN_RATING", "4.0"))
            self.min_reviews = int(os.getenv("MIN_REVIEWS", "400"))
            self.serpapi_enable_yelp = os.getenv("SERPAPI_ENABLE_YELP", "true").lower() == "true"
            self.serpapi_yelp_limit = int(os.getenv("SERPAPI_YELP_LIMIT", "30"))
            self.batch_size = int(os.getenv("BATCH_SIZE", "8"))
            self.max_retries = int(os.getenv("MAX_RETRIES", "3"))
            self.retry_delay = float(os.getenv("RETRY_DELAY", "1.0"))
            self.vector_dimension = int(os.getenv("VECTOR_DIMENSION", "1536"))
            self.similarity_threshold = float(os.getenv("SIMILARITY_THRESHOLD", "0.7"))
            self.monthly_budget = float(os.getenv("MONTHLY_BUDGET", "90.0"))
            self.cost_alert_threshold = float(os.getenv("COST_ALERT_THRESHOLD", "0.8"))
            self.supported_cities = ["Manhattan"]
            self.supported_cuisines = ["Italian", "Indian", "Chinese", "American", "Mexican"]


# Global settings instance with error handling
try:
    settings = Settings()
except Exception as e:
    print(f"Warning: Could not load settings: {e}")
    settings = None


def get_settings() -> Settings:
    """Get the global settings instance."""
    if settings is None:
        # Create a minimal settings object for deployment
        return Settings()
    return settings 