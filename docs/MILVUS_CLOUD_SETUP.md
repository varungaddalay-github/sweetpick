# Milvus Cloud Setup Guide

This guide will help you set up Milvus Cloud for the Sweet Morsels RAG application.

## 🚀 Getting Started with Milvus Cloud

### 1. Create a Milvus Cloud Account

1. **Visit Zilliz Cloud**: Go to [https://cloud.zilliz.com](https://cloud.zilliz.com)
2. **Sign Up**: Create a new account or sign in with existing credentials
3. **Verify Email**: Complete email verification process

### 2. Create a New Cluster

1. **Navigate to Clusters**: Click on "Clusters" in the left sidebar
2. **Create Cluster**: Click "Create Cluster"
3. **Choose Configuration**:
   - **Cloud Provider**: AWS, GCP, or Azure (choose based on your location)
   - **Region**: Select a region close to your application
   - **Cluster Name**: `sweet-morsels-rag` (or your preferred name)
   - **Cluster Type**: Free Tier (for development) or Paid Tier (for production)
   - **Compute**: Choose appropriate compute resources
   - **Storage**: Select storage size (start with minimum for development)

4. **Review and Create**: Review your configuration and click "Create"

### 3. Get Connection Details

Once your cluster is created and running (status shows "Active"):

1. **Click on your cluster** to view details
2. **Go to "Connect" tab**
3. **Copy the connection information**:

#### Option 1: API Token Authentication (Recommended)

```bash
# In your .env file:
MILVUS_URI=https://your-cluster-id.zillizcloud.com
MILVUS_TOKEN=your_api_token_here
MILVUS_DATABASE=default
```

#### Option 2: Username/Password Authentication

```bash
# In your .env file:
MILVUS_URI=https://your-cluster-id.zillizcloud.com
MILVUS_USERNAME=your_username_here
MILVUS_PASSWORD=your_password_here
MILVUS_DATABASE=default
```

### 4. Generate API Token (if using token authentication)

1. **Go to "API Keys"** in the left sidebar
2. **Click "Create API Key"**
3. **Set permissions**:
   - **Database**: Select your database (usually "default")
   - **Permissions**: Grant "Read" and "Write" permissions
4. **Copy the token** and store it securely

## 🔧 Configuration

### Environment Variables

Update your `.env` file with the correct Milvus Cloud configuration:

```env
# Milvus Cloud Configuration
MILVUS_URI=https://your-cluster-id.zillizcloud.com
MILVUS_TOKEN=your_api_token_here
MILVUS_DATABASE=default

# Optional: Username/Password (alternative to token)
# MILVUS_USERNAME=your_username_here
# MILVUS_PASSWORD=your_password_here
```

### URI Format Validation

The application validates that your Milvus URI follows the correct format:
- ✅ `https://your-cluster.zillizcloud.com`
- ✅ `https://cluster-123456.zillizcloud.com`
- ❌ `your-cluster.zillizcloud.com` (missing protocol)
- ❌ `http://your-cluster.zillizcloud.com` (should use HTTPS)

## 🧪 Testing Connection

### 1. Use the Environment Check Script

```bash
python check_environment.py
```

This will verify:
- Python version compatibility
- All dependencies are installed
- Environment variables are set correctly
- Milvus Cloud connection

### 2. Test Milvus Connection Manually

```python
# Test script: test_milvus_connection.py
import os
from pymilvus import connections
from dotenv import load_dotenv

load_dotenv()

def test_milvus_connection():
    try:
        # Connect using API token
        connections.connect(
            alias="test",
            uri=os.getenv("MILVUS_URI"),
            token=os.getenv("MILVUS_TOKEN"),
            db_name=os.getenv("MILVUS_DATABASE", "default")
        )
        
        print("✅ Successfully connected to Milvus Cloud!")
        
        # Test basic operations
        from pymilvus import utility
        databases = utility.list_database()
        print(f"📊 Available databases: {databases}")
        
        # Disconnect
        connections.disconnect("test")
        print("🔌 Disconnected from Milvus Cloud")
        
    except Exception as e:
        print(f"❌ Failed to connect to Milvus Cloud: {e}")
        print("Please check your configuration in .env file")

if __name__ == "__main__":
    test_milvus_connection()
```

Run the test:
```bash
python test_milvus_connection.py
```

## 📊 Database Management

### Creating Collections

The application automatically creates the required collections when it starts:

1. **restaurants_enhanced**: Stores restaurant information and embeddings
2. **dishes_detailed**: Stores dish information and embeddings  
3. **locations_metadata**: Stores location metadata and embeddings

### Collection Schemas

Each collection is optimized for vector similarity search:

```python
# Example: restaurants_enhanced collection
{
    'restaurant_id': 'VARCHAR(100)',  # Primary key
    'restaurant_name': 'VARCHAR(200)',
    'city': 'VARCHAR(50)',
    'cuisine_type': 'VARCHAR(50)',
    'rating': 'FLOAT',
    'review_count': 'INT64',
    'vector_embedding': 'FLOAT_VECTOR(1536)',  # OpenAI embeddings
    # ... other fields
}
```

### Indexes

The application creates optimized indexes for vector similarity search:
- **Index Type**: IVF_FLAT
- **Metric Type**: COSINE
- **Parameters**: nlist=1024

## 🔒 Security Best Practices

### 1. API Token Security

- **Store tokens securely**: Never commit API tokens to version control
- **Use environment variables**: Store tokens in `.env` file (not in code)
- **Rotate tokens regularly**: Generate new tokens periodically
- **Limit permissions**: Grant only necessary permissions to API keys

### 2. Network Security

- **Use HTTPS**: Always use HTTPS for Milvus Cloud connections
- **Firewall rules**: Configure firewalls to allow connections to Milvus Cloud
- **VPC peering**: For production, consider VPC peering for enhanced security

### 3. Access Control

- **Database-level permissions**: Use different databases for different environments
- **Collection-level permissions**: Restrict access to specific collections if needed
- **Audit logging**: Monitor access patterns and unusual activity

## 🚨 Troubleshooting

### Common Connection Issues

#### Error: "Illegal uri: [your_milvus_uri_here]"

**Cause**: Invalid URI format
**Solution**: Ensure URI starts with `https://` and follows the correct format

```env
# ✅ Correct format
MILVUS_URI=https://your-cluster.zillizcloud.com

# ❌ Incorrect formats
MILVUS_URI=your-cluster.zillizcloud.com
MILVUS_URI=http://your-cluster.zillizcloud.com
```

#### Error: "Authentication failed"

**Cause**: Invalid credentials
**Solutions**:
1. **Check API token**: Verify the token is correct and not expired
2. **Check username/password**: Ensure credentials are correct
3. **Check permissions**: Verify the token has necessary permissions

#### Error: "Connection timeout"

**Cause**: Network issues or cluster not accessible
**Solutions**:
1. **Check cluster status**: Ensure cluster is "Active" in Zilliz Cloud console
2. **Check network**: Verify internet connection and firewall settings
3. **Check region**: Ensure you're connecting to the correct region

#### Error: "Database not found"

**Cause**: Database doesn't exist or wrong database name
**Solution**: 
1. **Check database name**: Verify `MILVUS_DATABASE` value
2. **Create database**: Create the database in Zilliz Cloud console if needed
3. **Use default**: Set `MILVUS_DATABASE=default` for the default database

### Performance Issues

#### Slow Query Performance

**Solutions**:
1. **Optimize indexes**: Ensure proper indexes are created
2. **Check cluster resources**: Upgrade cluster if needed
3. **Optimize queries**: Use appropriate search parameters

#### High Latency

**Solutions**:
1. **Choose closer region**: Select a region closer to your application
2. **Use connection pooling**: Implement connection pooling for better performance
3. **Optimize batch operations**: Use batch operations for bulk data

## 📈 Monitoring and Scaling

### 1. Monitor Cluster Health

- **Zilliz Cloud Console**: Monitor cluster metrics and performance
- **Application Logs**: Check application logs for connection issues
- **Health Checks**: Implement health checks in your application

### 2. Scaling Considerations

- **Free Tier Limits**: Free tier has limitations on data size and operations
- **Paid Tier Benefits**: Paid tiers offer better performance and higher limits
- **Auto-scaling**: Consider auto-scaling for production workloads

### 3. Cost Optimization

- **Right-size clusters**: Choose appropriate compute and storage resources
- **Monitor usage**: Track API calls and data storage usage
- **Optimize queries**: Use efficient search parameters and filters

## 🔄 Migration from Self-Hosted Milvus

If you're migrating from a self-hosted Milvus instance:

1. **Export data**: Export existing collections and data
2. **Update configuration**: Change connection settings to use Milvus Cloud
3. **Import data**: Import data into new Milvus Cloud collections
4. **Update application**: Update application configuration
5. **Test thoroughly**: Verify all functionality works with Milvus Cloud

## 📚 Additional Resources

- [Zilliz Cloud Documentation](https://docs.zilliz.com/)
- [PyMilvus Documentation](https://milvus.io/docs/pymilvus.md)
- [Milvus Cloud API Reference](https://docs.zilliz.com/reference)
- [Best Practices Guide](https://docs.zilliz.com/best-practices)

## 🆘 Getting Help

- **Zilliz Cloud Support**: Contact support through the Zilliz Cloud console
- **Community Forum**: [Milvus Community](https://milvus.io/community)
- **GitHub Issues**: Report issues in the project repository
- **Documentation**: Check the comprehensive documentation at [docs.zilliz.com](https://docs.zilliz.com/) 