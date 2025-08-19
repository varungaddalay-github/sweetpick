# Troubleshooting Guide

This guide helps resolve common issues when setting up and running the Sweet Morsels RAG application.

## 🐍 Python Version Issues

### Error: "Python 3.12.2+ required"
**Solution**: Upgrade Python to version 3.12.2 or higher
```bash
# Check current version
python --version

# On macOS with Homebrew
brew install python@3.12

# On Ubuntu/Debian
sudo apt update
sudo apt install python3.12 python3.12-pip

# On Windows
# Download from https://www.python.org/downloads/
```

## 📦 Dependency Installation Issues

### Error: "No module named 'pydantic-settings'"
**Solution**: Install the missing package
```bash
pip install pydantic-settings==2.1.0
```

### Error: "No module named 'google-search-results'"
**Solution**: Install SerpAPI client
```bash
pip install google-search-results==2.4.2
```

### Error: "AttributeError: module 'marshmallow' has no attribute '__version_info__'"
**Solution**: Fix marshmallow compatibility
```bash
# Uninstall and reinstall marshmallow
pip uninstall marshmallow -y
pip install 'marshmallow>=3.20.0,<4.0.0'
```

### Error: "pip install failed"
**Solution**: Try these steps in order
```bash
# 1. Upgrade pip
python -m pip install --upgrade pip

# 2. Clear pip cache
pip cache purge

# 3. Install with --user flag
pip install --user -r requirements.txt

# 4. Use virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## 🔧 Environment Variable Issues

### Error: "Missing required environment variable"
**Solution**: Set up environment variables
```bash
# Copy example file
cp config.env.example .env

# Edit .env file with your API keys
nano .env  # or use your preferred editor
```

**Required Variables:**
```env
OPENAI_API_KEY=your_openai_api_key_here
SERPAPI_KEY=your_serpapi_key_here
MILVUS_URI=your_milvus_uri_here
DATABASE_URL=postgresql://user:password@localhost/sweet_morsels
REDIS_URL=redis://localhost:6379
```

## 🔴 Redis Connection Issues

### Error: "Redis connection failed"
**Solution**: Start Redis server
```bash
# On macOS
brew services start redis

# On Ubuntu/Debian
sudo systemctl start redis-server

# On Windows
# Download and install Redis from https://redis.io/download

# Check Redis status
redis-cli ping
# Should return: PONG
```

## 🗄️ Milvus Cloud Connection Issues

### Error: "Failed to connect to Milvus Cloud"
**Solution**: Check Milvus Cloud configuration
```bash
# Verify your Milvus Cloud credentials
# Check MILVUS_URI and MILVUS_TOKEN in .env

# Test connection using the test script
python test_milvus_connection.py

# Or test manually
curl -H "Authorization: Bearer YOUR_TOKEN" YOUR_MILVUS_URI/health
```

**Common Milvus Cloud Issues**:
- **Invalid URI format**: Ensure URI starts with `https://` and follows format: `https://your-cluster.zillizcloud.com`
- **Authentication failed**: Check API token or username/password
- **Cluster not active**: Verify cluster status in Zilliz Cloud console
- **Network issues**: Check firewall settings and internet connection

### Error: "Illegal uri: [your_milvus_uri_here]"
**Solution**: Fix URI format
```env
# ✅ Correct format
MILVUS_URI=https://your-cluster.zillizcloud.com

# ❌ Incorrect formats
MILVUS_URI=your-cluster.zillizcloud.com
MILVUS_URI=http://your-cluster.zillizcloud.com
```

## 🚀 Application Startup Issues

### Error: "Module not found during startup"
**Solution**: Check Python path and imports
```bash
# Ensure you're in the project root directory
pwd  # Should show path ending with 'sweet_morsels'

# Check if src directory exists
ls -la src/

# Try running with explicit Python path
PYTHONPATH=. python run.py
```

### Error: "Port already in use"
**Solution**: Change port or kill existing process
```bash
# Find process using port 8000
lsof -i :8000

# Kill the process
kill -9 <PID>

# Or use different port
uvicorn src.api.main:app --port 8001
```

## 🧪 Testing Issues

### Error: "pytest not found"
**Solution**: Install testing dependencies
```bash
pip install -r requirements-dev.txt
```

### Error: "Test failures"
**Solution**: Check test environment
```bash
# Run tests with verbose output
pytest -v tests/

# Run specific test
pytest tests/test_query_parser.py -v

# Check test coverage
pytest --cov=src tests/
```

## 📊 Performance Issues

### Slow API responses
**Solutions**:
1. **Check Redis**: Ensure Redis is running and accessible
2. **Monitor API calls**: Check OpenAI and SerpAPI rate limits
3. **Enable caching**: Verify cache is working properly
4. **Check logs**: Look for slow database queries

### High memory usage
**Solutions**:
1. **Reduce batch size**: Lower `BATCH_SIZE` in config
2. **Limit concurrent requests**: Add rate limiting
3. **Monitor Milvus**: Check vector database performance

## 🔍 Debugging

### Enable debug logging
```bash
# Set log level in .env
LOG_LEVEL=DEBUG

# Or set environment variable
export LOG_LEVEL=DEBUG
```

### Check application logs
```bash
# Run with debug output
python run.py --debug

# Check log files (if configured)
tail -f logs/app.log
```

### Use environment check script
```bash
python check_environment.py
```

## 🆘 Getting Help

### Common Solutions
1. **Restart the application** after fixing issues
2. **Check the logs** for detailed error messages
3. **Verify all dependencies** are installed correctly
4. **Ensure environment variables** are set properly

### Still Having Issues?
1. **Check the implementation guide**: `docs/IMPLEMENTATION_GUIDE.md`
2. **Run the environment check**: `python check_environment.py`
3. **Use the installation script**: `python install.py`
4. **Check Python compatibility**: `python tests/test_python_compatibility.py`

### System Requirements
- **Python**: 3.12.2+
- **Memory**: 4GB+ RAM recommended
- **Storage**: 2GB+ free space
- **Network**: Stable internet connection for API calls

## 🚨 Emergency Recovery

### Complete Reset
If all else fails, try a complete reset:
```bash
# Remove virtual environment (if using one)
rm -rf venv/

# Remove installed packages
pip freeze | xargs pip uninstall -y

# Clear pip cache
pip cache purge

# Reinstall everything
python install.py
```

### Fallback Installation
```bash
# Install only essential packages
pip install fastapi uvicorn pydantic pydantic-settings openai redis

# Test basic functionality
python -c "import fastapi; print('FastAPI installed successfully')"
``` 