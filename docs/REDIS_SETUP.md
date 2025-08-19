# Redis Setup Guide

This guide helps you set up Redis for the Sweet Morsels RAG application.

## 🔴 What is Redis?

Redis is an in-memory data store used by the application for:
- **Caching API responses** (SerpAPI, OpenAI)
- **Storing processed data** temporarily
- **Rate limiting** and request tracking
- **Performance optimization**

## 🚀 Quick Setup

### Option 1: macOS (Recommended)

```bash
# Install Redis using Homebrew
brew install redis

# Start Redis service
brew services start redis

# Verify Redis is running
redis-cli ping
# Should return: PONG
```

### Option 2: Ubuntu/Debian

```bash
# Install Redis
sudo apt update
sudo apt install redis-server

# Start Redis service
sudo systemctl start redis-server
sudo systemctl enable redis-server

# Verify Redis is running
redis-cli ping
```

### Option 3: Windows

1. **Download Redis**: Visit https://redis.io/download
2. **Install Redis**: Follow the installation instructions
3. **Start Redis**: Run the Redis server
4. **Verify**: Open command prompt and run `redis-cli ping`

### Option 4: Docker

```bash
# Run Redis in Docker
docker run -d --name redis -p 6379:6379 redis:latest

# Verify it's running
docker ps

# Test connection
docker exec redis redis-cli ping
```

## 🔧 Configuration

### Default Settings
The application uses these default Redis settings:
- **Host**: localhost
- **Port**: 6379
- **URL**: redis://localhost:6379

### Custom Configuration
You can customize Redis settings in your `.env` file:
```env
REDIS_URL=redis://localhost:6379
# or
REDIS_URL=redis://username:password@localhost:6379
```

## 🧪 Testing Redis Connection

### Test with redis-cli
```bash
# Connect to Redis
redis-cli

# Test basic operations
SET test "Hello Redis"
GET test
DEL test

# Exit
exit
```

### Test with Python
```python
import redis

# Test connection
r = redis.Redis(host='localhost', port=6379, db=0)
r.ping()  # Should return True
print("Redis connection successful!")
```

### Test with Application
```bash
# Run the environment check
python check_environment.py

# Should show: ✅ Redis connection is working
```

## 🚨 Troubleshooting

### Issue: "Redis connection failed"

**Solutions:**
1. **Check if Redis is running**:
   ```bash
   redis-cli ping
   ```

2. **Start Redis service**:
   ```bash
   # macOS
   brew services start redis
   
   # Ubuntu/Debian
   sudo systemctl start redis-server
   
   # Windows
   # Start Redis from the installed location
   ```

3. **Check Redis port**:
   ```bash
   # Check if port 6379 is in use
   lsof -i :6379
   ```

4. **Check Redis logs**:
   ```bash
   # macOS
   tail -f /usr/local/var/log/redis.log
   
   # Ubuntu/Debian
   sudo journalctl -u redis-server -f
   ```

### Issue: "Permission denied"

**Solutions:**
1. **Check file permissions**:
   ```bash
   sudo chown -R redis:redis /var/lib/redis
   sudo chmod 755 /var/lib/redis
   ```

2. **Check Redis configuration**:
   ```bash
   sudo nano /etc/redis/redis.conf
   ```

### Issue: "Address already in use"

**Solutions:**
1. **Find and kill existing Redis process**:
   ```bash
   ps aux | grep redis
   kill -9 <PID>
   ```

2. **Use different port**:
   ```bash
   redis-server --port 6380
   ```

## 🔄 Running Without Redis (Fallback)

If you can't install Redis, the application will still work but with reduced performance:

### What happens without Redis:
- ✅ **Application starts normally**
- ⚠️ **No API response caching**
- ⚠️ **Slower performance** (repeated API calls)
- ⚠️ **Higher API costs** (no caching)

### How to run without Redis:
1. **Skip Redis installation**
2. **Start the application**: `python run.py`
3. **Application will log warnings** but continue working

## 📊 Redis Monitoring

### Check Redis Status
```bash
# Check Redis info
redis-cli info

# Check memory usage
redis-cli info memory

# Check connected clients
redis-cli client list
```

### Monitor Redis in Real-time
```bash
# Monitor Redis commands
redis-cli monitor

# Check Redis logs
tail -f /var/log/redis/redis-server.log
```

## 🔒 Security Considerations

### For Production:
1. **Set a password**:
   ```bash
   # Edit redis.conf
   requirepass your_password_here
   ```

2. **Bind to specific IP**:
   ```bash
   # Edit redis.conf
   bind 127.0.0.1
   ```

3. **Disable dangerous commands**:
   ```bash
   # Edit redis.conf
   rename-command FLUSHDB ""
   rename-command FLUSHALL ""
   ```

## 📚 Additional Resources

- [Redis Official Documentation](https://redis.io/documentation)
- [Redis Commands Reference](https://redis.io/commands)
- [Redis Configuration](https://redis.io/topics/config)

## 🆘 Getting Help

If you're still having issues:
1. Check the [Troubleshooting Guide](TROUBLESHOOTING.md)
2. Verify your system requirements
3. Check Redis logs for specific error messages
4. Consider running without Redis for testing 