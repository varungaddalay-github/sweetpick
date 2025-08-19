# SweetPick Abuse Protection System

## Overview

The SweetPick RAG system includes comprehensive abuse protection mechanisms to prevent malicious attacks, spam, and resource exhaustion. The system implements multiple layers of security to ensure reliable and safe operation.

## Security Features

### 1. Rate Limiting

**Purpose**: Prevents API abuse and resource exhaustion through request frequency control.

**Configuration**:
```python
RateLimitConfig(
    max_requests_per_minute=60,    # 60 requests per minute
    max_requests_per_hour=1000,    # 1000 requests per hour  
    max_requests_per_day=10000,    # 10000 requests per day
    burst_limit=10,                # Max 10 requests in 5 seconds
    burst_window=5                 # 5-second burst window
)
```

**Features**:
- Multi-window rate limiting (minute, hour, day)
- Burst protection to prevent sudden spikes
- Automatic IP blocking for burst violations
- Configurable limits per time window

### 2. Content Filtering

**Purpose**: Blocks malicious content and inappropriate queries.

**Blocked Patterns**:
- **SQL Injection**: `'; DROP TABLE restaurants; --`, `SELECT * FROM users WHERE id = 1 OR 1=1`
- **XSS Attacks**: `<script>alert('xss')</script>`, `javascript:`, `onclick=`
- **Command Injection**: `cat /etc/passwd`, `rm -rf /`, `|`, `;`, `$()`
- **Path Traversal**: `../`, `..\`
- **Excessive Repetition**: Same character repeated 10+ times
- **Suspicious Files**: `.php`, `.asp`, `.exe`, `.bat`, `.sh`

**Suspicious Patterns** (warnings):
- Admin-related terms: `admin`, `root`, `password`, `login`
- Destructive actions: `delete`, `remove`, `kill`, `destroy`
- Excessive punctuation: `!!!!!`, `.....`

### 3. Input Validation

**Purpose**: Ensures query integrity and prevents malformed requests.

**Validation Rules**:
- **Length Limits**: 3-500 characters
- **Character Validation**: No null bytes or control characters
- **Whitespace Check**: No empty or whitespace-only queries
- **Type Validation**: Must be non-empty string
- **Parameter Validation**: Only allowed fields accepted

### 4. Concurrent Request Limiting

**Purpose**: Prevents resource exhaustion from too many simultaneous requests.

**Configuration**:
- Maximum 10 concurrent requests per client
- Automatic rejection of excess requests
- Real-time tracking of active requests

### 5. Token Limit Estimation

**Purpose**: Prevents excessive resource usage from very long queries.

**Implementation**:
- Rough token estimation: `len(query.split()) * 1.3`
- Maximum 1000 estimated tokens per request
- Automatic rejection of overly long queries

## API Integration

### Main Query Endpoint Protection

The `/query` endpoint automatically includes abuse protection:

```python
@app.post("/query", response_model=QueryResponse)
async def process_query(request: QueryRequest, background_tasks: BackgroundTasks, http_request: Request):
    # 🔒 ABUSE PROTECTION: Check request before processing
    client_id = get_client_id(http_request)
    is_allowed, security_error, security_report = await abuse_protection.check_request(
        client_id, request.query, request.dict()
    )
    
    if not is_allowed:
        raise HTTPException(
            status_code=429 if "rate limit" in security_error.lower() else 400,
            detail=f"Request blocked: {security_error}"
        )
```

### Client Identification

The system identifies clients using:
- **Real IP Address**: From `X-Forwarded-For` header or direct connection
- **User Agent Hash**: Additional uniqueness factor
- **Combined Hash**: `MD5(IP:UserAgent)[:16]`

## Security Monitoring

### Security Statistics Endpoint

`GET /security/stats`

Returns comprehensive security metrics:
```json
{
  "security_overview": {
    "total_events": 150,
    "event_counts": {
      "REQUEST_ALLOWED": 120,
      "CONTENT_BLOCKED": 15,
      "RATE_LIMIT_EXCEEDED": 10,
      "INVALID_INPUT": 5
    },
    "recent_blocked_requests": 30,
    "concurrent_requests": 3,
    "max_concurrent_requests": 10,
    "rate_limiter_stats": {
      "tracked_clients": 25,
      "blocked_ips": 2
    }
  },
  "timestamp": "2024-01-15T10:30:00",
  "system_status": "protected"
}
```

### Client Status Endpoint

`GET /security/client/{client_id}`

Returns detailed status for a specific client:
```json
{
  "client_id": "abc123def456",
  "status": {
    "status": "active",
    "rate_limits": {
      "minute": 5,
      "hour": 45,
      "day": 120,
      "burst": 2
    },
    "limits": {
      "max_per_minute": 60,
      "max_per_hour": 1000,
      "max_per_day": 10000,
      "burst_limit": 10
    }
  },
  "timestamp": "2024-01-15T10:30:00"
}
```

## Management Endpoints

### Block Client

`POST /security/block/{client_id}?duration_seconds=3600&reason=Manual block`

Manually block a client for specified duration.

### Unblock Client

`POST /security/unblock/{client_id}`

Manually unblock a previously blocked client.

## Configuration

### Security Configuration

```python
SecurityConfig(
    max_query_length=500,
    min_query_length=3,
    max_tokens_per_request=1000,
    max_concurrent_requests=10,
    enable_content_filtering=True,
    enable_rate_limiting=True,
    enable_input_validation=True
)
```

### Custom Pattern Configuration

You can customize blocked and suspicious patterns:

```python
custom_blocked_patterns = [
    r'your_custom_pattern',
    r'another_pattern'
]

custom_suspicious_patterns = [
    r'suspicious_pattern',
    r'warning_pattern'
]

security_config = SecurityConfig(
    blocked_patterns=custom_blocked_patterns,
    suspicious_patterns=custom_suspicious_patterns
)
```

## Testing

### Test Script

Run the comprehensive test suite:

```bash
python test_abuse_protection.py
```

The test script covers:
- Content filtering with various malicious inputs
- Rate limiting with burst protection
- Input validation edge cases
- Comprehensive protection scenarios
- Security monitoring and statistics
- API endpoint integration

### Test Cases

The system is tested against:
- **SQL Injection**: `'; DROP TABLE restaurants; --`
- **XSS Attacks**: `<script>alert('xss')</script>`
- **Command Injection**: `cat /etc/passwd`
- **Excessive Length**: 1000+ character queries
- **Excessive Repetition**: Spam patterns
- **Empty/Invalid Inputs**: Null bytes, control characters
- **Rate Limit Violations**: Burst requests, sustained abuse

## Security Event Logging

All security events are logged with:
- **Timestamp**: ISO format
- **Client ID**: Hashed identifier
- **Event Type**: Classification of the event
- **Details**: Truncated details (max 200 chars)

Event Types:
- `REQUEST_ALLOWED`: Successful request
- `CONTENT_BLOCKED`: Malicious content detected
- `RATE_LIMIT_EXCEEDED`: Rate limit violation
- `INVALID_INPUT`: Input validation failure
- `CONCURRENT_LIMIT_EXCEEDED`: Too many concurrent requests
- `TOKEN_LIMIT_EXCEEDED`: Query too long
- `MANUAL_BLOCK`: Administrator block
- `MANUAL_UNBLOCK`: Administrator unblock

## Best Practices

### 1. Regular Monitoring

- Monitor `/security/stats` endpoint regularly
- Set up alerts for unusual activity patterns
- Review blocked requests for false positives

### 2. Configuration Tuning

- Adjust rate limits based on legitimate usage patterns
- Fine-tune content filtering patterns for your use case
- Monitor false positive rates and adjust accordingly

### 3. Incident Response

- Use manual blocking for confirmed malicious clients
- Investigate patterns in blocked requests
- Update patterns based on new attack vectors

### 4. Performance Considerations

- Security checks add minimal overhead (~1-5ms per request)
- Rate limiting uses memory-efficient data structures
- Event logging is non-blocking and bounded

## Troubleshooting

### Common Issues

1. **False Positives**: Legitimate queries being blocked
   - Solution: Review content filtering patterns
   - Adjust suspicious pattern thresholds

2. **Rate Limit Too Strict**: Legitimate users hitting limits
   - Solution: Increase rate limit values
   - Consider user-specific limits

3. **Performance Impact**: Security checks slowing down API
   - Solution: Monitor processing times
   - Optimize pattern matching if needed

### Debugging

Enable detailed logging:
```python
import logging
logging.getLogger('src.security').setLevel(logging.DEBUG)
```

Check client status:
```bash
curl "http://localhost:8000/security/client/{client_id}"
```

## Future Enhancements

### Planned Features

1. **Machine Learning Detection**: AI-powered anomaly detection
2. **Geographic Rate Limiting**: Different limits by region
3. **Behavioral Analysis**: Pattern-based threat detection
4. **Integration with WAF**: Web Application Firewall integration
5. **Real-time Dashboards**: Live security monitoring UI

### Extensibility

The abuse protection system is designed to be extensible:
- Add new content filter patterns
- Implement custom rate limiting strategies
- Integrate with external security services
- Add custom validation rules

## Conclusion

The SweetPick abuse protection system provides comprehensive security through multiple layers of defense. The system is designed to be robust, configurable, and maintainable while providing detailed monitoring and management capabilities.

For questions or issues, refer to the security endpoints or contact the development team.
