"""
Abuse protection and security measures for the SweetPick RAG system.
Implements rate limiting, input validation, content filtering, and other security techniques.
"""

import time
import re
import hashlib
import json
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
import asyncio
from src.utils.logger import app_logger
from src.utils.config import get_settings


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    max_requests_per_minute: int = 60
    max_requests_per_hour: int = 1000
    max_requests_per_day: int = 10000
    burst_limit: int = 10  # Max requests in short burst
    burst_window: int = 5  # Seconds for burst window


@dataclass
class SecurityConfig:
    """Configuration for security measures."""
    max_query_length: int = 500
    min_query_length: int = 3
    max_tokens_per_request: int = 1000
    blocked_patterns: List[str] = None
    suspicious_patterns: List[str] = None
    max_concurrent_requests: int = 10
    enable_content_filtering: bool = True
    enable_rate_limiting: bool = True
    enable_input_validation: bool = True


class RateLimiter:
    """Rate limiter with multiple time windows and burst protection."""
    
    def __init__(self, config: RateLimitConfig):
        self.config = config
        self.requests = defaultdict(lambda: {
            'minute': deque(),
            'hour': deque(),
            'day': deque(),
            'burst': deque()
        })
        self.blocked_ips = {}  # IP -> (block_until, reason)
    
    def is_allowed(self, client_id: str) -> Tuple[bool, Optional[str]]:
        """Check if request is allowed for the client."""
        now = time.time()
        
        # Check if IP is blocked
        if client_id in self.blocked_ips:
            block_until, reason = self.blocked_ips[client_id]
            if now < block_until:
                return False, f"IP blocked until {datetime.fromtimestamp(block_until)}: {reason}"
            else:
                del self.blocked_ips[client_id]
        
        # Clean old requests
        self._cleanup_old_requests(client_id, now)
        
        # Check burst limit
        burst_requests = self.requests[client_id]['burst']
        if len(burst_requests) >= self.config.burst_limit:
            # Block for burst window
            block_until = now + self.config.burst_window
            self.blocked_ips[client_id] = (block_until, "Burst limit exceeded")
            return False, f"Burst limit exceeded. Try again in {self.config.burst_window} seconds"
        
        # Check minute limit
        minute_requests = self.requests[client_id]['minute']
        if len(minute_requests) >= self.config.max_requests_per_minute:
            return False, "Rate limit exceeded (per minute)"
        
        # Check hour limit
        hour_requests = self.requests[client_id]['hour']
        if len(hour_requests) >= self.config.max_requests_per_hour:
            return False, "Rate limit exceeded (per hour)"
        
        # Check day limit
        day_requests = self.requests[client_id]['day']
        if len(day_requests) >= self.config.max_requests_per_day:
            return False, "Rate limit exceeded (per day)"
        
        # Record request
        self._record_request(client_id, now)
        
        return True, None
    
    def _cleanup_old_requests(self, client_id: str, now: float):
        """Remove old requests from tracking."""
        requests = self.requests[client_id]
        
        # Clean minute requests (older than 60 seconds)
        while requests['minute'] and now - requests['minute'][0] > 60:
            requests['minute'].popleft()
        
        # Clean hour requests (older than 3600 seconds)
        while requests['hour'] and now - requests['hour'][0] > 3600:
            requests['hour'].popleft()
        
        # Clean day requests (older than 86400 seconds)
        while requests['day'] and now - requests['day'][0] > 86400:
            requests['day'].popleft()
        
        # Clean burst requests (older than burst window)
        while requests['burst'] and now - requests['burst'][0] > self.config.burst_window:
            requests['burst'].popleft()
    
    def _record_request(self, client_id: str, timestamp: float):
        """Record a new request."""
        requests = self.requests[client_id]
        requests['minute'].append(timestamp)
        requests['hour'].append(timestamp)
        requests['day'].append(timestamp)
        requests['burst'].append(timestamp)


class ContentFilter:
    """Content filtering for malicious or inappropriate content."""
    
    def __init__(self, config: SecurityConfig):
        self.config = config
        self.blocked_patterns = config.blocked_patterns or [
            # SQL Injection patterns
            r'(\b(union|select|insert|update|delete|drop|create|alter)\b)',
            r'(\b(or|and)\b\s+\d+\s*=\s*\d+)',
            r'(\b(union|select)\b.*\bfrom\b)',
            
            # XSS patterns
            r'<script[^>]*>.*?</script>',
            r'javascript:',
            r'on\w+\s*=',
            
            # Command injection
            r'(\b(cat|ls|rm|chmod|chown|wget|curl|nc|telnet)\b)',
            r'(\||;|\$\(|\`)',
            
            # Path traversal
            r'\.\./',
            r'\.\.\\',
            
            # Excessive repetition (spam)
            r'(.)\1{10,}',  # Same character repeated 10+ times
            
            # Suspicious file extensions
            r'\.(php|asp|jsp|exe|bat|sh|py|pl)\b',
        ]
        
        self.suspicious_patterns = config.suspicious_patterns or [
            # Potential injection attempts
            r'(\b(admin|root|test|debug)\b)',
            r'(\b(password|passwd|pwd)\b)',
            r'(\b(login|auth|session)\b)',
            
            # Suspicious queries
            r'(\b(delete|remove|kill|destroy)\b)',
            r'(\b(secret|private|hidden)\b)',
            
            # Excessive punctuation
            r'[!?]{5,}',
            r'[.]{5,}',
        ]
        
        # Compile patterns for efficiency
        self.blocked_regex = [re.compile(pattern, re.IGNORECASE) for pattern in self.blocked_patterns]
        self.suspicious_regex = [re.compile(pattern, re.IGNORECASE) for pattern in self.suspicious_patterns]
    
    def check_content(self, text: str) -> Tuple[bool, Optional[str], List[str]]:
        """Check content for malicious patterns."""
        if not text:
            return True, None, []
        
        text_lower = text.lower()
        warnings = []
        
        # Check blocked patterns
        for pattern in self.blocked_regex:
            if pattern.search(text):
                return False, f"Content blocked: matches pattern {pattern.pattern}", []
        
        # Check suspicious patterns
        for pattern in self.suspicious_regex:
            if pattern.search(text):
                warnings.append(f"Suspicious content: matches pattern {pattern.pattern}")
        
        # Check for excessive length
        if len(text) > self.config.max_query_length:
            return False, f"Query too long (max {self.config.max_query_length} characters)", []
        
        if len(text) < self.config.min_query_length:
            return False, f"Query too short (min {self.config.min_query_length} characters)", []
        
        # Check for excessive whitespace
        if len(text.strip()) == 0:
            return False, "Query contains only whitespace", []
        
        # Check for excessive repetition
        words = text.split()
        if len(words) > 1:  # Only check repetition for multi-word queries
            word_freq = {}
            for word in words:
                word_freq[word] = word_freq.get(word, 0) + 1
            
            max_freq = max(word_freq.values())
            if max_freq > len(words) * 0.5:  # More than 50% repetition
                return False, "Excessive word repetition detected", []
        
        return True, None, warnings


class InputValidator:
    """Input validation for queries and parameters."""
    
    def __init__(self, config: SecurityConfig):
        self.config = config
    
    def validate_query(self, query: str) -> Tuple[bool, Optional[str]]:
        """Validate user query."""
        if not query or not isinstance(query, str):
            return False, "Query must be a non-empty string"
        
        # Check length
        if len(query) > self.config.max_query_length:
            return False, f"Query too long (max {self.config.max_query_length} characters)"
        
        if len(query) < self.config.min_query_length:
            return False, f"Query too short (min {self.config.min_query_length} characters)"
        
        # Check for null bytes
        if '\x00' in query:
            return False, "Query contains null bytes"
        
        # Check for control characters
        if any(ord(char) < 32 and char not in '\t\n\r' for char in query):
            return False, "Query contains invalid control characters"
        
        # Check for excessive whitespace
        if len(query.strip()) == 0:
            return False, "Query contains only whitespace"
        
        return True, None
    
    def validate_parameters(self, params: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate API parameters."""
        if not isinstance(params, dict):
            return False, "Parameters must be a dictionary"
        
        # Check for required fields
        if 'query' not in params:
            return False, "Missing required field: query"
        
        # Validate query field
        is_valid, error = self.validate_query(params['query'])
        if not is_valid:
            return False, f"Invalid query: {error}"
        
        # Check for unexpected fields
        allowed_fields = {
            'query', 'session_id', 'max_results', 'location', 'cuisine_type',
            'user_location', 'cuisine_preference', 'price_range'  # Standard API fields
        }
        unexpected_fields = set(params.keys()) - allowed_fields
        if unexpected_fields:
            return False, f"Unexpected fields: {unexpected_fields}"
        
        # Validate max_results if present
        if 'max_results' in params:
            max_results = params['max_results']
            if not isinstance(max_results, int) or max_results < 1 or max_results > 50:
                return False, "max_results must be an integer between 1 and 50"
        
        return True, None


class AbuseProtection:
    """Main abuse protection class that coordinates all security measures."""
    
    def __init__(self):
        self.settings = get_settings()
        
        # Initialize security configuration
        self.security_config = SecurityConfig(
            max_query_length=500,
            min_query_length=3,
            max_tokens_per_request=1000,
            max_concurrent_requests=10,
            enable_content_filtering=True,
            enable_rate_limiting=True,
            enable_input_validation=True
        )
        
        # Initialize rate limiter
        self.rate_limiter = RateLimiter(RateLimitConfig(
            max_requests_per_minute=60,
            max_requests_per_hour=1000,
            max_requests_per_day=10000,
            burst_limit=10,
            burst_window=5
        ))
        
        # Initialize content filter
        self.content_filter = ContentFilter(self.security_config)
        
        # Initialize input validator
        self.input_validator = InputValidator(self.security_config)
        
        # Track concurrent requests
        self.concurrent_requests = 0
        self.max_concurrent = self.security_config.max_concurrent_requests
        
        # Security event logging
        self.security_events = deque(maxlen=1000)
    
    async def check_request(self, client_id: str, query: str, params: Dict[str, Any] = None) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        """Comprehensive security check for incoming requests."""
        start_time = time.time()
        security_report = {
            'timestamp': datetime.now().isoformat(),
            'client_id': client_id,
            'checks_passed': 0,
            'checks_failed': 0,
            'warnings': [],
            'processing_time': 0
        }
        
        try:
            # Check concurrent requests
            if self.concurrent_requests >= self.max_concurrent:
                self._log_security_event(client_id, "CONCURRENT_LIMIT_EXCEEDED", query)
                return False, "Too many concurrent requests", security_report
            
            self.concurrent_requests += 1
            
            # Check 1: Rate Limiting
            if self.security_config.enable_rate_limiting:
                is_allowed, rate_limit_error = self.rate_limiter.is_allowed(client_id)
                if not is_allowed:
                    self._log_security_event(client_id, "RATE_LIMIT_EXCEEDED", query)
                    security_report['checks_failed'] += 1
                    return False, rate_limit_error, security_report
                security_report['checks_passed'] += 1
            
            # Check 2: Input Validation
            if self.security_config.enable_input_validation:
                is_valid, validation_error = self.input_validator.validate_query(query)
                if not is_valid:
                    self._log_security_event(client_id, "INVALID_INPUT", query)
                    security_report['checks_failed'] += 1
                    return False, validation_error, security_report
                security_report['checks_passed'] += 1
                
                # Validate parameters if provided
                if params:
                    is_valid, param_error = self.input_validator.validate_parameters(params)
                    if not is_valid:
                        self._log_security_event(client_id, "INVALID_PARAMETERS", str(params))
                        security_report['checks_failed'] += 1
                        return False, param_error, security_report
                    security_report['checks_passed'] += 1
            
            # Check 3: Content Filtering
            if self.security_config.enable_content_filtering:
                is_safe, content_error, warnings = self.content_filter.check_content(query)
                if not is_safe:
                    self._log_security_event(client_id, "CONTENT_BLOCKED", query)
                    security_report['checks_failed'] += 1
                    return False, content_error, security_report
                security_report['checks_passed'] += 1
                security_report['warnings'].extend(warnings)
            
            # Check 4: Token Limit (estimate)
            estimated_tokens = len(query.split()) * 1.3  # Rough estimation
            if estimated_tokens > self.security_config.max_tokens_per_request:
                self._log_security_event(client_id, "TOKEN_LIMIT_EXCEEDED", query)
                security_report['checks_failed'] += 1
                return False, f"Query too long (estimated {estimated_tokens:.0f} tokens, max {self.security_config.max_tokens_per_request})", security_report
            security_report['checks_passed'] += 1
            
            # All checks passed
            security_report['processing_time'] = time.time() - start_time
            self._log_security_event(client_id, "REQUEST_ALLOWED", query)
            
            return True, None, security_report
            
        except Exception as e:
            app_logger.error(f"Security check error: {e}")
            self._log_security_event(client_id, "SECURITY_ERROR", str(e))
            return False, "Security check failed", security_report
        
        finally:
            self.concurrent_requests -= 1
    
    def _log_security_event(self, client_id: str, event_type: str, details: str):
        """Log security events for monitoring."""
        event = {
            'timestamp': datetime.now().isoformat(),
            'client_id': client_id,
            'event_type': event_type,
            'details': details[:200]  # Truncate long details
        }
        
        self.security_events.append(event)
        app_logger.warning(f"Security event: {event_type} from {client_id}: {details[:100]}...")
    
    def get_security_stats(self) -> Dict[str, Any]:
        """Get security statistics."""
        recent_events = list(self.security_events)
        
        # Count event types
        event_counts = defaultdict(int)
        for event in recent_events:
            event_counts[event['event_type']] += 1
        
        # Get recent blocked requests
        recent_blocked = [e for e in recent_events if 'BLOCKED' in e['event_type'] or 'LIMIT' in e['event_type']]
        
        return {
            'total_events': len(recent_events),
            'event_counts': dict(event_counts),
            'recent_blocked_requests': len(recent_blocked),
            'concurrent_requests': self.concurrent_requests,
            'max_concurrent_requests': self.max_concurrent,
            'rate_limiter_stats': {
                'tracked_clients': len(self.rate_limiter.requests),
                'blocked_ips': len(self.rate_limiter.blocked_ips)
            }
        }
    
    def block_client(self, client_id: str, duration_seconds: int, reason: str):
        """Manually block a client."""
        block_until = time.time() + duration_seconds
        self.rate_limiter.blocked_ips[client_id] = (block_until, reason)
        self._log_security_event(client_id, "MANUAL_BLOCK", f"Blocked for {duration_seconds}s: {reason}")
    
    def unblock_client(self, client_id: str):
        """Unblock a client."""
        if client_id in self.rate_limiter.blocked_ips:
            del self.rate_limiter.blocked_ips[client_id]
            self._log_security_event(client_id, "MANUAL_UNBLOCK", "Client unblocked")
    
    def get_client_status(self, client_id: str) -> Dict[str, Any]:
        """Get status of a specific client."""
        now = time.time()
        
        # Check if blocked
        if client_id in self.rate_limiter.blocked_ips:
            block_until, reason = self.rate_limiter.blocked_ips[client_id]
            if now < block_until:
                return {
                    'status': 'blocked',
                    'block_until': datetime.fromtimestamp(block_until).isoformat(),
                    'reason': reason,
                    'remaining_seconds': int(block_until - now)
                }
            else:
                del self.rate_limiter.blocked_ips[client_id]
        
        # Get rate limit info
        requests = self.rate_limiter.requests.get(client_id, {})
        return {
            'status': 'active',
            'rate_limits': {
                'minute': len(requests.get('minute', [])),
                'hour': len(requests.get('hour', [])),
                'day': len(requests.get('day', [])),
                'burst': len(requests.get('burst', []))
            },
            'limits': {
                'max_per_minute': self.rate_limiter.config.max_requests_per_minute,
                'max_per_hour': self.rate_limiter.config.max_requests_per_hour,
                'max_per_day': self.rate_limiter.config.max_requests_per_day,
                'burst_limit': self.rate_limiter.config.burst_limit
            }
        }


# Global instance
abuse_protection = AbuseProtection()
