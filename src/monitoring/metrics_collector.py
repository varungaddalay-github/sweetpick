"""
Comprehensive monitoring system for SweetPick RAG application.
Implements Prometheus metrics, distributed tracing, structured logging, and alerting.
"""
import time
import asyncio
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
import json
import hashlib
import uuid
from contextlib import asynccontextmanager
from src.utils.config import get_settings
from src.utils.logger import app_logger


@dataclass
class MetricPoint:
    """A single metric data point."""
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class AlertRule:
    """Alert rule configuration."""
    name: str
    metric: str
    threshold: float
    operator: str  # 'gt', 'lt', 'eq', 'gte', 'lte'
    duration: int  # seconds
    severity: str  # 'info', 'warning', 'critical'
    message: str


class PrometheusMetrics:
    """Prometheus-style metrics collection."""
    
    def __init__(self):
        self.metrics = defaultdict(list)
        self.counters = defaultdict(int)
        self.gauges = defaultdict(float)
        self.histograms = defaultdict(list)
        self.lock = asyncio.Lock()
    
    async def record_metric(self, metric_name: str, value: float, labels: Dict[str, str] = None):
        """Record a metric value."""
        async with self.lock:
            point = MetricPoint(
                timestamp=time.time(),
                value=value,
                labels=labels or {}
            )
            self.metrics[metric_name].append(point)
            
            # Keep only last 1000 points per metric
            if len(self.metrics[metric_name]) > 1000:
                self.metrics[metric_name] = self.metrics[metric_name][-1000:]
    
    async def increment_counter(self, counter_name: str, value: int = 1, labels: Dict[str, str] = None):
        """Increment a counter."""
        async with self.lock:
            key = self._get_metric_key(counter_name, labels)
            self.counters[key] += value
    
    async def set_gauge(self, gauge_name: str, value: float, labels: Dict[str, str] = None):
        """Set a gauge value."""
        async with self.lock:
            key = self._get_metric_key(gauge_name, labels)
            self.gauges[key] = value
    
    async def record_histogram(self, histogram_name: str, value: float, labels: Dict[str, str] = None):
        """Record a histogram value."""
        async with self.lock:
            point = MetricPoint(
                timestamp=time.time(),
                value=value,
                labels=labels or {}
            )
            self.histograms[histogram_name].append(point)
            
            # Keep only last 1000 points per histogram
            if len(self.histograms[histogram_name]) > 1000:
                self.histograms[histogram_name] = self.histograms[histogram_name][-1000:]
    
    def _get_metric_key(self, name: str, labels: Dict[str, str] = None) -> str:
        """Generate metric key with labels."""
        if not labels:
            return name
        
        label_str = ",".join([f"{k}={v}" for k, v in sorted(labels.items())])
        return f"{name}{{{label_str}}}"
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get all metrics in Prometheus format."""
        result = {
            "metrics": {},
            "counters": dict(self.counters),
            "gauges": dict(self.gauges),
            "histograms": {}
        }
        
        # Convert metrics to summary format
        for name, points in self.metrics.items():
            if points:
                values = [p.value for p in points]
                result["metrics"][name] = {
                    "count": len(values),
                    "sum": sum(values),
                    "avg": sum(values) / len(values),
                    "min": min(values),
                    "max": max(values),
                    "latest": points[-1].value,
                    "latest_timestamp": points[-1].timestamp
                }
        
        # Convert histograms to summary format
        for name, points in self.histograms.items():
            if points:
                values = [p.value for p in points]
                result["histograms"][name] = {
                    "count": len(values),
                    "sum": sum(values),
                    "avg": sum(values) / len(values),
                    "min": min(values),
                    "max": max(values),
                    "latest": points[-1].value,
                    "latest_timestamp": points[-1].timestamp
                }
        
        return result


class DistributedTracing:
    """Distributed tracing implementation."""
    
    def __init__(self):
        self.traces = defaultdict(list)
        self.active_spans = {}
        self.lock = asyncio.Lock()
    
    @asynccontextmanager
    async def trace_span(self, operation_name: str, trace_id: str = None, parent_span_id: str = None):
        """Create a trace span."""
        span_id = str(uuid.uuid4())
        trace_id = trace_id or str(uuid.uuid4())
        
        span = {
            "span_id": span_id,
            "trace_id": trace_id,
            "parent_span_id": parent_span_id,
            "operation_name": operation_name,
            "start_time": time.time(),
            "tags": {},
            "logs": []
        }
        
        # Store active span
        self.active_spans[span_id] = span
        
        try:
            yield span
        finally:
            # Complete span
            span["end_time"] = time.time()
            span["duration"] = span["end_time"] - span["start_time"]
            
            # Store completed span
            async with self.lock:
                self.traces[trace_id].append(span)
            
            # Remove from active spans
            if span_id in self.active_spans:
                del self.active_spans[span_id]
    
    async def add_span_tag(self, span_id: str, key: str, value: str):
        """Add a tag to a span."""
        if span_id in self.active_spans:
            self.active_spans[span_id]["tags"][key] = value
    
    async def add_span_log(self, span_id: str, message: str, level: str = "info"):
        """Add a log to a span."""
        if span_id in self.active_spans:
            log_entry = {
                "timestamp": time.time(),
                "message": message,
                "level": level
            }
            self.active_spans[span_id]["logs"].append(log_entry)
    
    def get_trace(self, trace_id: str) -> List[Dict]:
        """Get a complete trace."""
        return self.traces.get(trace_id, [])
    
    def get_active_spans(self) -> Dict[str, Dict]:
        """Get all active spans."""
        return dict(self.active_spans)


class StructuredLogging:
    """Enhanced structured logging with correlation IDs."""
    
    def __init__(self):
        self.log_buffer = deque(maxlen=10000)
        self.correlation_ids = {}
        self.lock = asyncio.Lock()
    
    async def log(self, level: str, message: str, correlation_id: str = None, 
                  extra_data: Dict[str, Any] = None, trace_id: str = None):
        """Log a structured message."""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "level": level,
            "message": message,
            "correlation_id": correlation_id,
            "trace_id": trace_id,
            "extra_data": extra_data or {},
            "thread_id": id(asyncio.current_task()) if asyncio.current_task() else None
        }
        
        async with self.lock:
            self.log_buffer.append(log_entry)
        
        # Also log to standard logger
        if level == "error":
            app_logger.error(f"[{correlation_id}] {message}", extra=extra_data)
        elif level == "warning":
            app_logger.warning(f"[{correlation_id}] {message}", extra=extra_data)
        elif level == "info":
            app_logger.info(f"[{correlation_id}] {message}", extra=extra_data)
        else:
            app_logger.debug(f"[{correlation_id}] {message}", extra=extra_data)
    
    async def set_correlation_id(self, task_id: str, correlation_id: str):
        """Set correlation ID for a task."""
        async with self.lock:
            self.correlation_ids[task_id] = correlation_id
    
    async def get_correlation_id(self, task_id: str) -> Optional[str]:
        """Get correlation ID for a task."""
        return self.correlation_ids.get(task_id)
    
    def get_recent_logs(self, limit: int = 100) -> List[Dict]:
        """Get recent log entries."""
        return list(self.log_buffer)[-limit:]


class AlertManager:
    """Alert management system."""
    
    def __init__(self):
        self.alert_rules = []
        self.active_alerts = {}
        self.alert_history = []
        self.metrics_collector = None
        self.lock = asyncio.Lock()
    
    def add_alert_rule(self, rule: AlertRule):
        """Add an alert rule."""
        self.alert_rules.append(rule)
    
    def set_metrics_collector(self, metrics_collector: PrometheusMetrics):
        """Set the metrics collector for alert evaluation."""
        self.metrics_collector = metrics_collector
    
    async def evaluate_alerts(self):
        """Evaluate all alert rules."""
        if not self.metrics_collector:
            return
        
        metrics = self.metrics_collector.get_metrics()
        
        for rule in self.alert_rules:
            await self._evaluate_rule(rule, metrics)
    
    async def _evaluate_rule(self, rule: AlertRule, metrics: Dict[str, Any]):
        """Evaluate a single alert rule."""
        # Get metric value
        metric_value = self._get_metric_value(rule.metric, metrics)
        if metric_value is None:
            return
        
        # Check condition
        is_triggered = self._check_condition(metric_value, rule.operator, rule.threshold)
        
        alert_key = f"{rule.name}_{rule.metric}"
        
        if is_triggered:
            # Check if alert is already active
            if alert_key not in self.active_alerts:
                # Create new alert
                alert = {
                    "id": str(uuid.uuid4()),
                    "rule_name": rule.name,
                    "metric": rule.metric,
                    "threshold": rule.threshold,
                    "current_value": metric_value,
                    "severity": rule.severity,
                    "message": rule.message,
                    "triggered_at": time.time(),
                    "status": "active"
                }
                
                async with self.lock:
                    self.active_alerts[alert_key] = alert
                    self.alert_history.append(alert)
                
                # Log alert
                await self._log_alert(alert, "triggered")
        else:
            # Check if alert should be resolved
            if alert_key in self.active_alerts:
                alert = self.active_alerts[alert_key]
                alert["resolved_at"] = time.time()
                alert["status"] = "resolved"
                
                async with self.lock:
                    del self.active_alerts[alert_key]
                
                # Log alert resolution
                await self._log_alert(alert, "resolved")
    
    def _get_metric_value(self, metric_name: str, metrics: Dict[str, Any]) -> Optional[float]:
        """Get current value of a metric."""
        # Check metrics
        if metric_name in metrics["metrics"]:
            return metrics["metrics"][metric_name]["latest"]
        
        # Check gauges
        if metric_name in metrics["gauges"]:
            return metrics["gauges"][metric_name]
        
        # Check histograms
        if metric_name in metrics["histograms"]:
            return metrics["histograms"][metric_name]["latest"]
        
        return None
    
    def _check_condition(self, value: float, operator: str, threshold: float) -> bool:
        """Check if condition is met."""
        if operator == "gt":
            return value > threshold
        elif operator == "lt":
            return value < threshold
        elif operator == "eq":
            return value == threshold
        elif operator == "gte":
            return value >= threshold
        elif operator == "lte":
            return value <= threshold
        return False
    
    async def _log_alert(self, alert: Dict, action: str):
        """Log alert action."""
        message = f"Alert {action}: {alert['rule_name']} - {alert['message']}"
        app_logger.warning(message, extra={
            "alert_id": alert["id"],
            "severity": alert["severity"],
            "metric": alert["metric"],
            "current_value": alert["current_value"],
            "threshold": alert["threshold"]
        })
    
    def get_active_alerts(self) -> List[Dict]:
        """Get all active alerts."""
        return list(self.active_alerts.values())
    
    def get_alert_history(self, limit: int = 100) -> List[Dict]:
        """Get alert history."""
        return list(self.alert_history)[-limit:]


class ComprehensiveMonitoring:
    """Comprehensive monitoring system."""
    
    def __init__(self):
        self.metrics = PrometheusMetrics()
        self.tracing = DistributedTracing()
        self.logging = StructuredLogging()
        self.alert_manager = AlertManager()
        
        # Set up alert manager
        self.alert_manager.set_metrics_collector(self.metrics)
        self._setup_default_alerts()
        
        # Statistics
        self.stats = {
            "metrics_recorded": 0,
            "traces_created": 0,
            "alerts_triggered": 0,
            "start_time": time.time()
        }
    
    def _setup_default_alerts(self):
        """Set up default alert rules."""
        default_alerts = [
            AlertRule(
                name="high_response_time",
                metric="query_response_time",
                threshold=2.0,
                operator="gt",
                duration=300,
                severity="warning",
                message="Query response time is above 2 seconds"
            ),
            AlertRule(
                name="high_error_rate",
                metric="error_rate",
                threshold=0.05,
                operator="gt",
                duration=300,
                severity="critical",
                message="Error rate is above 5%"
            ),
            AlertRule(
                name="low_cache_hit_rate",
                metric="cache_hit_rate",
                threshold=0.7,
                operator="lt",
                duration=300,
                severity="warning",
                message="Cache hit rate is below 70%"
            ),
            AlertRule(
                name="high_memory_usage",
                metric="memory_usage",
                threshold=0.8,
                operator="gt",
                duration=300,
                severity="warning",
                message="Memory usage is above 80%"
            )
        ]
        
        for alert in default_alerts:
            self.alert_manager.add_alert_rule(alert)
    
    async def record_query_metrics(self, query_type: str, response_time: float, 
                                  success: bool, result_count: int = 0):
        """Record metrics for a query."""
        # Record response time
        await self.metrics.record_histogram("query_response_time", response_time, 
                                          {"query_type": query_type})
        
        # Record success/failure
        await self.metrics.increment_counter("query_total", 1, {"query_type": query_type})
        if success:
            await self.metrics.increment_counter("query_success", 1, {"query_type": query_type})
        else:
            await self.metrics.increment_counter("query_failure", 1, {"query_type": query_type})
        
        # Record result count
        if result_count > 0:
            await self.metrics.record_histogram("query_result_count", result_count, 
                                              {"query_type": query_type})
        
        # Calculate error rate
        total_queries = await self._get_counter_value("query_total", {"query_type": query_type})
        failed_queries = await self._get_counter_value("query_failure", {"query_type": query_type})
        
        if total_queries > 0:
            error_rate = failed_queries / total_queries
            await self.metrics.set_gauge("error_rate", error_rate, {"query_type": query_type})
        
        self.stats["metrics_recorded"] += 1
    
    async def record_vector_search_metrics(self, search_type: str, latency: float, 
                                         result_count: int, cache_hit: bool):
        """Record metrics for vector search operations."""
        # Record search latency
        await self.metrics.record_histogram("vector_search_latency", latency, 
                                          {"search_type": search_type})
        
        # Record result count
        await self.metrics.record_histogram("vector_search_results", result_count, 
                                          {"search_type": search_type})
        
        # Record cache performance
        await self.metrics.increment_counter("vector_search_total", 1, {"search_type": search_type})
        if cache_hit:
            await self.metrics.increment_counter("vector_search_cache_hit", 1, {"search_type": search_type})
        else:
            await self.metrics.increment_counter("vector_search_cache_miss", 1, {"search_type": search_type})
        
        # Calculate cache hit rate
        total_searches = await self._get_counter_value("vector_search_total", {"search_type": search_type})
        cache_hits = await self._get_counter_value("vector_search_cache_hit", {"search_type": search_type})
        
        if total_searches > 0:
            cache_hit_rate = cache_hits / total_searches
            await self.metrics.set_gauge("cache_hit_rate", cache_hit_rate, {"search_type": search_type})
    
    async def record_system_metrics(self, memory_usage: float, cpu_usage: float, 
                                   active_connections: int):
        """Record system-level metrics."""
        await self.metrics.set_gauge("memory_usage", memory_usage)
        await self.metrics.set_gauge("cpu_usage", cpu_usage)
        await self.metrics.set_gauge("active_connections", active_connections)
    
    async def record_business_metrics(self, recommendations_generated: int, 
                                    user_satisfaction: float = None):
        """Record business metrics."""
        await self.metrics.increment_counter("recommendations_generated", recommendations_generated)
        
        if user_satisfaction is not None:
            await self.metrics.record_histogram("user_satisfaction", user_satisfaction)
    
    @asynccontextmanager
    async def trace_operation(self, operation_name: str, trace_id: str = None, 
                             parent_span_id: str = None):
        """Trace an operation."""
        async with self.tracing.trace_span(operation_name, trace_id, parent_span_id) as span:
            self.stats["traces_created"] += 1
            yield span
    
    async def log_structured(self, level: str, message: str, correlation_id: str = None,
                           extra_data: Dict[str, Any] = None, trace_id: str = None):
        """Log a structured message."""
        await self.logging.log(level, message, correlation_id, extra_data, trace_id)
    
    async def evaluate_alerts(self):
        """Evaluate all alert rules."""
        await self.alert_manager.evaluate_alerts()
        self.stats["alerts_triggered"] = len(self.alert_manager.get_active_alerts())
    
    async def _get_counter_value(self, counter_name: str, labels: Dict[str, str] = None) -> int:
        """Get current value of a counter."""
        metrics = self.metrics.get_metrics()
        key = self.metrics._get_metric_key(counter_name, labels)
        return metrics["counters"].get(key, 0)
    
    def get_monitoring_data(self) -> Dict[str, Any]:
        """Get comprehensive monitoring data."""
        return {
            "metrics": self.metrics.get_metrics(),
            "active_alerts": self.alert_manager.get_active_alerts(),
            "alert_history": self.alert_manager.get_alert_history(50),
            "recent_logs": self.logging.get_recent_logs(100),
            "active_spans": self.tracing.get_active_spans(),
            "statistics": self.stats,
            "uptime": time.time() - self.stats["start_time"]
        }
    
    async def start_monitoring_loop(self):
        """Start the monitoring evaluation loop."""
        while True:
            try:
                await self.evaluate_alerts()
                await asyncio.sleep(60)  # Check every minute
            except Exception as e:
                app_logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(60)


# Global monitoring instance
monitoring = ComprehensiveMonitoring()
