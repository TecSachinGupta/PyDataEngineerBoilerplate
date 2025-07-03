"""Monitoring and metrics utilities."""

import time
from typing import Dict, Any, Optional
from functools import wraps
from contextlib import contextmanager

from prometheus_client import Counter, Histogram, Gauge, start_http_server
import structlog

logger = structlog.get_logger()

# Prometheus metrics
JOB_COUNTER = Counter('spark_jobs_total', 'Total number of Spark jobs', ['job_name', 'status'])
JOB_DURATION = Histogram('spark_job_duration_seconds', 'Spark job duration', ['job_name'])
ROWS_PROCESSED = Counter('spark_rows_processed_total', 'Total rows processed', ['job_name', 'table'])
ACTIVE_JOBS = Gauge('spark_active_jobs', 'Number of active Spark jobs')
DATA_QUALITY_CHECKS = Counter('data_quality_checks_total', 'Data quality checks', ['job_name', 'check_type', 'status'])


class MetricsCollector:
    """Collect and expose application metrics."""
    
    def __init__(self, port: int = 8000):
        self.port = port
        self._server_started = False
    
    def start_server(self):
        """Start Prometheus metrics server."""
        if not self._server_started:
            start_http_server(self.port)
            self._server_started = True
            logger.info(f"Metrics server started on port {self.port}")
    
    @staticmethod
    def track_job_execution(job_name: str):
        """Decorator to track job execution metrics."""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.time()
                ACTIVE_JOBS.inc()
                
                try:
                    result = func(*args, **kwargs)
                    JOB_COUNTER.labels(job_name=job_name, status='success').inc()
                    return result
                except Exception as e:
                    JOB_COUNTER.labels(job_name=job_name, status='failed').inc()
                    raise
                finally:
                    duration = time.time() - start_time
                    JOB_DURATION.labels(job_name=job_name).observe(duration)
                    ACTIVE_JOBS.dec()
            
            return wrapper
        return decorator
    
    @staticmethod
    def track_rows_processed(job_name: str, table_name: str, count: int):
        """Track number of rows processed."""
        ROWS_PROCESSED.labels(job_name=job_name, table=table_name).inc(count)
    
    @staticmethod
    def track_data_quality_check(job_name: str, check_type: str, status: str):
        """Track data quality check results."""
        DATA_QUALITY_CHECKS.labels(
            job_name=job_name, 
            check_type=check_type, 
            status=status
        ).inc()


@contextmanager
def execution_timer(operation_name: str):
    """Context manager to time operations."""
    start_time = time.time()
    logger.info(f"Starting {operation_name}")
    
    try:
        yield
    finally:
        duration = time.time() - start_time
        logger.info(f"Completed {operation_name} in {duration:.2f}s")


class PerformanceMonitor:
    """Monitor Spark application performance."""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.sc = spark_session.sparkContext
    
    def get_executor_metrics(self) -> Dict[str, Any]:
        """Get executor-level metrics."""
        status = self.sc.statusTracker()
        
        metrics = {
            'executor_count': len(status.getExecutorInfos()),
            'active_jobs': len(status.getActiveJobIds()),
            'active_stages': len(status.getActiveStageIds()),
        }
        
        # Get memory usage
        for executor in status.getExecutorInfos():
            metrics.update({
                f'executor_{executor.executorId}_memory_used': executor.memoryUsed,
                f'executor_{executor.executorId}_memory_total': executor.maxMemory,
                f'executor_{executor.executorId}_disk_used': executor.diskUsed,
                f'executor_{executor.executorId}_cores': executor.totalCores,
            })
        
        return metrics
    
    def log_performance_metrics(self):
        """Log current performance metrics."""
        metrics = self.get_executor_metrics()
        logger.info("Performance metrics", **metrics)
        
        return metrics