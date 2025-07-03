"""Enhanced data quality validation utilities."""

from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
from enum import Enum
import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull, regexp_extract
from pyspark.sql.types import NumericType, StringType, DateType, TimestampType
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
import structlog

from .monitoring import MetricsCollector

logger = structlog.get_logger()


class CheckSeverity(Enum):
    """Severity levels for data quality checks."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class DataQualityResult:
    """Result of a data quality check."""
    check_name: str
    status: str
    severity: CheckSeverity
    message: str
    details: Optional[Dict[str, Any]] = None
    row_count: Optional[int] = None
    failed_rows: Optional[int] = None


class DataQualityValidator:
    """Comprehensive data quality validation."""
    
    def __init__(self, spark_session, job_name: str):
        self.spark = spark_session
        self.job_name = job_name
        self.results: List[DataQualityResult] = []
        self.metrics_collector = MetricsCollector()
    
    def validate_completeness(self, df: DataFrame, columns: List[str], 
                            threshold: float = 0.95) -> DataQualityResult:
        """Validate data completeness."""
        total_rows = df.count()
        
        for column in columns:
            if column not in df.columns:
                result = DataQualityResult(
                    check_name=f"completeness_{column}",
                    status="failed",
                    severity=CheckSeverity.ERROR,
                    message=f"Column {column} not found in DataFrame",
                    row_count=total_rows
                )
            else:
                null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
                completeness_ratio = (total_rows - null_count) / total_rows if total_rows > 0 else 0
                
                if completeness_ratio >= threshold:
                    status = "passed"
                    severity = CheckSeverity.INFO
                    message = f"Column {column} completeness: {completeness_ratio:.2%}"
                else:
                    status = "failed"
                    severity = CheckSeverity.ERROR
                    message = f"Column {column} completeness {completeness_ratio:.2%} below threshold {threshold:.2%}"
                
                result = DataQualityResult(
                    check_name=f"completeness_{column}",
                    status=status,
                    severity=severity,
                    message=message,
                    details={"completeness_ratio": completeness_ratio, "null_count": null_count},
                    row_count=total_rows,
                    failed_rows=null_count
                )
            
            self.results.append(result)
            self.metrics_collector.track_data_quality_check(
                self.job_name, f"completeness_{column}", result.status
            )
        
        return result
    
    def validate_uniqueness(self, df: DataFrame, columns: List[str]) -> DataQualityResult:
        """Validate data uniqueness."""
        total_rows = df.count()
        unique_rows = df.select(*columns).distinct().count()
        duplicate_rows = total_rows - unique_rows
        
        if duplicate_rows == 0:
            status = "passed"
            severity = CheckSeverity.INFO
            message = f"No duplicates found in columns: {columns}"
        else:
            status = "failed"
            severity = CheckSeverity.WARNING
            message = f"Found {duplicate_rows} duplicate rows in columns: {columns}"
        
        result = DataQualityResult(
            check_name="uniqueness",
            status=status,
            severity=severity,
            message=message,
            details={"duplicate_count": duplicate_rows, "unique_count": unique_rows},
            row_count=total_rows,
            failed_rows=duplicate_rows
        )
        
        self.results.append(result)
        self.metrics_collector.track_data_quality_check(
            self.job_name, "uniqueness", result.status
        )
        
        return result
    
    def validate_format(self, df: DataFrame, column: str, pattern: str, 
                       pattern_name: str = "format") -> DataQualityResult:
        """Validate data format using regex."""
        if column not in df.columns:
            result = DataQualityResult(
                check_name=f"format_{column}",
                status="failed",
                severity=CheckSeverity.ERROR,
                message=f"Column {column} not found in DataFrame"
            )
        else:
            total_rows = df.count()
            valid_rows = df.filter(col(column).rlike(pattern)).count()
            invalid_rows = total_rows - valid_rows
            
            if invalid_rows == 0:
                status = "passed"
                severity = CheckSeverity.INFO
                message = f"All rows in {column} match {pattern_name} pattern"
            else:
                status = "failed"
                severity = CheckSeverity.ERROR
                message = f"{invalid_rows} rows in {column} don't match {pattern_name} pattern"
            
            result = DataQualityResult(
                check_name=f"format_{column}",
                status=status,
                severity=severity,
                message=message,
                details={"pattern": pattern, "valid_count": valid_rows},
                row_count=total_rows,
                failed_rows=invalid_rows
            )
        
        self.results.append(result)
        self.metrics_collector.track_data_quality_check(
            self.job_name, f"format_{column}", result.status
        )
        
        return result
    
    def validate_range(self, df: DataFrame, column: str, min_val: Any = None, 
                      max_val: Any = None) -> DataQualityResult:
        """Validate numeric ranges."""
        if column not in df.columns:
            result = DataQualityResult(
                check_name=f"range_{column}",
                status="failed",
                severity=CheckSeverity.ERROR,
                message=f"Column {column} not found in DataFrame"
            )
        else:
            total_rows = df.count()
            
            conditions = []
            if min_val is not None:
                conditions.append(col(column) >= min_val)
            if max_val is not None:
                conditions.append(col(column) <= max_val)
            
            if conditions:
                valid_condition = conditions[0]
                for condition in conditions[1:]:
                    valid_condition = valid_condition & condition
                
                valid_rows = df.filter(valid_condition).count()
                invalid_rows = total_rows - valid_rows
                
                if invalid_rows == 0:
                    status = "passed"
                    severity = CheckSeverity.INFO
                    message = f"All values in {column} within range [{min_val}, {max_val}]"
                else:
                    status = "failed"
                    severity = CheckSeverity.ERROR
                    message = f"{invalid_rows} values in {column} outside range [{min_val}, {max_val}]"
                
                result = DataQualityResult(
                    check_name=f"range_{column}",
                    status=status,
                    severity=severity,
                    message=message,
                    details={"min_val": min_val, "max_val": max_val, "valid_count": valid_rows},
                    row_count=total_rows,
                    failed_rows=invalid_rows
                )
            else:
                result = DataQualityResult(
                    check_name=f"range_{column}",
                    status="skipped",
                    severity=CheckSeverity.INFO,
                    message="No range constraints specified"
                )
        
        self.results.append(result)
        self.metrics_collector.track_data_quality_check(
            self.job_name, f"range_{column}", result.status
        )
        
        return result
    
    def validate_referential_integrity(self, df1: DataFrame, df2: DataFrame, 
                                     join_keys: List[str]) -> DataQualityResult:
        """Validate referential integrity between datasets."""
        # Find orphaned records
        orphaned = df1.join(df2, join_keys, "left_anti")
        orphaned_count = orphaned.count()
        total_rows = df1.count()
        
        if orphaned_count == 0:
            status = "passed"
            severity = CheckSeverity.INFO
            message = f"No orphaned records found for keys: {join_keys}"
        else:
            status = "failed"
            severity = CheckSeverity.ERROR
            message = f"Found {orphaned_count} orphaned records for keys: {join_keys}"
        
        result = DataQualityResult(
            check_name="referential_integrity",
            status=status,
            severity=severity,
            message=message,
            details={"join_keys": join_keys, "orphaned_count": orphaned_count},
            row_count=total_rows,
            failed_rows=orphaned_count
        )
        
        self.results.append(result)
        self.metrics_collector.track_data_quality_check(
            self.job_name, "referential_integrity", result.status
        )
        
        return result
    
    def validate_custom(self, df: DataFrame, check_name: str, 
                       validation_func: Callable[[DataFrame], bool],
                       error_message: str) -> DataQualityResult:
        """Validate using custom function."""
        try:
            is_valid = validation_func(df)
            
            if is_valid:
                status = "passed"
                severity = CheckSeverity.INFO
                message = f"Custom validation '{check_name}' passed"
            else:
                status = "failed"
                severity = CheckSeverity.ERROR
                message = error_message
            
            result = DataQualityResult(
                check_name=check_name,
                status=status,
                severity=severity,
                message=message,
                row_count=df.count()
            )
        except Exception as e:
            result = DataQualityResult(
                check_name=check_name,
                status="error",
                severity=CheckSeverity.ERROR,
                message=f"Custom validation failed with error: {str(e)}"
            )
        
        self.results.append(result)
        self.metrics_collector.track_data_quality_check(
            self.job_name, check_name, result.status
        )
        
        return result
    
    def get_summary(self) -> Dict[str, Any]:
        """Get validation summary."""
        total_checks = len(self.results)
        passed_checks = sum(1 for r in self.results if r.status == "passed")
        failed_checks = sum(1 for r in self.results if r.status == "failed")
        error_checks = sum(1 for r in self.results if r.status == "error")
        
        return {
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
            "error_checks": error_checks,
            "success_rate": passed_checks / total_checks if total_checks > 0 else 0,
            "results": [
                {
                    "check_name": r.check_name,
                    "status": r.status,
                    "severity": r.severity.value,
                    "message": r.message,
                    "details": r.details
                }
                for r in self.results
            ]
        }
    
    def log_results(self):
        """Log validation results."""
        summary = self.get_summary()
        logger.info("Data quality validation completed", **summary)
        
        # Log individual failures
        for result in self.results:
            if result.status in ["failed", "error"]:
                logger.warning(
                    f"Data quality check failed: {result.check_name}",
                    status=result.status,
                    message=result.message,
                    details=result.details
                )