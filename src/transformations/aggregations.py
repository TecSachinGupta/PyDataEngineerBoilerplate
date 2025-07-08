"""Common aggregation transformation patterns."""

from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, max as spark_max, min as spark_min,
    count, countDistinct, stddev, variance, skewness, kurtosis,
    percentile_approx, collect_list, collect_set, first, last,
    when, coalesce, lit, round as spark_round
)
from pyspark.sql.types import NumericType
import structlog

logger = structlog.get_logger()


class AggregationTransformer:
    """Common aggregation transformation patterns."""
    
    def __init__(self, df: DataFrame):
        self.df = df
    
    def basic_aggregations(self, group_by_cols: List[str], 
                          agg_cols: List[str]) -> DataFrame:
        """Perform basic aggregations (sum, avg, count, min, max)."""
        agg_exprs = []
        
        for col_name in agg_cols:
            if col_name in self.df.columns:
                # Check if column is numeric
                col_type = dict(self.df.dtypes)[col_name]
                if any(numeric_type in col_type for numeric_type in ['int', 'long', 'float', 'double', 'decimal']):
                    agg_exprs.extend([
                        spark_sum(col(col_name)).alias(f"{col_name}_sum"),
                        avg(col(col_name)).alias(f"{col_name}_avg"),
                        spark_min(col(col_name)).alias(f"{col_name}_min"),
                        spark_max(col(col_name)).alias(f"{col_name}_max"),
                        stddev(col(col_name)).alias(f"{col_name}_stddev")
                    ])
                
                # Count and distinct count for all columns
                agg_exprs.extend([
                    count(col(col_name)).alias(f"{col_name}_count"),
                    countDistinct(col(col_name)).alias(f"{col_name}_distinct_count")
                ])
        
        result = self.df.groupBy(*group_by_cols).agg(*agg_exprs)
        logger.info(f"Applied basic aggregations grouped by {group_by_cols}")
        return result
    
    def percentile_aggregations(self, group_by_cols: List[str], 
                               numeric_cols: List[str],
                               percentiles: List[float] = [0.25, 0.5, 0.75, 0.95]) -> DataFrame:
        """Calculate percentiles for numeric columns."""
        agg_exprs = []
        
        for col_name in numeric_cols:
            if col_name in self.df.columns:
                for p in percentiles:
                    agg_exprs.append(
                        percentile_approx(col(col_name), p).alias(f"{col_name}_p{int(p*100)}")
                    )
        
        result = self.df.groupBy(*group_by_cols).agg(*agg_exprs)
        logger.info(f"Applied percentile aggregations for percentiles {percentiles}")
        return result
    
    def advanced_statistical_aggregations(self, group_by_cols: List[str], 
                                        numeric_cols: List[str]) -> DataFrame:
        """Calculate advanced statistical measures."""
        agg_exprs = []
        
        for col_name in numeric_cols:
            if col_name in self.df.columns:
                agg_exprs.extend([
                    variance(col(col_name)).alias(f"{col_name}_variance"),
                    skewness(col(col_name)).alias(f"{col_name}_skewness"),
                    kurtosis(col(col_name)).alias(f"{col_name}_kurtosis")
                ])
        
        result = self.df.groupBy(*group_by_cols).agg(*agg_exprs)
        logger.info(f"Applied advanced statistical aggregations")
        return result
    
    def collect_aggregations(self, group_by_cols: List[str], 
                           collect_cols: List[str], 
                           distinct: bool = False) -> DataFrame:
        """Collect values into arrays."""
        agg_exprs = []
        
        for col_name in collect_cols:
            if col_name in self.df.columns:
                if distinct:
                    agg_exprs.append(collect_set(col(col_name)).alias(f"{col_name}_set"))
                else:
                    agg_exprs.append(collect_list(col(col_name)).alias(f"{col_name}_list"))
        
        result = self.df.groupBy(*group_by_cols).agg(*agg_exprs)
        logger.info(f"Applied collect aggregations (distinct={distinct})")
        return result
    
    def custom_aggregation(self, group_by_cols: List[str], 
                          custom_aggs: Dict[str, str]) -> DataFrame:
        """Apply custom aggregation expressions."""
        agg_exprs = []
        
        for alias, expression in custom_aggs.items():
            # This allows for complex custom aggregations
            # Example: {"revenue_per_customer": "sum(revenue) / count(distinct customer_id)"}
            agg_exprs.append(expr(expression).alias(alias))
        
        result = self.df.groupBy(*group_by_cols).agg(*agg_exprs)
        logger.info(f"Applied custom aggregations: {list(custom_aggs.keys())}")
        return result


### src/project_name/transformations/window_functions.py
```python
"""Window function transformation patterns."""

from typing import List, Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, row_number, rank, dense_rank, lag, lead, first, last,
    sum as spark_sum, avg, max as spark_max, min as spark_min,
    count, ntile, percent_rank, cume_dist, when, coalesce
)
from pyspark.sql.window import Window
import structlog

logger = structlog.get_logger()


class WindowTransformer:
    """Window function transformation patterns."""
    
    def __init__(self, df: DataFrame):
        self.df = df
    
    def add_row_numbers(self, partition_cols: List[str], 
                       order_cols: List[str]) -> DataFrame:
        """Add row numbers within partitions."""
        window_spec = Window.partitionBy(*partition_cols).orderBy(*order_cols)
        
        result = self.df.withColumn("row_number", row_number().over(window_spec))
        logger.info(f"Added row numbers partitioned by {partition_cols}, ordered by {order_cols}")
        return result
    
    def add_rankings(self, partition_cols: List[str], 
                    order_cols: List[str], 
                    ranking_types: List[str] = ["rank", "dense_rank"]) -> DataFrame:
        """Add various ranking columns."""
        window_spec = Window.partitionBy(*partition_cols).orderBy(*order_cols)
        result = self.df
        
        if "rank" in ranking_types:
            result = result.withColumn("rank", rank().over(window_spec))
        if "dense_rank" in ranking_types:
            result = result.withColumn("dense_rank", dense_rank().over(window_spec))
        if "percent_rank" in ranking_types:
            result = result.withColumn("percent_rank", percent_rank().over(window_spec))
        if "ntile" in ranking_types:
            # Default to 4 tiles (quartiles)
            result = result.withColumn("quartile", ntile(4).over(window_spec))
        
        logger.info(f"Added rankings: {ranking_types}")
        return result
    
    def add_lag_lead_columns(self, partition_cols: List[str], 
                            order_cols: List[str],
                            lag_lead_configs: List[Dict[str, Any]]) -> DataFrame:
        """Add lag and lead columns."""
        window_spec = Window.partitionBy(*partition_cols).orderBy(*order_cols)
        result = self.df
        
        for config in lag_lead_configs:
            col_name = config["column"]
            operation = config["operation"]  # "lag" or "lead"
            offset = config.get("offset", 1)
            default_value = config.get("default", None)
            alias = config.get("alias", f"{operation}_{col_name}_{offset}")
            
            if operation == "lag":
                result = result.withColumn(
                    alias, 
                    lag(col(col_name), offset, default_value).over(window_spec)
                )
            elif operation == "lead":
                result = result.withColumn(
                    alias, 
                    lead(col(col_name), offset, default_value).over(window_spec)
                )
        
        logger.info(f"Added lag/lead columns: {[c.get('alias', f'{c[\"operation\"]}_{c[\"column\"]}') for c in lag_lead_configs]}")
        return result
    
    def add_running_aggregations(self, partition_cols: List[str], 
                               order_cols: List[str],
                               agg_configs: List[Dict[str, Any]]) -> DataFrame:
        """Add running/cumulative aggregations."""
        window_spec = (Window.partitionBy(*partition_cols)
                      .orderBy(*order_cols)
                      .rowsBetween(Window.unboundedPreceding, Window.currentRow))
        
        result = self.df
        
        for config in agg_configs:
            col_name = config["column"]
            operation = config["operation"]  # "sum", "avg", "count", "min", "max"
            alias = config.get("alias", f"running_{operation}_{col_name}")
            
            if operation == "sum":
                result = result.withColumn(alias, spark_sum(col(col_name)).over(window_spec))
            elif operation == "avg":
                result = result.withColumn(alias, avg(col(col_name)).over(window_spec))
            elif operation == "count":
                result = result.withColumn(alias, count(col(col_name)).over(window_spec))
            elif operation == "min":
                result = result.withColumn(alias, spark_min(col(col_name)).over(window_spec))
            elif operation == "max":
                result = result.withColumn(alias, spark_max(col(col_name)).over(window_spec))
        
        logger.info(f"Added running aggregations: {[c.get('alias', f'running_{c[\"operation\"]}_{c[\"column\"]}') for c in agg_configs]}")
        return result
    
    def add_first_last_values(self, partition_cols: List[str], 
                             order_cols: List[str],
                             value_cols: List[str]) -> DataFrame:
        """Add first and last values in each partition."""
        window_spec = Window.partitionBy(*partition_cols).orderBy(*order_cols)
        result = self.df
        
        for col_name in value_cols:
            if col_name in self.df.columns:
                result = result.withColumn(
                    f"first_{col_name}",
                    first(col(col_name), ignorenulls=True).over(window_spec)
                ).withColumn(
                    f"last_{col_name}",
                    last(col(col_name), ignorenulls=True).over(window_spec)
                )
        
        logger.info(f"Added first/last values for columns: {value_cols}")
        return result
    
    def add_moving_averages(self, partition_cols: List[str], 
                           order_cols: List[str],
                           numeric_cols: List[str],
                           window_sizes: List[int] = [3, 7, 30]) -> DataFrame:
        """Add moving averages with different window sizes."""
        result = self.df
        
        for window_size in window_sizes:
            window_spec = (Window.partitionBy(*partition_cols)
                          .orderBy(*order_cols)
                          .rowsBetween(-window_size + 1, Window.currentRow))
            
            for col_name in numeric_cols:
                if col_name in self.df.columns:
                    result = result.withColumn(
                        f"ma_{window_size}_{col_name}",
                        avg(col(col_name)).over(window_spec)
                    )
        
        logger.info(f"Added moving averages with window sizes {window_sizes} for columns {numeric_cols}")
        return result


### src/project_name/readers/base_reader.py
```python
"""Base reader class for data sources."""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame, SparkSession
import structlog

logger = structlog.get_logger()


class BaseReader(ABC):
    """Base class for all data readers."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.logger = logger.bind(reader=self.__class__.__name__)
    
    @abstractmethod
    def read(self) -> DataFrame:
        """Read data and return DataFrame."""
        pass
    
    def validate_config(self, required_keys: List[str]) -> None:
        """Validate that required configuration keys are present."""
        missing_keys = [key for key in required_keys if key not in self.config]
        if missing_keys:
            raise ValueError(f"Missing required configuration keys: {missing_keys}")
    
    def apply_schema(self, df: DataFrame, schema_name: Optional[str] = None) -> DataFrame:
        """Apply schema validation if specified."""
        if schema_name and "schemas" in self.config:
            # Schema validation logic would go here
            self.logger.info(f"Applied schema validation: {schema_name}")
        return df
    
    def apply_filters(self, df: DataFrame) -> DataFrame:
        """Apply any configured filters."""
        if "filters" in self.config:
            for filter_expr in self.config["filters"]:
                df = df.filter(filter_expr)
                self.logger.info(f"Applied filter: {filter_expr}")
        return df


### src/project_name/readers/file_reader.py
```python
"""File-based data readers."""

from typing import Dict, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
import structlog

from .base_reader import BaseReader

logger = structlog.get_logger()


class FileReader(BaseReader):
    """Generic file reader supporting multiple formats."""
    
    SUPPORTED_FORMATS = ["parquet", "csv", "json", "avro", "orc", "delta"]
    
    def read(self) -> DataFrame:
        """Read file(s) based on format."""
        self.validate_config(["path", "format"])
        
        format_type = self.config["format"].lower()
        path = self.config["path"]
        
        if format_type not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {format_type}. Supported: {self.SUPPORTED_FORMATS}")
        
        self.logger.info(f"Reading {format_type} files from {path}")
        
        # Get format-specific options
        options = self.config.get("options", {})
        
        if format_type == "csv":
            df = self._read_csv(path, options)
        elif format_type == "json":
            df = self._read_json(path, options)
        elif format_type == "parquet":
            df = self._read_parquet(path, options)
        elif format_type == "delta":
            df = self._read_delta(path, options)
        elif format_type == "avro":
            df = self._read_avro(path, options)
        elif format_type == "orc":
            df = self._read_orc(path, options)
        else:
            # Generic read
            df = self.spark.read.format(format_type).options(**options).load(path)
        
        # Apply post-read processing
        df = self.apply_schema(df, self.config.get("schema"))
        df = self.apply_filters(df)
        
        self.logger.info(f"Successfully read {df.count()} rows from {path}")
        return df
    
    def _read_csv(self, path: str, options: Dict[str, Any]) -> DataFrame:
        """Read CSV files with common defaults."""
        default_options = {
            "header": "true",
            "inferSchema": "true",
            "timestampFormat": "yyyy-MM-dd HH:mm:ss",
            "dateFormat": "yyyy-MM-dd"
        }
        default_options.update(options)
        
        reader = self.spark.read.options(**default_options)
        
        # Apply schema if provided
        if "schema" in self.config:
            schema = self.config["schema"]
            if isinstance(schema, StructType):
                reader = reader.schema(schema)
        
        return reader.csv(path)
    
    def _read_json(self, path: str, options: Dict[str, Any]) -> DataFrame:
        """Read JSON files."""
        default_options = {
            "timestampFormat": "yyyy-MM-dd HH:mm:ss",
            "dateFormat": "yyyy-MM-dd",
            "multiLine": "false"
        }
        default_options.update(options)
        
        return self.spark.read.options(**default_options).json(path)
    
    def _read_parquet(self, path: str, options: Dict[str, Any]) -> DataFrame:
        """Read Parquet files."""
        return self.spark.read.options(**options).parquet(path)
    
    def _read_delta(self, path: str, options: Dict[str, Any]) -> DataFrame:
        """Read Delta Lake tables."""
        return self.spark.read.options(**options).format("delta").load(path)
    
    def _read_avro(self, path: str, options: Dict[str, Any]) -> DataFrame:
        """Read Avro files."""
        return self.spark.read.options(**options).format("avro").load(path)
    
    def _read_orc(self, path: str, options: Dict[str, Any]) -> DataFrame:
        """Read ORC files."""
        return self.spark.read.options(**options).orc(path)


class ParquetReader(FileReader):
    """Specialized Parquet reader with optimization."""
    
    def read(self) -> DataFrame:
        """Read Parquet with optimizations."""
        self.validate_config(["path"])
        
        path = self.config["path"]
        options = self.config.get("options", {})
        
        # Parquet-specific optimizations
        reader = self.spark.read
        
        # Enable predicate pushdown if filters are specified
        if "partition_filters" in self.config:
            for filter_condition in self.config["partition_filters"]:
                reader = reader.filter(filter_condition)
        
        # Column pruning if specified
        if "select_columns" in self.config:
            df = reader.parquet(path)
            df = df.select(*self.config["select_columns"])
        else:
            df = reader.parquet(path)
        
        self.logger.info(f"Read Parquet files from {path} with optimizations")
        return df


### src/project_name/writers/base_writer.py
```python
"""Base writer class for data outputs."""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame
import structlog

logger = structlog.get_logger()


class BaseWriter(ABC):
    """Base class for all data writers."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logger.bind(writer=self.__class__.__name__)
    
    @abstractmethod
    def write(self, df: DataFrame) -> None:
        """Write DataFrame to destination."""
        pass
    
    def validate_config(self, required_keys: List[str]) -> None:
        """Validate that required configuration keys are present."""
        missing_keys = [key for key in required_keys if key not in self.config]
        if missing_keys:
            raise ValueError(f"Missing required configuration keys: {missing_keys}")
    
    def apply_pre_write_transformations(self, df: DataFrame) -> DataFrame:
        """Apply any pre-write transformations."""
        if "repartition" in self.config:
            num_partitions = self.config["repartition"]
            if "partition_by" in self.config:
                df = df.repartition(num_partitions, *self.config["partition_by"])
            else:
                df = df.repartition(num_partitions)
            self.logger.info(f"Repartitioned data to {num_partitions} partitions")
        
        if "coalesce" in self.config:
            num_partitions = self.config["coalesce"]
            df = df.coalesce(num_partitions)
            self.logger.info(f"Coalesced data to {num_partitions} partitions")
        
        return df


### src/project_name/writers/file_writer.py
```python
"""File-based data writers."""

from typing import Dict, Any, Optional
from pyspark.sql import DataFrame
import structlog

from .base_writer import BaseWriter

logger = structlog.get_logger()


class FileWriter(BaseWriter):
    """Generic file writer supporting multiple formats."""
    
    SUPPORTED_FORMATS = ["parquet", "csv", "json", "avro", "orc", "delta"]
    
    def write(self, df: DataFrame) -> None:
        """Write DataFrame to file(s) based on format."""
        self.validate_config(["path", "format"])
        
        format_type = self.config["format"].lower()
        path = self.config["path"]
        
        if format_type not in self.SUPPORTED_FORMATS:
            raise ValueError(f"Unsupported format: {format_type}. Supported: {self.SUPPORTED_FORMATS}")
        
        # Apply pre-write transformations
        df = self.apply_pre_write_transformations(df)
        
        self.logger.info(f"Writing {format_type} files to {path}")
        
        # Get format-specific options
        options = self.config.get("options", {})
        mode = self.config.get("mode", "overwrite")
        
        writer = df.write.mode(mode).options(**options)
        
        # Handle partitioning
        if "partition_by" in self.config:
            writer = writer.partitionBy(*self.config["partition_by"])
        
        # Write based on format
        if format_type == "csv":
            self._write_csv(writer, path, options)
        elif format_type == "json":
            self._write_json(writer, path)
        elif format_type == "parquet":
            self._write_parquet(writer, path)
        elif format_type == "delta":
            self._write_delta(writer, path)
        elif format_type == "avro":
            self._write_avro(writer, path)
        elif format_type == "orc":
            self._write_orc(writer, path)
        
        self.logger.info(f"Successfully wrote data to {path}")
    
    def _write_csv(self, writer, path: str, options: Dict[str, Any]) -> None:
        """Write CSV files with common defaults."""
        default_options = {
            "header": "true",
            "timestampFormat": "yyyy-MM-dd HH:mm:ss",
            "dateFormat": "yyyy-MM-dd"
        }
        default_options.update(options)
        writer.options(**default_options).csv(path)
    
    def _write_json(self, writer, path: str) -> None:
        """Write JSON files."""
        writer.json(path)
    
    def _write_parquet(self, writer, path: str) -> None:
        """Write Parquet files."""
        writer.parquet(path)
    
    def _write_delta(self, writer, path: str) -> None:
        """Write Delta Lake tables."""
        writer.format("delta").save(path)
    
    def _write_avro(self, writer, path: str) -> None:
        """Write Avro files."""
        writer.format("avro").save(path)
    
    def _write_orc(self, writer, path: str) -> None:
        """Write ORC files."""
        writer.orc(path)


class DeltaWriter(BaseWriter):
    """Specialized Delta Lake writer with advanced features."""
    
    def write(self, df: DataFrame) -> None:
        """Write to Delta Lake with optimizations."""
        self.validate_config(["path"])
        
        path = self.config["path"]
        mode = self.config.get("mode", "overwrite")
        
        # Apply pre-write transformations
        df = self.apply_pre_write_transformations(df)
        
        writer = df.write.format("delta").mode(mode)
        
        # Handle partitioning
        if "partition_by" in self.config:
            writer = writer.partitionBy(*self.config["partition_by"])
        
        # Delta-specific options
        if "merge_schema" in self.config:
            writer = writer.option("mergeSchema", str(self.config["merge_schema"]).lower())
        
        if "optimize_write" in self.config:
            writer = writer.option("optimizeWrite", str(self.config["optimize_write"]).lower())
        
        if "auto_compact" in self.config:
            writer = writer.option("autoCompact", str(self.config["auto_compact"]).lower())
        
        writer.save(path)
        
        # Post-write optimization
        if self.config.get("optimize_after_write", False):
            self._optimize_delta_table(path)
        
        self.logger.info(f"Successfully wrote Delta table to {path}")
    
    def _optimize_delta_table(self, path: str) -> None:
        """Optimize Delta table after write."""
        from delta.tables import DeltaTable
        
        delta_table = DeltaTable.forPath(df.sparkSession, path)
        delta_table.optimize()
        
        if "z_order_by" in self.config:
            delta_table.optimize().executeZOrderBy(*self.config["z_order_by"])
        
        self.logger.info(f"Optimized Delta table at {path}")
```# Enterprise PySpark Python Boilerplate

A production-ready boilerplate for Python and PySpark projects following enterprise best practices.

## Repository Structure