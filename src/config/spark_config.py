"""Spark configuration utilities."""

from typing import Dict, Any
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from .settings import settings


def create_spark_session(
    app_name: str = None,
    config: Dict[str, Any] = None
) -> SparkSession:
    """Create and configure Spark session."""
    
    app_name = app_name or settings.spark_app_name
    
    # Default Spark configuration
    spark_config = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.repl.eagerEval.enabled": "true",
        "spark.sql.repl.eagerEval.maxNumRows": "20",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    }
    
    # Merge with custom config
    if config:
        spark_config.update(config)
    
    # Create Spark configuration
    conf = SparkConf()
    for key, value in spark_config.items():
        conf.set(key, value)
    
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(settings.spark_master)
        .config(conf=conf)
        .getOrCreate()
    )
    
    # Set log level
    spark.sparkContext.setLogLevel(settings.spark_log_level)
    
    return spark


def get_spark_config_for_environment(env: str) -> Dict[str, Any]:
    """Get Spark configuration for specific environment."""
    configs = {
        "dev": {
            "spark.sql.shuffle.partitions": "2",
            "spark.default.parallelism": "2",
        },
        "staging": {
            "spark.sql.shuffle.partitions": "200",
            "spark.default.parallelism": "100",
        },
        "prod": {
            "spark.sql.shuffle.partitions": "400",
            "spark.default.parallelism": "200",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",
        }
    }
    
    return configs.get(env, configs["dev"])