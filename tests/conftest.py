"""Pytest configuration and fixtures."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session for testing."""
    spark = (
        SparkSession.builder
        .appName("test")
        .master("local[2]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "input_path": "tests/fixtures/input",
        "output_path": "tests/fixtures/output",
        "batch_size": 1000
    }