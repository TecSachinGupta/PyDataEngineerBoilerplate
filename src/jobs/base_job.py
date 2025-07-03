"""Base job class for all Spark jobs."""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from ..config.spark_config import create_spark_session
from ..utils.logging_utils import setup_logging


class BaseJob(ABC):
    """Base class for all Spark jobs."""
    
    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        config: Optional[Dict[str, Any]] = None
    ):
        """Initialize the job."""
        self.config = config or {}
        self.spark = spark or create_spark_session()
        self.logger = setup_logging(self.__class__.__name__)
        
    def run(self) -> None:
        """Run the job."""
        try:
            self.logger.info(f"Starting job: {self.__class__.__name__}")
            self._validate_config()
            self.execute()
            self.logger.info(f"Completed job: {self.__class__.__name__}")
        except Exception as e:
            self.logger.error(f"Job failed: {self.__class__.__name__} - {str(e)}")
            raise
        finally:
            self._cleanup()
    
    @abstractmethod
    def execute(self) -> None:
        """Execute the main job logic."""
        pass
    
    def _validate_config(self) -> None:
        """Validate job configuration."""
        required_configs = self.get_required_configs()
        missing_configs = [
            config for config in required_configs
            if config not in self.config
        ]
        
        if missing_configs:
            raise ValueError(f"Missing required configurations: {missing_configs}")
    
    def get_required_configs(self) -> list:
        """Return list of required configuration keys."""
        return []
    
    def _cleanup(self) -> None:
        """Cleanup resources."""
        if hasattr(self, 'spark') and self.spark:
            # Don't stop spark session if it was passed in
            pass