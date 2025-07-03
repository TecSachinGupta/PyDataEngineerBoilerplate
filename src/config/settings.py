"""Configuration management for the project."""

import os
from typing import Any, Dict, Optional

import yaml
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Application settings."""
    
    # Environment
    environment: str = Field(default="dev", env="ENVIRONMENT")
    debug: bool = Field(default=False, env="DEBUG")
    
    # Spark Configuration
    spark_app_name: str = Field(default="ProjectApp", env="SPARK_APP_NAME")
    spark_master: str = Field(default="local[*]", env="SPARK_MASTER")
    spark_log_level: str = Field(default="WARN", env="SPARK_LOG_LEVEL")
    
    # Data Sources
    input_path: str = Field(default="data/input", env="INPUT_PATH")
    output_path: str = Field(default="data/output", env="OUTPUT_PATH")
    
    # Database
    database_url: Optional[str] = Field(default=None, env="DATABASE_URL")
    
    # Logging
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        env="LOG_FORMAT"
    )
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    if config_path is None:
        env = os.getenv("ENVIRONMENT", "dev")
        config_path = f"config/{env}.yaml"
    
    try:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        return {}


# Global settings instance
settings = Settings()