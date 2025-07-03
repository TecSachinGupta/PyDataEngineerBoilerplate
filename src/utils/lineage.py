"""Data lineage tracking utilities."""

import json
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum

import structlog
from pyspark.sql import DataFrame

logger = structlog.get_logger()


class DatasetType(Enum):
    """Types of datasets."""
    FILE = "file"
    TABLE = "table"
    VIEW = "view"
    STREAM = "stream"


@dataclass
class Dataset:
    """Dataset information for lineage tracking."""
    name: str
    type: DatasetType
    location: str
    schema: Optional[Dict[str, str]] = None
    partition_columns: Optional[List[str]] = None
    properties: Optional[Dict[str, Any]] = None


@dataclass
class Transformation:
    """Transformation information."""
    name: str
    type: str
    description: str
    sql: Optional[str] = None
    function_name: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None


@dataclass
class LineageNode:
    """Lineage node representing a dataset and its transformations."""
    run_id: str
    job_name: str
    dataset: Dataset
    inputs: List[Dataset]
    transformations: List[Transformation]
    timestamp: datetime
    user: str
    environment: str
    metrics: Optional[Dict[str, Any]] = None


class LineageTracker:
    """Track data lineage across transformations."""
    
    def __init__(self, job_name: str, environment: str = "dev"):
        self.job_name = job_name
        self.environment = environment
        self.run_id = str(uuid.uuid4())
        self.user = "system"  # Could be fetched from environment
        self.lineage_nodes: List[LineageNode] = []
        self.current_inputs: List[Dataset] = []
        self.current_transformations: List[Transformation] = []
    
    def track_input(self, dataset: Dataset):
        """Track an input dataset."""
        self.current_inputs.append(dataset)
        logger.info(f"Tracked input dataset: {dataset.name}")
    
    def track_transformation(self, transformation: Transformation):
        """Track a transformation."""
        self.current_transformations.append(transformation)
        logger.info(f"Tracked transformation: {transformation.name}")
    
    def track_output(self, dataset: Dataset, metrics: Optional[Dict[str, Any]] = None):
        """Track an output dataset and create lineage node."""
        node = LineageNode(
            run_id=self.run_id,
            job_name=self.job_name,
            dataset=dataset,
            inputs=self.current_inputs.copy(),
            transformations=self.current_transformations.copy(),
            timestamp=datetime.now(),
            user=self.user,
            environment=self.environment,
            metrics=metrics
        )
        
        self.lineage_nodes.append(node)
        logger.info(f"Tracked output dataset: {dataset.name}")
        
        # Reset for next transformation
        self.current_inputs.clear()
        self.current_transformations.clear()
    
    def get_lineage_graph(self) -> Dict[str, Any]:
        """Get the complete lineage graph."""
        return {
            'run_id': self.run_id,
            'job_name': self.job_name,
            'environment': self.environment,
            'nodes': [asdict(node) for node in self.lineage_nodes],
            'created_at': datetime.now().isoformat()
        }
    
    def export_lineage(self, output_path: str):
        """Export lineage to JSON file."""
        lineage_graph = self.get_lineage_graph()
        
        with open(output_path, 'w') as f:
            json.dump(lineage_graph, f, indent=2, default=str)
        
        logger.info(f"Lineage exported to: {output_path}")


def create_dataset_from_dataframe(df: DataFrame, name: str, dataset_type: DatasetType, 
                                 location: str) -> Dataset:
    """Create Dataset object from Spark DataFrame."""
    schema = {field.name: str(field.dataType) for field in df.schema.fields}
    
    return Dataset(
        name=name,
        type=dataset_type,
        location=location,
        schema=schema,
        properties={
            'row_count': df.count(),
            'column_count': len(df.columns),
            'columns': df.columns
        }
    )


def track_dataframe_lineage(lineage_tracker: LineageTracker, df: DataFrame, 
                          name: str, operation: str):
    """Decorator to automatically track DataFrame lineage."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Track transformation
            transformation = Transformation(
                name=operation,
                type="dataframe_operation",
                description=f"Applied {operation} to {name}",
                function_name=func.__name__
            )
            lineage_tracker.track_transformation(transformation)
            
            # Execute function
            result = func(*args, **kwargs)
            
            # Track output if result is DataFrame
            if isinstance(result, DataFrame):
                output_dataset = create_dataset_from_dataframe(
                    result, f"{name}_transformed", DatasetType.VIEW, "memory"
                )
                lineage_tracker.track_output(output_dataset)
            
            return result
        return wrapper
    return decorator