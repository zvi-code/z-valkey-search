

#!/usr/bin/env python3
"""
Workload and benchmark scenario definitions.
Provides workload parsing, scenario configuration, and related utilities.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Dict, Any, Tuple

# Import the tag configuration and vector types that BenchmarkScenario needs
from .tags_builder import TagSharingMode, TagsConfig
from .hash_generator import VectorAlgorithm, VectorMetric


class WorkloadType(Enum):
    """Types of workload operations"""
    DELETE = "del"
    QUERY = "query"
    INSERT = "insert"
    OVERWRITE = "overwrite"

@dataclass
class WorkloadOperation:
    """Single workload operation within a stage"""
    type: WorkloadType
    target_value: float = 0.0  # For percentage-based operations (0.0-1.0)
    duration_seconds: int = 0  # For time-based operations

    def __str__(self):
        if self.target_value != 0.0:
            if self.type in [WorkloadType.DELETE, WorkloadType.INSERT]:
                return f"{self.type.value}:{self.target_value*100:.0f}%"
            else:
                return f"{self.type.value}:{self.target_value:.0f}%"
        elif self.duration_seconds != 0:
            return f"{self.type.value}:{self.duration_seconds}s"
        else:
            return self.type.value

@dataclass
class WorkloadStage:
    """A stage containing one or more parallel operations"""
    name: str
    operations: List[WorkloadOperation]
    duration_seconds: int = 0  # Optional stage duration override
    
    def __str__(self):
        ops_str = "+".join(str(op) for op in self.operations)
        if self.duration_seconds:
            return f"{self.name}[{ops_str}]:{self.duration_seconds}s"
        return f"{self.name}[{ops_str}]"

class WorkloadParser:
    """Parser for workload stage descriptions"""
    
    @staticmethod
    def parse_workload_string(workload_str: str) -> List[WorkloadStage]:
        """
        Parse workload string like:
        "del:50%,query+insert:80%,query:5min,query+del:50%,insert:100%,overwrite:10%:1min"
        
        Returns list of WorkloadStage objects
        """
        stages = []
        stage_strings = workload_str.split(',')
        
        for i, stage_str in enumerate(stage_strings):
            stage_str = stage_str.strip()
            stage_name = f"stage_{i+1}"
            operations = []
            stage_duration = 0
            
            # Check if stage has explicit duration at the end
            if ':' in stage_str and (stage_str.endswith('min') or stage_str.endswith('s')):
                parts = stage_str.rsplit(':', 1)
                stage_str = parts[0]
                duration_str = parts[1]
                stage_duration = WorkloadParser._parse_duration(duration_str)
            
            # Parse parallel operations (separated by +)
            op_strings = stage_str.split('+')
            
            for op_str in op_strings:
                op_str = op_str.strip()
                operation = WorkloadParser._parse_operation(op_str)
                if operation:
                    operations.append(operation)
            
            if operations:
                stages.append(WorkloadStage(
                    name=stage_name,
                    operations=operations,
                    duration_seconds=stage_duration
                ))
        
        return stages
    
    @staticmethod
    def _parse_operation(op_str: str) -> Optional[WorkloadOperation]:
        """Parse a single operation string like 'del:50%' or 'query:5min'"""
        if ':' not in op_str:
            # Simple operation without parameters
            op_type = WorkloadParser._get_workload_type(op_str)
            if op_type:
                return WorkloadOperation(type=op_type)
            return None
        
        parts = op_str.split(':', 1)
        op_name = parts[0].strip()
        op_value = parts[1].strip()
        
        op_type = WorkloadParser._get_workload_type(op_name)
        if not op_type:
            return None
        
        # Delegate parsing to helper methods
        percentage = WorkloadParser._parse_percentage_value(op_value)
        if percentage is not None:
            return WorkloadOperation(type=op_type, target_value=percentage)
        
        duration = WorkloadParser._parse_duration_value(op_value)
        if duration is not None:
            return WorkloadOperation(type=op_type, duration_seconds=duration)
        
        raw_number = WorkloadParser._parse_raw_number(op_value)
        if raw_number is not None:
            return WorkloadOperation(type=op_type, target_value=raw_number)
        
        return None
    
    @staticmethod
    def _get_workload_type(name: str) -> Optional[WorkloadType]:
        """Convert string to WorkloadType enum"""
        name = name.lower().strip()
        for wt in WorkloadType:
            if wt.value == name:
                return wt
        # Also support full names
        if name == "delete":
            return WorkloadType.DELETE
        elif name == "query":
            return WorkloadType.QUERY
        elif name == "insert":
            return WorkloadType.INSERT
        elif name == "overwrite":
            return WorkloadType.OVERWRITE
        return None
    
    @staticmethod
    def _parse_duration(duration_str: str) -> Optional[int]:
        """Parse duration string to seconds"""
        duration_str = duration_str.strip()
        try:
            if duration_str.endswith('min'):
                return int(float(duration_str[:-3]) * 60)
            elif duration_str.endswith('s'):
                return int(float(duration_str[:-1]))
            else:
                return int(float(duration_str))
        except ValueError:
            return None

@dataclass
class BenchmarkScenario:
    """Configuration for a benchmark scenario"""
    name: str
    total_keys: int
    tags_config: TagsConfig
    description: str
    # Vector configuration
    vector_dim: int = 8
    vector_algorithm: VectorAlgorithm = VectorAlgorithm.HNSW
    vector_metric: VectorMetric = VectorMetric.COSINE
    hnsw_m: int = 16  # HNSW M parameter (number of connections)
    # Numeric configuration  
    include_numeric: bool = True
    numeric_fields: Dict[str, Tuple[float, float]] = field(default_factory=lambda: {
        "score": (0.0, 100.0),
        "timestamp": (1000000000.0, 2000000000.0)
    })
    # Workload stages configuration
    workload_stages: List[WorkloadStage] = field(default_factory=list)
    workload_string: str = ""  # Raw workload string to parse
    
    def __post_init__(self):
        """Parse workload string if provided"""
        if self.workload_string and not self.workload_stages:
            self.workload_stages = WorkloadParser.parse_workload_string(self.workload_string)
    def get_index_config(self) -> Dict[str, Any]:
        """Get index configuration based on scenario parameters"""
        # Prepare index configuration for monitor
        index_config = {
            'type': 'vector',
            'vector_dim': self.vector_dim,
            'vector_algorithm': self.vector_algorithm.name if hasattr(self.vector_algorithm, 'name') else str(self.vector_algorithm),
            'hnsw_m': self.hnsw_m,
            'num_tag_fields': 1,  # We always have tags field
            'num_numeric_fields': len(self.numeric_fields) if self.numeric_fields else 0,
            'num_vector_fields': 1,  # We always have vector field
            
            # Tag configuration from self
            'tag_avg_length': self.tags_config.tag_length.avg if self.tags_config.tag_length else 0,
            'tag_prefix_sharing': self.tags_config.tag_prefix.share_probability if self.tags_config.tag_prefix else 0,
            'tag_avg_per_key': self.tags_config.tags_per_key.avg if self.tags_config.tags_per_key else 0,
            
            # Calculate tag sharing metrics based on sharing mode
            'tag_avg_keys_per_tag': 0,  # Will be calculated based on sharing mode
            'tag_unique_ratio': 0,  # Will be calculated based on sharing mode
            'tag_reuse_factor': 0,  # Will be calculated based on sharing mode
            
            # Numeric fields info
            'numeric_fields_names': ','.join(self.numeric_fields.keys()) if self.numeric_fields else '',
            'numeric_fields_ranges': ';'.join([f"{k}:{v[0]}-{v[1]}" for k, v in self.numeric_fields.items()]) if self.numeric_fields else ''
        }
                # Calculate tag sharing metrics based on sharing mode
        if self.tags_config.sharing:
            if self.tags_config.sharing.mode == TagSharingMode.UNIQUE:
                index_config['tag_unique_ratio'] = 1.0
                index_config['tag_avg_keys_per_tag'] = 1.0
            elif self.tags_config.sharing.mode == TagSharingMode.SHARED_POOL:
                pool_size = self.tags_config.sharing.pool_size or 1000
                index_config['tag_unique_ratio'] = pool_size / self.total_keys if self.total_keys > 0 else 0
                index_config['tag_avg_keys_per_tag'] = self.total_keys / pool_size if pool_size > 0 else 0
                index_config['tag_reuse_factor'] = self.tags_config.sharing.reuse_probability
            elif self.tags_config.sharing.mode == TagSharingMode.GROUP_BASED:
                keys_per_group = self.tags_config.sharing.keys_per_group or 100
                index_config['tag_avg_keys_per_tag'] = keys_per_group
        return index_config 


