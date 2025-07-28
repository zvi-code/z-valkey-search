#!/usr/bin/env python3
"""
Hash key generator for Valkey with vector and tag fields.
Generates HASH keys compatible with FT.CREATE index schemas.
"""

import numpy as np
from typing import Iterator, Optional, Dict, List, Any, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum

from string_generator import (
    StringGenerator, GeneratorConfig, LengthConfig, PrefixConfig,
    Distribution, StringType
)
from tags_builder import (
    TagsBuilder, TagsConfig, TagDistribution, TagSharingConfig,
    TagSharingMode
)


class FieldType(Enum):
    """Valkey Search field types"""
    TEXT = "TEXT"
    TAG = "TAG"
    NUMERIC = "NUMERIC"
    GEO = "GEO"
    VECTOR = "VECTOR"


class VectorAlgorithm(Enum):
    """Vector similarity algorithms"""
    FLAT = "FLAT"
    HNSW = "HNSW"


class VectorMetric(Enum):
    """Vector distance metrics"""
    L2 = "L2"
    IP = "IP"  # Inner Product
    COSINE = "COSINE"


@dataclass
class VectorFieldSchema:
    """Schema for a VECTOR field"""
    algorithm: VectorAlgorithm = VectorAlgorithm.FLAT
    dim: int = 768
    distance_metric: VectorMetric = VectorMetric.L2
    datatype: str = "FLOAT32"
    # Optional: initial capacity, block size, etc.
    initial_cap: Optional[int] = None
    block_size: Optional[int] = None
    # HNSW specific parameters
    m: Optional[int] = None  # Number of connections for HNSW
    ef_construction: Optional[int] = None  # Size of dynamic candidate list
    ef_runtime: Optional[int] = None  # Size of dynamic candidate list for search
    epsilon: Optional[float] = None  # Relative factor for early termination


@dataclass
class FieldSchema:
    """Schema for a single field in the index"""
    name: str
    type: FieldType
    # For VECTOR fields
    vector_config: Optional[VectorFieldSchema] = None
    # For TAG fields
    separator: str = ","
    # For TEXT fields
    weight: float = 1.0
    sortable: bool = False
    no_index: bool = False
    # For NUMERIC fields
    numeric_range: Optional[Tuple[float, float]] = None  # (min, max)
    numeric_distribution: str = "uniform"  # "uniform", "normal", "exponential"


@dataclass
class IndexSchema:
    """Complete index schema for FT.CREATE"""
    index_name: str
    prefix: List[str]  # Key prefixes to index
    fields: List[FieldSchema]
    # Optional configurations
    filter: Optional[str] = None
    language: str = "english"
    score_field: Optional[str] = None
    payload_field: Optional[str] = None


@dataclass
class HashGeneratorConfig:
    """Configuration for hash key generation"""
    # Number of keys to generate
    num_keys: int
    
    # Index schema
    schema: IndexSchema
    
    # Key generation config
    key_prefix: Optional[str] = None  # If None, uses schema prefixes
    key_length: Optional[LengthConfig] = None
    key_string_type: StringType = StringType.ALPHANUMERIC
    
    # Tag generation config (if TAG fields exist)
    tags_config: Optional[TagsConfig] = None
    
    # Vector generation config
    vector_normalize: bool = False  # Normalize vectors for cosine similarity
    vector_distribution: str = "normal"  # "normal" or "uniform"
    vector_sparsity: float = 0.0  # Fraction of zero elements (0.0 to 1.0)
    
    # Numeric field generation config
    numeric_defaults: Dict[str, Tuple[float, float]] = field(default_factory=dict)  # field_name -> (min, max)
    
    # Additional fields to generate
    additional_fields: Dict[str, Any] = field(default_factory=dict)
    
    # Performance
    batch_size: int = 1000
    seed: Optional[int] = None


class HashKeyGenerator:
    """
    Generates HASH keys with fields matching a Valkey Search index schema.
    
    Example:
        # Define schema
        schema = IndexSchema(
            index_name="products",
            prefix=["product:"],
            fields=[
                FieldSchema(name="description", type=FieldType.TEXT),
                FieldSchema(name="tags", type=FieldType.TAG),
                FieldSchema(
                    name="embedding",
                    type=FieldType.VECTOR,
                    vector_config=VectorFieldSchema(dim=768)
                )
            ]
        )
        
        # Generate hashes
        config = HashGeneratorConfig(
            num_keys=10000,
            schema=schema,
            tags_config=TagsConfig(...)
        )
        
        gen = HashKeyGenerator(config)
        for batch in gen:
            # Each batch contains tuples of (key, fields_dict)
            for key, fields in batch:
                # Use HSET key field1 value1 field2 value2...
                pass
    """
    
    def __init__(self, config: HashGeneratorConfig):
        self.config = config
        self._validate_config()
        
        if config.seed:
            np.random.seed(config.seed)
        
        # Initialize generators
        self._key_generator = None
        self._tags_generator = None
        self._field_generators = {}
        
        self._initialize_generators()
    
    def _validate_config(self):
        """Validate configuration consistency"""
        if self.config.num_keys <= 0:
            raise ValueError("num_keys must be positive")
        
        # Check if we have TAG fields but no tags config
        has_tag_field = any(f.type == FieldType.TAG for f in self.config.schema.fields)
        if has_tag_field and self.config.tags_config is None:
            # Create default tags config
            self.config.tags_config = TagsConfig(
                num_keys=self.config.num_keys,
                tags_per_key=TagDistribution(avg=5, min=2, max=10),
                tag_length=LengthConfig(avg=10, min=5, max=20),
                batch_size=self.config.batch_size
            )
        
        # Set default key prefix if not specified
        if self.config.key_prefix is None:
            if self.config.schema.prefix:
                self.config.key_prefix = self.config.schema.prefix[0]
            else:
                self.config.key_prefix = "doc:"
        
        # Set default key length if not specified
        if self.config.key_length is None:
            self.config.key_length = LengthConfig(avg=10, min=8, max=15)
    
    def _initialize_generators(self):
        """Initialize and validate configurations without creating sub-generators"""
        # Only validate configurations - no actual generator creation
        for field in self.config.schema.fields:
            if field.type == FieldType.NUMERIC:
                # Validate numeric configuration
                if field.numeric_range:
                    min_val, max_val = field.numeric_range
                    if min_val >= max_val:
                        raise ValueError(f"Invalid numeric range for field {field.name}: min ({min_val}) >= max ({max_val})")
                
                if field.numeric_distribution not in ["uniform", "normal", "exponential"]:
                    raise ValueError(f"Invalid numeric distribution for field {field.name}: {field.numeric_distribution}")
            
            elif field.type == FieldType.VECTOR:
                # Validate vector configuration
                if not field.vector_config:
                    raise ValueError(f"Vector field {field.name} missing vector_config")
                
                cfg = field.vector_config
                if cfg.dim <= 0:
                    raise ValueError(f"Invalid vector dimension for field {field.name}: {cfg.dim}")
                
                if cfg.algorithm == VectorAlgorithm.HNSW:
                    # Set HNSW defaults if not specified
                    if cfg.m is None:
                        cfg.m = 16
                    if cfg.ef_construction is None:
                        cfg.ef_construction = 200
                    
                    # Validate HNSW parameters
                    if cfg.m <= 0:
                        raise ValueError(f"Invalid HNSW M parameter for field {field.name}: {cfg.m}")
                    if cfg.ef_construction and cfg.ef_construction < cfg.m:
                        raise ValueError(f"HNSW ef_construction ({cfg.ef_construction}) must be >= M ({cfg.m})")
                
                # Validate sparsity
                if self.config.vector_sparsity < 0 or self.config.vector_sparsity >= 1:
                    raise ValueError(f"Invalid vector sparsity: {self.config.vector_sparsity} (must be in [0, 1))")
    
    def _generate_vector(self, dim: int, metric: VectorMetric) -> bytes:
        """Generate a single vector based on configuration"""
        if self.config.vector_distribution == "uniform":
            vec = np.random.uniform(-1, 1, size=dim).astype(np.float32)
        else:  # normal
            vec = np.random.randn(dim).astype(np.float32)
        
        # Apply sparsity if requested
        if self.config.vector_sparsity > 0:
            # Randomly zero out elements
            mask = np.random.random(dim) > self.config.vector_sparsity
            vec = vec * mask
        
        # Normalize for cosine similarity
        if self.config.vector_normalize or metric == VectorMetric.COSINE:
            norm = np.linalg.norm(vec)
            if norm > 0:
                vec = vec / norm
        
        return vec.tobytes()
    
    def _generate_numeric_value(self, field_schema: FieldSchema) -> float:
        """Generate a numeric value based on field configuration"""
        # Check if there's a specific range configured for this field
        if field_schema.name in self.config.numeric_defaults:
            min_val, max_val = self.config.numeric_defaults[field_schema.name]
        elif field_schema.numeric_range:
            min_val, max_val = field_schema.numeric_range
        else:
            # Default range
            min_val, max_val = 0, 1000
        
        # Generate based on distribution
        if field_schema.numeric_distribution == "normal":
            # Generate normal distribution centered at midpoint
            center = (min_val + max_val) / 2
            stddev = (max_val - min_val) / 6  # 99.7% within range
            value = np.random.normal(center, stddev)
            # Clip to range
            value = np.clip(value, min_val, max_val)
        elif field_schema.numeric_distribution == "exponential":
            # Generate exponential distribution
            # Map [0, inf) to [min_val, max_val]
            exp_val = np.random.exponential(1.0)
            # Use a sigmoid-like transformation
            value = min_val + (max_val - min_val) * (1 - np.exp(-exp_val))
        else:  # uniform
            value = np.random.uniform(min_val, max_val)
        
        return float(value)
    
    def _generate_geo_value(self) -> str:
        """Generate a geo coordinate"""
        # Format: longitude,latitude
        lon = np.random.uniform(-180, 180)
        lat = np.random.uniform(-90, 90)
        return f"{lon:.6f},{lat:.6f}"
    
    def _generate_fields(self, key_index: int) -> Dict[str, Any]:
        """
        Generate fields for a single key on-demand.
        This allows for lazy generation without pre-materializing data.
        """
        fields = {}
        
        # Generate fields based on schema
        for field in self.config.schema.fields:
            if field.type == FieldType.VECTOR:
                # Generate vector
                vec = self._generate_vector(
                    field.vector_config.dim,
                    field.vector_config.distance_metric
                )
                fields[field.name] = vec
            
            elif field.type == FieldType.TAG:
                # Generate tags on-demand without pre-materialization
                if self.config.tags_config:
                    # Generate tags based on key index for consistency
                    tags = self._generate_tags_for_key(key_index)
                    fields[field.name] = tags
            
            elif field.type == FieldType.TEXT:
                # Generate text on-demand
                text_length = self._get_text_length_for_key(key_index)
                text = self._generate_text_content(text_length)
                fields[field.name] = text
            
            elif field.type == FieldType.NUMERIC:
                fields[field.name] = self._generate_numeric_value(field)
            
            elif field.type == FieldType.GEO:
                fields[field.name] = self._generate_geo_value()
        
        # Add any additional fields
        fields.update(self.config.additional_fields)
        
        return fields
    
    def _generate_tags_for_key(self, key_index: int) -> str:
        """Generate tags for a specific key index without pre-materialization"""
        if not self.config.tags_config:
            return ""
        
        tags_config = self.config.tags_config
        sharing = tags_config.sharing
        
        # Determine number of tags for this key
        np.random.seed(self.config.seed + key_index if self.config.seed else key_index)
        num_tags = np.random.randint(
            tags_config.tags_per_key.min,
            tags_config.tags_per_key.max + 1
        )
        
        tags = []
        
        if sharing.mode == TagSharingMode.UNIQUE:
            # Generate unique tags for this key
            base_tag_index = key_index * tags_config.tags_per_key.avg
            for i in range(num_tags):
                tag = self._generate_single_tag(base_tag_index + i)
                tags.append(tag)
        
        elif sharing.mode == TagSharingMode.SHARED_POOL:
            # Select from a virtual pool without materializing it
            pool_size = sharing.pool_size or max(10, self.config.num_keys // 10)
            for i in range(num_tags):
                # Use consistent hashing to select tags from virtual pool
                tag_index = (key_index * 31 + i * 17) % pool_size
                tag = self._generate_single_tag(tag_index)
                tags.append(tag)
        
        elif sharing.mode == TagSharingMode.PERFECT_OVERLAP:
            # All keys get the same tags
            for i in range(num_tags):
                tag = self._generate_single_tag(i)
                tags.append(tag)
        
        elif sharing.mode == TagSharingMode.GROUP_BASED:
            # Keys in same group share tags
            keys_per_group = sharing.keys_per_group or 100
            group_id = key_index // keys_per_group
            tags_per_group = sharing.tags_per_group or 10
            
            # Select tags for this group
            base_tag_index = group_id * tags_per_group
            tag_indices = np.random.choice(tags_per_group, size=num_tags, replace=False)
            for idx in tag_indices:
                tag = self._generate_single_tag(base_tag_index + idx)
                tags.append(tag)
        
        return ','.join(tags)
    
    def _generate_single_tag(self, tag_index: int) -> str:
        """Generate a single tag based on its index"""
        # Use deterministic generation based on tag index
        np.random.seed(tag_index)
        
        tag_length = self.config.tags_config.tag_length
        length = np.random.randint(tag_length.min, tag_length.max + 1)
        
        if self.config.tags_config.tag_string_type == StringType.ALPHANUMERIC:
            chars = 'abcdefghijklmnopqrstuvwxyz0123456789'
            tag = ''.join(np.random.choice(list(chars), size=length))
        else:
            # Default to random bytes
            tag = f"tag_{tag_index:08d}"
        
        return tag
    
    def _get_text_length_for_key(self, key_index: int) -> int:
        """Get text length for a specific key without pre-materialization"""
        np.random.seed(self.config.seed + key_index if self.config.seed else key_index)
        return np.random.randint(20, 100)
    
    def _generate_text_content(self, length: int) -> str:
        """Generate text content of specified length"""
        chars = 'abcdefghijklmnopqrstuvwxyz '
        return ''.join(np.random.choice(list(chars), size=length))
    
    def __iter__(self) -> Iterator[List[Tuple[str, Dict[str, Any]]]]:
        """
        Truly lazy iterator returning batches of (key, fields) tuples.
        Generates data on-demand without pre-materialization.
        """
        keys_generated = 0
        
        while keys_generated < self.config.num_keys:
            batch_size = min(self.config.batch_size, self.config.num_keys - keys_generated)
            batch = []
            
            for i in range(batch_size):
                key_index = keys_generated + i
                
                # Generate key
                key = f"{self.config.key_prefix}{key_index:08d}"
                
                # Generate fields on-demand
                fields = self._generate_fields(key_index)
                
                batch.append((key, fields))
            
            keys_generated += len(batch)
            yield batch
    
    def generate_ft_create_command(self) -> str:
        """Generate the FT.CREATE command for this schema"""
        cmd_parts = [
            "FT.CREATE",
            self.config.schema.index_name,
            "ON", "HASH"
        ]
        
        # Add prefix
        if self.config.schema.prefix:
            cmd_parts.extend(["PREFIX", str(len(self.config.schema.prefix))])
            cmd_parts.extend(self.config.schema.prefix)
        
        # Add schema
        cmd_parts.append("SCHEMA")
        
        for field in self.config.schema.fields:
            cmd_parts.append(field.name)
            
            if field.type == FieldType.VECTOR:
                cfg = field.vector_config
                cmd_parts.extend(["VECTOR", cfg.algorithm.value])
                
                # Build parameters list
                params = [
                    "TYPE", cfg.datatype,
                    "DIM", str(cfg.dim),
                    "DISTANCE_METRIC", cfg.distance_metric.value
                ]
                
                # Add algorithm-specific parameters
                if cfg.algorithm == VectorAlgorithm.FLAT:
                    if cfg.initial_cap is not None:
                        params.extend(["INITIAL_CAP", str(cfg.initial_cap)])
                    if cfg.block_size is not None:
                        params.extend(["BLOCK_SIZE", str(cfg.block_size)])
                elif cfg.algorithm == VectorAlgorithm.HNSW:
                    if cfg.m is not None:
                        params.extend(["M", str(cfg.m)])
                    if cfg.ef_construction is not None:
                        params.extend(["EF_CONSTRUCTION", str(cfg.ef_construction)])
                    if cfg.ef_runtime is not None:
                        params.extend(["EF_RUNTIME", str(cfg.ef_runtime)])
                    if cfg.epsilon is not None:
                        params.extend(["EPSILON", str(cfg.epsilon)])
                
                cmd_parts.extend([str(len(params))] + params)
            
            elif field.type == FieldType.TAG:
                cmd_parts.extend(["TAG", "SEPARATOR", field.separator])
            
            elif field.type == FieldType.TEXT:
                cmd_parts.append("TEXT")
                if field.weight != 1.0:
                    cmd_parts.extend(["WEIGHT", str(field.weight)])
                if field.sortable:
                    cmd_parts.append("SORTABLE")
            
            elif field.type == FieldType.NUMERIC:
                cmd_parts.append("NUMERIC")
                if field.sortable:
                    cmd_parts.append("SORTABLE")
            
            elif field.type == FieldType.GEO:
                cmd_parts.append("GEO")
        
        return " ".join(cmd_parts)


# Convenience functions
def create_simple_schema(
    index_name: str,
    vector_dim: int,
    include_tags: bool = True,
    include_text: bool = True,
    include_numeric: bool = False,
    prefix: str = "doc:",
    vector_algorithm: VectorAlgorithm = VectorAlgorithm.FLAT,
    vector_metric: VectorMetric = VectorMetric.L2
) -> IndexSchema:
    """Create a simple schema with common fields"""
    fields = []
    
    if include_text:
        fields.append(FieldSchema(name="content", type=FieldType.TEXT))
    
    if include_tags:
        fields.append(FieldSchema(name="tags", type=FieldType.TAG))
    
    if include_numeric:
        fields.append(FieldSchema(
            name="score",
            type=FieldType.NUMERIC,
            numeric_range=(0, 100),
            numeric_distribution="normal"
        ))
    
    # Always include vector
    vector_config = VectorFieldSchema(
        algorithm=vector_algorithm,
        dim=vector_dim,
        distance_metric=vector_metric
    )
    
    # Add HNSW defaults if using HNSW
    if vector_algorithm == VectorAlgorithm.HNSW:
        vector_config.m = 16
        vector_config.ef_construction = 200
    
    fields.append(FieldSchema(
        name="embedding",
        type=FieldType.VECTOR,
        vector_config=vector_config
    ))
    
    return IndexSchema(
        index_name=index_name,
        prefix=[prefix],
        fields=fields
    )


def create_hnsw_vector_field(
    name: str = "embedding",
    dim: int = 768,
    metric: VectorMetric = VectorMetric.L2,
    m: int = 16,
    ef_construction: int = 200,
    ef_runtime: Optional[int] = None,
    epsilon: Optional[float] = None
) -> FieldSchema:
    """Create an HNSW vector field with common configurations"""
    return FieldSchema(
        name=name,
        type=FieldType.VECTOR,
        vector_config=VectorFieldSchema(
            algorithm=VectorAlgorithm.HNSW,
            dim=dim,
            distance_metric=metric,
            m=m,
            ef_construction=ef_construction,
            ef_runtime=ef_runtime,
            epsilon=epsilon
        )
    )


def create_numeric_field(
    name: str,
    min_val: float = 0,
    max_val: float = 1000,
    distribution: str = "uniform",
    sortable: bool = False
) -> FieldSchema:
    """Create a numeric field with specified range and distribution"""
    return FieldSchema(
        name=name,
        type=FieldType.NUMERIC,
        numeric_range=(min_val, max_val),
        numeric_distribution=distribution,
        sortable=sortable
    )


def generate_hashes(
    schema: IndexSchema,
    num_keys: int,
    tags_config: Optional[TagsConfig] = None,
    **kwargs
) -> Iterator[List[Tuple[str, Dict[str, Any]]]]:
    """Convenience function to generate hashes for a schema"""
    config = HashGeneratorConfig(
        num_keys=num_keys,
        schema=schema,
        tags_config=tags_config,
        **kwargs
    )
    
    gen = HashKeyGenerator(config)
    return gen