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
    vector_normalize: bool = True  # Normalize vectors for cosine similarity
    vector_distribution: str = "normal"  # "normal" or "uniform"
    
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
        """Initialize all required generators"""
        # Key generator
        key_config = GeneratorConfig(
            count=self.config.num_keys,
            string_type=self.config.key_string_type,
            length=self.config.key_length,
            batch_size=self.config.batch_size,
            seed=None  # Use current random state
        )
        self._key_generator = StringGenerator(key_config)
        
        # Tags generator (if needed)
        if self.config.tags_config:
            self.config.tags_config.num_keys = self.config.num_keys
            self.config.tags_config.batch_size = self.config.batch_size
            self._tags_generator = TagsBuilder(self.config.tags_config)
        
        # Initialize other field generators
        for field in self.config.schema.fields:
            if field.type == FieldType.TEXT:
                # Text field generator
                text_config = GeneratorConfig(
                    count=self.config.num_keys,
                    string_type=StringType.ASCII,
                    length=LengthConfig(avg=50, min=20, max=100),
                    batch_size=self.config.batch_size,
                    seed=None
                )
                self._field_generators[field.name] = StringGenerator(text_config)
            
            elif field.type == FieldType.NUMERIC:
                # Will generate inline
                pass
    
    def _generate_vector(self, dim: int, metric: VectorMetric) -> bytes:
        """Generate a single vector based on configuration"""
        if self.config.vector_distribution == "uniform":
            vec = np.random.uniform(-1, 1, size=dim).astype(np.float32)
        else:  # normal
            vec = np.random.randn(dim).astype(np.float32)
        
        # Normalize for cosine similarity
        if self.config.vector_normalize and metric == VectorMetric.COSINE:
            norm = np.linalg.norm(vec)
            if norm > 0:
                vec = vec / norm
        
        return vec.tobytes()
    
    def _generate_numeric_value(self) -> float:
        """Generate a numeric value"""
        # Simple example - can be customized
        return np.random.uniform(0, 1000)
    
    def _generate_geo_value(self) -> str:
        """Generate a geo coordinate"""
        # Format: longitude,latitude
        lon = np.random.uniform(-180, 180)
        lat = np.random.uniform(-90, 90)
        return f"{lon:.6f},{lat:.6f}"
    
    def __iter__(self) -> Iterator[List[Tuple[str, Dict[str, Any]]]]:
        """
        Iterator returning batches of (key, fields) tuples.
        Fields are returned as a dict mapping field names to values.
        """
        # Create iterators
        key_iter = iter(self._key_generator)
        tags_iter = iter(self._tags_generator) if self._tags_generator else None
        text_iters = {
            name: iter(gen) for name, gen in self._field_generators.items()
        }
        
        keys_generated = 0
        
        while keys_generated < self.config.num_keys:
            batch_size = min(self.config.batch_size, self.config.num_keys - keys_generated)
            batch = []
            
            # Get batches from all generators
            try:
                key_batch = next(key_iter)
                tags_batch = next(tags_iter) if tags_iter else None
                text_batches = {
                    name: next(iter_) for name, iter_ in text_iters.items()
                }
            except StopIteration:
                break
            
            # Build hash entries
            for i in range(min(len(key_batch), batch_size)):
                # Generate key with prefix
                key_suffix = key_batch[i]
                if isinstance(key_suffix, bytes):
                    key_suffix = key_suffix.decode('utf-8', errors='replace')
                key = f"{self.config.key_prefix}{key_suffix}"
                
                # Build fields dict
                fields = {}
                
                # Add fields based on schema
                for field in self.config.schema.fields:
                    if field.type == FieldType.VECTOR:
                        # Generate vector
                        vec = self._generate_vector(
                            field.vector_config.dim,
                            field.vector_config.distance_metric
                        )
                        fields[field.name] = vec
                    
                    elif field.type == FieldType.TAG:
                        # Use tags from generator
                        if tags_batch and i < len(tags_batch):
                            fields[field.name] = tags_batch[i]
                    
                    elif field.type == FieldType.TEXT:
                        # Use text from generator
                        if field.name in text_batches:
                            text_batch = text_batches[field.name]
                            if i < len(text_batch):
                                text = text_batch[i]
                                if isinstance(text, bytes):
                                    text = text.decode('utf-8', errors='replace')
                                fields[field.name] = text
                    
                    elif field.type == FieldType.NUMERIC:
                        fields[field.name] = self._generate_numeric_value()
                    
                    elif field.type == FieldType.GEO:
                        fields[field.name] = self._generate_geo_value()
                
                # Add any additional fields
                fields.update(self.config.additional_fields)
                
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
                cmd_parts.extend([
                    "VECTOR",
                    cfg.algorithm.value,
                    "6",  # Number of parameters
                    "TYPE", cfg.datatype,
                    "DIM", str(cfg.dim),
                    "DISTANCE_METRIC", cfg.distance_metric.value
                ])
            
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
    prefix: str = "doc:"
) -> IndexSchema:
    """Create a simple schema with common fields"""
    fields = []
    
    if include_text:
        fields.append(FieldSchema(name="content", type=FieldType.TEXT))
    
    if include_tags:
        fields.append(FieldSchema(name="tags", type=FieldType.TAG))
    
    # Always include vector
    fields.append(FieldSchema(
        name="embedding",
        type=FieldType.VECTOR,
        vector_config=VectorFieldSchema(dim=vector_dim)
    ))
    
    return IndexSchema(
        index_name=index_name,
        prefix=[prefix],
        fields=fields
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