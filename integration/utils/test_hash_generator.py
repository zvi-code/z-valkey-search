#!/usr/bin/env python3
"""
Test suite for HashKeyGenerator - demonstrates hash generation
for various Valkey Search index schemas.
"""

import numpy as np
import struct
from typing import Dict, Any

from hash_generator import (
    HashKeyGenerator, HashGeneratorConfig, IndexSchema, FieldSchema,
    FieldType, VectorFieldSchema, VectorAlgorithm, VectorMetric,
    create_simple_schema, generate_hashes
)
from tags_builder import TagsConfig, TagDistribution, TagSharingConfig, TagSharingMode
from string_generator import LengthConfig, PrefixConfig, Distribution, StringType


def print_section(title: str):
    """Print a formatted section header"""
    print(f"\n{'='*70}")
    print(f" {title}")
    print('='*70)


def format_field_value(name: str, value: Any) -> str:
    """Format field value for display"""
    if isinstance(value, bytes):
        # Check if it's a vector (multiple of 4 bytes)
        if len(value) % 4 == 0 and len(value) >= 4:
            # Try to decode as float32 vector
            try:
                floats = np.frombuffer(value, dtype=np.float32)
                if len(floats) <= 5:
                    return f"vector[{len(floats)}]: {floats}"
                else:
                    return f"vector[{len(floats)}]: [{floats[0]:.3f}, {floats[1]:.3f}, ..., {floats[-1]:.3f}]"
            except:
                pass
        # Fallback to hex
        if len(value) <= 16:
            return f"bytes: {value.hex()}"
        else:
            return f"bytes[{len(value)}]: {value[:8].hex()}..."
    elif isinstance(value, str):
        if len(value) > 50:
            return f'"{value[:47]}..."'
        return f'"{value}"'
    elif isinstance(value, float):
        return f"{value:.2f}"
    else:
        return str(value)


def display_hash_batch(batch: list, max_items: int = 5):
    """Display a batch of hash keys with formatting"""
    for i, (key, fields) in enumerate(batch[:max_items]):
        print(f"\n  [{i}] Key: {key}")
        for field_name, field_value in fields.items():
            formatted = format_field_value(field_name, field_value)
            print(f"      {field_name}: {formatted}")
    
    if len(batch) > max_items:
        print(f"\n  ... and {len(batch) - max_items} more items")


def test_simple_schema():
    """Test basic schema with vector, tags, and text"""
    print_section("Test 1: Simple Schema")
    
    # Create schema
    schema = create_simple_schema(
        index_name="products",
        vector_dim=128,
        include_tags=True,
        include_text=True,
        prefix="product:"
    )
    
    # Print schema
    print("\nSchema fields:")
    for field in schema.fields:
        if field.type == FieldType.VECTOR:
            print(f"  - {field.name}: {field.type.value} (dim={field.vector_config.dim})")
        else:
            print(f"  - {field.name}: {field.type.value}")
    
    # Configure generation
    config = HashGeneratorConfig(
        num_keys=100,
        schema=schema,
        key_length=LengthConfig(avg=8, min=6, max=10),
        tags_config=TagsConfig(
            num_keys=100,
            tags_per_key=TagDistribution(avg=3, min=2, max=5),
            tag_length=LengthConfig(avg=10, min=5, max=15),
            sharing=TagSharingConfig(mode=TagSharingMode.SHARED_POOL, pool_size=20)
        ),
        batch_size=10,
        seed=42
    )
    
    # Generate and display
    gen = HashKeyGenerator(config)
    
    print(f"\nFT.CREATE command:")
    print(gen.generate_ft_create_command())
    
    print("\nGenerated hashes:")
    for batch in gen:
        display_hash_batch(batch)
        break  # Show only first batch


def test_complex_schema():
    """Test schema with all field types"""
    print_section("Test 2: Complex Schema with All Field Types")
    
    # Create complex schema
    schema = IndexSchema(
        index_name="complex_index",
        prefix=["item:", "doc:"],
        fields=[
            FieldSchema(name="title", type=FieldType.TEXT, weight=2.0),
            FieldSchema(name="description", type=FieldType.TEXT),
            FieldSchema(name="categories", type=FieldType.TAG, separator=";"),
            FieldSchema(name="price", type=FieldType.NUMERIC, sortable=True),
            FieldSchema(name="location", type=FieldType.GEO),
            FieldSchema(
                name="embedding",
                type=FieldType.VECTOR,
                vector_config=VectorFieldSchema(
                    algorithm=VectorAlgorithm.HNSW,
                    dim=384,
                    distance_metric=VectorMetric.COSINE
                )
            )
        ]
    )
    
    # Configure with specific tags
    tags_config = TagsConfig(
        num_keys=50,
        tags_per_key=TagDistribution(avg=4, min=2, max=6),
        tag_length=LengthConfig(avg=12, min=8, max=16),
        tag_string_type=StringType.ALPHANUMERIC,
        sharing=TagSharingConfig(
            mode=TagSharingMode.GROUP_BASED,
            keys_per_group=10,
            tags_per_group=8
        )
    )
    
    config = HashGeneratorConfig(
        num_keys=50,
        schema=schema,
        key_prefix="item:",  # Use first prefix
        tags_config=tags_config,
        vector_normalize=True,  # For cosine similarity
        batch_size=5,
        seed=42
    )
    
    gen = HashKeyGenerator(config)
    
    print(f"\nFT.CREATE command:")
    print(gen.generate_ft_create_command())
    
    print("\nGenerated hashes:")
    for i, batch in enumerate(gen):
        if i < 2:  # Show first 2 batches
            print(f"\n--- Batch {i} ---")
            display_hash_batch(batch, max_items=3)


def test_vector_similarity():
    """Test vector generation with different metrics"""
    print_section("Test 3: Vector Similarity Metrics")
    
    for metric in [VectorMetric.L2, VectorMetric.COSINE, VectorMetric.IP]:
        print(f"\n--- {metric.value} Distance ---")
        
        schema = IndexSchema(
            index_name=f"vectors_{metric.value.lower()}",
            prefix=["vec:"],
            fields=[
                FieldSchema(
                    name="embedding",
                    type=FieldType.VECTOR,
                    vector_config=VectorFieldSchema(
                        dim=4,  # Small for display
                        distance_metric=metric
                    )
                )
            ]
        )
        
        config = HashGeneratorConfig(
            num_keys=5,
            schema=schema,
            vector_normalize=(metric == VectorMetric.COSINE),
            batch_size=5,
            seed=42
        )
        
        gen = HashKeyGenerator(config)
        
        # Generate and analyze vectors
        vectors = []
        for batch in gen:
            for key, fields in batch:
                vec_bytes = fields['embedding']
                vec = np.frombuffer(vec_bytes, dtype=np.float32)
                vectors.append(vec)
                
                # Display vector properties
                norm = np.linalg.norm(vec)
                print(f"  {key}: {vec} (norm={norm:.3f})")
        
        # Check if normalized for cosine
        if metric == VectorMetric.COSINE:
            norms = [np.linalg.norm(v) for v in vectors]
            print(f"  Normalized: {all(abs(n - 1.0) < 0.01 for n in norms)}")


def test_prefix_patterns():
    """Test different key prefix patterns"""
    print_section("Test 4: Key Prefix Patterns")
    
    # Test custom prefix with pattern
    schema = create_simple_schema("test", vector_dim=128, prefix="user:profile:")
    
    config = HashGeneratorConfig(
        num_keys=20,
        schema=schema,
        key_string_type=StringType.NUMERIC,  # User IDs
        key_length=LengthConfig(avg=6, min=6, max=6),  # Fixed length IDs
        batch_size=10,
        seed=42
    )
    
    gen = HashKeyGenerator(config)
    
    print("\nGenerated keys with numeric IDs:")
    all_keys = []
    for batch in gen:
        for key, _ in batch:
            all_keys.append(key)
    
    # Show sample keys
    for i in [0, 5, 10, 15, 19]:
        print(f"  [{i}] {all_keys[i]}")


def test_tag_sharing_modes():
    """Test different tag sharing configurations"""
    print_section("Test 5: Tag Sharing Modes")
    
    schema = IndexSchema(
        index_name="products",
        prefix=["prod:"],
        fields=[
            FieldSchema(name="tags", type=FieldType.TAG),
            FieldSchema(name="embedding", type=FieldType.VECTOR,
                       vector_config=VectorFieldSchema(dim=64))
        ]
    )
    
    sharing_modes = [
        ("Unique Tags", TagSharingMode.UNIQUE, {}),
        ("Perfect Overlap", TagSharingMode.PERFECT_OVERLAP, {}),
        ("Shared Pool", TagSharingMode.SHARED_POOL, {'pool_size': 10}),
        ("Group Based", TagSharingMode.GROUP_BASED, {'keys_per_group': 5, 'tags_per_group': 5})
    ]
    
    for mode_name, mode, extra_config in sharing_modes:
        print(f"\n--- {mode_name} ---")
        
        tags_config = TagsConfig(
            num_keys=10,
            tags_per_key=TagDistribution(avg=3, min=3, max=3),  # Fixed for comparison
            tag_length=LengthConfig(avg=6, min=5, max=7),
            sharing=TagSharingConfig(mode=mode, **extra_config)
        )
        
        config = HashGeneratorConfig(
            num_keys=10,
            schema=schema,
            tags_config=tags_config,
            batch_size=10,
            seed=42
        )
        
        gen = HashKeyGenerator(config)
        
        # Collect all tags
        all_tags = []
        for batch in gen:
            for i, (key, fields) in enumerate(batch[:5]):  # Show first 5
                tags = fields['tags']
                all_tags.extend(tags.split(','))
                print(f"  Key {i}: {tags}")
        
        # Analyze uniqueness
        unique_tags = set(all_tags)
        print(f"  Unique tags: {len(unique_tags)} out of {len(all_tags)} total")


def test_batch_ingestion_format():
    """Test output format for Valkey ingestion"""
    print_section("Test 6: Batch Ingestion Format")
    
    schema = create_simple_schema("myindex", vector_dim=768, prefix="doc:")
    
    config = HashGeneratorConfig(
        num_keys=1000,
        schema=schema,
        batch_size=100,
        seed=42
    )
    
    gen = HashKeyGenerator(config)
    
    print("\nIngestion commands for first 3 items:")
    
    # Get first batch
    for batch in gen:
        for i, (key, fields) in enumerate(batch[:3]):
            # Build HSET command
            cmd_parts = ["HSET", key]
            for field_name, field_value in fields.items():
                cmd_parts.append(field_name)
                if isinstance(field_value, bytes):
                    # For vectors, show length instead of full content
                    cmd_parts.append(f"<{len(field_value)} bytes>")
                else:
                    cmd_parts.append(f'"{field_value}"')
            
            print(f"\n  [{i}] {' '.join(cmd_parts)}")
        break
    
    # Show statistics
    print("\n\nGeneration statistics:")
    total_keys = 0
    total_batches = 0
    for batch in HashKeyGenerator(config):
        total_keys += len(batch)
        total_batches += 1
    
    print(f"  Total keys: {total_keys}")
    print(f"  Total batches: {total_batches}")
    print(f"  Keys per batch: {total_keys / total_batches:.1f}")


def test_performance():
    """Test performance with realistic workload"""
    print_section("Test 7: Performance Test")
    
    import time
    
    # Realistic e-commerce schema
    schema = IndexSchema(
        index_name="products",
        prefix=["product:"],
        fields=[
            FieldSchema(name="name", type=FieldType.TEXT, weight=2.0),
            FieldSchema(name="description", type=FieldType.TEXT),
            FieldSchema(name="categories", type=FieldType.TAG),
            FieldSchema(name="tags", type=FieldType.TAG),
            FieldSchema(name="price", type=FieldType.NUMERIC, sortable=True),
            FieldSchema(name="rating", type=FieldType.NUMERIC),
            FieldSchema(name="image_embedding", type=FieldType.VECTOR,
                       vector_config=VectorFieldSchema(dim=512, distance_metric=VectorMetric.COSINE)),
            FieldSchema(name="text_embedding", type=FieldType.VECTOR,
                       vector_config=VectorFieldSchema(dim=768, distance_metric=VectorMetric.COSINE))
        ]
    )
    
    config = HashGeneratorConfig(
        num_keys=100_000,
        schema=schema,
        tags_config=TagsConfig(
            num_keys=100_000,
            tags_per_key=TagDistribution(avg=5, min=2, max=10, distribution=Distribution.NORMAL),
            sharing=TagSharingConfig(mode=TagSharingMode.SHARED_POOL, pool_size=500)
        ),
        batch_size=1000,
        seed=42
    )
    
    gen = HashKeyGenerator(config)
    
    print(f"\nGenerating {config.num_keys:,} hash keys...")
    print(f"Schema: {len(schema.fields)} fields including 2 vector fields")
    print(f"Batch size: {config.batch_size}")
    
    start_time = time.time()
    total_keys = 0
    total_bytes = 0
    
    for i, batch in enumerate(gen):
        total_keys += len(batch)
        
        # Estimate size
        if i == 0:  # Sample first batch
            for key, fields in batch[:10]:
                size = len(key.encode())
                for fname, fvalue in fields.items():
                    size += len(fname.encode())
                    if isinstance(fvalue, bytes):
                        size += len(fvalue)
                    else:
                        size += len(str(fvalue).encode())
                total_bytes += size * (len(batch) / 10)  # Estimate
        
        # Progress
        if (i + 1) % 10 == 0:
            elapsed = time.time() - start_time
            rate = total_keys / elapsed
            print(f"  Generated {total_keys:,} keys in {elapsed:.1f}s ({rate:,.0f} keys/sec)")
    
    elapsed = time.time() - start_time
    rate = total_keys / elapsed
    
    print(f"\nCompleted:")
    print(f"  Total keys: {total_keys:,}")
    print(f"  Estimated size: ~{total_bytes/1024/1024:.1f} MB")
    print(f"  Time: {elapsed:.2f} seconds")
    print(f"  Rate: {rate:,.0f} keys/second")


def run_all_tests():
    """Run all test cases"""
    print("HASH KEY GENERATOR TEST SUITE")
    print("=" * 70)
    
    test_simple_schema()
    test_complex_schema()
    test_vector_similarity()
    test_prefix_patterns()
    test_tag_sharing_modes()
    test_batch_ingestion_format()
    
    # Performance test (optional - comment out for quick tests)
    # test_performance()
    
    print("\n" + "="*70)
    print("ALL TESTS COMPLETED")
    print("="*70)


if __name__ == "__main__":
    run_all_tests()