#!/usr/bin/env python3
"""Test the completed hash_generator implementation with vector and numeric fields"""

import sys
sys.path.append('/home/ubuntu/valkey-search/integration')

from integration.utils.hash_generator import (
    HashKeyGenerator, HashGeneratorConfig, IndexSchema, FieldSchema,
    FieldType, VectorFieldSchema, VectorAlgorithm, VectorMetric,
    create_simple_schema, create_hnsw_vector_field, create_numeric_field
)
from integration.utils.tags_builder import TagsConfig, TagDistribution
from integration.utils.string_generator import LengthConfig

def test_hnsw_vector_generation():
    """Test HNSW vector field generation"""
    print("Testing HNSW vector generation...")
    
    # Create schema with HNSW vector
    schema = IndexSchema(
        index_name="test_hnsw",
        prefix=["doc:"],
        fields=[
            FieldSchema(name="tags", type=FieldType.TAG),
            create_hnsw_vector_field(
                name="embedding",
                dim=128,
                metric=VectorMetric.COSINE,
                m=32,
                ef_construction=400,
                ef_runtime=100,
                epsilon=0.01
            )
        ]
    )
    
    # Generate FT.CREATE command
    gen = HashKeyGenerator(HashGeneratorConfig(
        num_keys=10,
        schema=schema,
        vector_normalize=True,  # Important for cosine similarity
        batch_size=10
    ))
    
    create_cmd = gen.generate_ft_create_command()
    print(f"FT.CREATE command:\n{create_cmd}\n")
    
    # Generate some data
    for batch in gen:
        for key, fields in batch[:2]:  # Just show first 2
            print(f"Key: {key}")
            print(f"  Tags: {fields.get('tags', 'N/A')}")
            print(f"  Vector length: {len(fields.get('embedding', b''))} bytes")
            # Verify vector is normalized for cosine
            import numpy as np
            vec = np.frombuffer(fields['embedding'], dtype=np.float32)
            norm = np.linalg.norm(vec)
            print(f"  Vector norm: {norm:.6f} (should be ~1.0 for cosine)")
        break
    print()

def test_numeric_field_generation():
    """Test numeric field generation with different distributions"""
    print("Testing numeric field generation...")
    
    # Create schema with multiple numeric fields
    schema = IndexSchema(
        index_name="test_numeric",
        prefix=["item:"],
        fields=[
            create_numeric_field("price", min_val=0.99, max_val=999.99, distribution="uniform"),
            create_numeric_field("rating", min_val=1, max_val=5, distribution="normal"),
            create_numeric_field("views", min_val=0, max_val=1000000, distribution="exponential"),
            FieldSchema(name="tags", type=FieldType.TAG),
            FieldSchema(
                name="vec",
                type=FieldType.VECTOR,
                vector_config=VectorFieldSchema(dim=8)
            )
        ]
    )
    
    # Generate data
    config = HashGeneratorConfig(
        num_keys=100,
        schema=schema,
        batch_size=100
    )
    gen = HashKeyGenerator(config)
    
    # Collect numeric values to verify distributions
    prices = []
    ratings = []
    views = []
    
    for batch in gen:
        for key, fields in batch:
            prices.append(fields['price'])
            ratings.append(fields['rating'])
            views.append(fields['views'])
    
    # Analyze distributions
    import numpy as np
    print(f"Price (uniform): min={min(prices):.2f}, max={max(prices):.2f}, mean={np.mean(prices):.2f}, std={np.std(prices):.2f}")
    print(f"Rating (normal): min={min(ratings):.2f}, max={max(ratings):.2f}, mean={np.mean(ratings):.2f}, std={np.std(ratings):.2f}")
    print(f"Views (exponential): min={min(views):.0f}, max={max(views):.0f}, mean={np.mean(views):.0f}, median={np.median(views):.0f}")
    print()

def test_sparse_vectors():
    """Test sparse vector generation"""
    print("Testing sparse vector generation...")
    
    schema = create_simple_schema(
        index_name="test_sparse",
        vector_dim=100,
        include_numeric=True,
        vector_algorithm=VectorAlgorithm.FLAT,
        vector_metric=VectorMetric.L2
    )
    
    # Generate with 70% sparsity
    config = HashGeneratorConfig(
        num_keys=10,
        schema=schema,
        vector_sparsity=0.7,
        batch_size=10
    )
    gen = HashKeyGenerator(config)
    
    for batch in gen:
        for key, fields in batch[:2]:  # Just show first 2
            vec = np.frombuffer(fields['embedding'], dtype=np.float32)
            non_zero = np.count_nonzero(vec)
            sparsity = 1 - (non_zero / len(vec))
            print(f"Key: {key}")
            print(f"  Vector dimension: {len(vec)}")
            print(f"  Non-zero elements: {non_zero}")
            print(f"  Actual sparsity: {sparsity:.2%}")
            print(f"  Score: {fields.get('score', 'N/A'):.2f}")
        break
    print()

def test_complete_schema():
    """Test a complete schema with all field types"""
    print("Testing complete schema with all field types...")
    
    schema = IndexSchema(
        index_name="products",
        prefix=["product:"],
        fields=[
            FieldSchema(name="title", type=FieldType.TEXT, weight=2.0),
            FieldSchema(name="description", type=FieldType.TEXT),
            FieldSchema(name="tags", type=FieldType.TAG, separator=";"),
            create_numeric_field("price", 0, 1000, "uniform", sortable=True),
            create_numeric_field("stock", 0, 10000, "exponential"),
            FieldSchema(name="location", type=FieldType.GEO),
            create_hnsw_vector_field("image_embedding", 512, VectorMetric.COSINE),
            FieldSchema(
                name="text_embedding",
                type=FieldType.VECTOR,
                vector_config=VectorFieldSchema(
                    algorithm=VectorAlgorithm.FLAT,
                    dim=768,
                    distance_metric=VectorMetric.IP,
                    initial_cap=1000
                )
            )
        ]
    )
    
    # Show the FT.CREATE command
    gen = HashKeyGenerator(HashGeneratorConfig(
        num_keys=1,
        schema=schema,
        vector_normalize=True  # For cosine similarity
    ))
    print("FT.CREATE command:")
    print(gen.generate_ft_create_command())
    print()
    
    # Generate one example
    for batch in gen:
        key, fields = batch[0]
        print(f"Example document:")
        print(f"  Key: {key}")
        for field_name, value in fields.items():
            if isinstance(value, bytes):
                print(f"  {field_name}: <binary data, {len(value)} bytes>")
            else:
                print(f"  {field_name}: {value}")
        break

if __name__ == "__main__":
    test_hnsw_vector_generation()
    test_numeric_field_generation()
    test_sparse_vectors()
    test_complete_schema()
    print("All tests completed!")