#!/usr/bin/env python3
"""
Example showing how to configure prefix sharing for your specific use case:
- 1000 vectors where each shares 90% prefix with ~100 other vectors
- 20 byte average length with non-uniform distribution
"""

import numpy as np
from integration.utils.string_generator import (
    StringGenerator, GeneratorConfig, LengthConfig, PrefixConfig,
    Distribution, StringType
)


def generate_grouped_vectors_current_api():
    """
    Using current API - approximation of the requirement.
    This creates strings that share prefixes from a small pool.
    """
    print("=== Method 1: Using Current API with Small Prefix Pool ===\n")
    
    # Since we want each vector to share with ~100 others out of 1000,
    # we need about 10 unique prefix groups (1000/100 = 10)
    config = GeneratorConfig(
        count=1000,
        string_type=StringType.RANDOM_BYTES,
        length=LengthConfig(
            avg=20,
            min=15,
            max=25,
            distribution=Distribution.NORMAL,  # Non-uniform
            std_dev=3.0
        ),
        prefix=PrefixConfig(
            enabled=True,
            min_shared=14,  # ~70% of 20 bytes
            max_shared=22,  # ~90% of max length
            share_probability=0.9,  # High probability to reuse prefixes
            prefix_pool_size=10  # Creates ~10 groups of ~100 vectors each
        ),
        batch_size=100,
        seed=42
    )
    
    gen = StringGenerator(config)
    
    # Collect all vectors to analyze prefix sharing
    all_vectors = []
    for batch in gen:
        all_vectors.extend(batch)
    
    # Analyze prefix groups
    prefix_groups = {}
    for i, vec in enumerate(all_vectors):
        prefix = vec[:14]  # Check first 14 bytes
        prefix_key = prefix.hex()[:20]  # Use first 20 chars of hex for grouping
        if prefix_key not in prefix_groups:
            prefix_groups[prefix_key] = []
        prefix_groups[prefix_key].append(i)
    
    print(f"Generated {len(all_vectors)} vectors")
    print(f"Number of unique prefix groups: {len(prefix_groups)}")
    print("\nPrefix group sizes:")
    for i, (prefix, indices) in enumerate(list(prefix_groups.items())[:5]):
        print(f"  Group {i}: {len(indices)} vectors sharing prefix {prefix}...")
    
    return all_vectors


def generate_grouped_vectors_custom():
    """
    Custom implementation for exact requirements:
    Each vector shares 90% prefix with exactly 100 other vectors.
    """
    print("\n\n=== Method 2: Custom Implementation for Exact Requirements ===\n")
    
    np.random.seed(42)
    
    # Parameters
    total_vectors = 1000
    vectors_per_group = 100
    num_groups = total_vectors // vectors_per_group
    
    all_vectors = []
    
    for group_id in range(num_groups):
        # Generate lengths for this group (non-uniform distribution)
        lengths = np.random.normal(20, 3, vectors_per_group)
        lengths = np.clip(lengths, 15, 25).astype(int)
        
        # Determine shared prefix length (90% of minimum length in group)
        min_length = lengths.min()
        prefix_length = int(0.9 * min_length)
        
        # Generate shared prefix for this group
        shared_prefix = np.random.bytes(prefix_length)
        
        # Generate vectors with shared prefix
        for i in range(vectors_per_group):
            total_length = lengths[i]
            suffix_length = total_length - prefix_length
            suffix = np.random.bytes(suffix_length)
            vector = shared_prefix + suffix
            all_vectors.append(vector)
    
    # Shuffle to mix groups (optional)
    # np.random.shuffle(all_vectors)
    
    # Analysis
    print(f"Generated {len(all_vectors)} vectors in {num_groups} groups")
    print(f"Each group has {vectors_per_group} vectors sharing ~90% prefix")
    
    # Sample analysis of first group
    print("\nFirst group analysis:")
    for i in range(5):
        vec = all_vectors[i]
        print(f"  Vector {i}: length={len(vec)}, prefix_len={prefix_length}, "
              f"suffix_len={len(vec)-prefix_length}")
    
    return all_vectors


def generate_grouped_vectors_modified_api():
    """
    Proposed API modification for better group control.
    This shows how the API could be extended.
    """
    print("\n\n=== Method 3: Proposed API Extension ===\n")
    
    # This is a proposed configuration structure
    proposed_config = {
        'count': 1000,
        'string_type': 'RANDOM_BYTES',
        'length': {
            'avg': 20,
            'min': 15,
            'max': 25,
            'distribution': 'NORMAL',
            'std_dev': 3.0
        },
        'grouping': {  # New section for group-based generation
            'enabled': True,
            'vectors_per_group': 100,
            'prefix_ratio': 0.9,  # 90% of length as prefix
            'shuffle_output': False  # Keep groups together or shuffle
        }
    }
    
    print("Proposed configuration structure:")
    print(proposed_config)
    
    # For now, implement using current API with workaround
    config = GeneratorConfig(
        count=1000,
        string_type=StringType.RANDOM_BYTES,
        length=LengthConfig(
            avg=20,
            min=15,
            max=25,
            distribution=Distribution.NORMAL,
            std_dev=3.0
        ),
        prefix=PrefixConfig(
            enabled=True,
            min_shared=18,  # 90% of 20
            max_shared=18,
            share_probability=0.99,  # Very high to ensure sharing
            prefix_pool_size=10  # Approximate group count
        ),
        batch_size=100,  # Match group size
        seed=42
    )
    
    # Generate with controlled batching
    gen = StringGenerator(config)
    all_vectors = []
    
    for batch_idx, batch in enumerate(gen):
        # Each batch tends to share prefixes due to high share_probability
        all_vectors.extend(batch)
        if batch_idx < 3:  # Show first few batches
            shared_prefix_count = 0
            for i in range(1, len(batch)):
                if batch[i][:18] == batch[i-1][:18]:
                    shared_prefix_count += 1
            print(f"Batch {batch_idx}: {shared_prefix_count}/{len(batch)-1} "
                  f"consecutive vectors share prefix")
    
    return all_vectors


def analyze_prefix_sharing(vectors, prefix_length=18):
    """Analyze how many vectors each vector shares its prefix with"""
    print(f"\n=== Prefix Sharing Analysis (prefix_length={prefix_length}) ===")
    
    # Build prefix groups
    prefix_map = {}
    for i, vec in enumerate(vectors):
        if len(vec) >= prefix_length:
            prefix = vec[:prefix_length].hex()
            if prefix not in prefix_map:
                prefix_map[prefix] = []
            prefix_map[prefix].append(i)
    
    # Analyze sharing patterns
    group_sizes = [len(group) for group in prefix_map.values()]
    
    print(f"Total vectors: {len(vectors)}")
    print(f"Unique prefixes: {len(prefix_map)}")
    print(f"Average group size: {np.mean(group_sizes):.1f}")
    print(f"Group size distribution: min={min(group_sizes)}, "
          f"max={max(group_sizes)}, median={np.median(group_sizes):.1f}")
    
    # Show distribution
    print("\nGroup size histogram:")
    hist, bins = np.histogram(group_sizes, bins=10)
    for i in range(len(hist)):
        print(f"  {bins[i]:.0f}-{bins[i+1]:.0f}: {'#' * (hist[i] // 2)} ({hist[i]})")


def main():
    """Run all examples"""
    
    # Method 1: Current API
    vectors1 = generate_grouped_vectors_current_api()
    analyze_prefix_sharing(vectors1[:200], prefix_length=14)  # Analyze first 200
    
    # Method 2: Custom implementation
    vectors2 = generate_grouped_vectors_custom()
    analyze_prefix_sharing(vectors2[:200], prefix_length=13)  # 90% of 15 (min length)
    
    # Method 3: Proposed API
    vectors3 = generate_grouped_vectors_modified_api()
    analyze_prefix_sharing(vectors3[:200], prefix_length=18)
    
    print("\n=== Summary ===")
    print("\nFor your specific requirement (1000 vectors, each sharing 90% prefix with 100 others):")
    print("- Method 1: Use small prefix_pool_size (10) with high share_probability (0.9+)")
    print("- Method 2: Implement custom generation logic for exact control")
    print("- Method 3: Proposed API extension for group-based generation")


if __name__ == "__main__":
    main()