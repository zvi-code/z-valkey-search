#!/usr/bin/env python3
"""
Test suite for the string generator - demonstrates all configurations
with small sizes for easy visualization.
"""

import sys
import os
from typing import List, Union
import numpy as np

# Import the generator module (assuming it's in the same directory)
# In practice, you'd import from the actual module
from integration.utils.string_generator import (
    StringGenerator, GeneratorConfig, LengthConfig, PrefixConfig,
    Distribution, StringType, generate_strings, generate_float_vectors
)


def print_section(title: str):
    """Print a formatted section header"""
    print(f"\n{'='*60}")
    print(f" {title}")
    print('='*60)


def print_batch_samples(batch: Union[np.ndarray, List], max_items: int = 10, max_chars: int = 50):
    """Print samples from a batch with formatting"""
    items_to_show = min(len(batch), max_items)
    for i in range(items_to_show):
        item = batch[i]
        if isinstance(item, bytes):
            # Show both hex and ASCII representation
            hex_repr = item.hex()
            try:
                ascii_repr = item.decode('ascii', errors='replace')
            except:
                ascii_repr = "non-ASCII"
            
            display_hex = hex_repr[:max_chars] + "..." if len(hex_repr) > max_chars else hex_repr
            display_ascii = ascii_repr[:max_chars//2] + "..." if len(ascii_repr) > max_chars//2 else ascii_repr
            
            print(f"  [{i:2d}] len={len(item):2d} | hex={display_hex} | ascii={display_ascii}")
        else:
            print(f"  [{i:2d}] {item}")
    
    if len(batch) > max_items:
        print(f"  ... and {len(batch) - max_items} more items")


def test_string_types():
    """Test all string type configurations"""
    print_section("STRING TYPES TEST")
    
    base_config = {
        'count': 20,
        'length': LengthConfig(avg=8, min=5, max=10),
        'batch_size': 20,
        'seed': 42
    }
    
    # Test each string type
    string_types = [
        (StringType.RANDOM_BYTES, {}),
        (StringType.ASCII, {}),
        (StringType.ALPHANUMERIC, {}),
        (StringType.NUMERIC, {}),
        (StringType.CUSTOM_CHARSET, {'charset': 'ABC123!@#'}),
    ]
    
    for string_type, extra_config in string_types:
        print(f"\n--- String Type: {string_type.value} ---")
        config = GeneratorConfig(
            string_type=string_type,
            **base_config,
            **extra_config
        )
        
        gen = StringGenerator(config)
        for batch in gen:
            print_batch_samples(batch)
            break  # Only show first batch


def test_length_distributions():
    """Test all length distribution configurations"""
    print_section("LENGTH DISTRIBUTIONS TEST")
    
    distributions = [
        (Distribution.UNIFORM, {}),
        (Distribution.NORMAL, {'std_dev': 2.0}),
        (Distribution.EXPONENTIAL, {}),
        (Distribution.ZIPF, {'alpha': 2.0}),
        (Distribution.PARETO, {'alpha': 1.5}),
    ]
    
    for dist, extra_params in distributions:
        print(f"\n--- Distribution: {dist.value} ---")
        config = GeneratorConfig(
            count=30,
            string_type=StringType.ASCII,
            length=LengthConfig(
                avg=7,
                min=3,
                max=10,
                distribution=dist,
                **extra_params
            ),
            batch_size=30,
            seed=42
        )
        
        gen = StringGenerator(config)
        lengths = []
        for batch in gen:
            # Collect lengths for statistics
            for item in batch:
                lengths.append(len(item))
            print(f"  Length distribution: {sorted(lengths)}")
            print(f"  Min: {min(lengths)}, Max: {max(lengths)}, Avg: {sum(lengths)/len(lengths):.1f}")
            break


def test_prefix_sharing():
    """Test prefix sharing configurations"""
    print_section("PREFIX SHARING TEST")
    
    # Test without prefix sharing first
    print("\n--- No Prefix Sharing ---")
    config = GeneratorConfig(
        count=10,
        string_type=StringType.ALPHANUMERIC,
        length=LengthConfig(avg=8, min=6, max=10),
        batch_size=10,
        seed=42
    )
    
    gen = StringGenerator(config)
    for batch in gen:
        print_batch_samples(batch)
    
    # Test with prefix sharing
    print("\n--- With Prefix Sharing (50% probability) ---")
    config = GeneratorConfig(
        count=15,
        string_type=StringType.ALPHANUMERIC,
        length=LengthConfig(avg=8, min=6, max=10),
        prefix=PrefixConfig(
            enabled=True,
            min_shared=2,
            max_shared=4,
            share_probability=0.5,
            prefix_pool_size=3
        ),
        batch_size=15,
        seed=42
    )
    
    gen = StringGenerator(config)
    for batch in gen:
        print_batch_samples(batch)
        # Highlight shared prefixes
        print("\n  Checking for shared prefixes:")
        for i in range(1, min(len(batch), 10)):
            prev = batch[i-1]
            curr = batch[i]
            shared = 0
            for j in range(min(len(prev), len(curr))):
                if prev[j] == curr[j]:
                    shared += 1
                else:
                    break
            if shared >= 2:
                print(f"    Items {i-1} and {i} share {shared} byte prefix")


def test_float_vectors():
    """Test float32 vector generation"""
    print_section("FLOAT32 VECTORS TEST")
    
    print("\n--- Float32 Vectors (dim=2) ---")
    config = GeneratorConfig(
        count=5,
        string_type=StringType.FLOAT32_VECTOR,
        vector_dim=2,  # Small dimension for display
        batch_size=5,
        seed=42
    )
    
    gen = StringGenerator(config)
    for batch in gen:
        for i, item in enumerate(batch):
            # Decode float32 values
            floats = np.frombuffer(item, dtype=np.float32)
            print(f"  [{i}] vector: {floats} | bytes: {item.hex()}")


def test_batch_iteration():
    """Test batch iteration with different sizes"""
    print_section("BATCH ITERATION TEST")
    
    config = GeneratorConfig(
        count=25,
        string_type=StringType.NUMERIC,
        length=LengthConfig(avg=5, min=3, max=7),
        batch_size=10,
        seed=42
    )
    
    gen = StringGenerator(config)
    batch_num = 0
    total_items = 0
    
    for batch in gen:
        print(f"\n--- Batch {batch_num} (size: {len(batch)}) ---")
        print_batch_samples(batch, max_items=5)
        batch_num += 1
        total_items += len(batch)
    
    print(f"\nTotal batches: {batch_num}, Total items: {total_items}")


def test_convenience_functions():
    """Test convenience function interfaces"""
    print_section("CONVENIENCE FUNCTIONS TEST")
    
    print("\n--- generate_strings() convenience function ---")
    gen = generate_strings(
        count=10,
        string_type=StringType.ASCII,
        avg_length=6,
        min_length=4,
        max_length=8,
        enable_prefix_sharing=True,
        seed=42
    )
    
    for batch in gen:
        print_batch_samples(batch, max_items=5)
        break
    
    print("\n--- generate_float_vectors() convenience function ---")
    gen = generate_float_vectors(
        count=5,
        dimension=3,
        batch_size=5
    )
    
    for batch in gen:
        for i, item in enumerate(batch):
            floats = np.frombuffer(item, dtype=np.float32)
            print(f"  [{i}] dim={len(floats)} vector: {floats}")
        break


def test_edge_cases():
    """Test edge cases and error handling"""
    print_section("EDGE CASES TEST")
    
    # Single character strings
    print("\n--- Single character strings ---")
    config = GeneratorConfig(
        count=10,
        string_type=StringType.ASCII,
        length=LengthConfig(avg=1, min=1, max=1),
        batch_size=10,
        seed=42
    )
    
    gen = StringGenerator(config)
    for batch in gen:
        chars = [item.decode('ascii') for item in batch]
        print(f"  Generated characters: {' '.join(chars)}")
    
    # Maximum length strings with prefix sharing
    print("\n--- Max length with aggressive prefix sharing ---")
    config = GeneratorConfig(
        count=5,
        string_type=StringType.ALPHANUMERIC,
        length=LengthConfig(avg=10, min=10, max=10),
        prefix=PrefixConfig(
            enabled=True,
            min_shared=5,
            max_shared=8,
            share_probability=0.9,
            prefix_pool_size=2
        ),
        batch_size=5,
        seed=42
    )
    
    gen = StringGenerator(config)
    for batch in gen:
        print_batch_samples(batch)


def run_all_tests():
    """Run all test suites"""
    print("STRING GENERATOR COMPREHENSIVE TEST SUITE")
    print("=" * 60)
    
    test_string_types()
    test_length_distributions()
    test_prefix_sharing()
    test_float_vectors()
    test_batch_iteration()
    test_convenience_functions()
    test_edge_cases()
    
    print("\n" + "="*60)
    print("ALL TESTS COMPLETED")
    print("="*60)


if __name__ == "__main__":
    run_all_tests()