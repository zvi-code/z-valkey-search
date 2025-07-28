#!/usr/bin/env python3
"""
Test suite for TagsBuilder - demonstrates all configuration options
and sharing modes for tag generation.
"""

from collections import defaultdict

from integration.utils.tags_builder import (
    TagsBuilder, TagsConfig, TagDistribution, TagSharingConfig,
    TagSharingMode
)
from integration.utils.string_generator import (
    LengthConfig, PrefixConfig, Distribution, StringType
)


def print_section(title: str):
    """Print a formatted section header"""
    print(f"\n{'='*60}")
    print(f" {title}")
    print('='*60)


def example_unique_tags():
    """Each key has unique tags (1:1)"""
    print_section("Example 1: Unique Tags (1:1)")
    
    config = TagsConfig(
        num_keys=100,
        tags_per_key=TagDistribution(avg=3, min=2, max=4),
        tag_length=LengthConfig(avg=8, min=5, max=10),
        sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE),
        seed=42
    )
    
    builder = TagsBuilder(config)
    sample_tags = []
    for batch in builder:
        sample_tags.extend(batch[:5])  # Collect first 5
        if len(sample_tags) >= 5:
            break
    
    print("Sample tag strings:")
    for i, tags in enumerate(sample_tags[:5]):
        print(f"  Key {i}: {tags}")
    
    return builder


def example_shared_prefix():
    """Tags share prefixes for prefix queries"""
    print_section("Example 2: Tags with Shared Prefixes")
    
    config = TagsConfig(
        num_keys=100,
        tags_per_key=TagDistribution(avg=3, min=2, max=4),
        tag_length=LengthConfig(avg=10, min=8, max=12),
        tag_prefix=PrefixConfig(
            enabled=True,
            min_shared=5,
            max_shared=7,
            share_probability=0.8,
            prefix_pool_size=5  # 5 unique prefixes
        ),
        sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE),
        seed=42
    )
    
    builder = TagsBuilder(config)
    sample_tags = []
    for batch in builder:
        sample_tags.extend(batch[:10])
        break
    
    print("Sample tag strings (note shared prefixes):")
    for i, tags in enumerate(sample_tags[:5]):
        print(f"  Key {i}: {tags}")
        # Show individual tags
        individual = tags.split(',')
        for j, tag in enumerate(individual):
            print(f"    Tag {j}: {tag[:7]}... (prefix: {tag[:5]})")
    
    return builder


def example_perfect_overlap():
    """All keys share all tags"""
    print_section("Example 3: Perfect Overlap")
    
    config = TagsConfig(
        num_keys=100,
        tags_per_key=TagDistribution(avg=5, min=5, max=5),  # Fixed number
        tag_length=LengthConfig(avg=8, min=6, max=10),
        sharing=TagSharingConfig(mode=TagSharingMode.PERFECT_OVERLAP),
        seed=42
    )
    
    builder = TagsBuilder(config)
    sample_tags = []
    for batch in builder:
        sample_tags.extend(batch[:10])
        break
    
    print("Sample tag strings (all identical):")
    for i, tags in enumerate(sample_tags[:5]):
        print(f"  Key {i}: {tags}")
    
    return builder


def example_group_based_with_prefix():
    """Advanced: Group-based sharing with prefix queries"""
    print_section("Example 4: Group-Based Sharing + Prefix Queries")
    
    config = TagsConfig(
        num_keys=200,
        tags_per_key=TagDistribution(avg=4, min=2, max=6, distribution=Distribution.NORMAL),
        tag_length=LengthConfig(avg=12, min=10, max=15),
        tag_prefix=PrefixConfig(
            enabled=True,
            min_shared=6,
            max_shared=8,
            share_probability=0.7,
            prefix_pool_size=10  # 10 unique prefixes across all groups
        ),
        sharing=TagSharingConfig(
            mode=TagSharingMode.GROUP_BASED,
            keys_per_group=50,  # 50 keys per group
            tags_per_group=8   # 8 unique tags per group
        ),
        seed=42
    )
    
    builder = TagsBuilder(config)
    
    # Collect samples from different groups
    all_samples = []
    for i, batch in enumerate(builder):
        all_samples.extend(batch)
        if len(all_samples) >= 200:
            break
    
    # Show samples from different groups
    print("\nGroup 0 (keys 0-49):")
    for i in [0, 1, 49]:
        print(f"  Key {i}: {all_samples[i]}")
    
    print("\nGroup 1 (keys 50-99):")
    for i in [50, 51, 99]:
        print(f"  Key {i}: {all_samples[i]}")
    
    print("\nGroup 2 (keys 100-149):")
    for i in [100, 101, 149]:
        print(f"  Key {i}: {all_samples[i]}")
    
    # Analyze prefix sharing across groups
    print("\nPrefix analysis (first 6 chars of each tag):")
    for group in range(3):
        key_idx = group * 50
        tags = all_samples[key_idx].split(',')
        prefixes = [tag[:6] for tag in tags]
        print(f"  Group {group} prefixes: {prefixes}")
    
    return builder


def example_shared_pool():
    """Tags drawn from a shared pool with reuse"""
    print_section("Example 5: Shared Pool with Controlled Reuse")
    
    config = TagsConfig(
        num_keys=100,
        tags_per_key=TagDistribution(avg=3, min=1, max=5),
        tag_length=LengthConfig(avg=8, min=6, max=10),
        sharing=TagSharingConfig(
            mode=TagSharingMode.SHARED_POOL,
            pool_size=20,  # Only 20 unique tags total
            reuse_probability=0.3  # 30% chance to reuse
        ),
        seed=42
    )
    
    builder = TagsBuilder(config)
    
    # Collect all tags to analyze sharing
    all_tag_strings = []
    for batch in builder:
        all_tag_strings.extend(batch)
    
    # Analyze tag frequency
    tag_frequency = defaultdict(int)
    for tag_string in all_tag_strings:
        for tag in tag_string.split(','):
            tag_frequency[tag] += 1
    
    print(f"\nTotal unique tags: {len(tag_frequency)}")
    print("Most common tags:")
    sorted_tags = sorted(tag_frequency.items(), key=lambda x: x[1], reverse=True)
    for tag, count in sorted_tags[:5]:
        print(f"  '{tag}': used {count} times")
    
    print("\nSample tag strings:")
    for i in range(5):
        print(f"  Key {i}: {all_tag_strings[i]}")
    
    return builder


def example_realistic_workload():
    """Realistic e-commerce workload with product tags"""
    print_section("Example 6: Realistic E-commerce Product Tags")
    
    # E-commerce scenario: products have category tags
    # Some tags are very common (electronics, sale)
    # Some are category-specific (laptop, phone)
    # Tags often share prefixes (elec_, comp_, mobile_)
    
    config = TagsConfig(
        num_keys=10000,  # 10k products
        tags_per_key=TagDistribution(
            avg=5,
            min=2,
            max=10,
            distribution=Distribution.ZIPF  # Few products have many tags
        ),
        tag_length=LengthConfig(avg=15, min=8, max=25),
        tag_prefix=PrefixConfig(
            enabled=True,
            min_shared=5,
            max_shared=10,
            share_probability=0.6,
            prefix_pool_size=20  # Categories like elec_, comp_, home_
        ),
        sharing=TagSharingConfig(
            mode=TagSharingMode.SHARED_POOL,
            pool_size=200,  # 200 unique tags across all products
            reuse_probability=0.7  # High reuse for common tags
        ),
        tag_string_type=StringType.ALPHANUMERIC,
        seed=42
    )
    
    builder = TagsBuilder(config)
    
    # Collect sample for analysis
    sample_size = 100
    samples = []
    for batch in builder:
        samples.extend(batch)
        if len(samples) >= sample_size:
            break
    
    print(f"\nSimulating {config.num_keys} products with tags")
    print("\nSample product tags:")
    for i in [0, 10, 20, 30, 40]:
        tags = samples[i]
        tag_list = tags.split(',')
        print(f"  Product {i}: {len(tag_list)} tags - {tags[:60]}...")
    
    # Analyze tag distribution
    all_tags = []
    for tag_string in samples[:sample_size]:
        all_tags.extend(tag_string.split(','))
    
    tag_freq = defaultdict(int)
    for tag in all_tags:
        tag_freq[tag] += 1
    
    print(f"\nTag statistics from {sample_size} products:")
    print(f"  Total tags: {len(all_tags)}")
    print(f"  Unique tags: {len(tag_freq)}")
    print(f"  Avg tags per product: {len(all_tags)/sample_size:.1f}")
    
    # Show prefix patterns
    prefixes = defaultdict(int)
    for tag in tag_freq:
        if len(tag) >= 5:
            prefixes[tag[:5]] += 1
    
    print("\nCommon prefixes:")
    sorted_prefixes = sorted(prefixes.items(), key=lambda x: x[1], reverse=True)
    for prefix, count in sorted_prefixes[:5]:
        print(f"  '{prefix}': {count} tags")


def performance_test():
    """Test performance with large scale generation"""
    print_section("Performance Test: 1M Keys")
    
    import time
    
    config = TagsConfig(
        num_keys=1_000_000,
        tags_per_key=TagDistribution(avg=5, min=3, max=8),
        tag_length=LengthConfig(avg=20, min=10, max=30),
        sharing=TagSharingConfig(
            mode=TagSharingMode.GROUP_BASED,
            keys_per_group=1000,
            tags_per_group=50
        ),
        batch_size=10000,
        seed=42
    )
    
    print(f"Generating {config.num_keys:,} keys with tags...")
    print(f"  Sharing mode: {config.sharing.mode.value}")
    print(f"  Keys per group: {config.sharing.keys_per_group}")
    print(f"  Batch size: {config.batch_size}")
    
    builder = TagsBuilder(config)
    
    start_time = time.time()
    total_keys = 0
    total_bytes = 0
    
    for i, batch in enumerate(builder):
        total_keys += len(batch)
        total_bytes += sum(len(tag_string.encode()) for tag_string in batch)
        
        # Progress report every 100k keys
        if total_keys % 100_000 == 0:
            elapsed = time.time() - start_time
            rate = total_keys / elapsed
            print(f"  Processed {total_keys:,} keys in {elapsed:.1f}s "
                  f"({rate:,.0f} keys/sec)")
    
    elapsed = time.time() - start_time
    rate = total_keys / elapsed
    
    print(f"\nCompleted:")
    print(f"  Total keys: {total_keys:,}")
    print(f"  Total size: {total_bytes/1024/1024:.1f} MB")
    print(f"  Time: {elapsed:.2f} seconds")
    print(f"  Rate: {rate:,.0f} keys/second")
    print(f"  Avg tag string size: {total_bytes/total_keys:.1f} bytes")


def run_all_examples():
    """Run all example configurations"""
    print("TAGS BUILDER TEST SUITE")
    print("=" * 60)
    
    # Basic examples
    example_unique_tags()
    example_shared_prefix()
    example_perfect_overlap()
    example_group_based_with_prefix()
    example_shared_pool()
    
    # Advanced examples
    example_realistic_workload()
    
    # Performance test (optional - comment out for quick tests)
    # performance_test()
    
    print("\n" + "="*60)
    print("ALL TESTS COMPLETED")
    print("="*60)


if __name__ == "__main__":
    run_all_examples()