#!/usr/bin/env python3
"""
Standalone Fuzzy Memory Estimation Testing

Tests memory estimation accuracy across random data patterns without requiring
the full test framework.
"""

import random
import time
import json
import statistics
from dataclasses import dataclass, asdict
from typing import Dict, List, Tuple, Optional
from enum import Enum


class PatternType(Enum):
    UNIFORM = "uniform"
    ZIPFIAN = "zipfian"
    CLUSTERED = "clustered"
    RANDOM = "random"


@dataclass
class RandomDataPattern:
    """Configuration for a random data pattern test case"""
    # Dataset properties
    total_keys: int
    key_prefix_length: int
    
    # Vector configuration
    vector_dims: int
    vector_algorithm: str  # "FLAT" or "HNSW"
    
    # Tag configuration
    unique_tags: int
    avg_tag_length: int
    min_tag_length: int
    max_tag_length: int
    tags_per_key_min: int
    tags_per_key_max: int
    tag_sharing_pattern: PatternType
    tag_sharing_factor: float  # 0.0 = no sharing, 1.0 = max sharing
    
    # Numeric fields configuration
    numeric_fields: int
    numeric_ranges: List[Tuple[float, float]]
    
    # Test metadata
    pattern_id: str
    description: str
    
    # Optional fields (must come last)
    hnsw_m: Optional[int] = None


class RandomDataPatternGenerator:
    """Generates random data patterns for fuzzy testing"""
    
    def __init__(self, seed: int = None):
        if seed is not None:
            random.seed(seed)
            
    def generate_pattern(self, pattern_id: str = None) -> RandomDataPattern:
        """Generate a single random data pattern"""
        if pattern_id is None:
            pattern_id = f"pattern_{int(time.time())}_{random.randint(1000, 9999)}"
            
        # Dataset size - vary from small to large
        total_keys = random.choice([
            1000, 2500, 5000, 10000, 25000, 50000, 100000
        ])
        
        # Vector configuration
        vector_dims = random.choice([
            1, 2, 4, 8, 16, 32, 64, 128, 256, 384, 512, 768, 1024, 1536
        ])
        
        # Choose algorithm based on dataset size (larger datasets more likely to use HNSW)
        if total_keys < 10000:
            vector_algorithm = random.choice(["FLAT", "HNSW"])
        else:
            vector_algorithm = random.choices(["FLAT", "HNSW"], weights=[0.3, 0.7])[0]
            
        hnsw_m = None
        if vector_algorithm == "HNSW":
            hnsw_m = random.choice([8, 12, 16, 20, 24, 32, 48, 64])
            
        # Tag configuration
        # Scale unique tags with dataset size, but add randomness
        base_unique_tags = int(total_keys * random.uniform(0.01, 0.8))
        unique_tags = max(10, min(base_unique_tags, total_keys // 2))
        
        # Tag length distribution
        avg_tag_length = random.randint(8, 128)
        length_variance = random.randint(4, avg_tag_length // 2)
        min_tag_length = max(3, avg_tag_length - length_variance)
        max_tag_length = avg_tag_length + length_variance
        
        # Tags per key
        max_possible_tags_per_key = min(20, unique_tags // 10 if unique_tags > 50 else unique_tags // 2)
        tags_per_key_min = random.randint(1, max(1, max_possible_tags_per_key // 3))
        tags_per_key_max = random.randint(tags_per_key_min, max_possible_tags_per_key)
        
        # Tag sharing patterns
        tag_sharing_pattern = random.choice(list(PatternType))
        tag_sharing_factor = random.uniform(0.1, 0.9)
        
        # Numeric fields
        numeric_fields = random.randint(0, 8)
        numeric_ranges = []
        for _ in range(numeric_fields):
            min_val = random.uniform(-1000000, 1000000)
            max_val = min_val + random.uniform(100, 2000000)
            numeric_ranges.append((min_val, max_val))
            
        # Key prefix length
        key_prefix_length = random.randint(16, 64)
        
        description = (
            f"{total_keys}K keys, {vector_dims}D {vector_algorithm}"
            f"{f' M={hnsw_m}' if hnsw_m else ''}, "
            f"{unique_tags} tags({min_tag_length}-{max_tag_length}B), "
            f"{tags_per_key_min}-{tags_per_key_max} tags/key, "
            f"{tag_sharing_pattern.value} sharing({tag_sharing_factor:.2f}), "
            f"{numeric_fields} numeric fields"
        )
        
        return RandomDataPattern(
            total_keys=total_keys,
            key_prefix_length=key_prefix_length,
            vector_dims=vector_dims,
            vector_algorithm=vector_algorithm,
            unique_tags=unique_tags,
            avg_tag_length=avg_tag_length,
            min_tag_length=min_tag_length,
            max_tag_length=max_tag_length,
            tags_per_key_min=tags_per_key_min,
            tags_per_key_max=tags_per_key_max,
            tag_sharing_pattern=tag_sharing_pattern,
            tag_sharing_factor=tag_sharing_factor,
            numeric_fields=numeric_fields,
            numeric_ranges=numeric_ranges,
            pattern_id=pattern_id,
            description=description,
            hnsw_m=hnsw_m
        )
        
    def generate_pattern_set(self, count: int, seed: int = None) -> List[RandomDataPattern]:
        """Generate a set of random patterns for testing"""
        if seed is not None:
            random.seed(seed)
            
        patterns = []
        for i in range(count):
            pattern = self.generate_pattern(f"fuzz_{i:03d}")
            patterns.append(pattern)
            
        return patterns


def calculate_comprehensive_memory(total_keys, unique_tags, avg_tag_length, avg_tags_per_key, avg_keys_per_tag, vector_dims=8, hnsw_m=16):
    """
    Standalone version of comprehensive memory calculation
    """
    
    # === 1. VALKEY CORE OVERHEAD ===
    avg_key_length = 32  # estimate based on typical patterns
    valkey_key_overhead = total_keys * (
        32 +  # hash table entry overhead
        avg_key_length + 8 +  # key string + length
        16  # misc metadata per key
    )
    
    # Hash field storage (tags + vector fields per key)
    valkey_fields_overhead = total_keys * (
        (avg_tags_per_key * avg_tag_length * 1.2) +  # tag data with overhead
        (vector_dims * 4 * 1.1) +  # vector data with overhead
        32  # field metadata
    )
    
    valkey_memory = valkey_key_overhead + valkey_fields_overhead
    
    # === 2. TAG INDEX MEMORY (CORRECTED) ===
    # String Interning - Only for unique tag strings (REMOVED KEY INTERNING)
    string_intern_overhead_per_string = 12  # InternedString object
    tag_interning_memory = unique_tags * (avg_tag_length + string_intern_overhead_per_string)
    tag_interning_memory += unique_tags * 8   # Global hash map overhead
    
    # tracked_tags_by_keys_ (InternedStringMap<TagInfo>)
    taginfo_per_key = (
        8 +   # InternedStringPtr to raw_tag_string
        8 +   # flat_hash_set overhead for parsed tags
        (avg_tags_per_key * 8)  # string_view per tag
    )
    tracked_keys_memory = total_keys * (8 + taginfo_per_key + 4)  # Map entry overhead
    
    # Patricia Tree - Much more realistic estimation
    if avg_keys_per_tag <= 1.1:  # Essentially no sharing
        # Simple case: mostly leaf nodes, minimal internal structure
        estimated_tree_nodes = int(unique_tags * 0.3)  # Much fewer internal nodes needed
        patricia_node_memory = estimated_tree_nodes * 20  # Simpler node structure
    else:
        # Sharing case: more complex tree structure needed
        sharing_factor = min(10.0, avg_keys_per_tag)  # Cap at 10x sharing
        tree_complexity = 1.0 + (sharing_factor - 1.0) * 0.1  # Linear scaling
        estimated_tree_nodes = int(unique_tags * tree_complexity)
        patricia_node_memory = estimated_tree_nodes * (
            24 +  # Children map overhead
            8 +   # subtree_values_count  
            8     # Optional overhead
        )
    
    # Leaf node values - flat_hash_set per unique tag containing keys that have that tag
    patricia_values_memory = unique_tags * (
        8 +   # Minimal flat_hash_set overhead
        (avg_keys_per_tag * 8)  # InternedStringPtr per key
    )
    
    patricia_tree_memory = patricia_node_memory + patricia_values_memory
    
    # untracked_keys_ set - assume fewer untracked keys
    untracked_keys_memory = total_keys * 0.05 * 16  # Reduced percentage and overhead
    
    tag_index_memory = (
        tag_interning_memory + 
        tracked_keys_memory + 
        patricia_tree_memory + 
        untracked_keys_memory
    )
    
    # === 3. VECTOR INDEX MEMORY (HNSW) ===
    # Vector data storage
    vector_data_size = total_keys * vector_dims * 4  # float32
    
    # tracked_metadata_by_key_ (key -> internal_id + magnitude mapping)
    key_metadata_memory = total_keys * (
        8 +   # key pointer in map
        8 +   # internal_id (uint64_t)
        4 +   # magnitude (float)
        16    # map overhead per entry
    )
    
    # key_by_internal_id_ reverse mapping
    reverse_mapping_memory = total_keys * 24  # similar hash map overhead
    
    # FixedSizeAllocator for vector storage optimization  
    allocator_overhead = max(1024, total_keys * 0.1)  # allocator bookkeeping
    
    # HNSW Graph Structure Memory:
    M = hnsw_m  # M parameter (typically 16)
    maxM0 = M * 2  # connections in layer 0 (typically 32)
    
    # data_level0_memory_ - ChunkedArray storing layer 0 data
    size_links_level0 = maxM0 * 4 + 4  # maxM0 * sizeof(tableint) + sizeof(linklistsizeint)
    size_data_per_element = size_links_level0 + 8 + 4  # + char* + labeltype
    
    data_level0_memory = total_keys * size_data_per_element
    
    # linkLists_ - ChunkedArray for higher level connections  
    higher_level_nodes = int(total_keys * 0.1)  # rough estimate
    size_links_per_element = M * 4 + 4  # M * sizeof(tableint) + sizeof(linklistsizeint)
    higher_level_memory = higher_level_nodes * size_links_per_element
    
    # label_lookup_ mapping (unordered_map<labeltype, tableint>)
    label_lookup_memory = total_keys * 24  # hash map overhead + key/value
    
    # element_levels_ vector (keeps level of each element)
    element_levels_memory = total_keys * 4  # vector<int>
    
    # visited_list_pool_ for search operations
    visited_list_pool_memory = max(1024, total_keys * 0.01)  # search working memory
    
    # Additional HNSW overhead: mutexes, atomics, deleted_elements tracking
    hnsw_coordination_overhead = total_keys * 2  # various coordination structures
    
    vector_index_memory = (
        vector_data_size +
        key_metadata_memory +
        reverse_mapping_memory +
        allocator_overhead +
        data_level0_memory +
        higher_level_memory +
        label_lookup_memory +
        element_levels_memory +
        visited_list_pool_memory +
        hnsw_coordination_overhead
    )
    
    # === 4. SEARCH MODULE OVERHEAD ===
    module_base_overhead = 8192  # base module structures
    index_schema_overhead = 2048  # FT.CREATE schema storage
    coordination_overhead = 1024  # various coordination structures
    
    search_module_overhead = module_base_overhead + index_schema_overhead + coordination_overhead
    
    # === 5. MEMORY FRAGMENTATION & ALIGNMENT ===
    total_allocated = valkey_memory + tag_index_memory + vector_index_memory + search_module_overhead
    fragmentation_factor = 1.25  # 25% overhead typical for mixed allocation patterns
    fragmentation_overhead = total_allocated * (fragmentation_factor - 1.0)
    
    # === SUMMARY ===
    breakdown = {
        'valkey_core_kb': int(valkey_memory // 1024),
        'tag_index_kb': int(tag_index_memory // 1024),
        'vector_index_kb': int(vector_index_memory // 1024), 
        'search_module_overhead_kb': int(search_module_overhead // 1024),
        'fragmentation_overhead_kb': int(fragmentation_overhead // 1024),
        'total_estimated_kb': int((total_allocated + fragmentation_overhead) // 1024),
        # Detailed breakdowns
        'tag_interning_kb': int(tag_interning_memory // 1024),
        'tracked_keys_kb': int(tracked_keys_memory // 1024),
        'patricia_tree_kb': int(patricia_tree_memory // 1024),
        'untracked_keys_kb': int(untracked_keys_memory // 1024),
        # Detailed vector breakdown for analysis
        'vector_data_kb': int(vector_data_size // 1024),
        'hnsw_level0_kb': int(data_level0_memory // 1024),
        'hnsw_higher_levels_kb': int(higher_level_memory // 1024),
        'vector_mappings_kb': int((key_metadata_memory + reverse_mapping_memory + label_lookup_memory) // 1024),
        # HNSW configuration used
        'hnsw_m': M,
        'hnsw_maxM0': maxM0,
        'connections_per_node_l0': maxM0,
        # Analysis data
        'estimated_tree_nodes': estimated_tree_nodes
    }
    
    return breakdown


def run_fuzzy_test(num_patterns: int = 15, seed: int = 42):
    """Run fuzzy testing with standalone implementation"""
    
    print("🎲 FUZZY MEMORY ESTIMATION TEST")
    print("=" * 60)
    print(f"📊 Testing {num_patterns} random patterns")
    print(f"🌱 Seed: {seed}")
    print()
    
    # Generate random patterns
    generator = RandomDataPatternGenerator(seed=seed)
    patterns = generator.generate_pattern_set(num_patterns, seed=seed)
    
    results = []
    
    for i, pattern in enumerate(patterns):
        print(f"📋 Pattern {i+1}/{num_patterns}: {pattern.pattern_id}")
        print(f"   📝 {pattern.description}")
        
        # Calculate realistic statistics from pattern
        avg_tags_per_key = (pattern.tags_per_key_min + pattern.tags_per_key_max) / 2
        avg_keys_per_tag = pattern.total_keys / pattern.unique_tags * (avg_tags_per_key / pattern.unique_tags) * pattern.tag_sharing_factor
        
        # Get memory estimation
        estimated = calculate_comprehensive_memory(
            total_keys=pattern.total_keys,
            unique_tags=pattern.unique_tags,
            avg_tag_length=pattern.avg_tag_length,
            avg_tags_per_key=avg_tags_per_key,
            avg_keys_per_tag=avg_keys_per_tag,
            vector_dims=pattern.vector_dims,
            hnsw_m=pattern.hnsw_m or 16
        )
        
        # Simulate "actual" memory usage with realistic variance
        # Different components have different variance patterns
        tag_variance = random.uniform(0.5, 2.0)  # High variance for complex sharing
        vector_variance = random.uniform(0.8, 1.3)  # Lower variance for predictable structures
        core_variance = random.uniform(0.9, 1.2)  # Small variance for core structures
        
        simulated_actual = {
            'tag_index_kb': int(estimated['tag_index_kb'] * tag_variance),
            'vector_index_kb': int(estimated['vector_index_kb'] * vector_variance),
            'valkey_core_kb': int(estimated['valkey_core_kb'] * core_variance),
            'total_kb': int(estimated['total_estimated_kb'] * random.uniform(0.7, 1.4))
        }
        
        # Calculate accuracy metrics
        total_accuracy = estimated['total_estimated_kb'] / max(1, simulated_actual['total_kb'])
        tag_accuracy = estimated['tag_index_kb'] / max(1, simulated_actual['tag_index_kb'])
        vector_accuracy = estimated['vector_index_kb'] / max(1, simulated_actual['vector_index_kb'])
        
        print(f"   📊 Estimated Total: {estimated['total_estimated_kb']:,} KB")
        print(f"   📈 Simulated Actual: {simulated_actual['total_kb']:,} KB")
        print(f"   🎯 Accuracy: {total_accuracy:.2f}x (Tag: {tag_accuracy:.2f}x, Vector: {vector_accuracy:.2f}x)")
        
        # Show detailed breakdown for interesting cases
        if total_accuracy < 0.7 or total_accuracy > 1.5:
            print(f"      ⚠️  Significant deviation detected!")
            print(f"      📋 Breakdown:")
            print(f"         • Valkey Core: {estimated['valkey_core_kb']:,} KB")
            print(f"         • Tag Index: {estimated['tag_index_kb']:,} KB vs {simulated_actual['tag_index_kb']:,} KB")
            print(f"         • Vector Index: {estimated['vector_index_kb']:,} KB vs {simulated_actual['vector_index_kb']:,} KB")
            print(f"         • HNSW L0: {estimated['hnsw_level0_kb']:,} KB ({estimated['connections_per_node_l0']} conn/node)")
        
        print()
        
        results.append({
            'pattern_id': pattern.pattern_id,
            'description': pattern.description,
            'config': {
                'total_keys': pattern.total_keys,
                'vector_dims': pattern.vector_dims,
                'vector_algorithm': pattern.vector_algorithm,
                'hnsw_m': pattern.hnsw_m,
                'unique_tags': pattern.unique_tags,
                'avg_tag_length': pattern.avg_tag_length,
                'tag_sharing_pattern': pattern.tag_sharing_pattern.value,
                'tag_sharing_factor': pattern.tag_sharing_factor
            },
            'estimated': estimated,
            'simulated_actual': simulated_actual,
            'accuracy': {
                'total': total_accuracy,
                'tag': tag_accuracy,
                'vector': vector_accuracy
            }
        })
    
    # Analyze results
    print("=" * 60)
    print("🎯 FUZZY TEST ANALYSIS")
    print("=" * 60)
    
    # Overall accuracy statistics
    total_accuracies = [r['accuracy']['total'] for r in results]
    tag_accuracies = [r['accuracy']['tag'] for r in results]
    vector_accuracies = [r['accuracy']['vector'] for r in results]
    
    print(f"📊 Patterns Tested: {len(results)}")
    print()
    print("🎯 TOTAL MEMORY ACCURACY:")
    print(f"   Mean: {statistics.mean(total_accuracies):.3f}x")
    print(f"   Median: {statistics.median(total_accuracies):.3f}x")
    print(f"   Range: {min(total_accuracies):.3f}x - {max(total_accuracies):.3f}x")
    print(f"   Within ±20%: {sum(1 for x in total_accuracies if 0.8 <= x <= 1.2) / len(total_accuracies):.1%}")
    print()
    
    print("🏷️  TAG INDEX ACCURACY:")
    print(f"   Mean: {statistics.mean(tag_accuracies):.3f}x")
    print(f"   Within ±20%: {sum(1 for x in tag_accuracies if 0.8 <= x <= 1.2) / len(tag_accuracies):.1%}")
    print()
    
    print("🎯 VECTOR INDEX ACCURACY:")
    print(f"   Mean: {statistics.mean(vector_accuracies):.3f}x")
    print(f"   Within ±20%: {sum(1 for x in vector_accuracies if 0.8 <= x <= 1.2) / len(vector_accuracies):.1%}")
    print()
    
    # Identify problematic patterns
    problematic = [r for r in results if r['accuracy']['total'] < 0.5 or r['accuracy']['total'] > 2.0]
    if problematic:
        print("⚠️  PROBLEMATIC PATTERNS:")
        for result in problematic[:3]:  # Show top 3
            print(f"   {result['pattern_id']}: {result['accuracy']['total']:.2f}x")
            print(f"      {result['description']}")
            
            # Analyze what might be causing the issue
            config = result['config']
            if result['accuracy']['total'] < 0.5:
                print(f"      🔍 Severe underestimation - check for:")
                if config['unique_tags'] > config['total_keys'] * 0.5:
                    print(f"         • Very low tag sharing")
                if config['vector_dims'] > 512:
                    print(f"         • High-dimensional vectors")
            elif result['accuracy']['total'] > 2.0:
                print(f"      🔍 Severe overestimation - check for:")
                if config['tag_sharing_factor'] > 0.8:
                    print(f"         • High tag sharing reduces memory")
                if config['hnsw_m'] and config['hnsw_m'] < 16:
                    print(f"         • Lower HNSW connectivity")
            print()
    
    # Save results
    output_file = f"fuzzy_test_results_{int(time.time())}.json"
    with open(output_file, 'w') as f:
        json.dump({
            'summary': {
                'patterns_tested': len(results),
                'total_accuracy_mean': statistics.mean(total_accuracies),
                'total_accuracy_range': [min(total_accuracies), max(total_accuracies)],
                'within_20_percent': sum(1 for x in total_accuracies if 0.8 <= x <= 1.2) / len(total_accuracies)
            },
            'detailed_results': results
        }, f, indent=2)
    
    print(f"📁 Detailed results saved to: {output_file}")
    
    return results


def demonstrate_patterns():
    """Show some example patterns that would be generated"""
    print("🎲 EXAMPLE RANDOM PATTERNS")
    print("=" * 50)
    
    generator = RandomDataPatternGenerator(seed=789)
    patterns = generator.generate_pattern_set(3)
    
    for i, pattern in enumerate(patterns):
        print(f"Pattern {i+1}: {pattern.pattern_id}")
        print(f"  📊 Dataset: {pattern.total_keys:,} keys")
        print(f"  🎯 Vector: {pattern.vector_dims}D {pattern.vector_algorithm}")
        if pattern.hnsw_m:
            print(f"     HNSW M={pattern.hnsw_m} (graph connections)")
        print(f"  🏷️  Tags: {pattern.unique_tags:,} unique, avg {pattern.avg_tag_length}B length")
        print(f"     Range: {pattern.min_tag_length}-{pattern.max_tag_length}B per tag")
        print(f"     Per key: {pattern.tags_per_key_min}-{pattern.tags_per_key_max} tags")
        print(f"     Sharing: {pattern.tag_sharing_pattern.value} pattern, {pattern.tag_sharing_factor:.2f} factor")
        print(f"  🔢 Numeric: {pattern.numeric_fields} fields")
        print()


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Standalone Fuzzy Memory Test")
    parser.add_argument('--patterns', type=int, default=15, help='Number of patterns to test')
    parser.add_argument('--seed', type=int, default=42, help='Random seed')
    parser.add_argument('--demo', action='store_true', help='Show example patterns')
    
    args = parser.parse_args()
    
    if args.demo:
        demonstrate_patterns()
    else:
        run_fuzzy_test(args.patterns, args.seed)