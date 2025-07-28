#!/usr/bin/env python3
"""
Simple Fuzzy Test Runner

Quick runner for fuzzy memory estimation testing with fewer dependencies.
"""

import os
import sys
import random
import time
import json
from typing import Dict, List

# Add integration directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from integration.unittest.fuzzy_memory_test import RandomDataPatternGenerator, FuzzyMemoryTester


def run_quick_fuzzy_test(num_patterns: int = 10):
    """Run a quick fuzzy test with minimal setup"""
    
    print("🎲 Quick Fuzzy Memory Estimation Test")
    print(f"   Testing {num_patterns} random patterns")
    print("   (Using simulated measurements - not real valkey)")
    print()
    
    # Create pattern generator
    generator = RandomDataPatternGenerator(seed=42)
    patterns = generator.generate_pattern_set(num_patterns)
    
    # Results storage
    results = []
    
    for i, pattern in enumerate(patterns):
        print(f"📋 Pattern {i+1}/{num_patterns}: {pattern.pattern_id}")
        print(f"   📝 {pattern.description}")
        
        # Import the memory calculation function directly
        from test_memory_benchmark import MemoryBenchmarkTest
        benchmark = MemoryBenchmarkTest()
        
        # Generate data and calculate stats (simplified)
        unique_tags = pattern.unique_tags
        avg_tag_length = pattern.avg_tag_length
        avg_tags_per_key = (pattern.tags_per_key_min + pattern.tags_per_key_max) / 2
        avg_keys_per_tag = pattern.total_keys / unique_tags * (avg_tags_per_key / unique_tags)
        
        # Get memory estimation
        estimated = benchmark.calculate_comprehensive_memory(
            total_keys=pattern.total_keys,
            unique_tags=unique_tags,
            avg_tag_length=avg_tag_length,
            avg_tags_per_key=avg_tags_per_key,
            avg_keys_per_tag=avg_keys_per_tag,
            vector_dims=pattern.vector_dims,
            hnsw_m=pattern.hnsw_m or 16
        )
        
        # Simulate "actual" values with some variance
        variance = random.uniform(0.7, 1.4)
        simulated_actual = int(estimated['total_estimated_kb'] * variance)
        
        accuracy = estimated['total_estimated_kb'] / max(1, simulated_actual)
        
        print(f"   📊 Estimated: {estimated['total_estimated_kb']:,} KB")
        print(f"   📈 Simulated: {simulated_actual:,} KB")
        print(f"   🎯 Accuracy: {accuracy:.2f}x")
        
        # Detailed breakdown
        print(f"      • Valkey Core: {estimated['valkey_core_kb']:,} KB")
        print(f"      • Key Interning: {estimated['key_interning_kb']:,} KB") 
        print(f"      • Tag Index: {estimated['tag_index_kb']:,} KB")
        print(f"      • Vector Index: {estimated['vector_index_kb']:,} KB")
        print(f"        - Vector Data: {estimated['vector_data_kb']:,} KB")
        print(f"        - HNSW L0: {estimated['hnsw_level0_kb']:,} KB")
        print(f"        - Mappings: {estimated['vector_mappings_kb']:,} KB")
        print(f"      • Fragmentation: {estimated['fragmentation_overhead_kb']:,} KB")
        print()
        
        results.append({
            'pattern': pattern.pattern_id,
            'description': pattern.description,
            'estimated_kb': estimated['total_estimated_kb'],
            'simulated_actual_kb': simulated_actual,
            'accuracy': accuracy,
            'breakdown': estimated
        })
    
    # Summary statistics
    accuracies = [r['accuracy'] for r in results]
    avg_accuracy = sum(accuracies) / len(accuracies)
    min_accuracy = min(accuracies)
    max_accuracy = max(accuracies)
    within_20_percent = sum(1 for a in accuracies if 0.8 <= a <= 1.2) / len(accuracies)
    
    print("=" * 60)
    print("🎯 FUZZY TEST SUMMARY")
    print("=" * 60)
    print(f"📊 Patterns Tested: {len(results)}")
    print(f"🎯 Average Accuracy: {avg_accuracy:.2f}x")
    print(f"📈 Accuracy Range: {min_accuracy:.2f}x - {max_accuracy:.2f}x")
    print(f"✅ Within ±20%: {within_20_percent:.1%}")
    print()
    
    # Identify best and worst cases
    best_result = max(results, key=lambda r: 1/abs(r['accuracy'] - 1.0))  # Closest to 1.0
    worst_result = min(results, key=lambda r: 1/abs(r['accuracy'] - 1.0))  # Farthest from 1.0
    
    print(f"✅ Best Case: {best_result['pattern']} ({best_result['accuracy']:.2f}x)")
    print(f"   {best_result['description']}")
    print()
    print(f"⚠️  Worst Case: {worst_result['pattern']} ({worst_result['accuracy']:.2f}x)")
    print(f"   {worst_result['description']}")
    print()
    
    # Save results
    output_file = f"quick_fuzzy_results_{int(time.time())}.json"
    with open(output_file, 'w') as f:
        json.dump({
            'summary': {
                'patterns_tested': len(results),
                'average_accuracy': avg_accuracy,
                'min_accuracy': min_accuracy,
                'max_accuracy': max_accuracy,
                'within_20_percent': within_20_percent
            },
            'results': results
        }, f, indent=2)
    
    print(f"📁 Results saved to: {output_file}")
    
    return results


def demonstrate_pattern_variety():
    """Demonstrate the variety of patterns generated"""
    print("🎲 DEMONSTRATING PATTERN VARIETY")
    print("=" * 50)
    
    generator = RandomDataPatternGenerator(seed=123)
    patterns = generator.generate_pattern_set(5)
    
    for i, pattern in enumerate(patterns):
        print(f"Pattern {i+1}: {pattern.pattern_id}")
        print(f"  📊 Dataset: {pattern.total_keys:,} keys")
        print(f"  🎯 Vector: {pattern.vector_dims}D {pattern.vector_algorithm}")
        if pattern.hnsw_m:
            print(f"     HNSW M={pattern.hnsw_m}")
        print(f"  🏷️  Tags: {pattern.unique_tags:,} unique, {pattern.avg_tag_length} avg length")
        print(f"     {pattern.tags_per_key_min}-{pattern.tags_per_key_max} per key")
        print(f"     {pattern.tag_sharing_pattern.value} sharing ({pattern.tag_sharing_factor:.2f})")
        print(f"  🔢 Numeric: {pattern.numeric_fields} fields")
        print()


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description="Quick Fuzzy Memory Test")
    parser.add_argument('--patterns', type=int, default=10, help='Number of patterns to test')
    parser.add_argument('--demo', action='store_true', help='Just demonstrate pattern variety')
    
    args = parser.parse_args()
    
    if args.demo:
        demonstrate_pattern_variety()
    else:
        run_quick_fuzzy_test(args.patterns)