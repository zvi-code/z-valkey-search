#!/usr/bin/env python3
"""
Simple Tag Memory Estimation Test

Tests tag memory estimation with the simplest possible scenario:
- Fixed length tags
- No sharing between keys 
- No prefix sharing
- Predictable memory usage

This should be easily estimated and serve as a baseline for validation.
"""

from integration.unittest.standalone_fuzzy_test import calculate_comprehensive_memory

def test_simple_tag_scenarios():
    """Test simple, predictable tag scenarios"""
    
    print("🧪 SIMPLE TAG MEMORY ESTIMATION TEST")
    print("=" * 60)
    print("Testing fixed-length tags with no sharing for baseline validation")
    print()
    
    # Test Case 1: Small dataset, single tag per key
    print("📋 Test Case 1: Small dataset, 1 tag per key")
    print("   Config: 1000 keys, 1 tag/key, 20B tags, no sharing")
    
    # Parameters for no sharing scenario
    total_keys = 1000
    unique_tags = 1000  # No sharing = each key has unique tag
    avg_tag_length = 20
    avg_tags_per_key = 1
    avg_keys_per_tag = 1  # No sharing = each tag used by 1 key
    
    result1 = calculate_comprehensive_memory(
        total_keys=total_keys,
        unique_tags=unique_tags, 
        avg_tag_length=avg_tag_length,
        avg_tags_per_key=avg_tags_per_key,
        avg_keys_per_tag=avg_keys_per_tag,
        vector_dims=8,
        hnsw_m=16
    )
    
    # Manual calculation for validation
    expected_tag_data = unique_tags * avg_tag_length  # 1000 * 20 = 20KB raw data
    expected_total_simple = expected_tag_data * 3  # rough 3x overhead estimate
    
    print(f"   📊 Estimated tag index: {result1['tag_index_kb']:,} KB")
    print(f"   📝 Expected raw tag data: {expected_tag_data // 1024} KB")
    print(f"   📈 Simple 3x overhead estimate: {expected_total_simple // 1024} KB")
    print(f"   🎯 Estimation ratio: {result1['tag_index_kb'] / (expected_total_simple // 1024):.2f}x")
    print()
    
    # Test Case 2: Larger dataset, multiple tags per key
    print("📋 Test Case 2: Larger dataset, 5 tags per key")
    print("   Config: 5000 keys, 5 tags/key, 30B tags, no sharing")
    
    total_keys = 5000
    unique_tags = 25000  # 5000 keys * 5 tags = 25000 unique tags
    avg_tag_length = 30
    avg_tags_per_key = 5
    avg_keys_per_tag = 1  # Still no sharing
    
    result2 = calculate_comprehensive_memory(
        total_keys=total_keys,
        unique_tags=unique_tags,
        avg_tag_length=avg_tag_length, 
        avg_tags_per_key=avg_tags_per_key,
        avg_keys_per_tag=avg_keys_per_tag,
        vector_dims=8,
        hnsw_m=16
    )
    
    expected_tag_data2 = unique_tags * avg_tag_length  # 25000 * 30 = 750KB
    expected_total_simple2 = expected_tag_data2 * 3
    
    print(f"   📊 Estimated tag index: {result2['tag_index_kb']:,} KB")
    print(f"   📝 Expected raw tag data: {expected_tag_data2 // 1024} KB") 
    print(f"   📈 Simple 3x overhead estimate: {expected_total_simple2 // 1024} KB")
    print(f"   🎯 Estimation ratio: {result2['tag_index_kb'] / (expected_total_simple2 // 1024):.2f}x")
    print()
    
    # Test Case 3: Perfect sharing scenario (opposite extreme)
    print("📋 Test Case 3: Perfect sharing scenario")
    print("   Config: 10000 keys, 3 tags/key, 25B tags, all keys share same 100 tags")
    
    total_keys = 10000
    unique_tags = 100  # Perfect sharing - only 100 unique tags total
    avg_tag_length = 25
    avg_tags_per_key = 3
    avg_keys_per_tag = 300  # 10000 keys * 3 tags / 100 unique = 300 keys per tag
    
    result3 = calculate_comprehensive_memory(
        total_keys=total_keys,
        unique_tags=unique_tags,
        avg_tag_length=avg_tag_length,
        avg_tags_per_key=avg_tags_per_key, 
        avg_keys_per_tag=avg_keys_per_tag,
        vector_dims=8,
        hnsw_m=16
    )
    
    expected_tag_data3 = unique_tags * avg_tag_length  # 100 * 25 = 2.5KB raw data
    expected_total_simple3 = expected_tag_data3 * 3
    
    print(f"   📊 Estimated tag index: {result3['tag_index_kb']:,} KB")
    print(f"   📝 Expected raw tag data: {expected_tag_data3 // 1024} KB")
    print(f"   📈 Simple 3x overhead estimate: {expected_total_simple3 // 1024} KB")
    print(f"   🎯 Estimation ratio: {result3['tag_index_kb'] / max(1, expected_total_simple3 // 1024):.2f}x")
    print()
    
    # Analysis
    print("🔍 ANALYSIS:")
    print("=" * 60)
    
    # Check if estimations are reasonable
    ratios = [
        result1['tag_index_kb'] / (expected_total_simple // 1024),
        result2['tag_index_kb'] / (expected_total_simple2 // 1024),
        result3['tag_index_kb'] / max(1, expected_total_simple3 // 1024)
    ]
    
    print(f"📊 Estimation ratios: {ratios[0]:.1f}x, {ratios[1]:.1f}x, {ratios[2]:.1f}x")
    
    if all(0.5 <= r <= 3.0 for r in ratios):
        print("✅ Tag estimation appears reasonable (0.5x - 3.0x range)")
    else:
        print("⚠️  Tag estimation has issues:")
        for i, ratio in enumerate(ratios, 1):
            if ratio < 0.5:
                print(f"   • Test {i}: Severe underestimation ({ratio:.1f}x)")
            elif ratio > 3.0:
                print(f"   • Test {i}: Severe overestimation ({ratio:.1f}x)")
    
    print()
    print("🔍 Detailed breakdown for Test Case 1:")
    for key, value in result1.items():
        if 'kb' in key.lower():
            print(f"   {key}: {value:,} KB")
    
    return result1, result2, result3

def compare_with_manual_calculation():
    """Compare our estimation with manual calculation for simple case"""
    
    print()
    print("🧮 MANUAL CALCULATION COMPARISON")
    print("=" * 60)
    print("Scenario: 1000 keys, 1 tag/key, 20B tags, no sharing")
    print()
    
    # Manual calculation breakdown
    total_keys = 1000
    unique_tags = 1000
    avg_tag_length = 20
    
    print("📝 Manual calculation:")
    
    # Raw tag data
    raw_tag_data = unique_tags * avg_tag_length
    print(f"   Raw tag data: {unique_tags} tags × {avg_tag_length}B = {raw_tag_data:,} bytes = {raw_tag_data // 1024} KB")
    
    # String interning overhead
    string_intern_overhead = unique_tags * 32  # 32 bytes per interned string
    print(f"   String interning: {unique_tags} × 32B = {string_intern_overhead:,} bytes = {string_intern_overhead // 1024} KB")
    
    # Patricia tree (minimal for no sharing)
    patricia_overhead = unique_tags * 50  # rough estimate for tree nodes
    print(f"   Patricia tree: {unique_tags} × 50B = {patricia_overhead:,} bytes = {patricia_overhead // 1024} KB")
    
    # Key tracking overhead
    key_tracking = total_keys * 40  # tracking each key's tags
    print(f"   Key tracking: {total_keys} × 40B = {key_tracking:,} bytes = {key_tracking // 1024} KB")
    
    manual_total = raw_tag_data + string_intern_overhead + patricia_overhead + key_tracking
    print(f"   Manual total: {manual_total:,} bytes = {manual_total // 1024} KB")
    
    # Compare with our function
    result = calculate_comprehensive_memory(
        total_keys=total_keys,
        unique_tags=unique_tags,
        avg_tag_length=avg_tag_length,
        avg_tags_per_key=1,
        avg_keys_per_tag=1,
        vector_dims=8,
        hnsw_m=16
    )
    
    print()
    print(f"🔍 Our estimation: {result['tag_index_kb']:,} KB")
    print(f"📊 Manual calculation: {manual_total // 1024} KB")
    print(f"🎯 Ratio: {result['tag_index_kb'] / (manual_total // 1024):.2f}x")
    
    if abs(result['tag_index_kb'] / (manual_total // 1024) - 1.0) > 0.5:
        print("⚠️  Significant discrepancy detected!")
        print("   This suggests an issue in the tag estimation function.")
    else:
        print("✅ Reasonable agreement between methods")

if __name__ == '__main__':
    test_simple_tag_scenarios()
    compare_with_manual_calculation()