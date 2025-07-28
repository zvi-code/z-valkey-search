#!/usr/bin/env python3
"""
Test the fixed estimation against simple scenarios
"""

from integration.scripts.fixed_tag_estimation import calculate_fixed_tag_memory
from integration.unittest.standalone_fuzzy_test import calculate_comprehensive_memory

def compare_old_vs_new():
    """Compare old vs new estimation on simple test cases"""
    
    print("🔄 COMPARING OLD vs NEW TAG ESTIMATION")
    print("=" * 60)
    
    test_cases = [
        {
            'name': 'No sharing, 1 tag/key',
            'total_keys': 1000,
            'unique_tags': 1000,
            'avg_tag_length': 20,
            'avg_tags_per_key': 1,
            'avg_keys_per_tag': 1
        },
        {
            'name': 'No sharing, 5 tags/key',
            'total_keys': 5000,
            'unique_tags': 25000,
            'avg_tag_length': 30,
            'avg_tags_per_key': 5,
            'avg_keys_per_tag': 1
        },
        {
            'name': 'Perfect sharing',
            'total_keys': 10000,
            'unique_tags': 100,
            'avg_tag_length': 25,
            'avg_tags_per_key': 3,
            'avg_keys_per_tag': 300
        },
        {
            'name': 'Moderate sharing',
            'total_keys': 5000,
            'unique_tags': 1000,
            'avg_tag_length': 40,
            'avg_tags_per_key': 4,
            'avg_keys_per_tag': 20
        }
    ]
    
    for case in test_cases:
        print(f"📋 {case['name']}:")
        print(f"   {case['total_keys']} keys, {case['unique_tags']} unique tags, {case['avg_tag_length']}B each")
        print(f"   {case['avg_tags_per_key']} tags/key, {case['avg_keys_per_tag']} keys/tag")
        
        # Old estimation
        old_result = calculate_comprehensive_memory(
            total_keys=case['total_keys'],
            unique_tags=case['unique_tags'],
            avg_tag_length=case['avg_tag_length'],
            avg_tags_per_key=case['avg_tags_per_key'],
            avg_keys_per_tag=case['avg_keys_per_tag'],
            vector_dims=8,
            hnsw_m=16
        )
        
        # New estimation
        new_result = calculate_fixed_tag_memory(
            total_keys=case['total_keys'],
            unique_tags=case['unique_tags'],
            avg_tag_length=case['avg_tag_length'],
            avg_tags_per_key=case['avg_tags_per_key'],
            avg_keys_per_tag=case['avg_keys_per_tag'],
            vector_dims=8,
            hnsw_m=16
        )
        
        # Simple expected calculation
        raw_tag_data = case['unique_tags'] * case['avg_tag_length']
        simple_expected = raw_tag_data * 2.5  # 2.5x overhead estimate
        
        print(f"   📊 Old tag estimation: {old_result['tag_index_kb']:,} KB")
        print(f"   🔧 New tag estimation: {new_result['tag_index_kb']:,} KB")
        print(f"   📝 Raw tag data: {raw_tag_data // 1024} KB")
        print(f"   📈 Simple expected (~2.5x): {simple_expected // 1024} KB")
        
        old_ratio = old_result['tag_index_kb'] / (simple_expected // 1024)
        new_ratio = new_result['tag_index_kb'] / (simple_expected // 1024)
        improvement = old_result['tag_index_kb'] / new_result['tag_index_kb']
        
        print(f"   🎯 Old ratio: {old_ratio:.1f}x, New ratio: {new_ratio:.1f}x")
        print(f"   ✅ Improvement: {improvement:.1f}x reduction")
        print()
    
    print("🏆 SUMMARY:")
    print("   The new estimation should be much closer to reasonable expectations")
    print("   and show better accuracy across different sharing scenarios.")

if __name__ == '__main__':
    compare_old_vs_new()