#!/usr/bin/env python3
"""
Fixed Tag Memory Estimation

Corrected version of tag memory estimation with realistic overhead calculations.
"""

def calculate_fixed_tag_memory(total_keys, unique_tags, avg_tag_length, avg_tags_per_key, avg_keys_per_tag, vector_dims=8, hnsw_m=16):
    """
    Fixed comprehensive memory calculation with corrected tag estimation
    """
    
    # === 1. VALKEY CORE OVERHEAD ===
    avg_key_length = 32
    valkey_key_overhead = total_keys * (
        24 +  # hash table entry overhead (reduced from 32)
        avg_key_length + 8 +  # key string + length
        8   # misc metadata per key (reduced from 16)
    )
    
    # Hash field storage (tags + vector fields per key) 
    # Note: This is the raw data storage, not index overhead
    valkey_fields_overhead = total_keys * (
        (avg_tags_per_key * avg_tag_length * 1.1) +  # tag field data (reduced overhead)
        (vector_dims * 4 * 1.05) +  # vector data (reduced overhead)
        16  # field metadata (reduced from 32)
    )
    
    valkey_memory = valkey_key_overhead + valkey_fields_overhead
    
    # === 2. TAG INDEX MEMORY (CORRECTED) ===
    
    # String Interning - Only for unique tag strings
    string_intern_overhead_per_string = 12  # Further reduced (InternedString object)
    tag_interning_memory = unique_tags * (avg_tag_length + string_intern_overhead_per_string)
    tag_interning_memory += unique_tags * 8   # Global hash map overhead (further reduced)
    
    # tracked_tags_by_keys_ (InternedStringMap<TagInfo>)
    # This maps each key to its TagInfo containing tag data
    taginfo_per_key = (
        8 +   # InternedStringPtr to raw_tag_string
        8 +   # flat_hash_set overhead for parsed tags (further reduced)
        (avg_tags_per_key * 8)  # string_view per tag
    )
    tracked_keys_memory = total_keys * (8 + taginfo_per_key + 4)  # Map entry overhead (further reduced)
    
    # Patricia Tree - Much more realistic estimation
    # For unique tags (no sharing), patricia tree is mostly linear with minimal branching
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
        8 +   # Minimal flat_hash_set overhead (reduced from 16)
        (avg_keys_per_tag * 8)  # InternedStringPtr per key
    )
    
    patricia_tree_memory = patricia_node_memory + patricia_values_memory
    
    # untracked_keys_ set - assume fewer untracked keys
    untracked_keys_memory = total_keys * 0.05 * 16  # Reduced percentage and overhead
    
    # REMOVED KEY INTERNING - This shouldn't be part of tag index
    
    tag_index_memory = (
        tag_interning_memory + 
        tracked_keys_memory + 
        patricia_tree_memory + 
        untracked_keys_memory
    )
    
    # === 3. VECTOR INDEX MEMORY (unchanged, this was accurate) ===
    vector_data_size = total_keys * vector_dims * 4
    
    key_metadata_memory = total_keys * (
        8 +   # key pointer in map
        8 +   # internal_id (uint64_t)
        4 +   # magnitude (float)
        16    # map overhead per entry
    )
    
    reverse_mapping_memory = total_keys * 24
    allocator_overhead = max(1024, total_keys * 0.1)
    
    M = hnsw_m
    maxM0 = M * 2
    
    size_links_level0 = maxM0 * 4 + 4
    size_data_per_element = size_links_level0 + 8 + 4
    data_level0_memory = total_keys * size_data_per_element
    
    higher_level_nodes = int(total_keys * 0.1)
    size_links_per_element = M * 4 + 4
    higher_level_memory = higher_level_nodes * size_links_per_element
    
    label_lookup_memory = total_keys * 24
    element_levels_memory = total_keys * 4
    visited_list_pool_memory = max(1024, total_keys * 0.01)
    hnsw_coordination_overhead = total_keys * 2
    
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
    module_base_overhead = 4096  # Reduced from 8192
    index_schema_overhead = 1024  # Reduced from 2048
    coordination_overhead = 512   # Reduced from 1024
    
    search_module_overhead = module_base_overhead + index_schema_overhead + coordination_overhead
    
    # === 5. MEMORY FRAGMENTATION & ALIGNMENT ===
    total_allocated = valkey_memory + tag_index_memory + vector_index_memory + search_module_overhead
    fragmentation_factor = 1.15  # Reduced from 1.25 (15% overhead)
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
        
        'vector_data_kb': int(vector_data_size // 1024),
        'hnsw_level0_kb': int(data_level0_memory // 1024),
        'hnsw_higher_levels_kb': int(higher_level_memory // 1024),
        'vector_mappings_kb': int((key_metadata_memory + reverse_mapping_memory + label_lookup_memory) // 1024),
        
        # HNSW configuration used
        'hnsw_m': M,
        'hnsw_maxM0': maxM0,
        'connections_per_node_l0': maxM0,
        
        # Analysis data
        'estimated_tree_nodes': estimated_tree_nodes,
        'avg_keys_per_tag': avg_keys_per_tag
    }
    
    return breakdown

if __name__ == '__main__':
    print("🔧 TESTING FIXED TAG ESTIMATION")
    print("=" * 50)
    
    # Test the simple case that was off by 6x
    print("Test Case: 1000 keys, 1 tag/key, 20B tags, no sharing")
    
    result = calculate_fixed_tag_memory(
        total_keys=1000,
        unique_tags=1000,
        avg_tag_length=20,
        avg_tags_per_key=1,
        avg_keys_per_tag=1,
        vector_dims=8,
        hnsw_m=16
    )
    
    print(f"Fixed tag index: {result['tag_index_kb']} KB")
    print()
    print("Breakdown:")
    print(f"  Tag interning: {result['tag_interning_kb']} KB")
    print(f"  Tracked keys: {result['tracked_keys_kb']} KB") 
    print(f"  Patricia tree: {result['patricia_tree_kb']} KB")
    print(f"  Untracked keys: {result['untracked_keys_kb']} KB")
    print()
    
    # Expected: ~20KB raw data + ~40KB overhead = ~60KB total
    expected_reasonable = 60
    ratio = result['tag_index_kb'] / expected_reasonable
    print(f"Expected reasonable estimate: ~{expected_reasonable} KB")
    print(f"Fixed estimation ratio: {ratio:.2f}x")
    
    if 0.5 <= ratio <= 2.0:
        print("✅ Much more reasonable!")
    else:
        print("⚠️ Still needs adjustment")
        
    print()
    print("Full breakdown:")
    for key, value in result.items():
        if 'kb' in key.lower():
            print(f"  {key}: {value} KB")