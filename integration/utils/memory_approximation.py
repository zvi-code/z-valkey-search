import csv
import math
from dataclasses import dataclass
import sys
from typing import List, Dict, Tuple

@dataclass
class MemoryMetrics:
    vectors_memory: int = 0
    vectors_memory_marked_deleted: int = 0
    hnsw_nodes: int = 0
    hnsw_nodes_marked_deleted: int = 0
    hnsw_edges: int = 0
    hnsw_edges_marked_deleted: int = 0
    flat_nodes: int = 0
    tags: int = 0
    tags_memory: int = 0
    numeric_records: int = 0
    interned_strings: int = 0
    interned_strings_marked_deleted: int = 0
    interned_strings_memory: int = 0
    keys_memory: int = 0

def parse_csv_to_metrics(csv_file_path: str) -> List[Tuple[Dict, MemoryMetrics]]:
    """
    Parse CSV file and convert each row to MemoryMetrics.
    Returns list of tuples (row_data, metrics).
    """
    results = []
    
    with open(csv_file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Extract values from CSV
            total_keys = int(row['total_keys'])
            vector_dims = int(row['vector_dims']) if row['vector_dims'] else 0
            vector_algorithm = row['vector_algorithm']
            hnsw_m = int(row['hnsw_m']) if row['hnsw_m'] else 0
            
            # From the CSV, we have these raw memory values in KB
            vector_memory_kb = float(row['vector_memory_kb']) if row['vector_memory_kb'] else 0
            tag_index_memory_kb = float(row['tag_index_memory_kb']) if row['tag_index_memory_kb'] else 0
            numeric_memory_kb = float(row['numeric_memory_kb']) if row['numeric_memory_kb'] else 0
            key_interning_kb = float(row['key_interning_kb']) if row['key_interning_kb'] else 0
            
            # Number of numeric fields
            numeric_fields_count = int(row['numeric_fields_count']) if row['numeric_fields_count'] else 0
            
            # Tag information
            unique_tags = int(row['unique_tags']) if row['unique_tags'] else 0
            tag_avg_length = int(row['tag_avg_length']) if row['tag_avg_length'] else 0
            
            # Create MemoryMetrics
            metrics = MemoryMetrics()
            
            # Convert KB to bytes
            metrics.vectors_memory = int(vector_memory_kb * 1024)
            metrics.tags_memory = int(tag_index_memory_kb * 1024)
            metrics.keys_memory = int(key_interning_kb * 1024)
            
            # HNSW vs FLAT
            if vector_algorithm == "HNSW" and hnsw_m > 0:
                metrics.hnsw_nodes = total_keys
                # Estimate edges based on the algorithm
                # For HNSW, each node has up to M*2 connections in layer 0
                # and M connections in higher layers
                # Average connections per node is roughly M
                metrics.hnsw_edges = total_keys * hnsw_m
            elif vector_algorithm == "FLAT":
                metrics.flat_nodes = total_keys
            
            # Tags
            metrics.tags = unique_tags
            
            # Numeric records
            metrics.numeric_records = total_keys * numeric_fields_count
            
            # String interning count
            # From the CSV structure, it seems keys + tags + vectors are interned
            metrics.interned_strings = (
                total_keys +  # Keys
                unique_tags +  # Unique tag strings
                (total_keys if vector_dims > 0 else 0)  # Vectors
            )
            
            # Total interned memory (sum of components)
            metrics.interned_strings_memory = (
                metrics.keys_memory + 
                metrics.tags_memory + 
                metrics.vectors_memory
            )
            
            results.append((row, metrics))
    
    return results

def approximate_total_memory(m: MemoryMetrics) -> int:
    """
    Approximates total memory usage for valkey-search based on metrics.
    Returns total memory in bytes.
    """
    total = 0
    
    # 1. String Interning overhead
    if m.interned_strings > 0:
        # Hash map overhead for string interning
        HASH_MAP_ENTRY = 40  # string key (24) + weak_ptr (16)
        HASH_MAP_LOAD_FACTOR = 0.875
        hash_map_overhead = int((m.interned_strings + m.interned_strings_marked_deleted) * 
                               HASH_MAP_ENTRY / HASH_MAP_LOAD_FACTOR)
        total += hash_map_overhead
        
        # InternedString object overhead
        total += (m.interned_strings + m.interned_strings_marked_deleted) * 24
    
    # 2. HNSW Index Memory
    if m.hnsw_nodes > 0 or m.hnsw_nodes_marked_deleted > 0:
        # Per-node overhead
        SIZE_DATA_PER_ELEMENT = 148  # 132 (links) + 8 (ptr) + 8 (label)
        total += (m.hnsw_nodes + m.hnsw_nodes_marked_deleted) * SIZE_DATA_PER_ELEMENT
        
        # Edge storage
        total += m.hnsw_edges * 4
        total += m.hnsw_edges_marked_deleted * 4
        
        # Higher level nodes
        total_nodes = m.hnsw_nodes + m.hnsw_nodes_marked_deleted
        higher_level_nodes = total_nodes // 16
        total += higher_level_nodes * 8
        
        # Metadata structures
        total += total_nodes * 4   # element_levels_
        total += total_nodes * 64  # link_list_locks_
        total += total_nodes * 24  # label_lookup_
    
    # 3. Flat Index Memory
    if m.flat_nodes > 0:
        total += m.flat_nodes * 8   # vectors_ array
        total += m.flat_nodes * 32  # tracking map
        total += m.flat_nodes * 16  # internal structures
    
    # 4. Vector Base Class Overhead
    vector_count = m.hnsw_nodes + m.hnsw_nodes_marked_deleted + m.flat_nodes
    if vector_count > 0:
        total += vector_count * 40  # tracked_metadata_by_key_
        total += vector_count * 24  # key_by_internal_id_
        total += vector_count * 4   # allocator overhead
    
    # 5. Tag Index Memory
    if m.tags > 0:
        # Patricia tree nodes
        patricia_nodes = int(m.tags * 0.3)
        total += patricia_nodes * 80
        
        # Document sets
        avg_docs_per_tag = max(1, vector_count // m.tags if m.tags > 0 else 1)
        total += m.tags * avg_docs_per_tag * 8
        
        # Tracking maps
        if vector_count > 0:
            total += vector_count * 48  # tracked_tags_by_keys
            total += vector_count * 32  # doc_to_fields
    
    # 6. Numeric Index Memory
    if m.numeric_records > 0:
        total += m.numeric_records * 24  # tracked_keys_
        
        unique_values = max(int(math.sqrt(m.numeric_records)), 100)
        total += unique_values * 64   # BTree nodes
        total += unique_values * 80   # SegmentTree
        total += m.numeric_records * 8  # doc sets
    
    # 7. Global Key-to-ID Mapping
    if vector_count > 0:
        total += int(vector_count * 16 * 1.14)
    
    # 8. Raw data
    total += m.vectors_memory + m.vectors_memory_marked_deleted
    total += m.tags_memory
    total += m.keys_memory
    
    # 9. Fixed Overhead
    FIXED_OVERHEAD = 100 * 1024
    total += FIXED_OVERHEAD
    
    return total

def analyze_results(csv_file_path: str):
    """Analyze CSV results and compare with estimates."""
    results = parse_csv_to_metrics(csv_file_path)
    
    print(f"{'Scenario':<40} {'Actual (KB)':>12} {'Estimated (KB)':>14} {'Accuracy':>10} {'Diff (KB)':>12}")
    print("-" * 90)
    
    total_error = 0
    count = 0
    
    for row, metrics in results:
        scenario = row['scenario_name']
        actual_kb = int(row['search_used_memory_kb'])
        
        # Calculate estimate
        estimated = approximate_total_memory(metrics)
        estimated_kb = estimated // 1024
        
        accuracy = (estimated_kb / actual_kb * 100) if actual_kb > 0 else 0
        diff_kb = estimated_kb - actual_kb
        
        print(f"{scenario:<40} {actual_kb:>12,} {estimated_kb:>14,} {accuracy:>9.1f}% {diff_kb:>+12,}")
        
        total_error += abs(accuracy - 100)
        count += 1
    
    print("-" * 90)
    print(f"Average absolute error: {total_error / count:.1f}%")
    
    # Analyze specific patterns
    print("\n\nDetailed Analysis for Selected Scenarios:")
    print("=" * 60)
    
    # Find specific scenarios for detailed analysis
    for row, metrics in results:
        if row['scenario_name'] in ['Unique_Keys100000_TagLen1000', 'Unique_Keys500000_TagLen100']:
            print(f"\nScenario: {row['scenario_name']}")
            print(f"  Total keys: {row['total_keys']}")
            print(f"  Vector dims: {row['vector_dims']}")
            print(f"  Algorithm: {row['vector_algorithm']}")
            print(f"  Unique tags: {row['unique_tags']}")
            print(f"  Tag length: {row['tag_avg_length']}")
            
            print("\nMemory breakdown:")
            print(f"  Raw vector data: {metrics.vectors_memory // 1024:,} KB")
            print(f"  Raw tag data: {metrics.tags_memory // 1024:,} KB")
            print(f"  Raw key data: {metrics.keys_memory // 1024:,} KB")
            
            if metrics.hnsw_nodes > 0:
                hnsw_overhead = (
                    metrics.hnsw_nodes * 148 +  # base structure
                    metrics.hnsw_edges * 4 +     # edges
                    metrics.hnsw_nodes * 92      # metadata
                ) // 1024
                print(f"  HNSW overhead: {hnsw_overhead:,} KB")
            
            print(f"\n  Actual total: {int(row['search_used_memory_kb']):,} KB")
            print(f"  Estimated total: {approximate_total_memory(metrics) // 1024:,} KB")

if __name__ == "__main__":
    # Example usage
    # get .csv file path from command line or hardcode for testing
    # read from arguments or use a fixed path
    # read from command line arguments
    # get arguments = sys.argv[1:]
    # For example, if the file is named "memory_results.csv"
    # analyze_results("memory_results.csv")
    file_name = sys.argv[1] if len(sys.argv) > 1 else "./memory_results.csv"

    analyze_results(file_name)
    
    # # For testing with a single row
    # test_row = {
    #     'scenario_name': 'Unique_Keys500000_TagLen100',
    #     'total_keys': '500000',
    #     'vector_dims': '1500',
    #     'vector_algorithm': 'HNSW',
    #     'hnsw_m': '16',
    #     'vector_memory_kb': '2929687',
    #     'tag_index_memory_kb': '48291',
    #     'numeric_memory_kb': '48291',
    #     'key_interning_kb': '48828',
    #     'numeric_fields_count': '4',
    #     'unique_tags': '500000',
    #     'tag_avg_length': '100',
    #     'search_used_memory_kb': '3936443'
    # }
    
    # metrics = MemoryMetrics()
    # metrics.vectors_memory = int(float(test_row['vector_memory_kb']) * 1024)
    # metrics.tags_memory = int(float(test_row['tag_index_memory_kb']) * 1024)
    # metrics.keys_memory = int(float(test_row['key_interning_kb']) * 1024)
    # metrics.hnsw_nodes = int(test_row['total_keys'])
    # metrics.hnsw_edges = int(test_row['total_keys']) * int(test_row['hnsw_m'])
    # metrics.tags = int(test_row['unique_tags'])
    # metrics.numeric_records = int(test_row['total_keys']) * int(test_row['numeric_fields_count'])
    # metrics.interned_strings = (
    #     int(test_row['total_keys']) + 
    #     int(test_row['unique_tags']) + 
    #     int(test_row['total_keys'])
    # )
    
    # estimated = approximate_total_memory(metrics)
    # estimated_kb = estimated // 1024
    # actual_kb = int(test_row['search_used_memory_kb'])
    
    # print(f"Test scenario: {test_row['scenario_name']}")
    # print(f"Actual: {actual_kb:,} KB")
    # print(f"Estimated: {estimated_kb:,} KB")
    # print(f"Accuracy: {estimated_kb / actual_kb * 100:.1f}%")
# import math
# from dataclasses import dataclass
# from typing import Optional

# @dataclass
# class MemoryMetrics:
#     vectors_memory: int = 0
#     vectors_memory_marked_deleted: int = 0
#     hnsw_nodes: int = 0
#     hnsw_nodes_marked_deleted: int = 0
#     hnsw_edges: int = 0
#     hnsw_edges_marked_deleted: int = 0
#     flat_nodes: int = 0
#     tags: int = 0
#     tags_memory: int = 0
#     numeric_records: int = 0
#     interned_strings: int = 0
#     interned_strings_marked_deleted: int = 0
#     interned_strings_memory: int = 0
#     keys_memory: int = 0

# def approximate_total_memory(m: MemoryMetrics) -> int:
#     """
#     Approximates total memory usage for valkey-search based on metrics.
#     Returns total memory in bytes.
    
#     Note: The input metrics contain raw data sizes only:
#     - vectors_memory: sum of all vector data bytes
#     - tags_memory: sum of all tag string lengths
#     - keys_memory: sum of all key string lengths
#     - interned_strings_memory: redundant (sum of keys + tags + vectors)
#     """
#     total = 0
    
#     # 1. String Interning - the actual data is already in the specific metrics
#     # We only need to add the overhead for the interning infrastructure
#     if m.interned_strings > 0:
#         # Hash map overhead for string interning
#         HASH_MAP_ENTRY = 40  # string key (24) + weak_ptr (16)
#         HASH_MAP_LOAD_FACTOR = 0.875  # absl::flat_hash_map default
#         hash_map_overhead = int((m.interned_strings + m.interned_strings_marked_deleted) * 
#                                HASH_MAP_ENTRY / HASH_MAP_LOAD_FACTOR)
#         total += hash_map_overhead
        
#         # InternedString object overhead (~24 bytes per string)
#         total += (m.interned_strings + m.interned_strings_marked_deleted) * 24
    
#     # 2. HNSW Index Memory
#     if m.hnsw_nodes > 0 or m.hnsw_nodes_marked_deleted > 0:
#         # Per-node overhead in data_level0_memory_
#         SIZE_DATA_PER_ELEMENT = 148  # 132 (links) + 8 (ptr) + 8 (label)
#         total += (m.hnsw_nodes + m.hnsw_nodes_marked_deleted) * SIZE_DATA_PER_ELEMENT
        
#         # Edge storage
#         total += m.hnsw_edges * 4  # 4 bytes per edge (tableint)
#         total += m.hnsw_edges_marked_deleted * 4
        
#         # Higher level nodes overhead
#         total_nodes = m.hnsw_nodes + m.hnsw_nodes_marked_deleted
#         higher_level_nodes = total_nodes // 16  # ~6.25%
#         total += higher_level_nodes * 8  # Pointer overhead
        
#         # Metadata structures
#         total += total_nodes * 4   # element_levels_ vector
#         total += total_nodes * 64  # link_list_locks_ mutexes
#         total += total_nodes * 24  # label_lookup_ hash map overhead
    
#     # 3. Flat Index Memory
#     if m.flat_nodes > 0:
#         # Pointer array overhead
#         total += m.flat_nodes * 8  # vectors_ array
        
#         # Tracking map overhead
#         FLAT_HASH_MAP_ENTRY = 32  # key + value + hash overhead
#         total += m.flat_nodes * FLAT_HASH_MAP_ENTRY
        
#         # BruteforceSearch internal structures
#         total += m.flat_nodes * 16  # dict_external_to_internal overhead
    
#     # 4. Vector Base Class Overhead (shared by HNSW and Flat)
#     vector_count = m.hnsw_nodes + m.hnsw_nodes_marked_deleted + m.flat_nodes
#     if vector_count > 0:
#         # tracked_metadata_by_key_
#         METADATA_ENTRY = 40  # InternedStringPtr + Metadata struct
#         total += vector_count * METADATA_ENTRY
        
#         # key_by_internal_id_
#         KEY_BY_ID_ENTRY = 24  # map entry overhead
#         total += vector_count * KEY_BY_ID_ENTRY
        
#         # FixedSizeAllocator overhead (minimal, mostly just bookkeeping)
#         total += vector_count * 4
    
#     # 5. Tag Index Memory
#     if m.tags > 0:
#         # Patricia tree nodes (assuming ~30% compression)
#         patricia_nodes = int(m.tags * 0.3)
#         total += patricia_nodes * 80  # Node structure + children map
        
#         # Document sets in Patricia tree
#         # This is hard to estimate without knowing distribution
#         # Assume average 100 docs per unique tag (highly variable)
#         avg_docs_per_tag = max(1, (m.hnsw_nodes + m.flat_nodes) // m.tags if m.tags > 0 else 1)
#         total += m.tags * avg_docs_per_tag * 8  # InternedStringPtr per doc
        
#         # tracked_tags_by_keys_ map
#         # Assume number of docs with tags equals vector count
#         num_docs_with_tags = vector_count
#         if num_docs_with_tags > 0:
#             total += num_docs_with_tags * 48  # TagInfo struct
            
#             # doc_to_fields_ map
#             total += num_docs_with_tags * 32  # Assuming 1 tag field per doc
    
#     # 6. Numeric Index Memory
#     if m.numeric_records > 0:
#         # tracked_keys_ map
#         TRACKED_KEY_ENTRY = 24  # InternedStringPtr + double
#         total += m.numeric_records * TRACKED_KEY_ENTRY
        
#         # BTreeNumeric overhead
#         unique_values = max(int(math.sqrt(m.numeric_records)), 100)
        
#         BTREE_NODE_SIZE = 64  # btree_map node overhead
#         total += unique_values * BTREE_NODE_SIZE
        
#         # SegmentTree overhead
#         SEGMENT_TREE_ENTRY = 80  # Per unique value
#         total += unique_values * SEGMENT_TREE_ENTRY
        
#         # Document sets in btree
#         total += m.numeric_records * 8  # InternedStringPtr in flat_hash_set
    
#     # 7. Global Key-to-ID Mapping
#     # Need to estimate number of documents from the data
#     # Assume it's the same as vector count (all docs are indexed)
#     num_documents = vector_count
#     if num_documents > 0:
#         # flat_hash_map<InternedStringPtr, uint64_t>
#         total += int(num_documents * 16 * 1.14)  # 16 bytes per entry + load factor
    
#     # 8. Raw data (already counted in specific metrics)
#     total += m.vectors_memory + m.vectors_memory_marked_deleted
#     total += m.tags_memory  # Raw tag string lengths
#     total += m.keys_memory  # Raw key string lengths
    
#     # 9. Fixed Overhead
#     FIXED_OVERHEAD = 100 * 1024  # 100KB for various singletons, pools, etc.
#     total += FIXED_OVERHEAD
    
#     return total

# def approximate_total_memory_simple(m: MemoryMetrics) -> int:
#     """
#     Simplified formula for quick estimation.
#     Returns total memory in bytes.
#     """
#     return (m.vectors_memory + 
#             m.vectors_memory_marked_deleted +
#             (m.hnsw_nodes + m.hnsw_nodes_marked_deleted) * 212 +  # 148 + 64 for overhead
#             (m.hnsw_edges + m.hnsw_edges_marked_deleted) * 4 +
#             m.flat_nodes * 56 +  # 8 + 32 + 16 overhead
#             int(m.tags_memory * 1.3) +  # 30% overhead for Patricia nodes
#             m.numeric_records * 112 +  # 24 + 80 + 8 per record
#             m.interned_strings_memory +  # Already complete
#             (m.interned_strings + m.interned_strings_marked_deleted) * 46 +  # Hash map overhead
#             int(m.keys_memory * 1.14) +  # 14% hash map overhead
#             100 * 1024)  # Fixed overhead

# def format_bytes(bytes_val: int) -> str:
#     """Format bytes into human-readable string."""
#     if bytes_val < 1024:
#         return f"{bytes_val}B"
#     elif bytes_val < 1024 * 1024:
#         return f"{bytes_val / 1024:.1f}KB"
#     elif bytes_val < 1024 * 1024 * 1024:
#         return f"{bytes_val / (1024 * 1024):.1f}MB"
#     else:
#         return f"{bytes_val / (1024 * 1024 * 1024):.2f}GB"

# def generate_metrics_from_test_data(
#     total_keys: int,
#     vector_dim: int,
#     vector_algorithm: str,
#     tags_per_key_mean: float,
#     tag_length_avg: int,
#     num_tag_fields: int,
#     num_numeric_fields: int,
#     hnsw_m: int = 0
# ) -> MemoryMetrics:
#     """
#     Generate MemoryMetrics from test scenario parameters.
    
#     The metrics represent raw data sizes:
#     - vectors_memory: total bytes of vector data
#     - tags_memory: total bytes of tag strings
#     - keys_memory: total bytes of key strings
#     - interned_strings_memory: sum of above (redundant)
#     """
#     m = MemoryMetrics()
    
#     # Vector data
#     if vector_dim > 0:
#         m.vectors_memory = total_keys * vector_dim * 4  # float32
    
#     # HNSW vs FLAT
#     if vector_algorithm == "HNSW" and hnsw_m > 0:
#         m.hnsw_nodes = total_keys
#         # Estimate edges: layer 0 has average M connections per node
#         # Higher layers add ~25% more edges total
#         m.hnsw_edges = int(total_keys * hnsw_m * 1.25)
#     elif vector_algorithm == "FLAT":
#         m.flat_nodes = total_keys
    
#     # Tags
#     # For unique tag scenarios, each key has unique tags
#     total_tag_instances = int(total_keys * tags_per_key_mean)
#     m.tags = total_tag_instances  # Assuming all unique in these test cases
#     m.tags_memory = total_tag_instances * tag_length_avg
    
#     # Keys - assuming format "keyXXXXXX" ~10 bytes average
#     m.keys_memory = total_keys * 10
    
#     # Numeric records
#     m.numeric_records = total_keys * num_numeric_fields
    
#     # String interning count
#     # Keys + tag strings + vectors (if present)
#     m.interned_strings = (
#         total_keys +  # Keys
#         total_keys * num_tag_fields +  # Tag strings (one per field per key)
#         (total_keys if vector_dim > 0 else 0)  # Vector data
#     )
    
#     # Total interned string memory (redundant, but matches the metric definition)
#     m.interned_strings_memory = m.keys_memory + m.tags_memory + m.vectors_memory
    
#     return m

# # Example: Process test scenarios from your data
# def process_test_scenarios():
#     """Process some example scenarios from the test data."""
    
#     test_cases = [
#         # FLAT index cases
#         {
#             "name": "Unique_Keys1000_TagLen100",
#             "total_keys": 1000,
#             "vector_dim": 8,
#             "algorithm": "FLAT",
#             "tags_per_key": 1,
#             "tag_length": 100,
#             "num_tag_fields": 1,
#             "num_numeric_fields": 2,
#             "actual_kb": 1809
#         },
#         {
#             "name": "Unique_Keys500000_TagLen1000",
#             "total_keys": 500000,
#             "vector_dim": 8,
#             "algorithm": "FLAT",
#             "tags_per_key": 1,
#             "tag_length": 1000,
#             "num_tag_fields": 1,
#             "num_numeric_fields": 2,
#             "actual_kb": 1710641
#         },
#         # HNSW index cases
#         {
#             "name": "Unique_Keys100000_TagLen1000_HNSW",
#             "total_keys": 100000,
#             "vector_dim": 1500,
#             "algorithm": "HNSW",
#             "hnsw_m": 16,
#             "tags_per_key": 1,
#             "tag_length": 1000,
#             "num_tag_fields": 1,
#             "num_numeric_fields": 4,
#             "actual_kb": 961557
#         },
#         {
#             "name": "Unique_Keys500000_TagLen100_HNSW",
#             "total_keys": 500000,
#             "vector_dim": 1500,
#             "algorithm": "HNSW",
#             "hnsw_m": 16,
#             "tags_per_key": 1,
#             "tag_length": 100,
#             "num_tag_fields": 1,
#             "num_numeric_fields": 4,
#             "actual_kb": 3936443
#         }
#     ]
    
#     for test in test_cases:
#         metrics = generate_metrics_from_test_data(
#             total_keys=test["total_keys"],
#             vector_dim=test["vector_dim"],
#             vector_algorithm=test["algorithm"],
#             tags_per_key_mean=test["tags_per_key"],
#             tag_length_avg=test["tag_length"],
#             num_tag_fields=test["num_tag_fields"],
#             num_numeric_fields=test["num_numeric_fields"],
#             hnsw_m=test.get("hnsw_m", 0)
#         )
        
#         estimated = approximate_total_memory(metrics)
#         estimated_kb = estimated // 1024
#         actual_kb = test["actual_kb"]
#         accuracy = (estimated_kb / actual_kb * 100) if actual_kb > 0 else 0
        
#         print(f"\n{test['name']}:")
#         print(f"  Estimated: {estimated_kb:,} KB")
#         print(f"  Actual: {actual_kb:,} KB")
#         print(f"  Accuracy: {accuracy:.1f}%")
#         print(f"  Difference: {estimated_kb - actual_kb:+,} KB ({accuracy - 100:+.1f}%)")

# if __name__ == "__main__":
#     process_test_scenarios()