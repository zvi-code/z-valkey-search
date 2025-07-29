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