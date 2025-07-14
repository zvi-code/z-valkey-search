#!/usr/bin/env python3
"""
Verify that the RDB corruption only affected index data and not schema/structure
"""

import sys
import os

# Import the parsing functions from your main script
# You'll need to adjust this import based on your file structure
from analyze_rdb_structure_clean import (
    read_rdb_length, read_varint, parse_rdb_section_protobuf,
    parse_index_schema_protobuf, analyze_valkey_search_module_payload
)

def compare_rdb_structures(original_file, corrupted_file):
    """Compare the structure of original and corrupted RDB files."""
    
    print("Comparing RDB structures...")
    print(f"Original:  {original_file}")
    print(f"Corrupted: {corrupted_file}")
    print("-" * 60)
    
    with open(original_file, 'rb') as f:
        original_data = f.read()
    
    with open(corrupted_file, 'rb') as f:
        corrupted_data = f.read()
    
    # Basic checks
    print(f"Original size:  {len(original_data)} bytes")
    print(f"Corrupted size: {len(corrupted_data)} bytes")
    
    if len(original_data) != len(corrupted_data):
        print("⚠️  WARNING: File sizes differ!")
    else:
        print("✓ File sizes match")
    
    # Compare headers (should be identical)
    header_size = 100  # First 100 bytes should be identical
    if original_data[:header_size] == corrupted_data[:header_size]:
        print("✓ RDB headers match")
    else:
        print("❌ RDB headers differ!")
    
    # Find differences
    print("\nAnalyzing differences...")
    diff_count = 0
    diff_regions = []
    in_diff_region = False
    region_start = 0
    
    for i in range(min(len(original_data), len(corrupted_data))):
        if original_data[i] != corrupted_data[i]:
            if not in_diff_region:
                in_diff_region = True
                region_start = i
            diff_count += 1
        else:
            if in_diff_region:
                in_diff_region = False
                diff_regions.append((region_start, i))
    
    if in_diff_region:
        diff_regions.append((region_start, i))
    
    print(f"Total bytes different: {diff_count}")
    print(f"Number of different regions: {len(diff_regions)}")
    
    # Show first few different regions
    for i, (start, end) in enumerate(diff_regions[:5]):
        print(f"\nDifference region {i+1}: offset 0x{start:04x} - 0x{end:04x} ({end-start} bytes)")
        
        # Show some context
        context_before = 16
        context_after = 16
        
        start_context = max(0, start - context_before)
        end_context = min(len(original_data), end + context_after)
        
        print("  Original:")
        print_hex_excerpt(original_data[start_context:end_context], start_context, highlight_start=start, highlight_end=end)
        
        print("  Corrupted:")
        print_hex_excerpt(corrupted_data[start_context:end_context], start_context, highlight_start=start, highlight_end=end)
    
    if len(diff_regions) > 5:
        print(f"\n... and {len(diff_regions) - 5} more regions")

def print_hex_excerpt(data, offset, highlight_start=None, highlight_end=None):
    """Print a hex dump excerpt with highlighting."""
    for i in range(0, len(data), 16):
        line_offset = offset + i
        hex_part = ""
        ascii_part = ""
        
        for j in range(16):
            if i + j < len(data):
                byte_offset = offset + i + j
                byte_val = data[i + j]
                
                # Highlight different bytes
                if highlight_start and highlight_end and highlight_start <= byte_offset < highlight_end:
                    hex_part += f"\033[91m{byte_val:02x}\033[0m "  # Red
                else:
                    hex_part += f"{byte_val:02x} "
                
                ascii_part += chr(byte_val) if 32 <= byte_val <= 126 else "."
            else:
                hex_part += "   "
                ascii_part += " "
        
        print(f"    {line_offset:08x}  {hex_part} |{ascii_part}|")

# Example usage script
def create_usage_example():
    """Create an example usage script."""
    example = """
#!/bin/bash

# Example usage of the RDB corruptor

# 1. Create a corrupted RDB with random corruption (10% of index data)
python3 corrupt_valkey_search_index.py dump.rdb dump_corrupted_random.rdb --corruption-type random --corruption-rate 0.1

# 2. Create a corrupted RDB with bit flipping (5% of index data)
python3 corrupt_valkey_search_index.py dump.rdb dump_corrupted_flip.rdb --corruption-type flip --corruption-rate 0.05

# 3. Create a corrupted RDB with zeroed data (20% of index data)
python3 corrupt_valkey_search_index.py dump.rdb dump_corrupted_zero.rdb --corruption-type zero --corruption-rate 0.2

# 4. Create a corrupted RDB with shuffled chunks (30% of chunks)
python3 corrupt_valkey_search_index.py dump.rdb dump_corrupted_shuffle.rdb --corruption-type shuffle --corruption-rate 0.3

# 5. Use a seed for reproducible corruption
python3 corrupt_valkey_search_index.py dump.rdb dump_corrupted_seeded.rdb --corruption-type random --corruption-rate 0.15 --seed 42

# 6. Verify the corruption only affected index data
python3 verify_corrupted_rdb.py dump.rdb dump_corrupted_random.rdb

# 7. Test loading the corrupted RDB in Valkey
valkey-server --dbfilename dump_corrupted_random.rdb --dir ./

# The server should load successfully but detect corrupted index data,
# triggering your index rebuilding feature
"""
    return example

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 verify_corrupted_rdb.py <original_rdb> <corrupted_rdb>")
        print("\nExample usage:")
        print(create_usage_example())
        sys.exit(1)
    
    compare_rdb_structures(sys.argv[1], sys.argv[2])