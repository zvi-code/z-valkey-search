#!/usr/bin/env python3
"""
ValkeySearch RDB Index Data Corruptor

This tool corrupts the vector index data (HNSW graph structure) in an RDB file
while preserving:
1. The index schema
2. The chunk structure
3. The key-to-ID mappings

This allows testing of index rebuilding features that can reconstruct
corrupted indexes from the data in Valkey and the index schema.
"""

import os
import sys
import struct
import random
import argparse
from pathlib import Path

# RDB Opcodes
RDB_OPCODE_MODULE_AUX = 0xF7
RDB_OPCODE_EOF = 0xFF
RDB_MODULE_OPCODE_EOF = 0

# Supplemental Content Types
SUPPLEMENTAL_CONTENT_INDEX_CONTENT = 1
SUPPLEMENTAL_CONTENT_KEY_TO_ID_MAP = 2

def read_rdb_length(data, offset):
    """Read RDB length encoding."""
    if offset >= len(data):
        return None, offset
    
    first_byte = data[offset]
    encoding_type = (first_byte & 0xC0) >> 6
    
    if encoding_type == 0:  # 00xxxxxx - 6-bit length
        length = first_byte & 0x3F
        return length, offset + 1
    
    elif encoding_type == 1:  # 01xxxxxx - 14-bit length
        if offset + 1 >= len(data):
            return None, offset
        length = ((first_byte & 0x3F) << 8) | data[offset + 1]
        return length, offset + 2
    
    elif encoding_type == 2:  # 10xxxxxx - 32-bit length
        if first_byte == 0x80:
            if offset + 4 >= len(data):
                return None, offset
            value = struct.unpack('>I', data[offset + 1:offset + 5])[0]
            return value, offset + 5
        else:
            if offset + 4 >= len(data):
                return None, offset
            length = struct.unpack('>I', data[offset + 1:offset + 5])[0]
            return length, offset + 5
    
    else:  # 11xxxxxx - special encoding
        special_type = first_byte & 0x3F
        
        if special_type == 0:  # 64-bit integer
            if offset + 8 >= len(data):
                return None, offset
            value = struct.unpack('<Q', data[offset + 1:offset + 9])[0]
            return value, offset + 9
        
        elif special_type == 1:  # 32-bit integer
            if offset + 4 >= len(data):
                return None, offset
            value = struct.unpack('<I', data[offset + 1:offset + 5])[0]
            return value, offset + 5
        
        elif special_type == 2:  # 16-bit integer
            if offset + 2 >= len(data):
                return None, offset
            value = struct.unpack('<H', data[offset + 1:offset + 3])[0]
            return value, offset + 3
        
        elif special_type == 3:  # 8-bit integer
            if offset + 1 >= len(data):
                return None, offset
            value = data[offset + 1]
            return value, offset + 2
        
        else:
            return None, offset

def write_rdb_length(value):
    """Write RDB length encoding."""
    if value < 64:  # 6-bit length
        return bytes([value])
    elif value < 16384:  # 14-bit length
        return bytes([0x40 | (value >> 8), value & 0xFF])
    else:  # 32-bit length
        return bytes([0x80]) + struct.pack('>I', value)

def read_varint(data, offset):
    """Read a protobuf varint."""
    if offset >= len(data):
        return None, offset
    
    result = 0
    shift = 0
    
    while offset < len(data):
        byte_val = data[offset]
        offset += 1
        
        result |= (byte_val & 0x7F) << shift
        
        if (byte_val & 0x80) == 0:
            break
            
        shift += 7
        if shift >= 64:
            return None, offset
    
    return result, offset

def find_valkey_search_module_aux(data):
    """Find the ValkeySearch MODULE_AUX section in the RDB."""
    # Expected module ID pattern for ValkeySearch
    module_id_pattern = bytes([0x81, 0x56, 0x4F, 0x92, 0x79, 0xAA, 0xDC, 0x84, 0x01])
    
    offset = 0
    while offset < len(data):
        # Look for MODULE_AUX opcode
        pos = data.find(bytes([RDB_OPCODE_MODULE_AUX]), offset)
        if pos == -1:
            break
        
        # Check if this is followed by ValkeySearch module ID
        if pos + 1 + len(module_id_pattern) < len(data):
            if data[pos + 1:pos + 1 + len(module_id_pattern)] == module_id_pattern:
                print(f"Found ValkeySearch MODULE_AUX at offset 0x{pos:04x}")
                return pos
        
        offset = pos + 1
    
    return None

def parse_supplemental_header(header_data):
    """Parse SupplementalContentHeader to determine type."""
    offset = 0
    while offset < len(header_data):
        tag, offset = read_varint(header_data, offset)
        if tag is None:
            break
            
        field_number = tag >> 3
        wire_type = tag & 0x7
        
        if field_number == 1 and wire_type == 0:  # type field
            value, offset = read_varint(header_data, offset)
            if value is not None:
                return value
        else:
            # Skip other fields
            if wire_type == 0:  # Varint
                _, offset = read_varint(header_data, offset)
            elif wire_type == 2:  # Length-delimited
                length, offset = read_varint(header_data, offset)
                if length is not None:
                    offset += length
            else:
                break
    
    return None

def corrupt_index_data(data, corruption_type='random', corruption_rate=0.1):
    """Corrupt the index data using various strategies."""
    corrupted = bytearray(data)
    
    if corruption_type == 'random':
        # Random byte corruption
        num_corruptions = int(len(data) * corruption_rate)
        for _ in range(num_corruptions):
            pos = random.randint(0, len(data) - 1)
            corrupted[pos] = random.randint(0, 255)
    
    elif corruption_type == 'zero':
        # Zero out portions of the data
        num_corruptions = int(len(data) * corruption_rate)
        for _ in range(num_corruptions):
            pos = random.randint(0, len(data) - 1)
            corrupted[pos] = 0
    
    elif corruption_type == 'flip':
        # Bit flipping
        num_corruptions = int(len(data) * corruption_rate)
        for _ in range(num_corruptions):
            pos = random.randint(0, len(data) - 1)
            bit = random.randint(0, 7)
            corrupted[pos] ^= (1 << bit)
    
    elif corruption_type == 'shuffle':
        # Shuffle chunks of data
        chunk_size = 16
        chunks = [corrupted[i:i+chunk_size] for i in range(0, len(corrupted), chunk_size)]
        num_shuffles = int(len(chunks) * corruption_rate)
        for _ in range(num_shuffles):
            i = random.randint(0, len(chunks) - 1)
            j = random.randint(0, len(chunks) - 1)
            chunks[i], chunks[j] = chunks[j], chunks[i]
        corrupted = bytearray(b''.join(chunks))
    
    return bytes(corrupted)

def corrupt_valkey_search_index(rdb_data, corruption_type='random', corruption_rate=0.1):
    """Corrupt the ValkeySearch index data in the RDB."""
    # Find the MODULE_AUX section
    module_aux_offset = find_valkey_search_module_aux(rdb_data)
    if module_aux_offset is None:
        print("ERROR: Could not find ValkeySearch MODULE_AUX section")
        return None
    
    # Convert to mutable bytearray
    corrupted_data = bytearray(rdb_data)
    
    # Parse the MODULE_AUX section
    offset = module_aux_offset + 1  # Skip MODULE_AUX opcode
    
    # Skip module ID (9 bytes for the pattern we found)
    offset += 9
    
    # Skip when_opcode and when_value
    _, offset = read_rdb_length(rdb_data, offset)
    _, offset = read_rdb_length(rdb_data, offset)
    
    # Parse the ValkeySearch payload
    # Skip semantic version
    if rdb_data[offset] == 0x02:  # Special encoding
        offset += 6  # Skip the special semantic version encoding
    else:
        _, offset = read_rdb_length(rdb_data, offset)
    
    # Read section count
    section_count, offset = read_rdb_length(rdb_data, offset)
    if section_count is None:
        print("ERROR: Failed to read section count")
        return None
    
    print(f"Found {section_count} RDB sections")
    section_count = min(section_count, 1)  # Limit to 100 sections for safety
    if section_count < 1:
        print("ERROR: No sections found in ValkeySearch index")
        return None
    # Process each section
    corrupted_chunks = 0
    preserved_chunks = 0
    
    for i in range(section_count):
        # Read section length
        section_len, offset = read_rdb_length(rdb_data, offset)
        if section_len is None:
            break
        
        # Skip the section data
        offset += section_len
        
        # Now we should be at supplemental content
        # Read supplemental count from the section (we'd need to parse it properly)
        # For now, let's assume 1 supplemental contents per section
        supplemental_count = 1
        
        for j in range(supplemental_count):
            if offset >= len(rdb_data):
                break
            
            # Read supplemental header length
            header_len_start = offset
            header_len, offset = read_rdb_length(rdb_data, offset)
            if header_len is None:
                break
            
            # Parse the header to determine type
            header_data = rdb_data[offset:offset + header_len]
            supp_type = parse_supplemental_header(header_data)
            
            offset += header_len
            
            # Now process chunks
            chunk_count = 0
            while offset < len(rdb_data):
                chunk_len_start = offset
                chunk_len, chunk_len_end = read_rdb_length(rdb_data, offset)
                if chunk_len is None:
                    break
                
                offset = chunk_len_end
                
                if chunk_len == 0:  # EOF marker
                    break
                
                chunk_count += 1
                
                # Only corrupt INDEX_CONTENT chunks, not KEY_TO_ID_MAP
                if supp_type == SUPPLEMENTAL_CONTENT_INDEX_CONTENT:
                    # Corrupt this chunk's data
                    chunk_data = rdb_data[offset:offset + chunk_len]
                    corrupted_chunk = corrupt_index_data(chunk_data, corruption_type, corruption_rate)
                    
                    # Replace in the corrupted data
                    corrupted_data[offset:offset + chunk_len] = corrupted_chunk
                    corrupted_chunks += 1
                    
                    print(f"  Corrupted INDEX_CONTENT chunk {chunk_count} ({chunk_len} bytes)")
                    break  # Only corrupt the first chunk for now
                else:
                    preserved_chunks += 1
                    print(f"  Preserved KEY_TO_ID_MAP chunk {chunk_count} ({chunk_len} bytes)")
                
                offset += chunk_len
    
    print(f"\nCorruption complete:")
    print(f"  - Corrupted chunks: {corrupted_chunks}")
    print(f"  - Preserved chunks: {preserved_chunks}")
    print(f"  - Corruption type: {corruption_type}")
    print(f"  - Corruption rate: {corruption_rate}")
    
    return bytes(corrupted_data)

def main():
    parser = argparse.ArgumentParser(description='Corrupt ValkeySearch index data in RDB file')
    parser.add_argument('input_rdb', help='Input RDB file')
    parser.add_argument('output_rdb', help='Output RDB file with corrupted index')
    parser.add_argument('--corruption-type', choices=['random', 'zero', 'flip', 'shuffle'],
                        default='random', help='Type of corruption to apply')
    parser.add_argument('--corruption-rate', type=float, default=0.1,
                        help='Corruption rate (0.0-1.0, default: 0.1)')
    parser.add_argument('--seed', type=int, help='Random seed for reproducibility')
    
    args = parser.parse_args()
    
    if args.seed:
        random.seed(args.seed)
    
    # Read the input RDB file
    print(f"Reading RDB file: {args.input_rdb}")
    with open(args.input_rdb, 'rb') as f:
        rdb_data = f.read()
    
    print(f"RDB size: {len(rdb_data)} bytes")
    
    # Corrupt the index data
    corrupted_rdb = corrupt_valkey_search_index(
        rdb_data, 
        args.corruption_type, 
        args.corruption_rate
    )
    
    if corrupted_rdb is None:
        print("ERROR: Failed to corrupt RDB file")
        return 1
    
    # Write the corrupted RDB
    print(f"\nWriting corrupted RDB to: {args.output_rdb}")
    with open(args.output_rdb, 'wb') as f:
        f.write(corrupted_rdb)
    
    print(f"Corrupted RDB size: {len(corrupted_rdb)} bytes")
    print("\nCorruption complete! The index data has been corrupted while preserving:")
    print("  - Index schema")
    print("  - Chunk structure")
    print("  - Key-to-ID mappings")
    print("\nThis RDB should trigger your index rebuilding feature.")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())