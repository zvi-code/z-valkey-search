#!/usr/bin/env python3
"""
ValkeySearch RDB Key-to-ID Map Corruptor

This tool corrupts the key names in the KEY_TO_ID_MAP supplemental content,
creating mismatches between the index's internal IDs and actual keys in Valkey.

This simulates realistic corruption scenarios:
1. Key renames that weren't reflected in the index
2. Partial writes during persistence
3. Synchronization issues between data and index

The corruption preserves the protobuf structure but modifies key names.
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

def write_varint(value):
    """Write a protobuf varint."""
    result = bytearray()
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)

def find_valkey_search_module_aux(data):
    """Find the ValkeySearch MODULE_AUX section in the RDB."""
    module_id_pattern = bytes([0x81, 0x56, 0x4F, 0x92, 0x79, 0xAA, 0xDC, 0x84, 0x01])
    
    offset = 0
    while offset < len(data):
        pos = data.find(bytes([RDB_OPCODE_MODULE_AUX]), offset)
        if pos == -1:
            break
        
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

def parse_key_to_id_chunk(chunk_data):
    """Parse a KEY_TO_ID_MAP chunk and extract key-ID pairs."""
    # The chunk contains TrackedKeyMetadata protobuf messages
    # Format: repeated TrackedKeyMetadata messages
    # TrackedKeyMetadata { key: string(1), internal_id: uint64(2), magnitude: float(3) }
    
    entries = []
    offset = 0
    
    while offset < len(chunk_data):
        # Each entry starts with a length prefix
        entry_len, offset = read_varint(chunk_data, offset)
        if entry_len is None or offset + entry_len > len(chunk_data):
            break
        
        entry_data = chunk_data[offset:offset + entry_len]
        offset += entry_len
        
        # Parse the TrackedKeyMetadata entry
        key_name = None
        internal_id = None
        magnitude = None
        
        entry_offset = 0
        while entry_offset < len(entry_data):
            tag, entry_offset = read_varint(entry_data, entry_offset)
            if tag is None:
                break
            
            field_number = tag >> 3
            wire_type = tag & 0x7
            
            if field_number == 1 and wire_type == 2:  # key field
                key_len, entry_offset = read_varint(entry_data, entry_offset)
                if key_len is not None and entry_offset + key_len <= len(entry_data):
                    key_name = entry_data[entry_offset:entry_offset + key_len]
                    entry_offset += key_len
            elif field_number == 2 and wire_type == 0:  # internal_id field
                internal_id, entry_offset = read_varint(entry_data, entry_offset)
            elif field_number == 3 and wire_type == 5:  # magnitude field (float)
                if entry_offset + 4 <= len(entry_data):
                    magnitude = struct.unpack('<f', entry_data[entry_offset:entry_offset + 4])[0]
                    entry_offset += 4
            else:
                # Skip unknown fields
                if wire_type == 0:
                    _, entry_offset = read_varint(entry_data, entry_offset)
                elif wire_type == 2:
                    length, entry_offset = read_varint(entry_data, entry_offset)
                    if length is not None:
                        entry_offset += length
                elif wire_type == 5:
                    entry_offset += 4
                else:
                    break
        
        if key_name is not None:
            entries.append({
                'key': key_name,
                'id': internal_id,
                'magnitude': magnitude,
                'offset': offset - entry_len,
                'length': entry_len,
                'raw_entry': entry_data
            })
    
    return entries

def create_corrupted_key_entry(original_entry, corruption_strategy):
    """Create a corrupted version of a TrackedKeyMetadata entry."""
    key_name = original_entry['key']
    internal_id = original_entry['id']
    magnitude = original_entry['magnitude']
    
    # Apply corruption strategy
    if corruption_strategy == 'append':
        # Append random suffix to key
        suffix = f"_corrupted_{random.randint(1000, 9999)}"
        corrupted_key = key_name + suffix.encode()
    elif corruption_strategy == 'replace':
        # Replace with completely different key
        corrupted_key = f"corrupted_key_{internal_id}".encode()
    elif corruption_strategy == 'swap_chars':
        # Swap characters in the key
        if len(key_name) >= 4:
            key_str = key_name.decode('utf-8', errors='ignore')
            # Swap doc0 -> doc1, doc1 -> doc2, etc.
            if key_str.startswith('doc') and key_str[3:].isdigit():
                num = int(key_str[3:])
                corrupted_key = f"doc{(num + 1) % 5}".encode()
            else:
                corrupted_key = key_name[::-1]  # Reverse the key
        else:
            corrupted_key = key_name + b"_bad"
    elif corruption_strategy == 'truncate':
        # Truncate the key
        if len(key_name) > 2:
            corrupted_key = key_name[:-1]
        else:
            corrupted_key = b"x"
    else:  # 'missing'
        # Create a key that doesn't exist in the data
        corrupted_key = f"missing_key_{internal_id}".encode()
    
    # Rebuild the protobuf entry
    entry = bytearray()
    
    # Field 1: key (string)
    entry.extend(write_varint(0x0a))  # Tag: field 1, wire type 2
    entry.extend(write_varint(len(corrupted_key)))
    entry.extend(corrupted_key)
    
    # Field 2: internal_id (uint64)
    if internal_id is not None:
        entry.extend(write_varint(0x10))  # Tag: field 2, wire type 0
        entry.extend(write_varint(internal_id))
    
    # Field 3: magnitude (float)
    if magnitude is not None:
        entry.extend(write_varint(0x1d))  # Tag: field 3, wire type 5
        entry.extend(struct.pack('<f', magnitude))
    
    # Return the complete entry with length prefix
    result = bytearray()
    result.extend(write_varint(len(entry)))
    result.extend(entry)
    
    return bytes(result)

def corrupt_key_to_id_map(rdb_data, corruption_strategy='swap_chars', corruption_rate=0.5):
    """Corrupt the KEY_TO_ID_MAP data in the RDB."""
    # Find the MODULE_AUX section
    module_aux_offset = find_valkey_search_module_aux(rdb_data)
    if module_aux_offset is None:
        print("ERROR: Could not find ValkeySearch MODULE_AUX section")
        return None
    
    # Convert to mutable bytearray
    corrupted_data = bytearray(rdb_data)
    
    # Parse the MODULE_AUX section
    offset = module_aux_offset + 1  # Skip MODULE_AUX opcode
    offset += 9  # Skip module ID
    _, offset = read_rdb_length(rdb_data, offset)  # Skip when_opcode
    _, offset = read_rdb_length(rdb_data, offset)  # Skip when_value
    
    # Skip semantic version
    if rdb_data[offset] == 0x02:
        offset += 6
    else:
        _, offset = read_rdb_length(rdb_data, offset)
    
    # Read section count
    section_count, offset = read_rdb_length(rdb_data, offset)
    if section_count is None:
        print("ERROR: Failed to read section count")
        return None
    
    print(f"Found {section_count} RDB sections")
    
    total_entries = 0
    corrupted_entries = 0
    
    # Process each section
    for i in range(section_count):
        # Read section length and skip section data
        section_len, offset = read_rdb_length(rdb_data, offset)
        if section_len is None:
            break
        offset += section_len
        
        # Process supplemental contents (assume 2 per section)
        for j in range(2):
            if offset >= len(rdb_data):
                break
            
            # Read supplemental header
            header_len_start = offset
            header_len, header_offset = read_rdb_length(rdb_data, offset)
            if header_len is None:
                break
            
            # Parse the header to determine type
            header_data = rdb_data[header_offset:header_offset + header_len]
            supp_type = parse_supplemental_header(header_data)
            
            offset = header_offset + header_len
            
            # Process chunks
            chunk_count = 0
            while offset < len(rdb_data):
                chunk_len_start = offset
                chunk_len, chunk_offset = read_rdb_length(rdb_data, offset)
                if chunk_len is None:
                    break
                
                if chunk_len == 0:  # EOF marker
                    offset = chunk_offset
                    break
                
                chunk_count += 1
                chunk_data_start = chunk_offset
                chunk_data = rdb_data[chunk_offset:chunk_offset + chunk_len]
                
                if supp_type == SUPPLEMENTAL_CONTENT_KEY_TO_ID_MAP:
                    # Parse and corrupt the KEY_TO_ID_MAP chunk
                    entries = parse_key_to_id_chunk(chunk_data)
                    
                    if entries:
                        print(f"\n  Found KEY_TO_ID_MAP chunk {chunk_count} with {len(entries)} entries:")
                        
                        # Build corrupted chunk
                        new_chunk = bytearray()
                        
                        for entry in entries:
                            total_entries += 1
                            key_str = entry['key'].decode('utf-8', errors='ignore')
                            
                            # Decide whether to corrupt this entry
                            if random.random() < corruption_rate:
                                corrupted_entry = create_corrupted_key_entry(entry, corruption_strategy)
                                new_chunk.extend(corrupted_entry)
                                corrupted_entries += 1
                                
                                # Show what we changed
                                new_entries = parse_key_to_id_chunk(corrupted_entry)
                                if new_entries:
                                    new_key = new_entries[0]['key'].decode('utf-8', errors='ignore')
                                    print(f"    Corrupted: '{key_str}' -> '{new_key}' (ID: {entry['id']})")
                            else:
                                # Keep original entry
                                new_chunk.extend(write_varint(len(entry['raw_entry'])))
                                new_chunk.extend(entry['raw_entry'])
                                print(f"    Preserved: '{key_str}' (ID: {entry['id']})")
                        
                        # Replace the chunk data
                        if len(new_chunk) == chunk_len:
                            corrupted_data[chunk_data_start:chunk_data_start + chunk_len] = new_chunk
                        else:
                            print(f"    WARNING: Chunk size mismatch: {len(new_chunk)} vs {chunk_len}")
                            # Need to update the length encoding
                            new_len_encoding = write_rdb_length(len(new_chunk))
                            
                            # This is tricky - we need to adjust the entire RDB
                            # For now, we'll pad/truncate to maintain size
                            if len(new_chunk) < chunk_len:
                                # Pad with zeros
                                new_chunk.extend(bytes(chunk_len - len(new_chunk)))
                                corrupted_data[chunk_data_start:chunk_data_start + chunk_len] = new_chunk
                            else:
                                # Truncate
                                corrupted_data[chunk_data_start:chunk_data_start + chunk_len] = new_chunk[:chunk_len]
                
                offset = chunk_offset + chunk_len
    
    print(f"\nCorruption complete:")
    print(f"  - Total key entries: {total_entries}")
    print(f"  - Corrupted entries: {corrupted_entries}")
    print(f"  - Corruption rate: {corruption_rate:.1%}")
    print(f"  - Corruption strategy: {corruption_strategy}")
    
    return bytes(corrupted_data)

def main():
    parser = argparse.ArgumentParser(description='Corrupt ValkeySearch KEY_TO_ID_MAP in RDB file')
    parser.add_argument('input_rdb', help='Input RDB file')
    parser.add_argument('output_rdb', help='Output RDB file with corrupted key mappings')
    parser.add_argument('--corruption-strategy', 
                        choices=['append', 'replace', 'swap_chars', 'truncate', 'missing'],
                        default='swap_chars', 
                        help='Strategy for corrupting key names')
    parser.add_argument('--corruption-rate', type=float, default=0.5,
                        help='Fraction of keys to corrupt (0.0-1.0, default: 0.5)')
    parser.add_argument('--seed', type=int, help='Random seed for reproducibility')
    
    args = parser.parse_args()
    
    if args.seed:
        random.seed(args.seed)
    
    # Read the input RDB file
    print(f"Reading RDB file: {args.input_rdb}")
    with open(args.input_rdb, 'rb') as f:
        rdb_data = f.read()
    
    print(f"RDB size: {len(rdb_data)} bytes")
    
    # Corrupt the key-to-ID mappings
    corrupted_rdb = corrupt_key_to_id_map(
        rdb_data, 
        args.corruption_strategy, 
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
    print("\nCorruption complete! The KEY_TO_ID mappings have been corrupted.")
    print("\nThis simulates scenarios where:")
    print("  - Keys were renamed but the index wasn't updated")
    print("  - Partial writes occurred during persistence")
    print("  - Synchronization issues between data and index")
    print("\nWhen loaded, ValkeySearch should detect the mismatch and trigger rebuilding.")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())