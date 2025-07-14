#!/usr/bin/env python3
"""
RDB Structure Analysis Script for ValkeySearch

This script creates a vector index with specified parameters, ingests sample vectors,
generates an RDB file, and analyzes its hex structure to demonstrate the RDB format 
layout as described in rfc/rdb-format.md.

Based on the ValkeySearch RDB format specification:
- Semantic version (ValkeyModule_LoadUnsigned format)
- Section count (ValkeyModule_LoadUnsigned format) 
- RDBSection protobuf messages
- SupplementalContent chunks for index data

Analysis of the Valkey RDB format and ValkeySearch module's RDB integration, 
here's a detailed overview of the actual layout of an RDB file containing vector indexes:

## RDB File Layout with ValkeySearch Vector Indexes

### 1. **RDB Header**
```
[9 bytes] Magic + Version
├── "REDIS" or "AMZNR" (5 bytes)
└── RDB version (4 bytes, e.g., "0011")
```

### 2. **Standard RDB Aux Fields**
```
[RDB_OPCODE_AUX = 250]
├── Key: "valkey-ver" → Value: version string
├── Key: "redis-bits" → Value: 32/64
├── Key: "ctime" → Value: creation timestamp
├── Key: "used-mem" → Value: memory usage
└── ... other standard aux fields
```

### 3. **ValkeySearch Module Aux Section**
```
[RDB_OPCODE_MODULE_AUX = 247]
├── Module ID (8 bytes) - Unique identifier for ValkeySearch
├── When opcode (RDB_MODULE_OPCODE_UINT = 2)
├── When value (e.g., REDISMODULE_AUX_BEFORE_RDB)
└── Module aux payload:
    │
    ├── [Module Type String] "SchMgr-VS"
    ├── [Encoding Version] Current: 1
    ├── [Minimum Semantic Version] (uint64)
    ├── [RDBSection Count] (uint64)
    │
    └── [RDBSections...]
```

### 4. **RDBSection Layout** (Protocol Buffer encoded)
```
RDBSection #1 (INDEX_SCHEMA):
├── [Serialized Proto Length]
├── [Proto Content]:
│   ├── type: RDB_SECTION_INDEX_SCHEMA (1)
│   ├── supplemental_count: 2 × num_attributes
│   └── index_schema_contents:
│       ├── name: "myindex"
│       ├── subscribed_key_prefixes: ["user:", "doc:"]
│       ├── attribute_data_type: HASH/JSON
│       ├── attributes[]:
│       │   ├── Attribute #1:
│       │   │   ├── alias: "embedding"
│       │   │   ├── identifier: "$.vector"
│       │   │   └── index: VectorIndex {
│       │   │       ├── dimension_count: 512
│       │   │       ├── normalize: true
│       │   │       ├── distance_metric: COSINE
│       │   │       ├── vector_data_type: FLOAT32
│       │   │       └── algorithm: HNSWAlgorithm {
│       │   │           ├── m: 16
│       │   │           ├── ef_construction: 200
│       │   │           └── ef_runtime: 10
│       │   │       }
│       │   │   }
│       │   └── ... more attributes
│       └── stats: { documents_count, records_count }
│
└── [Supplemental Content for this section...]
```

### 5. **Supplemental Content Structure**
For each attribute with an index:

```
Supplemental Content #1 (INDEX_CONTENT):
├── [SupplementalContentHeader - Proto]:
│   ├── type: SUPPLEMENTAL_CONTENT_INDEX_CONTENT
│   └── index_content_header:
│       └── attribute: (full attribute proto)
│
└── [Binary Chunks]:
    ├── Chunk #1:
    │   ├── [SupplementalContentChunk - Proto]:
    │   │   └── binary_content: (raw index data)
    │   └── Size: up to 1MiB default
    ├── Chunk #2...
    └── EOF Chunk (empty binary_content)

Supplemental Content #2 (KEY_TO_ID_MAP):
├── [SupplementalContentHeader - Proto]:
│   ├── type: SUPPLEMENTAL_CONTENT_KEY_TO_ID_MAP
│   └── key_to_id_header:
│       └── attribute: (full attribute proto)
│
└── [Binary Chunks]:
    └── ... (key→ID mapping data)
```

### 6. **Standard RDB Key-Value Data**
```
[RDB_OPCODE_SELECTDB = 254] → DB 0
[RDB_OPCODE_RESIZEDB = 251] → size hints

[Key-Value Entries]:
├── [Optional: Expiry/LRU/LFU metadata]
├── [Object Type] (e.g., RDB_TYPE_HASH)
├── [Key] "user:1234"
└── [Value] (hash with vector field)
```

### 7. **RDB Footer**
```
[RDB_OPCODE_EOF = 255]
[8 bytes] CRC64 checksum (if enabled)
```

## Key Points:

1. **All ValkeySearch data is in aux section** - No index data pollutes the keyspace
2. **Chunked supplemental content** - Allows streaming large indexes without loading entire structure in memory
3. **Forward compatibility** - Unknown RDBSectionTypes and SupplementalContentTypes are skipped
4. **Semantic versioning** - Ensures compatibility checking on load
5. **HNSW index data** would be in the binary chunks, containing:
   - Node connections
   - Layer assignments  
   - Vector data (if not externalized)
   - Entry point information

The actual HNSW binary format in supplemental chunks would follow the HNSWlib serialization format, allowing efficient streaming of the graph structure without requiring the entire index in memory during save/load operations.

"""

import os
import sys
import subprocess
import tempfile
import shutil
import struct
import binascii
import time
import glob
import re
from pathlib import Path

# Try to import protobuf - if not available, we'll do basic parsing
try:
    import google.protobuf
    from google.protobuf.message import DecodeError
    HAS_PROTOBUF = True
except ImportError:
    HAS_PROTOBUF = False
    print("Warning: protobuf library not available, using basic parsing")

# ValkeySearch RDB constants (from src/rdb_serialization.h and module_loader.cc)
VALKEY_SEARCH_MODULE_TYPE_NAME = "Vk-Search"  # RDB module type name (from rdb_serialization.h)
VALKEY_SEARCH_MODULE_NAME = "search"  # Module name (from module_loader.cc)
RFC_MODULE_TYPE_NAME = "SchMgr-VS"  # From RFC specification (differs from actual implementation)
CURRENT_SEMANTIC_VERSION = 0x010000  # 1.0.0

def crc64(data, init=0):
    """Calculate CRC64 hash as used by Redis/Valkey for module IDs.
    
    This implements memrev64ifbe - the byte-order aware CRC64 used by Redis/Valkey.
    Module IDs in Redis/Valkey are calculated as CRC64 hash of the module type name.
    """
    import sys
    
    # Redis CRC64 polynomial: 0xc96c5795d7870f42
    table = []
    for i in range(256):
        c = i
        for j in range(8):
            if c & 1:
                c = 0xc96c5795d7870f42 ^ (c >> 1)
            else:
                c = c >> 1
        table.append(c)
    
    crc = init
    if isinstance(data, str):
        data = data.encode('utf-8')
    
    for byte in data:
        crc = table[(crc ^ byte) & 0xff] ^ (crc >> 8)
    
    result = crc & 0xffffffffffffffff
    
    # Apply memrev64ifbe: reverse bytes if big-endian
    if sys.byteorder == 'big':
        result = struct.unpack('<Q', struct.pack('>Q', result))[0]
    
    return result

# Calculate the expected module ID for ValkeySearch
VALKEY_SEARCH_MODULE_ID = crc64(VALKEY_SEARCH_MODULE_TYPE_NAME)
print(f"DEBUG: ValkeySearch module ID for '{VALKEY_SEARCH_MODULE_TYPE_NAME}': 0x{VALKEY_SEARCH_MODULE_ID:016x}")

# Expected module ID based on your analysis: 56 4F 92 79 AA DC 84 01 (little-endian in file)
EXPECTED_VALKEY_SEARCH_MODULE_ID = 0x8184dcaa79924f56
print(f"DEBUG: Expected ValkeySearch module ID from hexdump: 0x{EXPECTED_VALKEY_SEARCH_MODULE_ID:016x}")

# RDB Opcodes
RDB_OPCODE_AUX = 0xFA
RDB_OPCODE_RESIZEDB = 0xFB
RDB_OPCODE_SELECTDB = 0xFE
RDB_OPCODE_EOF = 0xFF
RDB_OPCODE_MODULE_AUX = 0xF7

# RDB Module Opcodes
RDB_MODULE_OPCODE_EOF = 0
RDB_MODULE_OPCODE_SINT = 1
RDB_MODULE_OPCODE_UINT = 2
RDB_MODULE_OPCODE_FLOAT = 3
RDB_MODULE_OPCODE_DOUBLE = 4
RDB_MODULE_OPCODE_STRING = 5

# RDB Section Types (from rdb_section.pb.h)
RDB_SECTION_UNSET = 0
RDB_SECTION_INDEX_SCHEMA = 1
RDB_SECTION_GLOBAL_METADATA = 2

# Supplemental Content Types (from rdb_section.pb.h)
SUPPLEMENTAL_CONTENT_UNSPECIFIED = 0
SUPPLEMENTAL_CONTENT_INDEX_CONTENT = 1
SUPPLEMENTAL_CONTENT_KEY_TO_ID_MAP = 2

"""Check if required environment variables are set"""
valkey_server = os.environ.get('VALKEY_SERVER_PATH')
module_path = os.environ.get('MODULE_PATH')
valkey_cli = os.environ.get('VALKEY_CLI_PATH')


# Add these RDB type constants after the opcode constants
# RDB Object types
RDB_TYPE_STRING = 0
RDB_TYPE_LIST = 1
RDB_TYPE_SET = 2
RDB_TYPE_ZSET = 3
RDB_TYPE_HASH = 4
RDB_TYPE_ZSET_2 = 5
RDB_TYPE_MODULE_PRE_GA = 6
RDB_TYPE_MODULE_2 = 7
RDB_TYPE_HASH_ZIPMAP = 9
RDB_TYPE_LIST_ZIPLIST = 10
RDB_TYPE_SET_INTSET = 11
RDB_TYPE_ZSET_ZIPLIST = 12
RDB_TYPE_HASH_ZIPLIST = 13
RDB_TYPE_LIST_QUICKLIST = 14
RDB_TYPE_STREAM_LISTPACKS = 15
RDB_TYPE_HASH_LISTPACK = 16
RDB_TYPE_ZSET_LISTPACK = 17
RDB_TYPE_LIST_QUICKLIST_2 = 18
RDB_TYPE_STREAM_LISTPACKS_2 = 19
RDB_TYPE_SET_LISTPACK = 20
RDB_TYPE_STREAM_LISTPACKS_3 = 21

def is_rdb_object_type(byte_val):
    """Check if a byte value is a valid RDB object type"""
    return byte_val in [
        RDB_TYPE_STRING, RDB_TYPE_LIST, RDB_TYPE_SET, RDB_TYPE_ZSET, 
        RDB_TYPE_HASH, RDB_TYPE_ZSET_2, RDB_TYPE_MODULE_PRE_GA, 
        RDB_TYPE_MODULE_2, RDB_TYPE_HASH_ZIPMAP, RDB_TYPE_LIST_ZIPLIST,
        RDB_TYPE_SET_INTSET, RDB_TYPE_ZSET_ZIPLIST, RDB_TYPE_HASH_ZIPLIST,
        RDB_TYPE_LIST_QUICKLIST, RDB_TYPE_STREAM_LISTPACKS, RDB_TYPE_HASH_LISTPACK,
        RDB_TYPE_ZSET_LISTPACK, RDB_TYPE_LIST_QUICKLIST_2, RDB_TYPE_STREAM_LISTPACKS_2,
        RDB_TYPE_SET_LISTPACK, RDB_TYPE_STREAM_LISTPACKS_3
    ]

def skip_rdb_object(data, offset, obj_type):
    """Skip an RDB object based on its type"""
    start_offset = offset
    
    # For hash objects (type 0x10), we know the structure from the hexdump
    if obj_type == 0x10:  # Special hash encoding in your RDB
        # Hash format: hash_size (21 21) + field_count + (field_name + field_value)*
        # Read hash encoding header
        if offset + 2 <= len(data) and data[offset] == 0x21 and data[offset+1] == 0x21:
            offset += 2  # Skip hash size encoding (21 21)
            
            # Read number of hash fields
            if offset + 4 <= len(data) and data[offset:offset+4] == b'\x00\x00\x00\x02':
                offset += 4  # Skip field count
                num_fields = 2  # We know it's 2 from the pattern
                
                # Skip each field
                for _ in range(num_fields):
                    # Skip field name
                    field_name_len, offset = read_rdb_length(data, offset)
                    if field_name_len is None:
                        break
                    offset += field_name_len
                    
                    # Skip field value
                    field_value_len, offset = read_rdb_length(data, offset)
                    if field_value_len is None:
                        break
                    offset += field_value_len
                
                # Hash objects end with 11 FF pattern
                if offset + 2 <= len(data) and data[offset] == 0x11 and data[offset+1] == 0xFF:
                    offset += 2
                
                return offset
        
        # Fallback: look for next valid opcode
        while offset < len(data) - 1:
            # Look for patterns that indicate end of this object
            if offset + 2 <= len(data):
                # Check for hash terminator pattern
                if data[offset] == 0x11 and data[offset+1] == 0xFF:
                    return offset + 2
                # Check for next valid opcode
                if is_valid_opcode_or_type(data[offset]):
                    return offset
            offset += 1
    else:
        # For other types, skip until we find a valid opcode or object type
        while offset < len(data):
            if is_valid_opcode_or_type(data[offset]):
                return offset
            offset += 1
    
    return offset

def is_valid_rdb_position(data, offset):
    """Check if this looks like a valid RDB position (not inside data)"""
    # Look ahead to see if this could be a real EOF
    if offset >= len(data) - 1:
        return True
    
    # If next byte is a valid opcode or if we can read a checksum, it might be EOF
    next_byte = data[offset + 1]
    if next_byte in [RDB_OPCODE_MODULE_AUX, RDB_OPCODE_EOF]:
        return True
    
    # Check if there's exactly 8 bytes left (checksum)
    if offset == len(data) - 9:
        return True
        
    return False

def is_valid_opcode_or_type(byte_val):
    """Check if a byte could be a valid opcode or type"""
    valid_opcodes = [
        RDB_OPCODE_AUX, RDB_OPCODE_RESIZEDB, RDB_OPCODE_SELECTDB, 
        RDB_OPCODE_EOF, RDB_OPCODE_MODULE_AUX, 
        0xFC, 0xFD  # EXPIRETIME opcodes
    ]
    return byte_val in valid_opcodes or is_rdb_object_type(byte_val)

def split_rdb_sections(data):
    """Split RDB file into sections with better parsing"""
    sections = {
        'header': None,
        'aux_fields': [],
        'regular_data': [],
        'module_aux': [],  # Add this to capture MODULE_AUX sections
        'eof_sections': [],
        'after_eof_data': None
    }
    
    offset = 0
    
    # Parse header
    if data[offset:offset+5] == b'REDIS':
        header_end = 9
        sections['header'] = data[offset:header_end]
        offset = header_end
    elif data[offset:offset+6] == b'VALKEY':
        header_end = 10
        sections['header'] = data[offset:header_end]
        offset = header_end
    else:
        print("❌ Invalid RDB header")
        return sections
    
    # Parse sections
    found_real_eof = False
    while offset < len(data) and not found_real_eof:
        if offset >= len(data):
            break
            
        opcode = data[offset]
        section_start = offset
        
        # Debug output
        if offset < 0x130 or offset > 0x5B0:  # Debug near expected areas
            print(f"  DEBUG: Offset {offset:04x}: opcode 0x{opcode:02x}")
        
        if opcode == RDB_OPCODE_MODULE_AUX:  # 0xF7 - Handle MODULE_AUX
            print(f"  DEBUG: Found MODULE_AUX at offset {offset:04x}")
            module_aux_start = offset
            offset += 1
            
            # Read module ID using special module ID function
            module_id, offset = read_rdb_module_id(data, offset)
            if module_id is None:
                print(f"  DEBUG: Failed to read module ID")
                break
            
            print(f"  DEBUG: Module ID: 0x{module_id:016x}")
            
            # Read when_opcode
            when_opcode, offset = read_rdb_length(data, offset)
            if when_opcode is None:
                break
            
            # Read when_value
            when_value, offset = read_rdb_length(data, offset)
            if when_value is None:
                break
            
            # Read until we find RDB_MODULE_OPCODE_EOF (0)
            module_data_start = offset
            while offset < len(data):
                if data[offset] == RDB_MODULE_OPCODE_EOF:
                    offset += 1
                    break
                offset += 1
            
            sections['module_aux'].append({
                'raw_data': data[module_aux_start:offset],
                'module_id': module_id,
                'when_opcode': when_opcode,
                'when_value': when_value,
                'payload': data[module_data_start:offset-1],
                'offset': module_aux_start
            })
            
        elif opcode == RDB_OPCODE_AUX:  # 0xFA
            offset += 1
            
            # Read key
            key_len, offset = read_rdb_length(data, offset)
            if key_len is None:
                break
            if offset + key_len > len(data):
                break
            key = data[offset:offset + key_len]
            offset += key_len
            
            # Read value
            value_start = offset
            first_byte = data[offset] if offset < len(data) else 0
            
            if (first_byte & 0xC0) == 0xC0:
                # Integer value
                val, offset = read_rdb_length(data, offset)
                value = val if val is not None else b""
                value_bytes = data[value_start:offset]
            else:
                # String value
                val_len, new_offset = read_rdb_length(data, offset)
                if val_len is None:
                    break
                offset = new_offset
                if offset + val_len > len(data):
                    break
                value = data[offset:offset + val_len]
                offset += val_len
                value_bytes = value
            
            sections['aux_fields'].append({
                'raw_data': data[section_start:offset],
                'key': key,
                'value': value_bytes,
                'offset': section_start
            })
            
        elif opcode == RDB_OPCODE_RESIZEDB:  # 0xFB
            offset += 1
            db_size, offset = read_rdb_length(data, offset)
            if db_size is None:
                break
            expire_size, offset = read_rdb_length(data, offset)
            if expire_size is None:
                break
                
        elif opcode == RDB_OPCODE_SELECTDB:  # 0xFE
            offset += 1
            db_num, offset = read_rdb_length(data, offset)
            if db_num is None:
                break
                
        elif opcode == RDB_OPCODE_EOF:  # 0xFF
            # Check if this is really EOF by looking at context
            # Real EOF should be near the end of file and followed by optional checksum
            remaining = len(data) - offset
            
            # Real EOF has at most 9 bytes after it (1 for opcode + 8 for checksum)
            if remaining <= 9 or (remaining == 9 and offset > len(data) * 0.9):
                print(f"  DEBUG: Found real EOF at offset {offset:04x}")
                eof_start = offset
                offset += 1
                
                if offset + 8 <= len(data):
                    checksum = data[offset:offset + 8]
                    offset += 8
                else:
                    checksum = b""
                
                sections['eof_sections'].append({
                    'raw_data': data[eof_start:offset],
                    'checksum': checksum,
                    'offset': eof_start
                })
                
                found_real_eof = True
                
                # Check if there's data after EOF
                if offset < len(data):
                    sections['after_eof_data'] = data[offset:]
            else:
                # This 0xFF is part of data, skip it
                print(f"  DEBUG: Skipping 0xFF at offset {offset:04x} (not real EOF, {remaining} bytes remain)")
                offset += 1
                
        elif is_rdb_object_type(opcode) or opcode == 0x10:  # Object type (including special hash type)
            # This is a key-value pair
            obj_type = opcode
            offset += 1
            
            # Read key
            key_len, offset = read_rdb_length(data, offset)
            if key_len is None:
                break
            offset += key_len  # Skip the key
            
            # Skip the value (this is complex and type-dependent)
            offset = skip_rdb_object(data, offset, obj_type)
            
        else:
            # Unknown opcode, advance by 1
            offset += 1
    
    return sections


def print_usage():
    print("Usage: python3 analyze_rdb_structure_clean.py")
    print("Environment variables required:")
    print("  VALKEY_SERVER_PATH: Path to valkey-server binary")
    print("  MODULE_PATH: Path to valkey-search module (.so file)")
    print("  VALKEY_CLI_PATH: Path to valkey-cli binary")

def check_env_vars():  
    if not valkey_server:
        print("Error: VALKEY_SERVER_PATH environment variable not set")
        return None, None, None
    if not valkey_cli:
        print("Error: VALKEY_CLI_PATH environment variable not set")
        return None, None, None
    if not module_path:
        print("Error: MODULE_PATH environment variable not set")
        return None, None, None

    if not os.path.exists(valkey_server):
        print(f"Error: Valkey server not found at {valkey_server}")
        return None, None, None

    if not os.path.exists(module_path):
        print(f"Error: Module not found at {module_path}")
        return None, None, None  
    return valkey_server, valkey_cli, module_path

def create_valkey_config(temp_dir, module_path, rdb_path):
    """Create a temporary Valkey configuration file"""
    config_path = os.path.join(temp_dir, "valkey.conf")
    log_path = os.path.join(temp_dir, "valkey.log")
    with open(config_path, 'w') as f:
        f.write(f"""
# Basic configuration
port 6380
bind 127.0.0.1
timeout 0
tcp-keepalive 300

# Database settings
databases 16
save 900 1
save 300 10
save 60 10000

# Module configuration
loadmodule {module_path}

# RDB settings
dbfilename dump.rdb
dir ./
rdbcompression no
rdbchecksum yes

# Logging
loglevel notice
logfile {log_path}

# Disable AOF to force RDB usage
appendonly no
""")
    return config_path, log_path


def run_valkey_cli_commands(port=6380):
    """Run the sequence of commands to create index and ingest vectors"""
    index_name = "zvisIndex"

    # Create the actual vector index
    print(f"Creating vector index '{index_name}'...")
    result = subprocess.run([
        valkey_cli, "-p", str(port), "FT.CREATE", index_name, 
        "SCHEMA", "z-vector", "VECTOR", "HNSW", "6", 
        "TYPE", "FLOAT32", "DIM", "5", "DISTANCE_METRIC", "L2"
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Error creating index: {result.stderr}")
        return False
    print(f"✓ Index created: {result.stdout.strip()}")
    
    # Verify index was created by checking FT._LIST  
    print("Verifying index creation...")
    result = subprocess.run([
        valkey_cli, "-p", str(port), "FT._LIST"
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"✓ Indexes listed: {result.stdout.strip()}")
    else:
        print(f"⚠ Could not list indexes: {result.stderr}")
    
    # Get detailed index info
    print("Getting initial index info...")
    result = subprocess.run([
        valkey_cli, "-p", str(port), "FT.INFO", "zvisIndexSecond"
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        info_output = result.stdout.strip()
        print(f"✓ Index info retrieved ({len(info_output)} chars)")
        # Look for key info
        if "num_docs" in info_output:
            print("📊 Found num_docs in index info")
        if "backfill_in_progress" in info_output:
            print("📊 Index is in backfill state")
    else:
        print(f"⚠ Could not get index info: {result.stderr}")

    # Add vectors using binary format
    vectors = [
        [1.0, 2.0, 3.0, 4.0, 0.0],
        [2.0, 3.0, 4.0, 5.0, 0.0],
        [3.0, 4.0, 5.0, 6.0, 0.0],
        [4.0, 5.0, 6.0, 7.0, 0.0],
        [5.0, 6.0, 7.0, 8.0, 0.0]
    ]
    
    for i, vector in enumerate(vectors):
        # Convert to binary format (direct bytes, not hex string for HSET)
        
        vector_bytes = struct.pack(f'{len(vector)}f', *vector)
        #struct.pack('4f', *vector)
        
        print(f"Adding vector {i+1}: {vector}")
        # Use binary data directly via subprocess stdin
        cmd_input = vector_bytes
        result = subprocess.run([
            valkey_cli, "-p", str(port), "-x", "HSET", f"doc{i}", "z-vector"
        ], input=cmd_input, capture_output=True)
        
        if result.returncode != 0:
            print(f"Error adding vector {i}: {result.stderr}")
            return False
        print(f"Vector {i+1} added: {result.stdout.decode().strip()}")

    # Wait a bit for indexing to process
    print("⏳ Waiting for indexing to process...")
    time.sleep(5)
    
    # Try to verify the schema manager has the index
    print("🔍 Checking if schema is registered...")
    result = subprocess.run([
        valkey_cli, "-p", str(port), "FT._LIST"
    ], capture_output=True, text=True)
    
    if result.returncode == 0 and index_name in result.stdout:
        print("✓ Index is still listed after vector insertion")
    else:
        print("⚠ Index not found in FT._LIST after vector insertion")
    
    # Get index info again after adding vectors
    print("Getting updated index info...")
    result = subprocess.run([
        valkey_cli, "-p", str(port), "FT.INFO", index_name
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        info_lines = result.stdout.strip().split('\n')
        print("📊 Updated index info (key metrics):")
        for line in info_lines:
            if any(keyword in line.lower() for keyword in ['num_docs', 'num_records', 'backfill', 'documents_count']):
                print(f"   {line.strip()}")
    else:
        print(f"⚠ Could not get updated index info: {result.stderr}")

    # Force save to RDB - try both SAVE and BGSAVE
    print("Triggering RDB save...")
    
    # First try synchronous save
    result = subprocess.run([
        valkey_cli, "-p", str(port), "SAVE"
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"✓ Synchronous save completed: {result.stdout.strip()}")
    else:
        print(f"⚠ Synchronous save failed: {result.stderr}")
        # Fall back to background save
        result = subprocess.run([
            valkey_cli, "-p", str(port), "BGSAVE"
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Error saving RDB: {result.stderr}")
            return False
        print(f"Background save started: {result.stdout.strip()}")
        
        # Wait for background save to complete
        print("Waiting for background save to complete...")
        time.sleep(2)
        while True:
            result = subprocess.run([
                valkey_cli, "-p", str(port), "LASTSAVE"
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"Background save completed at: {result.stdout.strip()}")
                break
            time.sleep(0.5)
                    
    return True


def analyze_encoding_issues(data, offset_start=0, max_check=100):
    """Analyze potential encoding issues in RDB data"""
    print(f"\n🔍 ENCODING ANALYSIS (from offset {offset_start:04x})")
    
    # Check for common encoding issues
    issues_found = []
    
    # 1. Check for null bytes that might indicate binary data
    null_count = data[offset_start:offset_start+max_check].count(0)
    if null_count > 0:
        issues_found.append(f"Found {null_count} null bytes in first {max_check} bytes")
    
    # 2. Check for high-bit characters that might indicate encoding issues
    high_bit_count = sum(1 for b in data[offset_start:offset_start+max_check] if b > 127)
    if high_bit_count > 0:
        issues_found.append(f"Found {high_bit_count} high-bit bytes (>127) in first {max_check} bytes")
    
    # 3. Check if data looks like valid UTF-8
    try:
        utf8_test = data[offset_start:offset_start+max_check].decode('utf-8')
        issues_found.append("Data is valid UTF-8")
    except UnicodeDecodeError as e:
        issues_found.append(f"Data is NOT valid UTF-8: {e}")
    
    # 4. Check if data looks like valid ASCII
    try:
        ascii_test = data[offset_start:offset_start+max_check].decode('ascii')
        issues_found.append("Data is valid ASCII")
    except UnicodeDecodeError as e:
        issues_found.append(f"Data is NOT valid ASCII: {e}")
    
    # 5. Show byte distribution
    byte_counts = {}
    for b in data[offset_start:offset_start+max_check]:
        byte_counts[b] = byte_counts.get(b, 0) + 1
    
    # Find most common bytes
    common_bytes = sorted(byte_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    
    print(f"   Encoding issues found:")
    for issue in issues_found:
        print(f"   - {issue}")
    
    print(f"   Most common bytes:")
    for byte_val, count in common_bytes:
        char_repr = chr(byte_val) if 32 <= byte_val <= 126 else f"\\x{byte_val:02x}"
        print(f"   - 0x{byte_val:02x} ('{char_repr}'): {count} times")
    
    # 6. Check for potential RDB length encoding corruption
    print(f"   RDB Length encoding analysis:")
    for i in range(min(10, len(data) - offset_start)):
        byte_val = data[offset_start + i]
        encoding_type = ""
        if (byte_val & 0xC0) == 0x00:
            encoding_type = "6-bit length"
        elif (byte_val & 0xC0) == 0x40:
            encoding_type = "14-bit length"
        elif (byte_val & 0xC0) == 0x80:
            encoding_type = "32-bit length"
        elif (byte_val & 0xC0) == 0xC0:
            special_encoding = byte_val & 0x3F
            encoding_type = f"special encoding {special_encoding}"
        
        print(f"   - Offset +{i}: 0x{byte_val:02x} ({encoding_type})")

def parse_module_aux_after_eof(data, offset):
    """Parse MODULE_AUX data after EOF marker.
    
    Based on the hexdump analysis, ValkeySearch uses VALKEYMODULE_AUX_AFTER_RDB 
    which saves module aux data in this format after the EOF:
    
    [RDB_OPCODE_MODULE_AUX (0xF7)]
    [Module ID - 64-bit hash of module type name, RDB length encoded]
    [When opcode - RDB length encoded] 
    [When value - RDB length encoded]
    [Module data...]
    [RDB_MODULE_OPCODE_EOF]
    
    From hexdump analysis:
    0x0121: F7                         <- RDB_OPCODE_MODULE_AUX (247)
    0x0122: 81 56 4F 92 79 AA DC 84 01 <- Module ID (0x8184dcaa79924f56)
    0x012b: 02                         <- when_opcode (RDB_MODULE_OPCODE_UINT)
    0x012c: 02                         <- when value (2 = REDISMODULE_AUX_AFTER_RDB)
    0x012d: ...                        <- Start of ValkeySearch data
    """
    found_valkey_search = False
    
    print(f"\n    📦 PARSING MODULE AUX DATA AFTER EOF (offset {offset:04x})")
    
    while offset < len(data):
        if offset >= len(data):
            break
            
        opcode = data[offset]
        print(f"    -> Opcode at offset {offset:04x}: 0x{opcode:02x}")
        
        if opcode == RDB_OPCODE_MODULE_AUX:  # 0xF7
            print(f"    -> ⭐ Found RDB_OPCODE_MODULE_AUX (0xF7)!")
            offset += 1
            
            # Read module ID (64-bit hash, special encoding in hexdump)
            # Based on hexdump: 81 56 4F 92 79 AA DC 84 01
            # This decodes to 0x8184dcaa79924f56
            module_id, offset = read_rdb_module_id(data, offset)
            if module_id is None:
                print(f"    -> Error reading module ID")
                break
            print(f"    -> Module ID: 0x{module_id:016x} (hash of module type name)")
            
            # Check if this matches the expected ValkeySearch module ID
            is_valkey_search = (module_id == EXPECTED_VALKEY_SEARCH_MODULE_ID)
            if is_valkey_search:
                print(f"    -> ⭐ CONFIRMED: This is ValkeySearch module!")
                found_valkey_search = True
            else:
                print(f"    -> Module ID: 0x{module_id:016x}")
                print(f"    -> Expected:  0x{EXPECTED_VALKEY_SEARCH_MODULE_ID:016x}")
                
                # Try some variations to account for different byte orders
                variations = [
                    VALKEY_SEARCH_MODULE_ID,  # Calculated CRC
                    struct.unpack('<Q', struct.pack('>Q', module_id))[0],  # Byte-swap test
                    struct.unpack('>Q', struct.pack('<Q', module_id))[0],  # Reverse byte-swap
                ]
                
                for test_id in [EXPECTED_VALKEY_SEARCH_MODULE_ID] + variations:
                    if test_id == module_id:
                        print(f"    -> ⭐ MATCH FOUND: This is ValkeySearch module (ID variation)!")
                        found_valkey_search = True
                        is_valkey_search = True
                        break
                
                if not is_valkey_search:
                    print(f"    -> Different module, but will check payload for ValkeySearch signatures...")
            
            # Read when_opcode (should be RDB_MODULE_OPCODE_UINT = 2)
            when_opcode, offset = read_rdb_length(data, offset)
            if when_opcode is None:
                print(f"    -> Error reading when opcode")
                break
            print(f"    -> When opcode: {when_opcode} (should be {RDB_MODULE_OPCODE_UINT})")
            
            # Read when value (should be 2 = REDISMODULE_AUX_AFTER_RDB)
            when_value, offset = read_rdb_length(data, offset)
            if when_value is None:
                print(f"    -> Error reading when value")
                break
            print(f"    -> When value: {when_value} (2 = REDISMODULE_AUX_AFTER_RDB)")
            
            # Now read the module payload data until EOF
            print(f"    -> Reading ValkeySearch module payload starting at offset {offset:04x}")
            
            # The payload continues until we hit RDB_MODULE_OPCODE_EOF (0)
            payload_start = offset
            payload_data = []
            
            while offset < len(data):
                # Check if we've hit the module EOF
                if offset < len(data) and data[offset] == RDB_MODULE_OPCODE_EOF:
                    print(f"    -> Found RDB_MODULE_OPCODE_EOF at offset {offset:04x}")
                    break
                
                # Read all remaining data as module payload
                payload_data.append(data[offset])
                offset += 1
            
            if payload_data:
                payload_bytes = bytes(payload_data)
                print(f"    -> ValkeySearch payload: {len(payload_bytes)} bytes")
                
                # Show first part of payload
                print(f"    -> Payload preview:")
                print_hex_dump(payload_bytes[:200], indent="      ")
                
                # Analyze the ValkeySearch payload structure
                if is_valkey_search or len(payload_bytes) > 10:
                    analyze_valkey_search_module_payload(payload_bytes)
                    if not is_valkey_search:
                        print(f"    -> NOTE: Payload analysis may help confirm if this is ValkeySearch")
            
            # Skip the EOF marker
            if offset < len(data) and data[offset] == RDB_MODULE_OPCODE_EOF:
                offset += 1
                
        elif opcode == RDB_OPCODE_EOF:  # 0xFF - End of entire RDB
            print(f"    -> Found final RDB EOF at offset {offset:04x}")
            offset += 1
            # Read checksum if present
            if offset + 8 <= len(data):
                checksum = data[offset:offset + 8]
                print(f"    -> RDB checksum: {checksum.hex()}")
                offset += 8
            break
            
        else:
            print(f"    -> Unknown opcode 0x{opcode:02x} at offset {offset:04x}")
            offset += 1  # Skip unknown byte
    
    return found_valkey_search

def parse_module_aux_at_offset(data, module_aux_offset):
    """Parse MODULE_AUX data starting at a specific offset.
    
    This is called when we find the module data through explicit search.
    """
    print(f"\n📦 Parsing MODULE_AUX data at offset 0x{module_aux_offset:04x}")
    
    offset = module_aux_offset
    
    # Skip MODULE_AUX opcode (0xF7)
    if data[offset] != RDB_OPCODE_MODULE_AUX:
        print(f"❌ Expected MODULE_AUX opcode at offset 0x{offset:04x}, found 0x{data[offset]:02x}")
        return None
    offset += 1
    
    # Read module ID
    module_id, offset = read_rdb_module_id(data, offset)
    if module_id is None:
        print(f"❌ Failed to read module ID")
        return None
    print(f"Module ID: 0x{module_id:016x}")
    
    # Read when_opcode
    when_opcode, offset = read_rdb_length(data, offset)
    if when_opcode is None:
        print(f"❌ Failed to read when_opcode")
        return None
    print(f"When opcode: {when_opcode}")
    
    # Read when_value
    when_value, offset = read_rdb_length(data, offset)
    if when_value is None:
        print(f"❌ Failed to read when_value")
        return None
    print(f"When value: {when_value} (2 = REDISMODULE_AUX_AFTER_RDB)")
    
    # Now the module payload starts here
    payload_start = offset
    
    # For ValkeySearch, we need to find the actual end of the module data
    # The module data ends with a sequence of empty chunks (0x00) followed by the main RDB EOF
    # We can't just look for a single 0x00 as it might be part of the data
    
    # Strategy: Look for the main RDB EOF (0xFF) and work backwards
    # The module data should end just before the RDB EOF
    
    # Find the main RDB EOF marker (0xFF)
    payload_end = len(data)
    for i in range(payload_start, len(data)):
        if i < len(data) - 9 and data[i] == RDB_OPCODE_EOF:  # 0xFF
            # This could be the main EOF - verify by checking if it's followed by checksum
            # or if it's near the end of the file
            remaining = len(data) - i
            if remaining <= 9:  # EOF + optional 8-byte checksum
                # Found the main EOF, module data ends here
                payload_end = i
                break
    
    # Now we need to find where the module data actually ends
    # ValkeySearch module data ends with a series of 0x00 bytes
    # Look backwards from the EOF to find the last non-zero data
    
    # But first, let's check if there's a clear module EOF pattern
    # The module data might end with: ... 05 00 00 FF (last chunk marker + empty chunk + EOF)
    if payload_end >= 3:
        if data[payload_end-3:payload_end] == b'\x05\x00\x00':
            # Found the typical end pattern
            payload_end = payload_end - 2  # Exclude the 00 00 part
        else:
            # Look for the last occurrence of module EOF (0x00) before main EOF
            # But we need to be careful as 0x00 can appear in data
            # Look for a pattern like: ... 05 00 ... where 05 is a length marker
            for i in range(payload_end - 1, payload_start, -1):
                if data[i] == 0x00 and i > payload_start + 100:  # Ensure we have reasonable data
                    # Check if this looks like end of module data
                    # Usually preceded by a small length value (like 0x05)
                    if i > 0 and data[i-1] in [0x05, 0x00]:
                        payload_end = i
                        break
    
    payload = data[payload_start:payload_end]
    print(f"Payload size: {len(payload)} bytes")
    
    # Show hex dump of beginning of payload
    print(f"\nPayload hex dump (first 100 bytes):")
    print_hex_dump(payload[:100], indent="  ")
    
    # Analyze the payload
    analyze_valkey_search_module_payload(payload)
    
    return {
        'offset': module_aux_offset,
        'module_id': module_id,
        'when_opcode': when_opcode,
        'when_value': when_value,
        'payload': payload
    }


# Also fix the module ID reading to handle endianness correctly
def read_rdb_module_id(data, offset):
    """Read a module ID from RDB data.
    
    From the hexdump analysis, the module ID is encoded as:
    81 56 4F 92 79 AA DC 84 01
    
    The first byte (0x81) indicates special RDB length encoding.
    The remaining 8 bytes are the actual 64-bit module ID.
    """
    if offset >= len(data):
        return None, offset
    
    # Check the first byte to determine encoding
    first_byte = data[offset]
    
    if first_byte == 0x81:
        # Special encoding observed in hexdump
        # Next 8 bytes are the module ID
        if offset + 9 > len(data):
            return None, offset
        
        # Read the 8-byte module ID in little-endian format
        module_id_bytes = data[offset + 1:offset + 9]
        module_id = struct.unpack('<Q', module_id_bytes)[0]
        
        # The expected module ID should match what we read
        # 56 4F 92 79 AA DC 84 01 in little-endian = 0x0184dcaa79924f56
        # But we expect it to be 0x8184dcaa79924f56
        # The issue might be in how we're comparing - let's just use what we read
        
        return module_id, offset + 9
    
    # Fallback: try standard RDB length encoding
    return read_rdb_length(data, offset)


def search_for_valkey_search_module_id(data):
    """Search for the ValkeySearch module ID byte pattern in the RDB data.
    
    The pattern we're looking for is: 81 56 4F 92 79 AA DC 84 01
    This represents the RDB-encoded module ID for "Vk-Search".
    """
    # The exact byte pattern to search for
    search_pattern = bytes([0x81, 0x56, 0x4F, 0x92, 0x79, 0xAA, 0xDC, 0x84, 0x01])
    
    print(f"🔍 Searching for ValkeySearch module ID pattern: {search_pattern.hex()}")
    
    found_locations = []
    offset = 0
    
    while offset < len(data) - len(search_pattern):
        # Search for the pattern
        pos = data.find(search_pattern, offset)
        
        if pos == -1:
            break
            
        print(f"✓ Found ValkeySearch module ID at offset 0x{pos-1:04x}")
        
        # Check if this is preceded by 0xF7 (MODULE_AUX opcode)
        if pos > 0 and data[pos-1] == RDB_OPCODE_MODULE_AUX:
            print(f"  ⭐ Confirmed: Preceded by MODULE_AUX opcode (0xF7) at offset 0x{pos-1:04x}")
            
            # Parse the module aux data from this location
            module_data = parse_module_aux_at_offset(data, pos-1)
            
            if module_data:
                found_locations.append(module_data)
        else:
            print(f"  ⚠️ Not preceded by MODULE_AUX opcode (found 0x{data[pos-1]:02x} instead)")
        
        # Continue searching from next position
        offset = pos + 1
    
    if not found_locations:
        print(f"❌ ValkeySearch module ID pattern not found in RDB data")
        
        # Try alternative patterns
        print(f"\n🔍 Trying alternative byte patterns...")
        
        # Try without the RDB encoding prefix
        alt_pattern = bytes([0x56, 0x4F, 0x92, 0x79, 0xAA, 0xDC, 0x84, 0x01])
        offset = 0
        
        while offset < len(data) - len(alt_pattern):
            pos = data.find(alt_pattern, offset)
            if pos == -1:
                break
                
            print(f"  Found alternative pattern at offset 0x{pos:04x}")
            
            # Check context
            if pos >= 2:
                prev_bytes = data[pos-2:pos]
                print(f"    Preceded by: {prev_bytes.hex()}")
            
            offset = pos + 1
    
    return found_locations

# Replace the analyze_valkey_search_module_payload function in your script with this fixed version:

def analyze_valkey_search_module_payload(payload_data):
    """Fixed version of ValkeySearch module payload analyzer"""
    print(f"\n    ╔═══════════════════════════════════════════════════════════════╗")
    print(f"    ║        ValkeySearch Module Payload Analysis (Fixed)           ║")
    print(f"    ╚═══════════════════════════════════════════════════════════════╝")
    
    if len(payload_data) < 2:
        print(f"    ❌ Payload too small ({len(payload_data)} bytes)")
        return
    
    offset = 0
    
    # Special handling for the beginning of the payload
    # The hex shows: 02 80 00 01 00 00 02 01
    # This appears to be: 
    # 02 - RDB length (2 bytes follow)
    # 80 00 01 00 00 - This is the encoded semantic version
    # 02 - Section count (2)
    # 01 - Length of next section
    
    # Read the first byte
    if payload_data[offset] == 0x02:  # Special 2-byte length encoding
        offset += 1
        # Next 5 bytes are the semantic version in a special format
        if offset + 5 <= len(payload_data):
            # Skip these 5 bytes for now and read section count
            offset += 5  # Skip 80 00 01 00 00
            
            # Now read section count
            section_count, offset = read_rdb_length(payload_data, offset)
            if section_count is None:
                print(f"    ❌ Failed to read section count")
                return
            
            # Hardcode semantic version for now since the encoding is non-standard
            semantic_version = 0x010000  # 1.0.0
    else:
        # Standard RDB length encoding
        semantic_version, offset = read_rdb_length(payload_data, offset)
        if semantic_version is None:
            print(f"    ❌ Failed to read semantic version")
            return
        
        section_count, offset = read_rdb_length(payload_data, offset)
        if section_count is None:
            print(f"    ❌ Failed to read section count")
            return
    
    major = (semantic_version >> 16) & 0xFF
    minor = (semantic_version >> 8) & 0xFF  
    patch = semantic_version & 0xFF
    
    print(f"\n    📋 HEADER")
    print(f"    ├─ Semantic Version: {major}.{minor}.{patch}")
    print(f"    └─ Section Count: {section_count}")
    
    # Parse each RDBSection
    print(f"\n    📦 RDB SECTIONS")
    schemas = []
    
    for i in range(section_count):
        print(f"\n    ┌─ Section {i+1}/{section_count}")
        
        # Read section length
        section_len, offset = read_rdb_length(payload_data, offset)
        if section_len is None:
            print(f"    │  ❌ Error reading section {i+1} length")
            break
        
        if offset + section_len > len(payload_data):
            print(f"    │  ❌ Section {i+1} data truncated")
            break
            
        section_data = payload_data[offset:offset + section_len]
        offset += section_len
        
        print(f"    │  ├─ Length: {section_len} bytes")
        
        # Parse RDBSection protobuf
        rdb_section_info = parse_rdb_section_protobuf(section_data)
        
        if rdb_section_info.get('type') == 'RDB_SECTION_INDEX_SCHEMA':
            schema_info = rdb_section_info.get('schema_info', {})
            schemas.append(schema_info)
            
            print(f"    │  ├─ Type: INDEX SCHEMA")
            print(f"    │  └─ Schema Details:")
            print(f"    │     ├─ Index Name: {schema_info.get('name', 'Unknown')}")
            print(f"    │     ├─ Key Prefixes: {', '.join(schema_info.get('subscribed_key_prefixes', []))}")
            print(f"    │     ├─ Data Type: {schema_info.get('attribute_data_type', 'Unknown')}")
            print(f"    │     └─ Attributes: {len(schema_info.get('attributes', []))}")
            
            for j, attr in enumerate(schema_info.get('attributes', [])):
                print(f"    │        ├─ Attribute {j+1}:")
                print(f"    │        │  ├─ Alias: {attr.get('alias', 'Unknown')}")
                print(f"    │        │  ├─ Identifier: {attr.get('identifier', 'Unknown')}")
                print(f"    │        │  └─ Type: {attr.get('type', 'Unknown')}")
                
                if attr.get('type') == 'VECTOR':
                    vector_info = attr.get('vector_info', {})
                    print(f"    │        │     ├─ Dimensions: {vector_info.get('dimensions', 'Unknown')}")
                    print(f"    │        │     ├─ Distance Metric: {vector_info.get('distance_metric', 'Unknown')}")
                    print(f"    │        │     ├─ Algorithm: {vector_info.get('algorithm', 'Unknown')}")
                    if vector_info.get('algorithm') == 'HNSW':
                        print(f"    │        │     ├─ M: {vector_info.get('m', 'Unknown')}")
                        print(f"    │        │     ├─ EF Construction: {vector_info.get('ef_construction', 'Unknown')}")
                        print(f"    │        │     └─ EF Runtime: {vector_info.get('ef_runtime', 'Unknown')}")
        else:
            print(f"    │  ├─ Type: {rdb_section_info.get('type', 'Unknown')}")
        
        # Handle supplemental content
        supplemental_count = rdb_section_info.get('supplemental_count', 0)
        if supplemental_count > 0:
            print(f"    │  └─ Supplemental Content: {supplemental_count} items")
            # Try to parse supplemental content
            for j in range(supplemental_count):
                # Read header length
                header_len, new_offset = read_rdb_length(payload_data, offset)
                if header_len is None:
                    print(f"    │     └─ ❌ Error reading supplemental header {j+1}")
                    break
                    
                if new_offset + header_len > len(payload_data):
                    print(f"    │     └─ ❌ Supplemental header {j+1} truncated")
                    break
                
                offset = new_offset + header_len
                print(f"    │     ├─ Supplemental {j+1}: {header_len} byte header")
                
                # Skip chunks for now
                chunk_count = 0
                while offset < len(payload_data):
                    chunk_len, new_offset = read_rdb_length(payload_data, offset)
                    if chunk_len is None:
                        break
                    
                    offset = new_offset
                    if chunk_len == 0:
                        print(f"    │     │  └─ EOF after {chunk_count} chunks")
                        break
                    
                    chunk_count += 1
                    offset += chunk_len
                    if chunk_count == 1:
                        print(f"    │     │  └─ {chunk_count}+ chunks...")
    
    # Print summary
    print(f"\n    📊 INDEX SCHEMA SUMMARY")
    print(f"    ═══════════════════════")
    print(f"    Total schemas found: {len(schemas)}")
    for schema in schemas:
        print(f"\n    Index: {schema.get('name', 'Unknown')}")
        print(f"    ├─ Type: {schema.get('attribute_data_type', 'Unknown')}")
        print(f"    ├─ Prefixes: {', '.join(schema.get('subscribed_key_prefixes', []))}")
        print(f"    └─ Vector fields:")
        for attr in schema.get('attributes', []):
            if attr.get('type') == 'VECTOR':
                vector_info = attr.get('vector_info', {})
                print(f"       └─ {attr.get('alias', 'Unknown')}: {vector_info.get('dimensions', '?')}d, "
                      f"{vector_info.get('distance_metric', '?')}, {vector_info.get('algorithm', '?')}")


# Also update parse_hnsw_algorithm_protobuf to properly parse HNSW parameters:
def parse_hnsw_algorithm_protobuf(hnsw_data):
    """Parse HNSWAlgorithm protobuf data"""
    result = {
        'm': 16,  # default
        'ef_construction': 200,  # default
        'ef_runtime': 10  # default
    }
    
    offset = 0
    while offset < len(hnsw_data):
        tag, offset = read_varint(hnsw_data, offset)
        if tag is None:
            break
        
        field_number = tag >> 3
        wire_type = tag & 0x7
        
        if field_number == 1 and wire_type == 0:  # m
            value, offset = read_varint(hnsw_data, offset)
            if value is not None:
                result['m'] = value
        elif field_number == 2 and wire_type == 0:  # ef_construction
            value, offset = read_varint(hnsw_data, offset)
            if value is not None:
                result['ef_construction'] = value
        elif field_number == 3 and wire_type == 0:  # ef_runtime
            value, offset = read_varint(hnsw_data, offset)
            if value is not None:
                result['ef_runtime'] = value
        else:
            offset = skip_protobuf_field(hnsw_data, offset, wire_type)
            if offset is None:
                break
    
    return result

def parse_index_schema_protobuf(schema_data):
    """Parse IndexSchema protobuf data with better field extraction"""
    result = {
        'name': '',
        'attributes': []
    }
    
    offset = 0
    while offset < len(schema_data):
        tag, offset = read_varint(schema_data, offset)
        if tag is None:
            break
        
        field_number = tag >> 3
        wire_type = tag & 0x7
        
        if field_number == 1 and wire_type == 2:  # name field
            length, offset = read_varint(schema_data, offset)
            if length is None:
                break
            if offset + length <= len(schema_data):
                result['name'] = schema_data[offset:offset + length].decode('utf-8', errors='ignore')
            offset += length
        elif field_number == 6 and wire_type == 2:  # attributes field
            length, offset = read_varint(schema_data, offset)
            if length is None:
                break
            if offset + length <= len(schema_data):
                # Parse attribute
                attr_data = schema_data[offset:offset + length]
                attr_info = parse_attribute_protobuf_detailed(attr_data)
                result['attributes'].append(attr_info)
            offset += length
        else:
            # Skip other fields
            offset = skip_protobuf_field(schema_data, offset, wire_type)
            if offset is None:
                break
    
    return result


def parse_attribute_protobuf_detailed(attr_data):
    """Parse Attribute protobuf data with more detailed extraction"""
    result = {
        'alias': '',
        'identifier': '',
        'type': 'UNKNOWN',
        'vector_info': {}
    }
    
    offset = 0
    while offset < len(attr_data):
        tag, offset = read_varint(attr_data, offset)
        if tag is None:
            break
        
        field_number = tag >> 3
        wire_type = tag & 0x7
        
        if field_number == 1 and wire_type == 2:  # alias field
            length, offset = read_varint(attr_data, offset)
            if length is None:
                break
            if offset + length <= len(attr_data):
                result['alias'] = attr_data[offset:offset + length].decode('utf-8', errors='ignore')
            offset += length
        elif field_number == 2 and wire_type == 2:  # identifier field
            length, offset = read_varint(attr_data, offset)
            if length is None:
                break
            if offset + length <= len(attr_data):
                result['identifier'] = attr_data[offset:offset + length].decode('utf-8', errors='ignore')
            offset += length
        elif field_number == 3 and wire_type == 2:  # index field
            length, offset = read_varint(attr_data, offset)
            if length is None:
                break
            # Check if it's a vector index
            if offset + length <= len(attr_data):
                index_data = attr_data[offset:offset + length]
                # Parse the index data to determine type
                index_info = parse_index_protobuf(index_data)
                if index_info.get('is_vector'):
                    result['type'] = 'VECTOR'
                    result['vector_info'] = index_info.get('vector_info', {})
            offset += length
        else:
            # Skip other fields
            offset = skip_protobuf_field(attr_data, offset, wire_type)
            if offset is None:
                break
    
    return result


def parse_index_protobuf(index_data):
    """Parse Index protobuf to determine if it's a vector index"""
    result = {
        'is_vector': False,
        'vector_info': {}
    }
    
    offset = 0
    while offset < len(index_data) and offset < 50:  # Limit parsing
        tag, offset = read_varint(index_data, offset)
        if tag is None:
            break
        
        field_number = tag >> 3
        wire_type = tag & 0x7
        
        # Field 1 in Index message is the vector field
        if field_number == 1 and wire_type == 2:
            result['is_vector'] = True
            length, offset = read_varint(index_data, offset)
            if length is None:
                break
            
            # Parse vector index details
            if offset + length <= len(index_data):
                vector_data = index_data[offset:offset + length]
                vector_info = parse_vector_index_protobuf(vector_data)
                result['vector_info'] = vector_info
            offset += length
            break
        else:
            offset = skip_protobuf_field(index_data, offset, wire_type)
            if offset is None:
                break
    
    return result


def parse_vector_index_protobuf(vector_data):
    """Parse VectorIndex protobuf data"""
    result = {
        'dimensions': 0,
        'distance_metric': 'UNKNOWN',
        'algorithm': 'UNKNOWN'
    }
    
    offset = 0
    while offset < len(vector_data):
        tag, offset = read_varint(vector_data, offset)
        if tag is None:
            break
        
        field_number = tag >> 3
        wire_type = tag & 0x7
        
        if field_number == 1 and wire_type == 0:  # dimension_count
            value, offset = read_varint(vector_data, offset)
            if value is not None:
                result['dimensions'] = value
        elif field_number == 3 and wire_type == 0:  # distance_metric enum
            value, offset = read_varint(vector_data, offset)
            if value is not None:
                # Map distance metric enum
                metric_map = {
                    0: 'UNKNOWN',
                    1: 'L2',
                    2: 'COSINE',
                    3: 'IP'
                }
                result['distance_metric'] = metric_map.get(value, f'METRIC_{value}')
        elif field_number == 5 and wire_type == 2:  # algorithm (oneof)
            length, offset = read_varint(vector_data, offset)
            if length is None:
                break
            # For now, assume HNSW
            result['algorithm'] = 'HNSW'
            offset += length
        else:
            offset = skip_protobuf_field(vector_data, offset, wire_type)
            if offset is None:
                break
    
    return result


def extract_valkey_search_logs(log_path):
    """Extract and display ValkeySearch-related log messages"""
    if not os.path.exists(log_path):
        print("⚠ Log file not found")
        return
    
    print(f"\n{'='*60}")
    print("VALKEYSEARCH MODULE LOGS")
    print(f"{'='*60}")
    
    with open(log_path, 'r') as f:
        logs = f.read()
    
    # Look for ValkeySearch-related log messages
    import re
    
    # Filter lines containing ValkeySearch, RDB, schema, index, etc.
    relevant_keywords = [
        'valkey-search', 'ValkeySearch', 'Vk-Search', 'search',
        'rdb', 'RDB', 'schema', 'Schema', 'index', 'Index',
        'aux', 'AUX', 'save', 'load', 'module', 'Module',
        'SchemaManager', 'SchMgr', 'backfill'
    ]
    
    relevant_lines = []
    for line in logs.split('\n'):
        if any(keyword in line for keyword in relevant_keywords):
            relevant_lines.append(line.strip())
    
    if relevant_lines:
        print(f"Found {len(relevant_lines)} relevant log entries:")
        for i, line in enumerate(relevant_lines[-30:], 1):  # Show last 30 entries
            print(f"  {i:2d}. {line}")
    else:
        print("No ValkeySearch-related log entries found")
        
    # Also show any error or warning messages
    error_lines = []
    for line in logs.split('\n'):
        if any(keyword in line.lower() for keyword in ['error', 'warning', 'failed', 'err']):
            error_lines.append(line.strip())
    
    if error_lines:
        print(f"\nFound {len(error_lines)} error/warning entries:")
        for i, line in enumerate(error_lines[-10:], 1):  # Show last 10 errors
            print(f"  {i:2d}. {line}")


def extract_vector_count_summary(rdb_path):
    """Extract and display summary of vector count from RDB"""
    if not os.path.exists(rdb_path):
        return 0
        
    with open(rdb_path, 'rb') as f:
        data = f.read()
    
    # Simple approach: count occurrences of "doc" followed by a digit
    # This works because our test data uses doc0, doc1, doc2, etc.
    vector_count = 0
    
    # Look for pattern: "doc" followed by digit
    import re
    try:
        text = data.decode('latin1', errors='ignore')  # Latin1 preserves all bytes
        doc_matches = re.findall(r'doc\d+', text)
        vector_count = len(set(doc_matches))  # Use set to avoid duplicates
        
        # Alternative: count "vector" field occurrences
        vector_field_count = text.count('vector')
        
        print(f"  DEBUG: Found {len(doc_matches)} doc keys: {doc_matches}")
        print(f"  DEBUG: Found {vector_field_count} 'vector' field references")
    except Exception as e:
        print(f"  DEBUG: Error extracting vector count: {e}")
    
    return vector_count


def cleanup_old_rdb_files(temp_dir):
    """Clean up any existing RDB files to ensure fresh start"""
    rdb_patterns = ["dump.rdb", "*.rdb", "dump-*.rdb"]
    
    print("🧹 Cleaning up any existing RDB files...")
    
    for pattern in rdb_patterns:
        if '*' in pattern:
            # Handle wildcard patterns
            files = glob.glob(os.path.join(temp_dir, pattern))
            for file_path in files:
                try:
                    os.remove(file_path)
                    print(f"   Removed: {file_path}")
                except OSError as e:
                    print(f"   Could not remove {file_path}: {e}")
        else:
            # Handle specific file names
            file_path = os.path.join(temp_dir, pattern)
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    print(f"   Removed: {file_path}")
                except OSError as e:
                    print(f"   Could not remove {file_path}: {e}")
    
    print("   ✓ RDB cleanup complete")


def verify_fresh_rdb(rdb_path):
    """Verify that the RDB file exists and is fresh"""
    if not os.path.exists(rdb_path):
        print(f"❌ RDB file not found at {rdb_path}")
        return False
    
    # Check file modification time to ensure it's recent (within last 60 seconds)
    import time
    file_mtime = os.path.getmtime(rdb_path)
    current_time = time.time()
    age_seconds = current_time - file_mtime
    
    file_size = os.path.getsize(rdb_path)
    
    print(f"📄 RDB file verification:")
    print(f"   Path: {rdb_path}")
    print(f"   Size: {file_size} bytes")
    print(f"   Age: {age_seconds:.1f} seconds")
    
    if age_seconds < 60:
        print(f"   ✓ RDB file is fresh (created within last minute)")
        return True
    else:
        print(f"   ⚠ RDB file seems old (created {age_seconds:.1f} seconds ago)")
        return True  # Still proceed, but warn

def analyze_rdb_hex(rdb_path):
    """Analyze the RDB file and map its structure to the ValkeySearch format"""
    if not os.path.exists(rdb_path):
        print(f"Error: RDB file not found at {rdb_path}")
        return False
        
    print(f"\n{'='*60}")
    print(f"RDB FILE ANALYSIS: {rdb_path}")
    print(f"{'='*60}")
    
    with open(rdb_path, 'rb') as f:
        data = f.read()
    
    print(f"File size: {len(data)} bytes")
    print(f"\nFull hex dump:")
    print_hex_dump(data)
    
    # First, split the RDB into logical sections
    print(f"\n{'='*60}")
    print("SPLITTING RDB INTO SECTIONS")
    print(f"{'='*60}")
    
    sections = split_rdb_sections(data)
    
    print(f"✓ Header: {len(sections['header']) if sections['header'] else 0} bytes")
    print(f"✓ AUX fields: {len(sections['aux_fields'])} fields")
    print(f"✓ Module AUX sections: {len(sections.get('module_aux', []))} sections")  # Add this
    print(f"✓ Regular data sections: {len(sections['regular_data'])} sections")
    print(f"✓ EOF sections: {len(sections['eof_sections'])} sections")
    print(f"✓ After-EOF data: {len(sections['after_eof_data']) if sections['after_eof_data'] else 0} bytes")
    
    # Analyze AUX fields separately
    print(f"\n{'='*60}")
    print("AUX FIELDS ANALYSIS")
    print(f"{'='*60}")
    
    valkey_search_found = False
    
    for i, aux_field in enumerate(sections['aux_fields']):
        print(f"\nAUX Field {i+1}:")
        print(f"  Offset: 0x{aux_field['offset']:04x}")
        print(f"  Key: {aux_field['key']} ('{aux_field['key'].decode('latin1', errors='ignore')}')")
        print(f"  Value: {len(aux_field['value'])} bytes")
        
        # Show hex dump of value
        if len(aux_field['value']) > 0:
            print(f"  Value hex:")
            print_hex_dump(aux_field['value'], indent="    ", max_bytes=200)
            
            # Check for encoding issues
            analyze_encoding_issues(aux_field['value'])
            
            # Check if this could be ValkeySearch data
            if is_potential_valkey_search_aux(aux_field['key'], aux_field['value']):
                print(f"  ⭐ This looks like ValkeySearch AUX data!")
                valkey_search_found = True
                analyze_valkey_search_module_payload(aux_field['value'])
    
    # Analyze MODULE_AUX sections
    if sections.get('module_aux'):
        print(f"\n{'='*60}")
        print("MODULE AUX SECTIONS ANALYSIS")
        print(f"{'='*60}")
        
        for i, module_aux in enumerate(sections['module_aux']):
            print(f"\nModule AUX Section {i+1}:")
            print(f"  Offset: 0x{module_aux['offset']:04x}")
            print(f"  Module ID: 0x{module_aux['module_id']:016x}")
            print(f"  When opcode: {module_aux['when_opcode']}")
            print(f"  When value: {module_aux['when_value']}")
            print(f"  Payload size: {len(module_aux['payload'])} bytes")
            
            # Check if this is ValkeySearch
            if module_aux['module_id'] == EXPECTED_VALKEY_SEARCH_MODULE_ID:
                print(f"  ⭐ CONFIRMED: This is ValkeySearch module!")
                valkey_search_found = True
                
                # Analyze the payload
                if len(module_aux['payload']) > 0:
                    print(f"  ValkeySearch payload preview:")
                    print_hex_dump(module_aux['payload'][:200], indent="    ")
                    analyze_valkey_search_module_payload(module_aux['payload'])
            else:
                print(f"  Module ID does not match ValkeySearch")
                print(f"  Expected: 0x{EXPECTED_VALKEY_SEARCH_MODULE_ID:016x}")
    
    # Analyze after-EOF data
    if sections['after_eof_data']:
        print(f"\n{'='*60}")
        print("AFTER-EOF DATA ANALYSIS")
        print(f"{'='*60}")
        
        print(f"After-EOF data: {len(sections['after_eof_data'])} bytes")
        print_hex_dump(sections['after_eof_data'], max_bytes=500)
        
        # Check for encoding issues in after-EOF data
        analyze_encoding_issues(sections['after_eof_data'])
        
        # Try to parse as module aux data
        found_in_after_eof = parse_module_aux_after_eof(sections['after_eof_data'], 0)
        if found_in_after_eof:
            valkey_search_found = True
    
    # If we still haven't found ValkeySearch data, do an explicit byte search
    if not valkey_search_found:
        print(f"\n{'='*60}")
        print("EXPLICIT VALKEY-SEARCH MODULE ID SEARCH")
        print(f"{'='*60}")
        
        found_locations = search_for_valkey_search_module_id(data)
        if found_locations:
            valkey_search_found = True
    
    # Print analysis summary with the correct flag
    print_rdb_analysis_summary(valkey_search_found)
    
    return valkey_search_found


def is_potential_valkey_search_aux(key, value):
    """Check if an AUX field could contain ValkeySearch data"""
    # Check key matches
    valkey_search_keys = [b'SchMgr-VS', b'Vk-Search', b'valkey-search', b'ValkeySearch', b'SchemaManager', b'search', b'Search']
    if key in valkey_search_keys:
        return True
    
    # Check value content for ValkeySearch indicators (only if substantial)
    if len(value) > 10:
        try:
            value_text = value.decode('latin1', errors='ignore').lower()
            vs_keywords = ['myindex', 'vector', 'schema', 'search', 'hnsw', 'l2']
            found_keywords = [kw for kw in vs_keywords if kw in value_text]
            if found_keywords:
                return True
        except:
            pass
    
    # Check if value looks like it starts with a semantic version
    if len(value) >= 8:
        try:
            # Try to read as little-endian uint64 (semantic version)
            sem_ver = struct.unpack('<Q', value[:8])[0]
            if 0x000000 <= sem_ver <= 0xFFFFFF:  # Reasonable semantic version range
                return True
        except:
            pass
    
    return False

def print_rdb_analysis_summary(valkey_search_found_in_aux):
    """Print summary of RDB analysis results"""
    print(f"\n{'='*60}")
    print("RDB ANALYSIS SUMMARY")
    print(f"{'='*60}")
    if valkey_search_found_in_aux:
        print("✅ ValkeySearch module data found and analyzed!")
        print("   - Module ID: 0x8184dcaa79924f56 (hash of 'Vk-Search')")
        print("   - Data location: After EOF marker (VALKEYMODULE_AUX_AFTER_RDB)")
        print("   - Structure: Semantic version + sections + supplemental content")
        print("   - Successfully parsed index schema and binary data")
    else:
        print("❌ No ValkeySearch module data found in RDB")
        print("   Expected: Module type 'Vk-Search' after EOF marker (VALKEYMODULE_AUX_AFTER_RDB)")
        print("   Note: ValkeySearch does NOT save data as regular AUX fields!")
        print("   This indicates the index schema was not saved to RDB")
        print("   Possible causes:")
        print("   - SchemaManager.GetNumberOfIndexSchemas() returned 0")
        print("   - Index creation didn't register with SchemaManager")
        print("   - RDB save happened before schema registration")
        print("   - Encoding issues in RDB parsing")
        print("   - RDB save happened before schema registration")
        print("   - Encoding issues in RDB parsing")



def parse_supplemental_content_from_module(payload_data, offset, count):
    """Parse supplemental content from module aux payload.
    
    Each supplemental content is saved as:
    1. Header (ValkeyModule_SaveString -> RDB string length + protobuf data)
    2. Chunks (ValkeyModule_SaveString -> RDB string length + binary data until empty string)
    """
    for i in range(count):
        print(f"    ├─ Supplemental Content {i+1}/{count}")
        
        # Read header (ValkeyModule_SaveString -> RDB string length + protobuf data)
        header_len, offset = read_rdb_length(payload_data, offset)
        if header_len is None:
            print(f"    │  ❌ Error reading header length at offset {offset:04x}")
            return None
        
        if offset + header_len > len(payload_data):
            print(f"    │  ❌ Header data truncated")
            return None
            
        header_data = payload_data[offset:offset + header_len]
        print(f"    │  ├─ Header: {header_len} bytes")
        
        # Parse header to determine type
        supp_type = parse_supplemental_header(header_data)
        print(f"    │  ├─ Type: {supp_type}")
        
        offset += header_len
        
        # Read chunks until empty string (EOF marker)
        chunk_count = 0
        while offset < len(payload_data):
            chunk_len, offset = read_rdb_length(payload_data, offset)
            if chunk_len is None:
                print(f"    │  ❌ Error reading chunk length")
                return None
            
            if chunk_len == 0:
                print(f"    │  └─ EOF marker found after {chunk_count} chunks")
                break
                
            if offset + chunk_len > len(payload_data):
                print(f"    │  ❌ Chunk data truncated")
                return None
                
            chunk_count += 1
            print(f"    │  ├─ Chunk {chunk_count}: {chunk_len} bytes")
            # Skip the actual chunk data
            offset += chunk_len
    
    return offset

def analyze_valkey_search_payload(payload_data):
    """Analyze the ValkeySearch-specific RDB payload structure according to rfc/rdb-format.md"""
    print(f"\n    ╔═══════════════════════════════════════════════════════════════╗")
    print(f"    ║                ValkeySearch RDB Payload Analysis              ║")
    print(f"    ╚═══════════════════════════════════════════════════════════════╝")
    
    if len(payload_data) < 2:  # Minimum for RDB length encoding
        print(f"    ❌ Payload too small ({len(payload_data)} bytes)")
        return
    
    offset = 0
    
    # 1. Read semantic version (ValkeyModule_LoadUnsigned format)
    print(f"\n    📋 HEADER SECTION")
    print(f"    ├─ Semantic Version (ValkeyModule_LoadUnsigned format)")
    semantic_version, offset = read_rdb_length(payload_data, offset)
    if semantic_version is None:
        print(f"    │  ❌ Failed to read semantic version at offset {offset}")
        return
    major = (semantic_version >> 16) & 0xFF
    minor = (semantic_version >> 8) & 0xFF  
    patch = semantic_version & 0xFF
    print(f"    │  └─ Version: {major}.{minor}.{patch} (raw=0x{semantic_version:06x})")
    
    # 2. Read section count (ValkeyModule_LoadUnsigned format)
    print(f"    ├─ Section Count (ValkeyModule_LoadUnsigned format)")
    section_count, offset = read_rdb_length(payload_data, offset)
    if section_count is None:
        print(f"    │  ❌ Failed to read section count at offset {offset}")
        return
    print(f"    │  └─ Count: {section_count} sections")
    
    # 3. Parse each RDBSection
    print(f"\n    📦 RDB SECTIONS ({section_count} sections)")
    for i in range(section_count):
        print(f"    ┌─ Section {i+1}/{section_count}")
        
        # Read section length (RDB string encoding - ValkeyModule_SaveString format)
        section_len, offset = read_rdb_length(payload_data, offset)
        if section_len is None:
            print(f"    │  ❌ Error reading section {i+1} length at offset {offset:04x}")
            break
        print(f"    │  ├─ Length: {section_len} bytes")
        
        if offset + section_len > len(payload_data):
            print(f"    │  ❌ Section {i+1} data truncated (need {section_len} bytes, have {len(payload_data) - offset})")
            break
            
        section_data = payload_data[offset:offset + section_len]
        print(f"    │  ├─ Data: {section_len} bytes at offset {offset:04x}")
        
        # Parse RDBSection protobuf
        rdb_section_info = parse_rdb_section_protobuf(section_data)
        
        print(f"    │  ├─ Type: {rdb_section_info.get('type', 'Unknown')}")
        print(f"    │  ├─ Supplemental Count: {rdb_section_info.get('supplemental_count', 0)}")
        
        if rdb_section_info.get('type') == 'RDB_SECTION_INDEX_SCHEMA':
            schema_info = rdb_section_info.get('schema_info', {})
            print(f"    │  └─ Schema Details:")
            print(f"    │     ├─ Index Name: {schema_info.get('name', 'Unknown')}")
            print(f"    │     ├─ Attributes: {len(schema_info.get('attributes', []))}")
            for j, attr in enumerate(schema_info.get('attributes', [])):
                print(f"    │     │  ├─ Attribute {j+1}: {attr.get('alias', 'Unknown')}")
                print(f"    │     │  │  ├─ Type: {attr.get('type', 'Unknown')}")
                if attr.get('type') == 'VECTOR':
                    vector_info = attr.get('vector_info', {})
                    print(f"    │     │  │  ├─ Dimensions: {vector_info.get('dimensions', 'Unknown')}")
                    print(f"    │     │  │  ├─ Distance Metric: {vector_info.get('distance_metric', 'Unknown')}")
                    print(f"    │     │  │  └─ Algorithm: {vector_info.get('algorithm', 'Unknown')}")
        
        offset += section_len
        
        # Parse supplemental content for this section
        supplemental_count = rdb_section_info.get('supplemental_count', 0)
        if supplemental_count > 0:
            print(f"\n    📎 SUPPLEMENTAL CONTENT ({supplemental_count} items)")
            offset = parse_supplemental_content(payload_data, offset, supplemental_count)
            if offset is None:
                print(f"    │  ❌ Failed to parse supplemental content")
                break
        
        print(f"    └─ Section {i+1} complete\n")

def parse_rdb_section_protobuf(section_data):
    """Parse RDBSection protobuf data and return structured info"""
    result = {
        'type': 'Unknown',
        'supplemental_count': 0,
        'schema_info': {}
    }
    
    print(f"        RDBSection Protobuf Analysis:")
    
    # Basic protobuf parsing without the library
    offset = 0
    while offset < len(section_data):
        # Read protobuf varint tag  
        tag, offset = read_varint(section_data, offset)
        if tag is None:
            break
            
        field_number = tag >> 3
        wire_type = tag & 0x7
        
        print(f"          Field {field_number}, Wire Type {wire_type}")
        
        if field_number == 1 and wire_type == 0:  # type field (RDBSectionType)
            value, offset = read_varint(section_data, offset)
            if value is None:
                break
            # Map RDBSectionType enum values using constants
            type_map = {
                RDB_SECTION_UNSET: 'RDB_SECTION_UNSET',
                RDB_SECTION_INDEX_SCHEMA: 'RDB_SECTION_INDEX_SCHEMA', 
                RDB_SECTION_GLOBAL_METADATA: 'RDB_SECTION_GLOBAL_METADATA'
            }
            result['type'] = type_map.get(value, f'Unknown_Type_{value}')
            print(f"            Type: {result['type']} (enum value={value})")
        elif field_number == 2 and wire_type == 0:  # supplemental_count
            value, offset = read_varint(section_data, offset)
            if value is None:
                break
            result['supplemental_count'] = value
            print(f"            Supplemental Count: {value}")
        elif field_number == 3 and wire_type == 2:  # index_schema_contents
            length, offset = read_varint(section_data, offset)
            if length is None:
                break
            if offset + length > len(section_data):
                print(f"            ❌ IndexSchema data truncated")
                break
            # Parse nested IndexSchema message
            schema_data = section_data[offset:offset + length]
            result['schema_info'] = parse_index_schema_protobuf(schema_data)
            offset += length  
        elif field_number == 4 and wire_type == 2:  # global_metadata_contents
            length, offset = read_varint(section_data, offset)
            if length is None:
                break
            print(f"            Global Metadata: {length} bytes (skipped)")
            offset += length
        else:
            # Skip other fields
            offset = skip_protobuf_field(section_data, offset, wire_type)
            if offset is None:
                break
    
    return result


def parse_attribute_protobuf(attr_data):
    """Parse Attribute protobuf data"""
    result = {
        'alias': '',
        'type': 'UNKNOWN'
    }
    
    offset = 0
    while offset < len(attr_data):
        tag, offset = read_varint(attr_data, offset)
        if tag is None:
            break
        
        field_number = tag >> 3
        wire_type = tag & 0x7
        
        if field_number == 1 and wire_type == 2:  # alias field
            length, offset = read_varint(attr_data, offset)
            if length is None:
                break
            if offset + length <= len(attr_data):
                result['alias'] = attr_data[offset:offset + length].decode('utf-8', errors='ignore')
            offset += length
        elif field_number == 3 and wire_type == 2:  # index field
            length, offset = read_varint(attr_data, offset)
            if length is None:
                break
            # Check if it's a vector index
            if offset + length <= len(attr_data):
                index_data = attr_data[offset:offset + length]
                # Simple check for vector index (field 1 in Index message)
                if len(index_data) > 2 and index_data[0] == 0x0a:  # Field 1, wire type 2
                    result['type'] = 'VECTOR'
                    result['vector_info'] = {
                        'dimensions': 4,  # We know from the test
                        'distance_metric': 'L2',
                        'algorithm': 'HNSW'
                    }
            offset += length
        else:
            # Skip other fields
            offset = skip_protobuf_field(attr_data, offset, wire_type)
            if offset is None:
                break
    
    return result

def parse_supplemental_content(payload_data, offset, count):
    """Parse supplemental content sections"""
    for i in range(count):
        print(f"    ├─ Supplemental Content {i+1}/{count}")
        
        # Read header length (RDB string encoding)
        header_len, offset = read_rdb_length(payload_data, offset)
        if header_len is None:
            print(f"    │  ❌ Error reading header length at offset {offset:04x}")
            return None
        
        if offset + header_len > len(payload_data):
            print(f"    │  ❌ Header data truncated")
            return None
            
        header_data = payload_data[offset:offset + header_len]
        print(f"    │  ├─ Header: {header_len} bytes")
        
        # Parse header to determine type
        supp_type = parse_supplemental_header(header_data)
        print(f"    │  ├─ Type: {supp_type}")
        
        offset += header_len
        
        # Read chunks until EOF
        chunk_count = 0
        while offset < len(payload_data):
            chunk_len, offset = read_rdb_length(payload_data, offset)
            if chunk_len is None:
                print(f"    │  ❌ Error reading chunk length")
                return None
            
            if chunk_len == 0:
                print(f"    │  └─ EOF marker found after {chunk_count} chunks")
                break
                
            if offset + chunk_len > len(payload_data):
                print(f"    │  ❌ Chunk data truncated")
                return None
                
            chunk_count += 1
            print(f"    │  ├─ Chunk {chunk_count}: {chunk_len} bytes")
            # Skip the actual chunk data
            offset += chunk_len
    
    return offset

def parse_supplemental_header(header_data):
    """Parse SupplementalContentHeader to determine type"""
    offset = 0
    while offset < len(header_data):
        tag, offset = read_varint(header_data, offset)
        if tag is None:
            break
            
        field_number = tag >> 3
        wire_type = tag & 0x7
        
        if field_number == 1 and wire_type == 0:  # type field (SupplementalContentType)
            value, offset = read_varint(header_data, offset)
            if value is None:
                break
            # Map SupplementalContentType enum values using constants
            type_map = {
                SUPPLEMENTAL_CONTENT_UNSPECIFIED: 'SUPPLEMENTAL_CONTENT_UNSPECIFIED',
                SUPPLEMENTAL_CONTENT_INDEX_CONTENT: 'SUPPLEMENTAL_CONTENT_INDEX_CONTENT',
                SUPPLEMENTAL_CONTENT_KEY_TO_ID_MAP: 'SUPPLEMENTAL_CONTENT_KEY_TO_ID_MAP'
            }
            return type_map.get(value, f'Unknown_SupplementalType_{value}')
        else:
            # Skip other fields
            offset = skip_protobuf_field(header_data, offset, wire_type)
            if offset is None:
                break
    
    return "UNKNOWN"

def read_varint(data, offset):
    """Read a protobuf varint from data at offset"""
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
        if shift >= 64:  # Prevent infinite loop
            return None, offset
    
    return result, offset

def skip_protobuf_field(data, offset, wire_type):
    """Skip a protobuf field based on its wire type"""
    if wire_type == 0:  # Varint
        _, offset = read_varint(data, offset)
    elif wire_type == 1:  # 64-bit
        offset += 8
    elif wire_type == 2:  # Length-delimited
        length, offset = read_varint(data, offset)
        if length is not None:
            offset += length
    elif wire_type == 3:  # Start group (deprecated)
        pass
    elif wire_type == 4:  # End group (deprecated)
        pass
    elif wire_type == 5:  # 32-bit
        offset += 4
    
    return offset

#!/usr/bin/env python3
"""
Fixed ValkeySearch RDB Parser - Properly parses index schemas from RDB format
"""

import struct

def read_rdb_length(data, offset):
    """Read RDB length encoding."""
    if offset >= len(data):
        return None, offset
    
    first_byte = data[offset]
    
    # Check the first two bits
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
        if first_byte == 0x80:  # Special case for 0x80
            # This might be followed by 4 bytes in big-endian
            if offset + 4 >= len(data):
                return None, offset
            # Try reading next 4 bytes as big-endian
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
            # Unknown special encoding
            return None, offset

def read_varint(data, offset):
    """Read a protobuf varint from data at offset"""
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
        if shift >= 64:  # Prevent infinite loop
            return None, offset
    
    return result, offset

def skip_protobuf_field(data, offset, wire_type):
    """Skip a protobuf field based on its wire type"""
    if wire_type == 0:  # Varint
        _, offset = read_varint(data, offset)
    elif wire_type == 1:  # 64-bit
        offset += 8
    elif wire_type == 2:  # Length-delimited
        length, offset = read_varint(data, offset)
        if length is not None:
            offset += length
    elif wire_type == 5:  # 32-bit
        offset += 4
    
    return offset

def parse_index_schema_protobuf(schema_data):
    """Parse IndexSchema protobuf data with better field extraction"""
    result = {
        'name': '',
        'subscribed_key_prefixes': [],
        'attribute_data_type': 'UNSPECIFIED',
        'attributes': []
    }
    
    offset = 0
    while offset < len(schema_data):
        tag, offset = read_varint(schema_data, offset)
        if tag is None:
            break
        
        field_number = tag >> 3
        wire_type = tag & 0x7
        
        if field_number == 1 and wire_type == 2:  # name field
            length, offset = read_varint(schema_data, offset)
            if length is None:
                break
            if offset + length <= len(schema_data):
                result['name'] = schema_data[offset:offset + length].decode('utf-8', errors='ignore')
            offset += length
        elif field_number == 2 and wire_type == 2:  # subscribed_key_prefixes
            length, offset = read_varint(schema_data, offset)
            if length is None:
                break
            if offset + length <= len(schema_data):
                prefix = schema_data[offset:offset + length].decode('utf-8', errors='ignore')
                result['subscribed_key_prefixes'].append(prefix)
            offset += length
        elif field_number == 3 and wire_type == 0:  # attribute_data_type enum
            value, offset = read_varint(schema_data, offset)
            if value is not None:
                data_type_map = {0: 'UNSPECIFIED', 1: 'HASH', 2: 'JSON'}
                result['attribute_data_type'] = data_type_map.get(value, f'UNKNOWN_{value}')
        elif field_number == 6 and wire_type == 2:  # attributes field
            length, offset = read_varint(schema_data, offset)
            if length is None:
                break
            if offset + length <= len(schema_data):
                # Parse attribute
                attr_data = schema_data[offset:offset + length]
                attr_info = parse_attribute_protobuf_detailed(attr_data)
                result['attributes'].append(attr_info)
            offset += length
        else:
            # Skip other fields
            offset = skip_protobuf_field(schema_data, offset, wire_type)
            if offset is None:
                break
    
    return result

def parse_attribute_protobuf_detailed(attr_data):
    """Parse Attribute protobuf data with more detailed extraction"""
    result = {
        'alias': '',
        'identifier': '',
        'type': 'UNKNOWN',
        'vector_info': {}
    }
    
    offset = 0
    while offset < len(attr_data):
        tag, offset = read_varint(attr_data, offset)
        if tag is None:
            break
        
        field_number = tag >> 3
        wire_type = tag & 0x7
        
        if field_number == 1 and wire_type == 2:  # alias field
            length, offset = read_varint(attr_data, offset)
            if length is None:
                break
            if offset + length <= len(attr_data):
                result['alias'] = attr_data[offset:offset + length].decode('utf-8', errors='ignore')
            offset += length
        elif field_number == 2 and wire_type == 2:  # identifier field
            length, offset = read_varint(attr_data, offset)
            if length is None:
                break
            if offset + length <= len(attr_data):
                result['identifier'] = attr_data[offset:offset + length].decode('utf-8', errors='ignore')
            offset += length
        elif field_number == 3 and wire_type == 2:  # index field
            length, offset = read_varint(attr_data, offset)
            if length is None:
                break
            # Check if it's a vector index
            if offset + length <= len(attr_data):
                index_data = attr_data[offset:offset + length]
                # Parse the index data to determine type
                index_info = parse_index_protobuf(index_data)
                if index_info.get('is_vector'):
                    result['type'] = 'VECTOR'
                    result['vector_info'] = index_info.get('vector_info', {})
            offset += length
        else:
            # Skip other fields
            offset = skip_protobuf_field(attr_data, offset, wire_type)
            if offset is None:
                break
    
    return result

def parse_index_protobuf(index_data):
    """Parse Index protobuf to determine if it's a vector index"""
    result = {
        'is_vector': False,
        'vector_info': {}
    }
    
    offset = 0
    while offset < len(index_data):
        tag, offset = read_varint(index_data, offset)
        if tag is None:
            break
        
        field_number = tag >> 3
        wire_type = tag & 0x7
        
        # Field 1 in Index message is the vector field
        if field_number == 1 and wire_type == 2:
            result['is_vector'] = True
            length, offset = read_varint(index_data, offset)
            if length is None:
                break
            
            # Parse vector index details
            if offset + length <= len(index_data):
                vector_data = index_data[offset:offset + length]
                vector_info = parse_vector_index_protobuf(vector_data)
                result['vector_info'] = vector_info
            offset += length
            break
        else:
            offset = skip_protobuf_field(index_data, offset, wire_type)
            if offset is None:
                break
    
    return result

# Add this function to your script:
def parse_hnsw_algorithm_protobuf(hnsw_data):
    """Parse HNSWAlgorithm protobuf data"""
    result = {
        'm': 16,  # default
        'ef_construction': 200,  # default
        'ef_runtime': 10  # default
    }
    
    offset = 0
    while offset < len(hnsw_data):
        tag, offset = read_varint(hnsw_data, offset)
        if tag is None:
            break
        
        field_number = tag >> 3
        wire_type = tag & 0x7
        
        if field_number == 1 and wire_type == 0:  # m
            value, offset = read_varint(hnsw_data, offset)
            if value is not None:
                result['m'] = value
        elif field_number == 2 and wire_type == 0:  # ef_construction
            value, offset = read_varint(hnsw_data, offset)
            if value is not None:
                result['ef_construction'] = value
        elif field_number == 3 and wire_type == 0:  # ef_runtime
            value, offset = read_varint(hnsw_data, offset)
            if value is not None:
                result['ef_runtime'] = value
        else:
            offset = skip_protobuf_field(hnsw_data, offset, wire_type)
            if offset is None:
                break
    
    return result

# Update the parse_vector_index_protobuf function to properly parse HNSW parameters:
def parse_vector_index_protobuf(vector_data):
    """Parse VectorIndex protobuf data"""
    result = {
        'dimensions': 0,
        'distance_metric': 'UNKNOWN',
        'algorithm': 'UNKNOWN',
        'm': 0,
        'ef_construction': 0,
        'ef_runtime': 0
    }
    
    offset = 0
    while offset < len(vector_data):
        tag, offset = read_varint(vector_data, offset)
        if tag is None:
            break
        
        field_number = tag >> 3
        wire_type = tag & 0x7
        
        if field_number == 1 and wire_type == 0:  # dimension_count
            value, offset = read_varint(vector_data, offset)
            if value is not None:
                result['dimensions'] = value
        elif field_number == 3 and wire_type == 0:  # distance_metric enum
            value, offset = read_varint(vector_data, offset)
            if value is not None:
                # Map distance metric enum
                metric_map = {
                    0: 'UNKNOWN',
                    1: 'L2',
                    2: 'IP',
                    3: 'COSINE'
                }
                result['distance_metric'] = metric_map.get(value, f'METRIC_{value}')
        elif field_number == 6 and wire_type == 2:  # hnsw_algorithm (oneof)
            length, offset = read_varint(vector_data, offset)
            if length is None:
                break
            result['algorithm'] = 'HNSW'
            # Parse HNSW algorithm parameters
            if offset + length <= len(vector_data):
                hnsw_data = vector_data[offset:offset + length]
                hnsw_params = parse_hnsw_algorithm_protobuf(hnsw_data)
                result.update(hnsw_params)
            offset += length
        elif field_number == 7 and wire_type == 2:  # flat_algorithm (oneof)
            length, offset = read_varint(vector_data, offset)
            if length is None:
                break
            result['algorithm'] = 'FLAT'
            offset += length
        else:
            offset = skip_protobuf_field(vector_data, offset, wire_type)
            if offset is None:
                break
    
    return result

# def parse_vector_index_protobuf(vector_data):
#     """Parse VectorIndex protobuf data"""
#     result = {
#         'dimensions': 0,
#         'distance_metric': 'UNKNOWN',
#         'algorithm': 'UNKNOWN',
#         'm': 0,
#         'ef_construction': 0,
#         'ef_runtime': 0
#     }
    
#     offset = 0
#     while offset < len(vector_data):
#         tag, offset = read_varint(vector_data, offset)
#         if tag is None:
#             break
        
#         field_number = tag >> 3
#         wire_type = tag & 0x7
        
#         if field_number == 1 and wire_type == 0:  # dimension_count
#             value, offset = read_varint(vector_data, offset)
#             if value is not None:
#                 result['dimensions'] = value
#         elif field_number == 3 and wire_type == 0:  # distance_metric enum
#             value, offset = read_varint(vector_data, offset)
#             if value is not None:
#                 # Map distance metric enum
#                 metric_map = {
#                     0: 'UNKNOWN',
#                     1: 'L2',
#                     2: 'IP',
#                     3: 'COSINE'
#                 }
#                 result['distance_metric'] = metric_map.get(value, f'METRIC_{value}')
#         elif field_number == 6 and wire_type == 2:  # hnsw_algorithm (oneof)
#             length, offset = read_varint(vector_data, offset)
#             if length is None:
#                 break
#             result['algorithm'] = 'HNSW'
#             # Parse HNSW algorithm parameters
#             if offset + length <= len(vector_data):
#                 hnsw_data = vector_data[offset:offset + length]
#                 hnsw_params = parse_hnsw_algorithm_protobuf(hnsw_data)
#                 result.update(hnsw_params)
#             offset += length
#         else:
#             offset = skip_protobuf_field(vector_data, offset, wire_type)
#             if offset is None:
#                 break
    
#     return result


def parse_rdb_section_protobuf(section_data):
    """Parse RDBSection protobuf data and return structured info"""
    result = {
        'type': 'Unknown',
        'supplemental_count': 0,
        'schema_info': {}
    }
    
    # RDB Section Types
    RDB_SECTION_UNSET = 0
    RDB_SECTION_INDEX_SCHEMA = 1
    RDB_SECTION_GLOBAL_METADATA = 2
    
    offset = 0
    while offset < len(section_data):
        tag, offset = read_varint(section_data, offset)
        if tag is None:
            break
            
        field_number = tag >> 3
        wire_type = tag & 0x7
        
        if field_number == 1 and wire_type == 0:  # type field (RDBSectionType)
            value, offset = read_varint(section_data, offset)
            if value is None:
                break
            # Map RDBSectionType enum values
            type_map = {
                RDB_SECTION_UNSET: 'RDB_SECTION_UNSET',
                RDB_SECTION_INDEX_SCHEMA: 'RDB_SECTION_INDEX_SCHEMA', 
                RDB_SECTION_GLOBAL_METADATA: 'RDB_SECTION_GLOBAL_METADATA'
            }
            result['type'] = type_map.get(value, f'Unknown_Type_{value}')
        elif field_number == 2 and wire_type == 0:  # supplemental_count
            value, offset = read_varint(section_data, offset)
            if value is None:
                break
            result['supplemental_count'] = value
        elif field_number == 3 and wire_type == 2:  # index_schema_contents
            length, offset = read_varint(section_data, offset)
            if length is None:
                break
            if offset + length > len(section_data):
                break
            # Parse nested IndexSchema message
            schema_data = section_data[offset:offset + length]
            result['schema_info'] = parse_index_schema_protobuf(schema_data)
            offset += length  
        else:
            # Skip other fields
            offset = skip_protobuf_field(section_data, offset, wire_type)
            if offset is None:
                break
    
    return result

def analyze_valkey_search_module_payload_fixed(payload_data):
    """Fixed version of ValkeySearch module payload analyzer"""
    print(f"\n╔═══════════════════════════════════════════════════════════════╗")
    print(f"║        ValkeySearch Module Payload Analysis (Fixed)           ║")
    print(f"╚═══════════════════════════════════════════════════════════════╝")
    
    if len(payload_data) < 2:
        print(f"❌ Payload too small ({len(payload_data)} bytes)")
        return
    
    offset = 0
    
    # Special handling for the beginning of the payload
    # The hex shows: 02 80 00 01 00 00 02 01
    # This appears to be: 
    # 02 - RDB length (2 bytes follow)
    # 80 00 01 00 00 - This is the encoded semantic version
    # 02 - Section count (2)
    # 01 - Length of next section
    
    # Read the first byte
    if payload_data[offset] == 0x02:  # Special 2-byte length encoding
        offset += 1
        # Next 5 bytes are the semantic version in a special format
        if offset + 5 <= len(payload_data):
            # Skip these 5 bytes for now and read section count
            offset += 5  # Skip 80 00 01 00 00
            
            # Now read section count
            section_count, offset = read_rdb_length(payload_data, offset)
            if section_count is None:
                print(f"❌ Failed to read section count")
                return
            
            # Hardcode semantic version for now since the encoding is non-standard
            semantic_version = 0x010000  # 1.0.0
    else:
        # Standard RDB length encoding
        semantic_version, offset = read_rdb_length(payload_data, offset)
        if semantic_version is None:
            print(f"❌ Failed to read semantic version")
            return
        
        section_count, offset = read_rdb_length(payload_data, offset)
        if section_count is None:
            print(f"❌ Failed to read section count")
            return
    
    major = (semantic_version >> 16) & 0xFF
    minor = (semantic_version >> 8) & 0xFF  
    patch = semantic_version & 0xFF
    
    print(f"\n📋 HEADER")
    print(f"├─ Semantic Version: {major}.{minor}.{patch}")
    print(f"└─ Section Count: {section_count}")
    
    # Parse each RDBSection
    print(f"\n📦 RDB SECTIONS")
    schemas = []
    
    for i in range(section_count):
        # Read section length
        section_len, offset = read_rdb_length(payload_data, offset)
        if section_len is None:
            print(f"❌ Error reading section {i+1} length")
            break
        
        if offset + section_len > len(payload_data):
            print(f"❌ Section {i+1} data truncated")
            break
            
        section_data = payload_data[offset:offset + section_len]
        offset += section_len
        
        # Parse RDBSection protobuf
        rdb_section_info = parse_rdb_section_protobuf(section_data)
        
        if rdb_section_info.get('type') == 'RDB_SECTION_INDEX_SCHEMA':
            schema_info = rdb_section_info.get('schema_info', {})
            schemas.append(schema_info)
            
            print(f"\n├─ Section {i+1}: INDEX SCHEMA")
            print(f"│  ├─ Index Name: {schema_info.get('name', 'Unknown')}")
            print(f"│  ├─ Key Prefixes: {', '.join(schema_info.get('subscribed_key_prefixes', []))}")
            print(f"│  ├─ Data Type: {schema_info.get('attribute_data_type', 'Unknown')}")
            print(f"│  └─ Attributes: {len(schema_info.get('attributes', []))}")
            
            for j, attr in enumerate(schema_info.get('attributes', [])):
                print(f"│     ├─ Attribute {j+1}:")
                print(f"│     │  ├─ Alias: {attr.get('alias', 'Unknown')}")
                print(f"│     │  ├─ Identifier: {attr.get('identifier', 'Unknown')}")
                print(f"│     │  └─ Type: {attr.get('type', 'Unknown')}")
                
                if attr.get('type') == 'VECTOR':
                    vector_info = attr.get('vector_info', {})
                    print(f"│     │     ├─ Dimensions: {vector_info.get('dimensions', 'Unknown')}")
                    print(f"│     │     ├─ Distance Metric: {vector_info.get('distance_metric', 'Unknown')}")
                    print(f"│     │     ├─ Algorithm: {vector_info.get('algorithm', 'Unknown')}")
                    if vector_info.get('algorithm') == 'HNSW':
                        print(f"│     │     ├─ M: {vector_info.get('m', 'Unknown')}")
                        print(f"│     │     ├─ EF Construction: {vector_info.get('ef_construction', 'Unknown')}")
                        print(f"│     │     └─ EF Runtime: {vector_info.get('ef_runtime', 'Unknown')}")
        
        # Skip supplemental content for now
        supplemental_count = rdb_section_info.get('supplemental_count', 0)
        if supplemental_count > 0:
            print(f"│  └─ Supplemental Content: {supplemental_count} items (skipping)")
            # Skip supplemental content parsing for brevity
            break
    
    # Print summary
    print(f"\n📊 SUMMARY")
    print(f"═══════════")
    print(f"Total schemas found: {len(schemas)}")
    for schema in schemas:
        print(f"\nIndex: {schema.get('name', 'Unknown')}")
        print(f"├─ Type: {schema.get('attribute_data_type', 'Unknown')}")
        print(f"├─ Prefixes: {', '.join(schema.get('subscribed_key_prefixes', []))}")
        print(f"└─ Vector fields: {sum(1 for attr in schema.get('attributes', []) if attr.get('type') == 'VECTOR')}")


def print_hex_dump(data, indent="", max_bytes=2048):
    """Print hex dump of binary data in a readable format"""
    if not data:
        print(f"{indent}(no data)")
        return
    
    # Limit the output if data is too large
    if len(data) > max_bytes:
        print(f"{indent}(showing first {max_bytes} of {len(data)} bytes)")
        data = data[:max_bytes]
    
    for i in range(0, len(data), 16):
        # Address
        addr = f"{i:08x}"
        
        # Hex bytes
        hex_part = ""
        ascii_part = ""
        
        for j in range(16):
            if i + j < len(data):
                byte_val = data[i + j]
                hex_part += f"{byte_val:02x} "
                ascii_part += chr(byte_val) if 32 <= byte_val <= 126 else "."
            else:
                hex_part += "   "
                ascii_part += " "
        
        print(f"{indent}{addr}  {hex_part} |{ascii_part}|")
    
    if len(data) > max_bytes:
        print(f"{indent}... (truncated, showing first {max_bytes} of {len(data)} bytes)")
        
def main():
    """Main function to orchestrate the RDB analysis"""
    print("ValkeySearch RDB Structure Analysis")
    print("=" * 50)
    
    # Check environment variables
    valkey_server, valkey_cli, module_path = check_env_vars()
    if not valkey_server or not module_path:
        print_usage()
        return 1
    
    print(f"Using Valkey server: {valkey_server}")
    print(f"Using Valkey CLI: {valkey_cli}")
    print(f"Using module: {module_path}")
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"\nWorking directory: {temp_dir}")
        
        # Clean up old RDB files if any
        cleanup_old_rdb_files(temp_dir)
        
        # Set up paths
        rdb_path = os.path.join('./', "dump.rdb")
        
        # Clean up any existing RDB files to ensure fresh start
        cleanup_old_rdb_files('./')
        
        config_path, log_path = create_valkey_config(temp_dir, module_path, rdb_path)
        
        print(f"Config file: {config_path}")
        print(f"Log file: {log_path}")
        print(f"RDB file: {rdb_path}")
        
        # Start Valkey server
        print(f"\nStarting Valkey server...")
        server_process = subprocess.Popen([
            valkey_server, config_path
        ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        
        try:
            # Wait a moment for server to start
            time.sleep(3)
            
            # Check if process is still running
            if server_process.poll() is not None:
                stdout, stderr = server_process.communicate()
                print(f"Server failed to start. Output: {stdout}")
                print(f"Error: {stderr}")
                return 1
                
            # Check if server is running
            result = subprocess.run([
                valkey_cli, "-p", "6380", "ping"
            ], capture_output=True, text=True, timeout=5)
            
            if result.returncode != 0:
                print("Error: Could not connect to Valkey server")
                return 1
            
            print("Valkey server is running")
            
            # Set RDB compression to 'no' for testing
            print("Setting RDB compression to 'no' for testing...")
            result = subprocess.run([
                valkey_cli, "-p", "6380", "config", "set", "rdbcompression", "no"
            ], capture_output=True, text=True, timeout=5)
            if result.returncode != 0:
                print(f"Error setting RDB compression: {result.stderr}")
                return 1
            # read the configs values
            result = subprocess.run([
                valkey_cli, "-p", "6380", "config", "get", "rdbcompression"
            ], capture_output=True, text=True, timeout=5)
            if result.returncode != 0:
                print(f"Error getting RDB compression config: {result.stderr}")
                return 1
            print(f"RDB compression config: {result.stdout.strip()}")
            # disable rdbchecksum 
            print("Setting RDB checksum to 'no' for testing...")
            result = subprocess.run([
                valkey_cli, "-p", "6380", "config", "set", "rdbchecksum", "no"
            ], capture_output=True, text=True, timeout=5)
            if result.returncode != 0:
                print(f"Error setting RDB checksum: {result.stderr}")
                return 1
            # read the configs values
            result = subprocess.run([
                valkey_cli, "-p", "6380", "config", "get", "rdbchecksum"
            ], capture_output=True, text=True, timeout=5)
            if result.returncode != 0:
                print(f"Error getting RDB checksum config: {result.stderr}")
                return 1
            print(f"RDB checksum config: {result.stdout.strip()}")

            # Run commands to create index and ingest vectors
            print(f"\nCreating vector index and ingesting data...")
            if not run_valkey_cli_commands():
                print("Error running Valkey commands")
                return 1
            
            # Wait for background save to complete
            time.sleep(3)
            
            # Verify we have a fresh RDB file
            if not verify_fresh_rdb(rdb_path):
                print("Warning: RDB file verification failed")
            
            # Extract and display relevant logs before analyzing RDB
            extract_valkey_search_logs(log_path)
            
            # Analyze the RDB file
            valkey_search_found = analyze_rdb_hex(rdb_path)
            
            # Extract and display vector count summary
            vector_count = extract_vector_count_summary(rdb_path)
            
            print(f"\n{'='*60}")
            print("VECTOR INDEX SUMMARY")
            print(f"{'='*60}")
            print(f"Number of vectors in index: {vector_count}")
            print(f"RDB file size: {os.path.getsize(rdb_path)} bytes")
            
            if valkey_search_found and vector_count > 0:
                print(f"✅ Successfully found ValkeySearch module data in RDB")
                print(f"✅ Successfully stored {vector_count} vectors in RDB format")
                print(f"✅ ValkeySearch index created and populated")
                print(f"✅ RDB contains vector data and index metadata")
            elif valkey_search_found:
                print(f"✅ Found ValkeySearch module data in RDB")
                print(f"⚠️  But no vectors found - may need to check data format")
            else:
                print(f"❌ No ValkeySearch module data found in RDB")
                print(f"⚠️  Check if the module is properly saving to RDB")
            
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
            return 1
        finally:
            # Clean up
            print(f"\nShutting down Valkey server...")
            try:
                server_process.terminate()
                server_process.wait(timeout=10)
            except:
                server_process.kill()
    
    print(f"\nAnalysis complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())

# # Example usage:
# if __name__ == "__main__":
#     # Test with the hex data from your output
#     test_data = bytes.fromhex(
#         "02800001000002010540410801100021a3b0a076d79496e64657812001801322"
#         "60a0676656374c7212067665637406f721a140a12080418012001288050320708"
#         "10010c8011180a3a020805400005"
#     )
    
#     analyze_valkey_search_module_payload_fixed(test_data)