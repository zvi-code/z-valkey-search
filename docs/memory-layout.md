# Valkey-Search Memory Components Architecture

## Overview

This document describes the comprehensive memory usage breakdown for Valkey-Search indexes.

## 1. String Interning Store (Centralized Deduplication)

The string interning system provides centralized string deduplication across all index types.

### Structure
```
StringInternStore (Singleton)
├── absl::flat_hash_map<string, weak_ptr<InternedString>>
│   ├── "user:123" → weak_ptr
│   ├── "product:456" → weak_ptr
│   └── "index:main" → weak_ptr
├── Mutex protected
├── Reference counting
└── Automatic cleanup
```

### Memory Characteristics
- **Hash entry overhead**: ~32 bytes per entry
- **InternedString size**: 24 + strlen bytes
- **Shared across all indexes**

### Usage Pattern
- **Vector Indexes**: Keys only
- **Tag Index**: Keys and full tag strings
- **Numeric Index**: Keys only
- **Schema**: Attribute names

### Example Savings
For 100K documents with average key "doc:12345" (9 chars):
- Without interning: 100K × 9 = 900KB
- With interning: ~100KB (deduped prefixes)
- **Savings: ~800KB** just for keys

## 2. Vector Indexes

### Shared Components (VectorBase)
```
VectorBase
├── tracked_metadata_by_key_
│   ├── Type: InternedStringMap<Metadata>
│   ├── Size: num_vectors × 40 bytes
│   └── Protected by: RWMutex
├── key_by_internal_id_
│   ├── Type: map<uint64_t, InternedStringPtr>
│   ├── Size: num_vectors × 24 bytes
│   └── Purpose: Reverse lookups
└── vector_allocator_ (FixedSizeAllocator)
    ├── Block size: dimensions × sizeof(float)
    ├── Chunk-based allocation
    └── Free list management
```

### HNSW Index

#### Memory Layout
```
data_level0_memory_: ChunkedArray
├── size_links_level0 = maxM0 × 4 + 4 = 132 bytes
├── data pointer: 8 bytes (points to vector data)
└── label: 8 bytes (external ID)

linkLists_: ChunkedArray (for higher levels, sparse)
```

#### Parameters
- M = 16 (connections per level)
- maxM0 = 32 (layer 0 connections)
- mL = 1/ln(2.0) ≈ 1.44
- ef_construction = 200

#### Layer Distribution
- Layer 0: 100% of vectors
- Layer 1: ~6.25% (1/16)
- Layer 2: ~0.39% (1/256)
- Layer 3: ~0.024% (1/4096)

#### Memory Example (10K vectors, 384 dimensions)
- Vectors: 10K × 384 × 4 = **15.4MB**
- Level 0 links: 10K × 32 × 8 = **2.56MB**
- Higher levels: ~10K/16 × 16 × 8 × log(10K) ≈ **1MB**
- **Total: ~19MB**

### Flat (Brute Force) Index

#### Storage Structure
```
vectors_: vector<void*>
├── Size: num_vectors × 8 bytes
├── Dynamic resize by block_size (1000)
└── Cache-friendly linear scan

tracked_vectors_: flat_hash_map<id, key>
```

#### Memory Example (10K vectors, 384 dimensions)
- Vectors: 10K × 384 × 4 = **15.4MB**
- Pointers: 10K × 8 = **80KB**
- Tracking: 10K × 32 = **320KB**
- **Total: ~15.8MB**

## 3. Tag Index

### Patricia Tree Structure
```
PatriciaTree (Radix Tree)
├── PatriciaNode<T>
│   ├── prefix: string_view (into tag string)
│   ├── children: map<char, unique_ptr<Node>>
│   └── value: optional<InternedStringSet>
└── Memory per node
    ├── Node struct: ~48 bytes
    ├── Children map: 8-64 bytes
    └── Doc set: N × 8 bytes
```

### Storage Components
```
field_to_trie_
├── Type: unordered_map<InternedString, PatriciaTree>
└── Size: num_fields × (32 + tree_size)

doc_to_fields_
├── Type: unordered_map<InternedString, set<InternedString>>
└── Size: num_docs × avg_fields × 24

PatriciaNode value storage
├── Type: optional<InternedStringSet>
└── Size: num_docs_per_tag × 8 bytes (pointers)
```

### Memory Example (E-commerce: 100K products, 5 tag fields, 50 unique tags/field)
- Patricia nodes: 250 × 48 = **12KB**
- Doc sets: 250 tags × 400 docs/tag × 8 = **800KB**
- Doc maps: 100K × 5 × 24 = **12MB**
- Field maps: 5 × 32 = **160 bytes**
- **Total: ~12.8MB**

## 4. Numeric Index

### BTree + Segment Tree Structure
```
BTreeNumeric
├── btree_: btree_map<double, SetType>
│   └── SetType: flat_hash_set<InternedStringPtr>
├── segment_tree_: SegmentTree (range counts)
│   └── Overhead: ~80 bytes/entry
└── Properties
    ├── O(log n) search/insert/delete
    └── Efficient range queries + counts
```

### Components
```
tracked_keys_
├── Type: InternedStringMap<double>
└── Size: num_docs × (8 + 8 + overhead)

BTreeNumeric index_
├── btree_map<double, flat_hash_set<InternedStringPtr>>
├── SegmentTree for range counts
├── Overhead: ~80 bytes per unique value
└── Doc sets: num_docs × 8 bytes
```

### Memory Example (100K price documents)
- tracked_keys_: 100K × 24 = **2.4MB**
- BTree nodes: ~1K values × 80 = **80KB**
- Doc sets: 100K × 8 = **800KB**
- **Total: ~3.3MB**

## 5. Global Key → ID Mapping

### Primary Mapping (IndexSchema)
```
key_to_id_
├── Type: absl::flat_hash_map<InternedStringPtr, uint64_t>
├── Key: InternedStringPtr (8 bytes)
├── Value: uint64_t document_id (8 bytes)
├── Load factor: 0.875
└── Memory: entries × 16 × 1.14 (overhead)
```

### Performance Characteristics
- O(1) average lookup
- Cache-friendly layout
- No pointer chasing
- Fast key comparison (pointer equality)

### Memory Example (1M documents)
- Primary map: 1M × 16 × 1.14 = **18.2MB**
- Reverse maps: varies by index type

## Total Memory Example (1M Documents)

| Component | Memory Usage |
|-----------|-------------|
| String Interning | ~5MB |
| Vector Index (HNSW, 100K vectors) | ~20MB |
| Tag Indexes | ~15MB |
| Numeric Indexes | ~5MB |
| Key-ID Map | ~18MB |
| Metadata & Overhead | ~10MB |
| **Total** | **~73MB** |

*Note: Actual memory usage varies significantly with data characteristics*

