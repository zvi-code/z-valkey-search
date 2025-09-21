# Valkey-Search Memory Components Architecture

## Comprehensive Memory Usage Breakdown

---

## 🔤 String Interning Store (Centralized Deduplication)

The String Interning system provides centralized string deduplication across all index components.

```
┌─────────────────────────────────────────────────────────────────────┐
│                    StringInternStore (Singleton)                    │
├─────────────────────────────────────────────────────────────────────┤
│  absl::flat_hash_map<string, weak_ptr<InternedString>>             │
│  ┌─────────────────────────────────────────────────┐               │
│  │ "user:123"     → weak_ptr                       │               │
│  │ "product:456"  → weak_ptr                       │               │
│  │ "index:main"   → weak_ptr                       │               │
│  └─────────────────────────────────────────────────┘               │
│                                                                     │
│  • Mutex protected                                                 │
│  • Reference counting                                              │
│  • Automatic cleanup                                               │
└─────────────────────────────────────────────────────────────────────┘
```

### Memory Characteristics
- **Hash entry**: ~32 bytes per unique string
- **InternedString**: 24 + strlen bytes
- **Shared across all indexes** - single copy per unique string

### Used By All Components
- **Vector Indexes**: keys only
- **Tag Index**: keys, full tag strings  
- **Numeric Index**: keys only
- **Schema**: attribute names

### Benefits
- ✅ Single copy of each string in memory
- ✅ Fast pointer comparison (8 byte comparison vs full string)
- ✅ Automatic memory management via reference counting

### Metrics Tracking
```cpp
struct InternedStringWithMetadata {
    shared_ptr<InternedString> string;
    size_t memory_size;
    bool is_tracked;
};

// GlobalMetrics::interned_strings_memory_bytes
// Total: sum of all string sizes
```

### Example: 100K Documents
```
Without interning: 100K × 9 bytes ("doc:12345") = 900KB
With interning:    ~100KB (deduped prefixes)
Savings:           ~800KB just for keys!

Tag values example:
- Doc has tags: "red,large,cotton" 
- Stored once as full string: 17 bytes
- Patricia nodes use string_views (no copies)
- No individual tag interning needed!
```

---

## 🔍 Vector Indexes

### VectorBase (Shared Components)

```
┌──────────────────────────────────────────────────────────────────┐
│                     VectorBase Components                        │
├──────────────────────────────────────────────────────────────────┤
│ tracked_metadata_by_key_        │ key_by_internal_id_           │
│ InternedStringMap<Metadata>     │ map<uint64_t, InternedStrPtr> │
│ Size: num_vectors × 40 bytes    │ Size: num_vectors × 24 bytes  │
│ RWMutex protected               │ For reverse lookups           │
├──────────────────────────────────────────────────────────────────┤
│              vector_allocator_ (FixedSizeAllocator)             │
│              Block size: dimensions × sizeof(float)             │
│              Chunk-based allocation with free list              │
└──────────────────────────────────────────────────────────────────┘
```

### HNSW Index

```
                    HNSW Hierarchical Structure
    
    Layer 3 (0.024%):    *
                         |
    Layer 2 (0.39%):     *---*
                        /|\  |
    Layer 1 (6.25%):   *-*-*-*-*
                      /|X|X|X|\
    Layer 0 (100%):  *-*-*-*-*-*-*-*  (all vectors, maxM0=32 connections)
```

#### Memory Layout (from hnswalg.h)
```cpp
data_level0_memory_: ChunkedArray
  - size_links_level0 = maxM0 × 4 + 4 = 132 bytes
  - data pointer: 8 bytes (points to vector data)
  - label: 8 bytes (external ID)
  
linkLists_: ChunkedArray for higher levels (sparse)
```

#### Parameters
- **M** = 16 (connections per layer > 0)
- **maxM0** = 32 (connections at layer 0)
- **mL** = 1/ln(2.0) ≈ 1.44
- **ef_construction** = 200
- **allow_replace_deleted** = false

#### Memory Example (10K vectors, 384 dims)
```
Vectors:        10K × 384 × 4 = 15.4MB
Level 0 edges:  10K × 32 × 8 = 2.56MB (all vectors)
Higher levels:  ~10K/16 × 16 × 8 × log(10K) ≈ 1MB
Total:          ~19MB
```

### Flat (Brute Force) Index

```
┌────────────────────────────────────────────┐
│            Flat Index Storage              │
├────────────────────────────────────────────┤
│ vectors_: vector<void*>                    │
│   - Size: num_vectors × 8 bytes           │
│   - Dynamic resize by block_size (1000)   │
│                                            │
│ tracked_vectors_: flat_hash_map<id, key>  │
└────────────────────────────────────────────┘
```

#### Characteristics
- Linear scan search with 100% recall
- No graph overhead
- Physical deletion (no tombstones)
- Cache-friendly sequential scan

#### Memory Example (10K vectors, 384 dims)
```
Vectors:   10K × 384 × 4 = 15.4MB
Pointers:  10K × 8 = 80KB
Tracking:  10K × 32 = 320KB
Total:     ~15.8MB
```

---

## 🏷️ Tag Index

### Patricia Tree (Radix Tree) Structure

```
                     root
                    /  |  \
                prod  user  admin
                / \     |
            uct:A uct:B :active
```

#### PatriciaNode<T> Structure
```cpp
struct PatriciaNode {
    string_view prefix;        // Into interned tag string
    map<char, unique_ptr<Node>> children;
    optional<InternedStringSet> value;  // Doc IDs
};
```

**Memory per node:**
- Node struct: ~48 bytes
- Children map: 8-64 bytes  
- Doc set: N × 8 bytes (pointer per doc)

### Tag Storage Components

```
┌─────────────────────────────────────────────────────────┐
│                  field_to_trie_                        │
│  unordered_map<InternedString, PatriciaTree>          │
│  Maps field names to their tag trees                   │
│  Size: num_fields × (32 + tree_size)                  │
├─────────────────────────────────────────────────────────┤
│                  doc_to_fields_                        │
│  unordered_map<InternedString, set<InternedString>>   │
│  Document → field names mapping                        │
│  Size: num_docs × avg_fields × 24                     │
├─────────────────────────────────────────────────────────┤
│            PatriciaNode Value Storage                  │
│  optional<InternedStringSet> in each node             │
│  Document IDs stored as InternedStringPtr set         │
│  Size: num_docs_per_tag × 8 bytes                     │
└─────────────────────────────────────────────────────────┘
```

### Memory Example: E-commerce Dataset
```
100K products, 5 tag fields, 50 unique tags/field

Patricia nodes:  250 × 48 = 12KB
Doc sets:        250 tags × 400 docs/tag × 8 = 800KB  
Doc maps:        100K × 5 × 24 = 12MB
Field maps:      5 × 32 = 160 bytes
Total:           ~12.8MB
```

---

## 🔢 Numeric Index

### BTreeNumeric Structure

```
                [100, 500]           <- Internal node
               /     |     \
    [0, 50, 99]  [100-499]  [500, 750, 999]  <- Leaf nodes
         |           |              |
    doc_set_1    doc_set_2    doc_set_3
```

#### Data Structures
```cpp
btree_: btree_map<double, SetType>
segment_tree_: SegmentTree  // For range counts
SetType: flat_hash_set<InternedStringPtr>
```

#### Properties
- O(log n) search/insert/delete
- Efficient range queries with counts
- SegmentTree: ~80 bytes/entry overhead

### Numeric Index Components

```
┌──────────────────────────────────────────────────────┐
│                 tracked_keys_                        │
│         InternedStringMap<double>                   │
│         Maps document keys to numeric values        │
│         Size: num_docs × (8 + 8 + overhead)        │
├──────────────────────────────────────────────────────┤
│              BTreeNumeric index_                     │
│  • btree_map<double, flat_hash_set<InternedStrPtr>> │
│  • SegmentTree for range counts                     │
│  • Overhead: ~80 bytes per unique value             │
│  • Doc sets: num_docs × 8 bytes                     │
└──────────────────────────────────────────────────────┘
```

### Memory Example: Prices (100K docs)
```
tracked_keys_:  100K × 24 = 2.4MB
BTree nodes:    ~1K values × 80 = 80KB
Doc sets:       100K × 8 = 800KB
Total:          ~3.3MB
```

---

## 🔑 Global Key → ID Mapping

### IndexSchema: key_to_id_

```
┌────────────────────────────────────────────────┐
│      absl::flat_hash_map Structure            │
├────────────────────────────────────────────────┤
│ Key:   InternedStringPtr (8 bytes)            │
│ Value: uint64_t document_id (8 bytes)         │
│                                                │
│ Load factor: 0.875 (87.5% full)               │
│ Collision resolution: Open addressing         │
│ Memory: entries × 16 × 1.14 (overhead)        │
└────────────────────────────────────────────────┘
```

### Performance Characteristics
- ⚡ O(1) average lookup
- ⚡ Cache-friendly layout  
- ⚡ No pointer chasing
- ⚡ Automatic growth/rehash
- ⚡ Thread-safe with mutex
- ⚡ Fast key comparison (pointer equality)

### Reverse Mappings (Per Index)
```cpp
// Vector Indexes
key_by_internal_id_: map<uint64_t, InternedStringPtr>
// Size: num_vectors × 24 bytes
```

### Memory Example (1M documents)
```
Primary map:    1M × 16 × 1.14 = 18.2MB
Reverse maps:   varies by index type
```

---

## 📊 Total Memory Breakdown Example (1M Documents)

| Component | Memory Usage | Notes |
|-----------|-------------|-------|
| **String Interning** | ~5MB | Assuming 5KB unique strings |
| **Vector Index (HNSW)** | ~20MB | 100K vectors, 384 dims |
| **Tag Indexes** | ~15MB | Multiple fields with tags |
| **Numeric Indexes** | ~5MB | Price, quantity, etc. |
| **Key-ID Map** | ~18MB | Primary document mapping |
| **Metadata & Overhead** | ~10MB | Tracking, metrics, etc. |
| **Total** | **~73MB** | For 1M documents |

*Note: Memory usage varies significantly with data characteristics*

### Metrics Access
All metrics available via:
```
INFO SEARCH
```
- Updated atomically in real-time
- Per-index and global breakdowns
- Memory tracking for all components

---

## 🚀 Key Optimizations

1. **String Interning**: Massive savings on repeated strings
2. **Pointer-based comparisons**: 8-byte comparisons instead of full strings
3. **ChunkedArray allocation**: Reduces fragmentation for HNSW
4. **FixedSizeAllocator**: Efficient vector memory management
5. **Patricia trees with string_views**: No string copies for tag prefixes
6. **Flat hash maps**: Cache-friendly with open addressing
7. **Reference counting**: Automatic cleanup of unused strings

---

## 📝 Implementation Notes

- All string keys use `InternedStringPtr` for deduplication
- Metrics updated atomically via `GlobalMetrics` class
- Thread-safe access with appropriate mutex/RWMutex protection
- Memory tracked at component level for detailed profiling
- Supports both exact tracking and estimation modes