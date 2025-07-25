# Tag Index Memory Usage Analysis

## Overview

The Tag index in valkey-search supports efficient storage and retrieval of tagged documents. It uses several optimized data structures to minimize memory overhead while providing fast exact match and prefix queries.

## Key Data Structures

### 1. Core Storage Components

#### tracked_tags_by_keys_ (InternedStringMap<TagInfo>)
- **Type**: `absl::flat_hash_map<InternedStringPtr, TagInfo>`
- **Purpose**: Maps document keys to their tag information
- **Memory per entry**: 
  - Hash map entry overhead: ~32 bytes
  - InternedStringPtr (shared_ptr): 16 bytes
  - TagInfo struct (see below)

#### TagInfo Structure
```cpp
struct TagInfo {
    InternedStringPtr raw_tag_string;  // 16 bytes (shared_ptr)
    absl::flat_hash_set<absl::string_view> tags;  // Variable
};
```
- **raw_tag_string**: Interned pointer to original tag string
- **tags**: Set of parsed tag views (string_views into raw_tag_string)
- **Memory per TagInfo**: 
  - Base: ~16 bytes (raw_tag_string)
  - flat_hash_set overhead: ~32 bytes
  - Per tag in set: ~24 bytes (string_view + hash entry)

#### untracked_keys_ (InternedStringSet)
- **Type**: `absl::flat_hash_set<InternedStringPtr>`
- **Purpose**: Stores keys without tags
- **Memory per entry**: ~24 bytes (shared_ptr + hash entry)

### 2. Patricia Tree (for Prefix Queries)

#### PatriciaNode Structure
```cpp
template <typename T>
class PatriciaNode {
    absl::flat_hash_map<std::string, std::unique_ptr<PatriciaNode>> children;
    int64_t subtree_values_count;
    std::optional<absl::flat_hash_set<T>> value;  // T = InternedStringPtr
};
```

**Memory per node**:
- Base overhead: ~48 bytes
- Children map: ~32 bytes (empty) + per child entry
- Per child entry: ~40 bytes (string key + unique_ptr)
- Value set (if present): ~32 bytes + 24 bytes per document key

### 3. String Interning

#### Benefits:
- **Deduplication**: Multiple references to same key share single string allocation
- **Fast comparison**: Pointer equality for interned strings
- **Reduced allocations**: Reuses existing strings

#### Memory overhead:
- Per interned string: string length + 1 (null terminator) + ~40 bytes metadata
- Global intern store: One hash map entry (~32 bytes) per unique string

## Memory Usage Scenarios

### Scenario 1: Simple Tags
**Document**: `key1` with tags `"user:123,product:456,category:electronics"`

**Memory breakdown**:
- Key interning: ~10 bytes (key1) + 40 bytes overhead = 50 bytes
- Raw tag string interning: ~40 bytes + 40 bytes overhead = 80 bytes
- tracked_tags_by_keys_ entry: 32 + 16 = 48 bytes
- TagInfo: 16 + 32 + (3 × 24) = 120 bytes
- Patricia tree nodes: 
  - 3 leaf nodes: 3 × (48 + 32 + 24) = 312 bytes
  - Internal nodes (shared prefixes): Variable, typically 100-200 bytes
- **Total**: ~710-810 bytes per document

### Scenario 2: Many Documents, Shared Tags
**1000 documents**, each with 5 tags, 20% tag overlap

**Memory breakdown**:
- Unique keys: 1000 × 50 = 50KB
- Unique tag strings (800 unique): 800 × 80 = 64KB
- tracked_tags_by_keys_: 1000 × 48 = 48KB
- TagInfo structures: 1000 × (16 + 32 + 5×24) = 168KB
- Patricia tree:
  - Leaf nodes (800 unique tags): 800 × 104 = 83KB
  - Each leaf stores avg 250 keys: 800 × 250 × 24 = 4.8MB
  - Internal nodes: ~100KB
- **Total**: ~5.3MB for 1000 documents

### Scenario 3: Prefix-Heavy Usage
**Documents with hierarchical tags**: `"org:acme:dept:eng:team:backend"`

**Additional considerations**:
- Patricia tree benefits from shared prefixes
- Internal nodes store common prefixes once
- Memory savings: 20-40% compared to flat storage
- Example: 100 teams under 10 departments = ~60% prefix sharing

### Scenario 4: Case-Insensitive Mode
**Impact**:
- No additional string storage (uses same interned strings)
- Patricia tree performs case-insensitive comparisons
- Slight CPU overhead, no memory overhead

## Memory Optimization Strategies

### 1. String Interning Benefits
- **Deduplication ratio**: Typically 20-50% savings on key strings
- **Tag string sharing**: When documents share tags, significant savings
- **Fast lookups**: O(1) intern store lookups with pointer comparison

### 2. Patricia Tree Efficiency
- **Prefix compression**: Shared prefixes stored once
- **Sparse storage**: Only stores actual data at leaf nodes
- **Balanced growth**: Memory grows with unique tags, not documents

### 3. Memory vs Performance Tradeoffs
- **Flat hash maps**: Fast O(1) access, higher memory overhead
- **String views**: Avoids string copies, references interned data
- **Lazy deletion**: Soft deletes in untracked_keys_ avoid reallocation

## Estimated Memory Usage Formula

```
Total Memory = 
    (Unique Keys × 50) +                          // Interned keys
    (Unique Tag Strings × 80) +                   // Interned tag strings  
    (Documents × 48) +                            // Hash map entries
    (Documents × (168 + Tags_per_doc × 24)) +    // TagInfo structures
    (Unique Tags × 104) +                         // Patricia leaf nodes
    (Unique Tags × Docs_per_tag × 24) +          // Patricia value sets
    (Patricia Internal Nodes)                     // ~10% of leaf nodes
```

## Recommendations

1. **For High Tag Overlap**: Memory usage scales well with shared tags
2. **For Unique Tags**: Consider tag length; shorter tags reduce memory
3. **For Prefix Queries**: Hierarchical naming maximizes Patricia tree efficiency
4. **For Memory-Constrained Systems**: 
   - Monitor unique tag count vs document count ratio
   - Consider tag normalization to increase sharing
   - Use shorter, standardized tag prefixes

## Conclusion

The Tag index achieves efficient memory usage through:
- String interning for deduplication
- Patricia trees for prefix compression  
- String views to avoid copies
- Flat hash structures for performance

Memory usage scales primarily with:
1. Number of unique keys
2. Number of unique tags
3. Average tags per document
4. Tag string lengths

For typical workloads (5-10 tags per document, 20-30% tag overlap), expect ~1-2KB per document.