# Patricia Tree Memory Consumption Analysis

## Patricia Tree Memory Components

### Per-Node Memory Structure
```cpp
PatriciaNode {
    flat_hash_map<string, unique_ptr<PatriciaNode>> children;  // ~32 bytes base + per child
    int64_t subtree_values_count;                              // 8 bytes
    optional<flat_hash_set<InternedStringPtr>> value;          // ~32 bytes when present
}
```

**Base memory per node**: ~48 bytes (without children or values)

## Factors That Dramatically Increase Patricia Tree Memory

### 1. **Minimal Prefix Sharing with High Fan-out**

**Worst Case Pattern**: Tags that share only 1-2 characters before diverging
```
Example tags:
- a1234567890...
- a2345678901...
- a3456789012...
- b1234567890...
- b2345678901...
```

**Memory Impact**:
- Creates maximum number of internal nodes
- Each split point needs a new node (48 bytes + child map overhead)
- For 1M tags with minimal sharing: ~10-20M internal nodes possible
- **Memory multiplication**: 10-20x vs optimal case

### 2. **Alternating Pattern Lengths**

**Pattern**: Mix of very short and very long tags with no intermediate lengths
```
Example:
- "a"
- "ab"
- "abc"
- "abcdefghijklmnopqrstuvwxyz0123456789..." (1000 chars)
```

**Memory Impact**:
- Forces many single-child chains in the tree
- Each character might need its own node in the chain
- **Memory waste**: 48 bytes per node for single-child chains

### 3. **High Cardinality at Each Level**

**Pattern**: Tags that branch into many options at each prefix level
```
Example structure:
Level 1: a, b, c, d, e, f, g, h, i, j (10 branches)
Level 2: Each has 10 sub-branches (100 total)
Level 3: Each has 10 sub-branches (1000 total)
```

**Memory Impact**:
- Each parent node's children map grows large
- Hash map overhead increases with size
- **Per node overhead**: 32 + (40 bytes × num_children)

### 4. **Sparse Tag Distribution**

**Pattern**: Each tag has very few documents (1-2), but many unique tags
```
1M unique tags × 2 documents each = 2M tag assignments
vs
1K unique tags × 2000 documents each = 2M tag assignments
```

**Memory Impact for Sparse Case**:
- 1M leaf nodes × 104 bytes = 104 MB (vs 0.1 MB for dense)
- Each leaf node's value set overhead is ~32 bytes regardless of size
- **Memory inefficiency**: 95% overhead for small sets

### 5. **Anti-Pattern: Common Suffix, Unique Prefix**

**Pattern**: Tags structured with common endings but unique starts
```
Examples:
- 123456_common_suffix
- 789012_common_suffix
- 345678_common_suffix
```

**Memory Impact**:
- Patricia trees optimize prefixes, not suffixes
- Creates a flat, wide tree with no compression
- **Memory usage**: O(n) nodes instead of O(log n)

### 6. **Fragmented Deletions**

**Pattern**: Frequent additions and deletions creating "holes" in the tree
```
Lifecycle:
1. Add tags: a1, a2, a3, ..., a1000
2. Delete even numbered tags
3. Add new tags: b1, b2, b3, ..., b1000
```

**Memory Impact**:
- Nodes remain allocated even with empty value sets
- Internal nodes preserved for potential future use
- **Memory fragmentation**: Up to 50% wasted space

## Worst-Case Scenario Combinations

### Scenario A: "The Memory Explosion"
- **Properties**:
  - 10M keys, 5M unique tags
  - Tags are 100 chars of random hex strings
  - Each tag used by exactly 2 documents
  - No prefix sharing (random content)
  
- **Patricia Tree Memory**:
  - Leaf nodes: 5M × 104 bytes = 520 MB
  - Internal nodes: ~5M × 48 bytes = 240 MB (one per leaf)
  - Value sets: 5M × (32 + 2×24) = 400 MB
  - **Total**: ~1,160 MB (vs 100 MB for well-structured data)

### Scenario B: "The Pathological Case"
- **Properties**:
  - 10M keys, 100K unique base tags
  - Each base tag has 100 variations differing in last character
  - Pattern: `base_tag_00001_X` where X varies
  
- **Patricia Tree Memory**:
  - Must store full strings until final character
  - 100K × 100 × 20 internal nodes = 200M nodes
  - **Total**: ~10 GB just for internal nodes

### Scenario C: "The Birthday Paradox"
- **Properties**:
  - Tags are timestamps with microsecond precision
  - Natural clustering around certain times
  - Pattern: `2024-07-21T15:30:45.123456`
  
- **Patricia Tree Memory**:
  - Deep trees (26+ levels) for timestamp precision
  - Many single-child chains for unique microseconds
  - **Memory overhead**: 48 bytes × 26 levels = 1.2KB per unique timestamp path

## Memory Comparison Table

| Pattern Type | Internal Nodes | Memory Multiplier | Compression Ratio |
|--------------|----------------|-------------------|-------------------|
| Optimal (Hierarchical) | O(log n) | 1x | 10-20x |
| Good (Natural Clustering) | O(n^0.5) | 2-3x | 5-10x |
| Average (Some Sharing) | O(n^0.7) | 5-10x | 2-5x |
| Bad (Minimal Sharing) | O(n) | 10-20x | 1-2x |
| Worst (Anti-patterns) | O(n × log n) | 20-50x | <1x |

## Key Insights

1. **Prefix Length Distribution** matters more than total unique tags
   - Uniform prefix lengths → efficient tree
   - Highly variable lengths → inefficient chains

2. **Branching Factor** at each level impacts memory
   - Binary splits (2 children) → minimal overhead  
   - High fan-out (10+ children) → significant hash map overhead

3. **Value Set Density** affects efficiency
   - Dense sets (many docs per tag) → amortized overhead
   - Sparse sets (few docs per tag) → high overhead per document

4. **Tree Balance** is critical
   - Balanced trees → O(log n) depth
   - Degenerate trees → O(n) depth

5. **Deletion Patterns** can fragment memory
   - Bulk deletes → consider tree rebuild
   - Scattered deletes → persistent overhead

## Recommendations to Minimize Patricia Tree Memory

1. **Design tags with common prefixes**
   - Good: `category:subcategory:item`
   - Bad: `item_subcategory_category`

2. **Avoid high cardinality at root level**
   - Good: Start with limited set of prefixes
   - Bad: UUID or hash as first characters

3. **Maintain consistent tag lengths within levels**
   - Good: Fixed-length components
   - Bad: Mixing very short and very long tags

4. **Batch similar tags together**
   - Good: Related tags share prefixes
   - Bad: Random distribution

5. **Consider tag normalization**
   - Convert timestamps to buckets
   - Use standard separators
   - Limit precision where not needed