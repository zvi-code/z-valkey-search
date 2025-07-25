# Tag Index Memory Analysis - Fixed Dataset, Variable Content

## Fixed Dataset Parameters

**Constants across all scenarios:**
- **Number of Keys**: 10,000,000 (10M)
- **Average Key Length**: 20 characters
- **Number of Unique Tags**: 1,000,000 (1M)
- **Average Tag Length**: 50 characters
- **Average Tags per Key**: 10

**What varies**: How tags are distributed and their content patterns

## Content Pattern Scenarios

### Scenario A: Uniform Distribution
- **Pattern**: Each tag appears on exactly 100 keys
- **Prefix Structure**: Random strings, no shared prefixes
- **Patricia Tree**: Maximum nodes (no compression)
- **Example**: UUID-based tags like `a7f3d2e1-9b4c-4a5d-8e2f-1c3b5a7d9e2f-metadata`

### Scenario B: Power Law Distribution  
- **Pattern**: 20% of tags appear on 80% of keys
- **Prefix Structure**: Mixed - popular tags have prefixes, rare tags random
- **Patricia Tree**: Moderate compression for popular prefixes
- **Example**: `category:electronics`, `user:premium:12345`, `temp:xyz123`

### Scenario C: Hierarchical Taxonomy
- **Pattern**: Tree-like structure with shared prefixes
- **Prefix Structure**: 80% of tags share common prefixes (avg 40 chars)
- **Patricia Tree**: High compression, many internal nodes
- **Example**: `org:acme:dept:eng:team:backend:user:john`, `org:acme:dept:eng:team:frontend:user:jane`

### Scenario D: Clustered Tags
- **Pattern**: Tags form 100 clusters, keys mostly use tags from 1-2 clusters
- **Prefix Structure**: High sharing within clusters, none between
- **Patricia Tree**: Forest of compressed subtrees
- **Example**: Cluster1: `log:2024:01:*`, Cluster2: `metric:cpu:*`, etc.

### Scenario E: Temporal Patterns
- **Pattern**: 90% of recent keys use 10% of tags (hot tags)
- **Prefix Structure**: Hot tags share prefixes, cold tags don't
- **Patricia Tree**: Imbalanced - dense hot subtree, sparse cold nodes
- **Example**: Hot: `event:2024:07:*`, Cold: `archive:2020:*`

## Memory Usage Analysis

| Component | Scenario A | Scenario B | Scenario C | Scenario D | Scenario E |
|-----------|------------|------------|------------|------------|------------|
| **Fixed Components** |
| Interned Keys (10M × 60B) | 600 MB | 600 MB | 600 MB | 600 MB | 600 MB |
| Tag Sets (10M × 10 × 24B) | 2,400 MB | 2,400 MB | 2,400 MB | 2,400 MB | 2,400 MB |
| Hash Maps (10M × 96B) | 960 MB | 960 MB | 960 MB | 960 MB | 960 MB |
| **Variable Components** |
| **String Interning** |
| Unique tag strings | 1M | 850K | 600K | 900K | 950K |
| Interned tag data | 50 MB | 42.5 MB | 30 MB | 45 MB | 47.5 MB |
| Interning overhead | 40 MB | 34 MB | 24 MB | 36 MB | 38 MB |
| **Patricia Tree** |
| Leaf nodes (1M tags) | 104 MB | 104 MB | 104 MB | 104 MB | 104 MB |
| Internal nodes | 200 MB | 120 MB | 20 MB | 80 MB | 150 MB |
| Node value sets | 2,400 MB | 2,400 MB | 2,400 MB | 2,400 MB | 2,400 MB |
| String comparison overhead | High | Medium | Low | Medium | Medium |
| **Total Memory** | **6,754 MB** | **6,620 MB** | **6,438 MB** | **6,625 MB** | **6,699 MB** |
| **Difference from Uniform** | Baseline | -2.0% | -4.7% | -1.9% | -0.8% |

## Detailed Impact Analysis

### String Interning Efficiency

| Scenario | Unique Strings | Dedup Rate | Memory Saved |
|----------|----------------|------------|--------------|
| A: Uniform | 1,000,000 | 0% | 0 MB |
| B: Power Law | 850,000 | 15% | 8.5 MB |
| C: Hierarchical | 600,000 | 40% | 24 MB |
| D: Clustered | 900,000 | 10% | 5.5 MB |
| E: Temporal | 950,000 | 5% | 2.5 MB |

**Key Insight**: Hierarchical patterns enable 40% string deduplication through prefix sharing

### Patricia Tree Structure

| Scenario | Tree Depth | Internal Nodes | Compression Ratio |
|----------|------------|----------------|-------------------|
| A: Uniform | 3-5 | 200,000 | 1.0x |
| B: Power Law | 2-8 | 120,000 | 1.7x |
| C: Hierarchical | 5-12 | 20,000 | 10.0x |
| D: Clustered | 3-7 | 80,000 | 2.5x |
| E: Temporal | 2-10 | 150,000 | 1.3x |

**Key Insight**: Hierarchical taxonomy achieves 10x compression in Patricia tree

### Query Performance Implications

| Scenario | Exact Match | Prefix Query | Memory Locality |
|----------|-------------|--------------|-----------------|
| A: Uniform | O(1) hash | O(log n) | Poor |
| B: Power Law | O(1) hash | O(log n) | Good for hot |
| C: Hierarchical | O(1) hash | O(1) for common | Excellent |
| D: Clustered | O(1) hash | O(log k) per cluster | Good within cluster |
| E: Temporal | O(1) hash | O(1) for hot | Excellent for hot |

## Key Findings

1. **Same Statistics, Different Memory**: With identical counts, memory varies by ~5% based on content patterns

2. **Hierarchical Wins**: Hierarchical taxonomies save the most memory (4.7%) through:
   - Maximum prefix compression in Patricia tree
   - Highest string deduplication rate
   - Best memory locality

3. **Power Law Benefits**: Real-world power law distributions save 2% through:
   - Popular tags share prefixes naturally
   - Hot path optimization in Patricia tree

4. **Random is Worst**: Uniform/random distribution uses most memory:
   - No prefix sharing opportunities
   - Maximum Patricia tree nodes
   - Poor cache locality

5. **Clustering Helps**: Even without global prefixes, local clustering saves memory

## Optimization Strategies by Pattern

### For Uniform Distribution (Worst Case)
- Consider adding artificial prefixes for categorization
- Implement tag normalization to increase sharing
- Use shorter tag names where possible

### For Power Law (Common Case)
- Optimize hot path in Patricia tree
- Consider separate index for top 20% tags
- Cache popular tag lookups

### For Hierarchical (Best Case)
- Already optimized for memory
- Focus on query performance
- Consider depth limits to bound lookup time

### For Clustered
- Partition indices by cluster for better locality
- Optimize inter-cluster queries separately
- Consider cluster-aware caching

### For Temporal
- Implement age-based eviction for cold tags
- Use separate indices for hot/cold data
- Consider time-based partitioning

## Conclusion

Even with fixed dataset parameters, content patterns significantly impact memory usage:
- Best case (hierarchical): 6,438 MB
- Worst case (uniform): 6,754 MB
- **Difference**: 316 MB (4.7%)

This 5% variation comes entirely from:
1. String interning efficiency (0-40% deduplication)
2. Patricia tree compression (1-10x)
3. Memory locality effects

For 10M documents, choosing the right tagging strategy can save hundreds of MB without changing the fundamental data statistics.