# Tag Index Memory Usage Scenarios - 10M Keys

## Base Parameters
- **Total Keys**: 10,000,000 (10M)
- **Memory calculations** include all data structures: string interning, patricia tree, hash maps

## Scenario Definitions

### Scenario 1: Short Tags, Few per Key, High Prefix Sharing
- **Tag Length**: 15 chars average (uniform distribution 10-20)
- **Tags per Key**: 5 average (gaussian μ=5, σ=2, range 1-10)
- **Prefix Sharing**: High (80% tags share common prefixes)
- **Example tags**: `user:123`, `user:456`, `dept:eng`, `dept:hr`, `role:admin`

### Scenario 2: Medium Tags, Moderate per Key, Medium Prefix Sharing
- **Tag Length**: 30 chars average (gaussian μ=30, σ=10, range 10-50)
- **Tags per Key**: 10 average (gaussian μ=10, σ=3, range 3-20)
- **Prefix Sharing**: Medium (40% tags share prefixes)
- **Example tags**: `organization:acme:department:engineering`, `product:category:electronics:smartphones`

### Scenario 3: Long Tags, Many per Key, Low Prefix Sharing
- **Tag Length**: 100 chars average (gaussian μ=100, σ=30, range 50-200)
- **Tags per Key**: 20 average (gaussian μ=20, σ=5, range 10-40)
- **Prefix Sharing**: Low (10% tags share prefixes)
- **Example tags**: `metadata:project:2024:q1:customer:enterprise:region:north-america:priority:high:status:active`

### Scenario 4: Mixed Reality - E-commerce Platform
- **Tag Length**: 25 chars average (bimodal: 15 chars 60%, 40 chars 40%)
- **Tags per Key**: 8 average (power law distribution, most have 3-5, some have 20+)
- **Prefix Sharing**: Medium-High (60% for categories, 20% for attributes)
- **Example tags**: `cat:electronics`, `brand:apple`, `color:silver`, `price:500-1000`

## Memory Calculations

### Detailed Memory Breakdown by Scenario

| Component | Scenario 1 | Scenario 2 | Scenario 3 | Scenario 4 |
|-----------|------------|------------|------------|------------|
| **Key Statistics** |
| Total Keys | 10M | 10M | 10M | 10M |
| Avg Tags/Key | 5 | 10 | 20 | 8 |
| Avg Tag Length | 15 | 30 | 100 | 25 |
| Total Tag Instances | 50M | 100M | 200M | 80M |
| **Unique Counts** |
| Unique Tags (prefix sharing) | 500K | 2M | 8M | 1.5M |
| Unique Tag Strings | 8M | 9M | 9.8M | 8.5M |
| **Memory Usage** |
| **1. Interned Keys** |
| - String data | 80 MB | 80 MB | 80 MB | 80 MB |
| - Overhead (40B/key) | 400 MB | 400 MB | 400 MB | 400 MB |
| **2. Interned Tag Strings** |
| - String data | 120 MB | 270 MB | 980 MB | 213 MB |
| - Overhead (40B/string) | 320 MB | 360 MB | 392 MB | 340 MB |
| **3. tracked_tags_by_keys_** |
| - Map entries (48B/entry) | 480 MB | 480 MB | 480 MB | 480 MB |
| - TagInfo base (48B/entry) | 480 MB | 480 MB | 480 MB | 480 MB |
| - Tag sets (24B/tag) | 1,200 MB | 2,400 MB | 4,800 MB | 1,920 MB |
| **4. Patricia Tree** |
| - Leaf nodes (104B/node) | 52 MB | 208 MB | 832 MB | 156 MB |
| - Value sets (24B/key) | 1,200 MB | 2,400 MB | 4,800 MB | 1,920 MB |
| - Internal nodes | 10 MB | 100 MB | 500 MB | 50 MB |
| **5. String Intern Store** |
| - Hash map (32B/entry) | 280 MB | 320 MB | 360 MB | 300 MB |
| **Total Memory** | **4,622 MB** | **7,508 MB** | **13,924 MB** | **6,339 MB** |
| **Memory per Key** | **462 bytes** | **751 bytes** | **1,392 bytes** | **634 bytes** |

## Scenario Analysis

### Scenario 1: Short Tags, High Sharing
- **Characteristics**: Typical for user tagging systems, categories
- **Memory Efficiency**: Excellent - high prefix sharing reduces Patricia tree size
- **Best for**: Systems with well-defined taxonomies

### Scenario 2: Medium Tags, Medium Sharing  
- **Characteristics**: Hierarchical organizational data
- **Memory Efficiency**: Good - balanced between tag length and sharing
- **Best for**: Enterprise systems with structured data

### Scenario 3: Long Tags, Low Sharing
- **Characteristics**: Detailed metadata, unique identifiers
- **Memory Efficiency**: Poor - long strings with little deduplication
- **Best for**: Systems requiring rich, unique metadata

### Scenario 4: Mixed Reality
- **Characteristics**: Real-world e-commerce with varied tag types
- **Memory Efficiency**: Good - bimodal distribution allows optimization
- **Best for**: Production systems with mixed workloads

## Key Insights

1. **String Length Impact**: Moving from 15 to 100 char average increases memory by ~3x
2. **Tags per Key Impact**: Doubling tags/key roughly doubles memory usage
3. **Prefix Sharing Benefits**: High sharing (80% vs 10%) can save 30-40% memory
4. **Dominant Factors**: 
   - Tag sets in tracked_tags_by_keys_: 25-35% of total
   - Patricia tree value sets: 25-35% of total
   - String interning: 15-20% of total

## Optimization Recommendations by Scenario

### For Short Tags + High Sharing (Scenario 1)
- Already optimized, consider increasing cache efficiency
- Monitor Patricia tree depth for query performance

### For Long Tags + Low Sharing (Scenario 3)
- Consider tag compression or encoding schemes
- Evaluate if all tag data needs indexing
- Implement tag archival for inactive documents

### For Mixed Workloads (Scenario 4)
- Separate indexes for different tag types
- Use bloom filters for existence checks
- Implement tiered storage for hot/cold tags

## Extreme Case Scenarios

### Scenario 5: BEST CASE - Maximum Sharing with Long Tags
- **Tag Length**: 4,096 chars (4KB)
- **Tags per Key**: 10
- **Prefix Sharing**: 99% (almost all tags share 4,000+ char prefix)
- **Keys per Tag**: 1M keys per unique tag (only 100 unique tags total)
- **Example**: `log:2024:application:server:region:us-east-1:instance:i-1234567890:service:api:version:2.3.1:timestamp:...[4KB total]`

### Scenario 6: WORST CASE - No Sharing with Long Tags  
- **Tag Length**: 4,096 chars (4KB)
- **Tags per Key**: 10
- **Prefix Sharing**: 0% (every tag completely unique)
- **Keys per Tag**: 1 (each key has unique tags)
- **Example**: Random UUIDs or hashes as tags

### Scenario 7: Single Tag Per Key - High Sharing
- **Tag Length**: 4,096 chars (4KB)
- **Tags per Key**: 1
- **Prefix Sharing**: 95%
- **Keys per Tag**: 100K keys per unique tag (100 unique tags)

### Scenario 8: Many Short Unique Tags
- **Tag Length**: 10 chars
- **Tags per Key**: 100
- **Prefix Sharing**: 5%
- **Keys per Tag**: 1-2

## Extreme Cases Memory Breakdown

| Component | Best Case (5) | Worst Case (6) | Single Tag (7) | Many Short (8) |
|-----------|---------------|----------------|----------------|----------------|
| **Key Statistics** |
| Total Keys | 10M | 10M | 10M | 10M |
| Avg Tags/Key | 10 | 10 | 1 | 100 |
| Avg Tag Length | 4,096 | 4,096 | 4,096 | 10 |
| Total Tag Instances | 100M | 100M | 10M | 1,000M |
| **Unique Counts** |
| Unique Tags | 100 | 100M | 100 | 50M |
| Unique Tag Strings | 1M | 10M | 1M | 9.5M |
| **Memory Usage** |
| **1. Interned Keys** |
| - String data | 80 MB | 80 MB | 80 MB | 80 MB |
| - Overhead | 400 MB | 400 MB | 400 MB | 400 MB |
| **2. Interned Tag Strings** |
| - String data | 4,096 MB | 40,960 MB | 4,096 MB | 95 MB |
| - Overhead | 40 MB | 400 MB | 40 MB | 380 MB |
| **3. tracked_tags_by_keys_** |
| - Map entries | 480 MB | 480 MB | 480 MB | 480 MB |
| - TagInfo base | 480 MB | 480 MB | 480 MB | 480 MB |
| - Tag sets | 2,400 MB | 2,400 MB | 240 MB | 24,000 MB |
| **4. Patricia Tree** |
| - Leaf nodes | 0.01 MB | 10,400 MB | 0.01 MB | 5,200 MB |
| - Value sets | 240 MB | 2,400 MB | 240 MB | 2,400 MB |
| - Internal nodes | 0.4 MB | 100 MB | 0.4 MB | 500 MB |
| **5. Intern Store** |
| - Hash map | 32 MB | 320 MB | 32 MB | 304 MB |
| **Total Memory** | **8,252 MB** | **58,420 MB** | **6,398 MB** | **33,939 MB** |
| **Memory per Key** | **825 bytes** | **5,842 bytes** | **640 bytes** | **3,394 bytes** |

## Memory Growth Formula

For custom scenarios, use:
```
Memory (MB) = 10M × [
    48 + // base key overhead
    48 + // TagInfo base
    (avg_tags × 24) + // tag set entries
    (avg_tags × 24 × unique_tag_ratio) + // Patricia values
    (avg_tag_length × unique_string_ratio × 1.4) / 1M // string storage
] / 1,048,576

Where:
- unique_tag_ratio = (1 - prefix_sharing_rate) × 0.8
- unique_string_ratio = 0.8 to 0.98 depending on overlap
```