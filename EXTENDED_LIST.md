# Key Implementation Details:
## 1. Configuration

 - `use_extended_neighbors_`: Enable/disable the feature
 - `extended_list_factor_`: Factor to multiply M (e.g., 1.5 = 50% more neighbors)
 - Uses extended sizes for memory allocation when enabled

## 2. Neighbor List Structure

- Regular neighbors: indices 0 to M-1
- Extended neighbors: indices M to M×factor-1
- Count storage: Lower 16 bits = regular count, upper 16 bits = total count

## 3. Search Enhancement

- When encountering deleted neighbor, automatically tries extended neighbors
- Continues until finding non-deleted neighbor or exhausting extended list
- Still uses deleted nodes for graph traversal to maintain connectivity

## 4. Insertion/Update

- Collects candidates up to M×factor using heuristic
- Stores first M as regular, remainder as extended
- Updates consider all neighbors (regular + extended) for optimal connections

## 5. Node Replacement

- When replace_deleted=true, promotes extended neighbors before replacing
- Maintains graph quality by filling gaps left by replaced nodes
- Prevents graph degradation from accumulating deletions

## 6. Memory Management

- Allocates space for extended neighbors at all levels
- Save/load functions handle variable-length neighbor lists
- Backward compatible with non-extended indices

## Usage Example:
```
cpp// Create index with extended neighbors
hnswlib::HierarchicalNSW<float>* index = new hnswlib::HierarchicalNSW<float>(
    &space, 
    max_elements, 
    M,                        // Regular M=16
    ef_construction, 
    random_seed,
    true,                     // allow_replace_deleted
    true,                     // use_extended_neighbors
    1.5                       // extended_list_factor (50% more neighbors)
);

// Extended neighbors are used automatically during search
// when deleted nodes are encountered
```
The implementation handles all edge cases and provides robust deletion support while maintaining search quality through the extended neighbor mechanism.

----

# Extended Neighbors Workflow

## Key Functions and Their Roles

### 1. **During Search** (`searchBaseLayerST`)
When traversing the graph and we encounter a deleted neighbor:
```
Current node's neighbors: [A, B(deleted), C, D, E | F, G]
                          └─── Regular ────┘   └Extended┘

During search:
- Try to visit B (deleted)
- Automatically skip to F (first extended neighbor)
- If F is also deleted, try G
- Continue search with non-deleted neighbor
```

### 2. **During Insertion** (`mutuallyConnectNewElement`)
When connecting a new node:
```
Candidates from heuristic: [A, B, C, D, E, F, G, H]
With M=5, extended_factor=1.5:
- Regular slots: 5
- Extended slots: 2-3
- Store: [A, B, C, D, E | F, G]
         └─── Regular ──┘ └Extended┘
```

### 3. **During Node Replacement** (`addPoint` with `replace_deleted=true`)
When completely replacing a deleted node:

```
BEFORE: Node X is deleted and will be replaced
Node Y's neighbors: [A, B, X(deleted), D, E | F, G]
                     └──── Regular ────┘   └Extended┘

AFTER: Node X is replaced with new data
1. Find X in Y's regular neighbors
2. Promote F to regular position
3. Result: [A, B, F, D, E | G, -]
           └─── Regular ──┘ └Ext┘
```

## Complete Flow Example

### Initial State
```
Node 10: [11, 12, 13, 14, 15 | 16, 17]  (regular | extended)
Node 11: [10, 12, 18, 19, 20 | 21, 22]
Node 12: [10, 11, 13, 23, 24 | 25, 26]
```

### Step 1: Delete Node 11
```
markDelete(11)
Node 10: [11(D), 12, 13, 14, 15 | 16, 17]  // 11 marked deleted
Node 12: [10, 11(D), 13, 23, 24 | 25, 26]  // 11 marked deleted
```

### Step 2: Search Starting from Node 10
```
When traversing from Node 10:
- Encounter 11(D) in regular neighbors
- Automatically use 16 from extended neighbors instead
- Continue search normally
```

### Step 3: Replace Node 11 with New Data
```
addPoint(new_data, new_label, replace_deleted=true)

Before replacement:
Node 10: [11(D), 12, 13, 14, 15 | 16, 17]
Node 12: [10, 11(D), 13, 23, 24 | 25, 26]

After replacement:
Node 10: [16, 12, 13, 14, 15 | 17, -]     // 16 promoted
Node 12: [10, 25, 13, 23, 24 | 26, -]     // 25 promoted
Node 11: [new neighbors based on new data]
```

## Benefits

1. **Graph Connectivity**: Deleted nodes don't break graph traversal
2. **Automatic Healing**: When nodes are replaced, the graph "heals" by promoting extended neighbors
3. **Performance**: No need to rebuild entire neighbor lists
4. **Memory Efficiency**: Only pay for extra storage when configured