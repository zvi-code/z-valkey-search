# HNSW Vector Index Delete Flow Documentation

## Table of Contents
1. [Overview](#overview)
2. [Class Hierarchy and Data Structures](#class-hierarchy-and-data-structures)
3. [Delete Flow - Step by Step](#delete-flow---step-by-step)
4. [Deletion Types and Semantics](#deletion-types-and-semantics)
5. [Memory Management and Cleanup](#memory-management-and-cleanup)
6. [Thread Safety and Concurrency](#thread-safety-and-concurrency)
7. [Error Handling and Recovery](#error-handling-and-recovery)
8. [Performance Characteristics](#performance-characteristics)
9. [HNSW-Specific Deletion Semantics](#hnsw-specific-deletion-semantics)

## Overview

The HNSW vector index delete flow in Valkey Search implements a "soft delete" strategy where vectors are marked as deleted in the HNSW graph rather than being physically removed. This design choice maintains graph connectivity and search performance while enabling efficient cleanup and potential recovery. This document provides a comprehensive analysis of the exact delete flow.

## Class Hierarchy and Data Structures

### Core Classes
```cpp
IndexBase (abstract)
  ↳ VectorBase (abstract) 
    ↳ VectorHNSW<T> (template, typically T=float)
      ↳ Contains: std::unique_ptr<hnswlib::HierarchicalNSW<T>> algo_
```

### Key Data Structures for Deletion
- **tracked_metadata_by_key_**: InternedStringMap<TrackedKeyMetadata> - Removed during delete
- **key_by_internal_id_**: Maps internal IDs back to keys - Cleaned up during delete  
- **tracked_vectors_**: std::deque<InternedStringPtr> - Vectors remain (reference counted)
- **algo_->deleted_elements**: std::unordered_set<tableint> - Tracks deleted internal IDs
- **HNSW Delete Marks**: Bit flags in link list headers indicating deletion status

## Delete Flow - Step by Step

### Phase 1: Entry Point and Key Validation
**Location**: `VectorBase::RemoveRecord()`
**File**: `/src/indexes/vector_base.cc:286`

```cpp
absl::StatusOr<bool> VectorBase::RemoveRecord(const InternedStringPtr &key, indexes::DeletionType deletion_type)
```

**Steps**:
1. **Key Validation**: Check if key is empty or not tracked
2. **UnTrack Key**: Call `UnTrackKey(key)` to remove from tracking structures
3. **Physical Deletion**: If key was tracked, call `RemoveRecordImpl(internal_id)`
4. **Return Status**: Return true if key was found and deleted, false otherwise

**Input Parameters**:
- **key**: InternedStringPtr to the key being deleted
- **deletion_type**: Enum indicating deletion context (kRecord, kNone, kIdentifier)

### Phase 2: Key Untracking and Metadata Cleanup
**Location**: `VectorBase::UnTrackKey()`
**File**: `/src/indexes/vector_base.cc:297`

```cpp
absl::StatusOr<std::optional<uint64_t>> UnTrackKey(const InternedStringPtr &key)
```

**Steps**:
1. **Mutex Acquisition**: Acquire `key_to_metadata_mutex_` (WriterMutex) for exclusive access
2. **Key Lookup**: Find key in `tracked_metadata_by_key_`
3. **Internal ID Extraction**: Get `internal_id` from metadata  
4. **Vector Untracking**: Call `UnTrackVector(internal_id)` - HNSW-specific cleanup
5. **Metadata Removal**: Remove from `tracked_metadata_by_key_`
6. **Reverse Mapping Cleanup**: Remove from `key_by_internal_id_`
7. **Validation**: Ensure consistency between forward and reverse mappings

**Data Structure Updates**:
```cpp
// Before deletion
tracked_metadata_by_key_[key] = {internal_id: 123, magnitude: 1.0}
key_by_internal_id_[123] = key

// After deletion  
tracked_metadata_by_key_.erase(key)
key_by_internal_id_.erase(123)
// internal_id 123 is returned for HNSW deletion
```

### Phase 3: HNSW-Specific Vector Untracking
**Location**: `VectorHNSW<T>::UnTrackVector()`
**File**: `/src/indexes/vector_hnsw.cc:137`

```cpp
void VectorHNSW<T>::UnTrackVector(uint64_t internal_id) {}
```

**Important Note**: This function is intentionally empty in HNSW implementation!

**Rationale**:
- **Graph Preservation**: HNSW graphs maintain connectivity by keeping deleted vectors
- **Reference Counting**: Vector memory is managed by string interning system
- **Soft Delete Strategy**: Vectors are marked as deleted but remain in graph structure
- **Performance**: Avoids expensive graph reconstruction when removing nodes

### Phase 4: HNSW Algorithm Mark Delete
**Location**: `VectorHNSW<T>::RemoveRecordImpl()`
**File**: `/src/indexes/vector_hnsw.cc:297`

```cpp
absl::Status VectorHNSW<T>::RemoveRecordImpl(uint64_t internal_id)
```

**Steps**:
1. **Mutex Acquisition**: Acquire `resize_mutex_` (ReaderMutex) to prevent concurrent resize
2. **Mark Delete**: Call `algo_->markDelete(internal_id)`
3. **Exception Handling**: Catch and wrap any HNSW library exceptions
4. **Metrics Update**: Increment `hnsw_remove_exceptions_cnt` on failure

**HNSW Internal Mark Delete Process**:
```cpp
// In hnswlib::HierarchicalNSW<T>::markDelete()
1. Label Lock: Acquire per-label mutex for thread safety
2. Bit Flag Set: Set DELETE_MARK bit in link list header  
3. Counter Update: Increment num_deleted_ counter
4. Memory Tracking: Update reclaimable_memory metrics
5. Deleted Set: Add to deleted_elements set (if replace_deleted enabled)
```

### Phase 5: HNSW Delete Mark Implementation
**Location**: `hnswlib::HierarchicalNSW<T>::markDeletedInternal()`
**File**: `/third_party/hnswlib/hnswalg.h` (lines in attached file)

**Detailed Steps**:
1. **Bounds Check**: Verify `internal_id < cur_element_count_`
2. **Duplicate Check**: Ensure vector is not already marked as deleted
3. **Bit Manipulation**: Set DELETE_MARK bit in link list size field
   ```cpp
   unsigned char *ll_cur = ((unsigned char *)get_linklist0(internal_id)) + 2;
   *ll_cur |= DELETE_MARK;
   ```
4. **Counter Updates**: Increment `num_deleted_` and update metrics
5. **Replace Delete Logic**: If enabled, add to `deleted_elements` set for reuse

**Memory Layout of Delete Mark**:
```cpp
// Link list header structure (per vector)
[linklistsizeint][actual_links...][other_data...]
//      ^
//      |-- Last 16 bits used for deletion marking
//      |-- First 16 bits store actual link count
```

## Deletion Types and Semantics

### DeletionType Enumeration
```cpp
enum class DeletionType {
  kNone,         // Soft delete - field removed but key exists
  kRecord,       // Hard delete - entire key deleted
  kIdentifier    // Identifier-specific deletion
};
```

### Deletion Semantics by Type

#### kRecord (Hard Delete)
- **Trigger**: Entire hash key is deleted from Valkey
- **Behavior**: Complete removal from all index structures
- **Impact**: Vector becomes completely inaccessible
- **Use Case**: User explicitly deletes a document

#### kNone (Soft Delete)  
- **Trigger**: Vector field is removed but hash key remains
- **Behavior**: Marked as deleted but structure preserved
- **Impact**: Vector excluded from search results but graph intact
- **Use Case**: Field update removes vector attribute

#### kIdentifier (Field Delete)
- **Trigger**: Specific field identifier is removed
- **Behavior**: Similar to soft delete with field-specific tracking
- **Impact**: Field-aware deletion for multi-field documents
- **Use Case**: Schema changes or field-specific updates

## Memory Management and Cleanup

### Reference Counting and String Interning
```cpp
// Vector memory managed by reference counting
InternedStringPtr vector = ...;  // Reference count managed automatically
// When last reference is released, vector memory is freed
```

**Key Points**:
- **Automatic Cleanup**: Vector memory freed when reference count reaches zero
- **Shared Ownership**: Multiple indexes can reference same vector data
- **Thread Safety**: Reference counting is atomic and thread-safe

### HNSW Memory Retention Strategy
```cpp
// Deleted vectors remain in HNSW data structures
data_level0_memory_[deleted_id] = [links][deleted_data_ptr][label]
//                                        ^-- Still present
//                                        |-- DELETE_MARK set in header
```

**Benefits**:
- **Graph Connectivity**: Maintains navigable small world properties
- **Search Performance**: No degradation in search quality
- **Memory Overhead**: Only small bit flag overhead per deleted vector

### Reclaimable Memory Tracking
```cpp
// Memory accounting for deleted vectors
valkey_search::Metrics::GetStats().reclaimable_memory += vector_size_;
```

**Purpose**:
- **Memory Monitoring**: Track memory that could be reclaimed
- **Capacity Planning**: Understand actual vs. used memory
- **Optimization Opportunities**: Identify when compaction would be beneficial

## Thread Safety and Concurrency

### Mutex Acquisition Order
```cpp
// Delete operation locking sequence
1. key_to_metadata_mutex_ (WriterMutex) - for metadata cleanup
2. resize_mutex_ (ReaderMutex) - for HNSW operations  
3. algo_->getLabelOpMutex(internal_id) - for per-label thread safety
```

### Concurrent Delete Safety
- **Multiple Deletes**: Safe due to per-label locking in HNSW
- **Delete vs Insert**: Serialized by resize_mutex_ 
- **Delete vs Search**: Concurrent operation allowed
- **Delete vs Resize**: Deletes blocked during resize operations

### Race Condition Prevention
```cpp
// Double-checked deletion in HNSW
if (!isMarkedDeleted(internal_id)) {
  // Set delete mark only if not already deleted
  *ll_cur |= DELETE_MARK;
} else {
  throw std::runtime_error("Element already deleted");
}
```

## Error Handling and Recovery

### Exception Categories
1. **Key Not Found**: Returns false (no error)
2. **Already Deleted**: HNSW throws runtime_error  
3. **Internal HNSW Error**: Wrapped in `absl::InternalError`
4. **Consistency Errors**: Internal validation failures

### Recovery Mechanisms
```cpp
// No rollback needed for delete operations
// Delete is idempotent - safe to retry
// Metrics tracking for monitoring failures
++Metrics::GetStats().hnsw_remove_exceptions_cnt;
```

### Consistency Validation
```cpp
// Validate forward/reverse mapping consistency
auto key_by_internal_id_it = key_by_internal_id_.find(id);
if (key_by_internal_id_it == key_by_internal_id_.end()) {
  return absl::InvalidArgumentError("Inconsistent mapping state");
}
```

## Performance Characteristics

### Time Complexity
- **Key Lookup**: O(1) average case (hash map lookup)
- **HNSW Mark Delete**: O(1) (just bit flag setting)
- **Total Delete Time**: O(1) average case
- **No Graph Reconstruction**: Major performance advantage

### Space Complexity
- **Metadata Removal**: Reduces tracking overhead by O(1) per key
- **Vector Retention**: No immediate space savings
- **Delete Marking**: Minimal overhead (1 bit per vector)
- **Long-term Impact**: Gradual memory fragmentation

### Performance Optimizations
1. **Lazy Cleanup**: No immediate graph reconstruction
2. **Batch Deletes**: Can be processed independently
3. **Memory Reuse**: Deleted slots can be reused for new inserts (if enabled)

## HNSW-Specific Deletion Semantics

### Why Soft Delete in HNSW?

#### Graph Connectivity Preservation
```cpp
// Before delete: A ←→ B ←→ C ←→ D
// After delete B: A ←→ [B] ←→ C ←→ D  
//                      ^-- Marked deleted but links preserved
```

**Benefits**:
- **Navigation**: Search can still traverse through deleted nodes
- **Accuracy**: Graph topology remains optimal
- **Performance**: No expensive link reconstruction needed

#### Alternative: Hard Delete Problems
```cpp
// Hard delete would require: A ←→ C ←→ D
// Issues:
// 1. Expensive link updates for all neighbors
// 2. Potential graph fragmentation  
// 3. Search accuracy degradation
// 4. Complex concurrency management
```

### Delete Mark Bit Encoding
```cpp
// HNSW uses clever bit encoding in link list size
typedef unsigned int linklistsizeint;  // 32-bit field

// Layout:
// [31:16] - Reserved/Delete marks
// [15:0]  - Actual link count
static const unsigned char DELETE_MARK = 0x01;

// Access pattern:
unsigned char *ll_cur = ((unsigned char *)get_linklist0(internal_id)) + 2;
*ll_cur |= DELETE_MARK;  // Set delete bit
bool deleted = *ll_cur & DELETE_MARK;  // Check delete bit
```

### Search Behavior with Deleted Vectors
```cpp
// During HNSW search traversal
if (!isMarkedDeleted(candidate_id)) {
  // Include in search results
  top_candidates.emplace(dist, candidate_id);
} else {
  // Skip deleted vectors but continue traversal
  // This maintains graph connectivity while filtering results
}
```

### Memory Reclaim Strategies (Future Optimizations)
1. **Compaction**: Periodically rebuild graph excluding deleted vectors
2. **Replacement**: Reuse deleted slots for new inserts (partially implemented)
3. **Level Optimization**: Remove deleted vectors from higher levels
4. **Threshold-Based**: Trigger cleanup when deletion ratio exceeds threshold

---

This comprehensive documentation covers the complete HNSW vector index delete flow, emphasizing the unique soft-delete strategy that preserves graph connectivity while efficiently marking vectors as deleted. The approach prioritizes search performance and graph quality over immediate memory reclamation, making it well-suited for dynamic workloads with mixed insert/delete/search patterns.
