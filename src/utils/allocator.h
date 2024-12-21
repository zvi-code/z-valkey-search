/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef VALKEYSEARCH_SRC_UTILS_ALLOCATOR_H_
#define VALKEYSEARCH_SRC_UTILS_ALLOCATOR_H_

#include <stdbool.h>
#include <stddef.h>

#include <memory>
#include <stack>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "src/utils/intrusive_list.h"
#include "src/utils/intrusive_ref_count.h"

namespace valkey_search {
constexpr size_t kFreeEntriesPerChunkGroupSize = 7;
constexpr size_t kChunkBufferPages = 10;
constexpr size_t kChunkBufferMinEntryiesPerChunk = 8;

/*
FixedSizeAllocator is responsible for allocating and managing contiguous
memory chunks for a specific buffer size. Grouping buffers of identical sizes in
the same allocator minimizes maintenance overhead and improves CPU cache locality,
benefiting applications like vector search that frequently access same-sized
buffers.

The `FixedSizeAllocator` prioritizes allocation from heavily utilized chunks.
This approach enhances CPU cache locality and the formation of unutilized chunks
which are deallocated.
*/

class AllocatorChunk;

class Allocator {
 public:
  virtual char *Allocate(size_t size) = 0;
  static bool Free(char *ptr);
  virtual ~Allocator() = default;
  virtual size_t ChunkSize() const = 0;

 protected:
  virtual void Free(AllocatorChunk *chunk, char *ptr) = 0;
};

class FixedSizeAllocator;

struct AllocatorChunk {
  AllocatorChunk(Allocator *allocator, size_t size);
  ~AllocatorChunk();
  size_t entries_in_chunk;
  std::unique_ptr<char[]> data;
  std::stack<char *> free_list;
  Allocator *allocator;
  // Intrusive linked list.
  AllocatorChunk *next{nullptr};
  AllocatorChunk *prev{nullptr};
};

class FixedSizeAllocator : public IntrusiveRefCount, public Allocator {
 public:
  friend class IntrusiveRefCount;
  FixedSizeAllocator(size_t size, bool require_ptr_alignment);
  char *Allocate(size_t size) ABSL_LOCKS_EXCLUDED(mutex_) override;
  char *Allocate() ABSL_LOCKS_EXCLUDED(mutex_);
  size_t ActiveAllocations() const ABSL_LOCKS_EXCLUDED(mutex_) {
    absl::MutexLock lock(&mutex_);
    return active_allocations_;
  }
  size_t ChunkCount() const ABSL_LOCKS_EXCLUDED(mutex_);
  ~FixedSizeAllocator();
  size_t ChunkSize() const { return size_; }

 protected:
  void Free(AllocatorChunk *chunk, char *ptr) override;

 private:
  IntrusiveList<AllocatorChunk> chunks_grouped_by_free_entries_
      [kFreeEntriesPerChunkGroupSize] ABSL_GUARDED_BY(mutex_);
  size_t size_;
  IntrusiveList<AllocatorChunk> fully_used_chunks_ ABSL_GUARDED_BY(mutex_);
  AllocatorChunk *current_chunk_ ABSL_GUARDED_BY(mutex_) = nullptr;
  size_t active_allocations_ ABSL_GUARDED_BY(mutex_){0};
  mutable absl::Mutex mutex_;
  void HandleChunkEntryUsageChange(AllocatorChunk *chunk, int old_free_group)
      ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void SelectCurrentChunk() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void AllocateChunk() ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void _Free(char *ptr) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  bool require_ptr_alignment_;
};

DEFINE_UNIQUE_PTR_TYPE(Allocator);
DEFINE_UNIQUE_PTR_TYPE(FixedSizeAllocator);

size_t BufferSize(size_t size);
size_t EntriesFitInChunk(size_t size, size_t num_pages);

}  // namespace valkey_search

#endif  // VALKEYSEARCH_SRC_UTILS_ALLOCATOR_H_
