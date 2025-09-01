/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef VMSDK_SRC_MEMORY_TRACKER_H_
#define VMSDK_SRC_MEMORY_TRACKER_H_

#include <atomic>

#include "vmsdk/src/utils.h"

class MemoryPool {
 public:
  explicit MemoryPool(int64_t initial_value = 0) : counter_(initial_value) {}

  int64_t GetUsage() const { return counter_.load(); }

  void Add(int64_t delta) { counter_.fetch_add(delta); }

  void Reset() { counter_.store(0); }

 private:
  std::atomic<int64_t> counter_;
};

class MemoryScope {
 public:
  explicit MemoryScope(MemoryPool& pool);
  virtual ~MemoryScope() = default;

  VMSDK_NON_COPYABLE_NON_MOVABLE(MemoryScope);

  int64_t GetBaselineMemory() const { return baseline_memory_; }

 protected:
  MemoryPool& target_pool_;
  int64_t baseline_memory_ = 0;
};

/**
 * A memory scope that isolates memory changes within its lifetime.
 *
 * When this scope is destroyed, it calculates the net memory change during its
 * lifetime and adds it to the target pool, then resets the global memory delta
 * back to the baseline. This effectively "isolates" the memory tracking within
 * this scope, preventing memory changes from propagating to outer scopes.
 *
 * Use this when you want to track memory usage for a specific operation
 * without affecting the memory tracking of surrounding code.
 */
class IsolatedMemoryScope final : public MemoryScope {
 public:
  explicit IsolatedMemoryScope(MemoryPool& pool);
  ~IsolatedMemoryScope() override;

  VMSDK_NON_COPYABLE_NON_MOVABLE(IsolatedMemoryScope);
};

/**
 * A memory scope that allows memory changes to propagate to outer scopes.
 *
 * When this scope is destroyed, it calculates the net memory change during its
 * lifetime and adds it to the target pool, but does NOT reset the global memory
 * delta. This allows memory changes to "nest" and propagate to any outer scopes
 * that might be tracking memory usage.
 *
 * Use this when you want to track memory usage for a specific operation while
 * still allowing those changes to be visible to surrounding tracking code.
 */
class NestedMemoryScope final : public MemoryScope {
 public:
  explicit NestedMemoryScope(MemoryPool& pool);
  ~NestedMemoryScope() override;

  VMSDK_NON_COPYABLE_NON_MOVABLE(NestedMemoryScope);
};

#endif  // VMSDK_SRC_MEMORY_TRACKER_H_
