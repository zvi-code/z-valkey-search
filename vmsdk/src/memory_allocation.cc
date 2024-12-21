// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "vmsdk/src/memory_allocation.h"

#include <stdlib.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>

namespace vmsdk {

// Use the standard system allocator by default. Note that this is required
// since any allocation done before Redis module initialization (namely global
// static constructors that do heap allocation, which are run on dl_open) cannot
// invoke Redis modules api since the associated C function pointers are only
// initialized as part of the module initialization process. Refer
// https://redis.com/blog/using-the-redis-allocator-in-rust for more details.
//
// We use a combination of a thread local static variable and a global atomic
// variable to perform the switch to the new allocator. The global is only
// accessed during the initial loading phase, and once we switch allocators the
// thread local variable is exclusively used. This should guarantee that the
// switch is done atomically while not having performance impact during steady
// state.
thread_local static bool thread_using_valkey_module_alloc = false;
static std::atomic<bool> use_valkey_module_alloc_switch = false;

bool IsUsingValkeyAlloc() {
  if (!thread_using_valkey_module_alloc &&
      use_valkey_module_alloc_switch.load(std::memory_order_relaxed)) {
    thread_using_valkey_module_alloc = true;
    return true;
  }
  return thread_using_valkey_module_alloc;
}
void UseValkeyAlloc() {
  use_valkey_module_alloc_switch.store(true, std::memory_order_relaxed);
}
std::atomic<uint64_t> used_memory_bytes{0};

void ResetValkeyAlloc() {
  use_valkey_module_alloc_switch.store(false, std::memory_order_relaxed);
  thread_using_valkey_module_alloc = false;
  used_memory_bytes.store(0, std::memory_order_relaxed);
}

uint64_t GetUsedMemoryCnt() { return used_memory_bytes; }

void ReportAllocMemorySize(uint64_t size) {
  vmsdk::used_memory_bytes.fetch_add(size, std::memory_order_relaxed);
}
void ReportFreeMemorySize(uint64_t size) {
  if (size > used_memory_bytes) {
    vmsdk::used_memory_bytes.store(0, std::memory_order_relaxed);
  } else {
    vmsdk::used_memory_bytes.fetch_sub(size, std::memory_order_relaxed);
  }
}

}  // namespace vmsdk

