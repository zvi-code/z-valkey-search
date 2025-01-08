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

#ifndef VMSDK_SRC_MEMORY_ALLOCATION_OVERRIDES_H_
#define VMSDK_SRC_MEMORY_ALLOCATION_OVERRIDES_H_

#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <new>

extern "C" {
void* (*__real_malloc)(size_t) = malloc;
void (*__real_free)(void*) = free;
void* (*__real_calloc)(size_t, size_t) = calloc;
void* (*__real_realloc)(void*, size_t) = realloc;
void* (*__real_aligned_alloc)(size_t, size_t) = aligned_alloc;
int (*__real_posix_memalign)(void**, size_t, size_t) = posix_memalign;
void* (*__real_valloc)(size_t) = valloc;

// Our allocator doesn't support tracking system memory size, so we just
// return 0.
__attribute__((weak)) size_t empty_usable_size(void* ptr) noexcept;
}  // extern "C"

extern "C" {
// See https://www.gnu.org/software/libc/manual/html_node/Replacing-malloc.html
void* __wrap_malloc(size_t size) noexcept;
void __wrap_free(void* ptr) noexcept;
void* __wrap_calloc(size_t __nmemb, size_t size) noexcept;
void* __wrap_realloc(void* ptr, size_t size) noexcept;
void* __wrap_aligned_alloc(size_t __alignment, size_t __size) noexcept;
int __wrap_malloc_usable_size(void* ptr) noexcept;
int __wrap_posix_memalign(void** r, size_t __alignment, size_t __size) noexcept;
void* __wrap_valloc(size_t size) noexcept;
}  // extern "C"

#define malloc(...) __wrap_malloc(__VA_ARGS__)
#define calloc(...) __wrap_calloc(__VA_ARGS__)
#define realloc(...) __wrap_realloc(__VA_ARGS__)
#define free(...) __wrap_free(__VA_ARGS__)
#define aligned_alloc(...) __wrap_aligned_alloc(__VA_ARGS__)
#define posix_memalign(...) __wrap_posix_memalign(__VA_ARGS__)
#define valloc(...) __wrap_valloc(__VA_ARGS__)

void* operator new(size_t __sz) noexcept(false);
void operator delete(void* p) noexcept;
void operator delete(void* p, size_t __sz) noexcept;
void* operator new[](size_t __sz) noexcept(false);
void operator delete[](void* p) noexcept;
void operator delete[](void* p, size_t __sz) noexcept;
void* operator new(size_t __sz, const std::nothrow_t& nt) noexcept;
void* operator new[](size_t __sz, const std::nothrow_t& nt) noexcept;
void operator delete(void* p, const std::nothrow_t& nt) noexcept;
void operator delete[](void* p, const std::nothrow_t& nt) noexcept;
void* operator new(size_t __sz, std::align_val_t alignment) noexcept(false);
void* operator new(size_t __sz, std::align_val_t alignment,
                   const std::nothrow_t&) noexcept;
void operator delete(void* p, std::align_val_t alignment) noexcept;
void operator delete(void* p, std::align_val_t alignment,
                     const std::nothrow_t&) noexcept;
void operator delete(void* p, size_t __sz, std::align_val_t alignment) noexcept;
void* operator new[](size_t __sz, std::align_val_t alignment) noexcept(false);
void* operator new[](size_t __sz, std::align_val_t alignment,
                     const std::nothrow_t&) noexcept;
void operator delete[](void* p, std::align_val_t alignment) noexcept;
void operator delete[](void* p, std::align_val_t alignment,
                       const std::nothrow_t&) noexcept;
void operator delete[](void* p, size_t __sz,
                       std::align_val_t alignment) noexcept;

inline void SetRealAllocators(void* (*malloc_fn)(size_t),
                              void (*free_fn)(void*),
                              void* (*calloc_fn)(size_t, size_t),
                              void* (*realloc_fn)(void*, size_t),
                              void* (*aligned_alloc_fn)(size_t, size_t),
                              int (*posix_memalign_fn)(void**, size_t, size_t),
                              void* (*valloc_fn)(size_t)) {
  __real_malloc = malloc_fn;
  __real_free = free_fn;
  __real_calloc = calloc_fn;
  __real_realloc = realloc_fn;
  __real_aligned_alloc = aligned_alloc_fn;
  __real_posix_memalign = posix_memalign_fn;
  __real_valloc = valloc_fn;
}

#endif  // VMSDK_SRC_MEMORY_ALLOCATION_OVERRIDES_H_
