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

#ifndef VMSDK_SRC_MEMORY_ALLOCATION_H_
#define VMSDK_SRC_MEMORY_ALLOCATION_H_

#include <cstdint>

namespace vmsdk {

// Updates the custom allocator to perform any future allocations using the
// Valkey allocator.
void UseValkeyAlloc();
bool IsUsingValkeyAlloc();

// Switch back to the default allocator. No guarantees around atomicity. Only
// safe in single-threaded or testing environments.
void ResetValkeyAlloc();

// Report used memory counter.
uint64_t GetUsedMemoryCnt();

void ReportAllocMemorySize(uint64_t size);
void ReportFreeMemorySize(uint64_t size);

}  // namespace vmsdk

#endif  // VMSDK_SRC_MEMORY_ALLOCATION_H_
