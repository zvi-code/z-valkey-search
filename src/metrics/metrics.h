/*
 * Copyright (c) 2025, ValkeySearch contributors
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
 *   * Neither the name of the copyright holder nor the names of its
 *     contributors may be used to endorse or promote products derived from
 *     this software without specific prior written permission.
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

#pragma once

#include <cstddef>
#include <cstdint>

namespace valkey_search {
namespace metrics {

// Lifecycle management
// Returns 0 on success, non-zero on failure
int Init();
void Fini();

// Debug and introspection
// Returns the name of the active implementation (e.g., "sve2", "avx512", "scalar")
const char* ImplName();
// Returns the capability bitmask of the active implementation
uint32_t ImplCaps();

// L2 squared distance: sum of squared differences
float L2SqF32(const float* a, const float* b, size_t dim);
float L2SqF16(const uint16_t* a, const uint16_t* b, size_t dim);
float L2SqI8(const int8_t* a, const int8_t* b, size_t dim);

// Inner product (dot product)
float IpF32(const float* a, const float* b, size_t dim);
float IpF16(const uint16_t* a, const uint16_t* b, size_t dim);
float IpI8(const int8_t* a, const int8_t* b, size_t dim);

// Cosine similarity: dot(a,b) / (||a|| * ||b||)
float CosineF32(const float* a, const float* b, size_t dim);
float CosineF16(const uint16_t* a, const uint16_t* b, size_t dim);
float CosineI8(const int8_t* a, const int8_t* b, size_t dim);

// Batch operations for improved throughput with prefetching
void L2SqF32Batch(const float* query, const float* const* vectors, size_t dim,
                  size_t count, float* results);

void IpF32Batch(const float* query, const float* const* vectors, size_t dim,
                size_t count, float* results);

}  // namespace metrics
}  // namespace valkey_search
