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
namespace internal {

// Function pointer typedefs for metric signatures
using MetricsFnF32 = float (*)(const float*, const float*, size_t);
using MetricsFnF16 = float (*)(const uint16_t*, const uint16_t*, size_t);
using MetricsFnI8 = float (*)(const int8_t*, const int8_t*, size_t);

using MetricsBatchFnF32 =
    void (*)(const float*, const float* const*, size_t, size_t, float*);

// Vtable structure containing all metric function pointers and metadata
struct MetricsImpl {
  // L2 squared distance
  MetricsFnF32 l2sq_f32;
  MetricsFnF16 l2sq_f16;
  MetricsFnI8 l2sq_i8;

  // Inner product
  MetricsFnF32 ip_f32;
  MetricsFnF16 ip_f16;
  MetricsFnI8 ip_i8;

  // Cosine similarity
  MetricsFnF32 cosine_f32;
  MetricsFnF16 cosine_f16;
  MetricsFnI8 cosine_i8;

  // Batch operations
  MetricsBatchFnF32 l2sq_f32_batch;
  MetricsBatchFnF32 ip_f32_batch;

  // Metadata
  const char* name;
  uint32_t caps;
};

// Scalar implementation (always available)
extern const MetricsImpl kMetricsImplScalar;

// x86_64 implementations
#if defined(__x86_64__) || defined(_M_X64)
extern const MetricsImpl kMetricsImplAvx2;
extern const MetricsImpl kMetricsImplAvx512;
#endif

// ARM64 implementations
#if defined(__aarch64__) || defined(_M_ARM64)
extern const MetricsImpl kMetricsImplNeon;
extern const MetricsImpl kMetricsImplNeonDotprod;
extern const MetricsImpl kMetricsImplNeonI8mm;
extern const MetricsImpl kMetricsImplSve;
extern const MetricsImpl kMetricsImplSve2;
#endif

}  // namespace internal
}  // namespace metrics
}  // namespace valkey_search
