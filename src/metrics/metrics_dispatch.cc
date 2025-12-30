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

#include "metrics.h"

#include <atomic>

#include "cpu_caps.h"
#include "metrics_internal.h"

namespace valkey_search {
namespace metrics {

namespace {

// Global pointer to the active implementation vtable.
// Selected once during Init() based on detected CPU capabilities.
const internal::MetricsImpl* g_impl = nullptr;

// Initialization state machine:
// 0 = uninitialized
// 1 = initialization in progress
// 2 = initialized
std::atomic<int> g_init_state{0};

// Select the optimal implementation based on detected CPU capabilities.
// Returns a pointer to the best available implementation vtable.
const internal::MetricsImpl* SelectImplementation(uint32_t caps) {
#if defined(__x86_64__) || defined(_M_X64)
  // x86_64 implementation selection (Requirements 3.1, 3.2, 3.3)
  // Priority 1: AVX512F + AVX512DQ -> AVX512 implementation
  if ((caps & kCpuCapAvx512F) && (caps & kCpuCapAvx512Dq)) {
    return &internal::kMetricsImplAvx512;
  }
  // Priority 2: AVX2 + FMA -> AVX2 implementation
  if ((caps & kCpuCapAvx2) && (caps & kCpuCapFma)) {
    return &internal::kMetricsImplAvx2;
  }
  // Fallback: scalar implementation
  return &internal::kMetricsImplScalar;

#elif defined(__aarch64__) || defined(_M_ARM64)
  // ARM64 implementation selection (Requirements 3.4-3.9)
  // Priority 1: SVE2 + SVE_I8MM -> SVE2 implementation
  if ((caps & kCpuCapSve2) && (caps & kCpuCapSveI8mm)) {
    return &internal::kMetricsImplSve2;
  }
  // Priority 2: SVE + NEON_I8MM -> SVE implementation
  if ((caps & kCpuCapSve) && (caps & kCpuCapNeonI8mm)) {
    return &internal::kMetricsImplSve;
  }
  // Priority 3: NEON_I8MM -> NEON I8MM implementation
  if (caps & kCpuCapNeonI8mm) {
    return &internal::kMetricsImplNeonI8mm;
  }
  // Priority 4: NEON_DOTPROD -> NEON Dotprod implementation
  if (caps & kCpuCapNeonDotprod) {
    return &internal::kMetricsImplNeonDotprod;
  }
  // Priority 5: NEON (baseline ARM64) -> NEON implementation
  if (caps & kCpuCapNeon) {
    return &internal::kMetricsImplNeon;
  }
  // Fallback: scalar implementation
  return &internal::kMetricsImplScalar;

#else
  // Unsupported platform: use scalar implementation (Requirement 3.9)
  (void)caps;  // Suppress unused parameter warning
  return &internal::kMetricsImplScalar;
#endif
}

}  // namespace

// Initialize the metrics system.
// Thread-safe using atomic CAS for initialization state.
// Returns 0 on success.
// Requirements: 1.1, 1.2, 1.3, 1.4
int Init() {
  // Fast path: already initialized (Requirement 1.2)
  if (g_init_state.load(std::memory_order_acquire) == 2) {
    return 0;
  }

  // Try to claim initialization (Requirement 1.3 - thread safety)
  int expected = 0;
  if (!g_init_state.compare_exchange_strong(expected, 1,
                                            std::memory_order_acq_rel)) {
    // Another thread is initializing or already initialized
    // Spin until initialization is complete
    while (g_init_state.load(std::memory_order_acquire) == 1) {
      // Busy wait - initialization is fast so this is acceptable
    }
    // Check if initialization succeeded
    return (g_init_state.load(std::memory_order_acquire) == 2) ? 0 : -1;
  }

  // We claimed initialization - detect capabilities and select implementation
  // Requirement 1.1: detect CPU capabilities and select optimal vtable
  uint32_t caps = CpuCapsDetect();
  g_impl = SelectImplementation(caps);

  // Mark initialization complete
  g_init_state.store(2, std::memory_order_release);
  return 0;
}

// Clean up the metrics system.
// Resets state to allow re-initialization.
void Fini() {
  g_impl = nullptr;
  g_init_state.store(0, std::memory_order_release);
}

// Return the name of the active implementation.
// Requirement 6.1
const char* ImplName() { return g_impl ? g_impl->name : "uninitialized"; }

// Return the capability bitmask of the active implementation.
// Requirement 6.2
uint32_t ImplCaps() { return g_impl ? g_impl->caps : 0; }

// Dispatch functions - single indirect call through g_impl pointer.
// Requirement 4.8: exactly one pointer dereference plus one indirect call

// L2 Squared Distance (Requirements 4.1, 4.4, 4.6)
float L2SqF32(const float* a, const float* b, size_t dim) {
  return g_impl->l2sq_f32(a, b, dim);
}

float L2SqF16(const uint16_t* a, const uint16_t* b, size_t dim) {
  return g_impl->l2sq_f16(a, b, dim);
}

float L2SqI8(const int8_t* a, const int8_t* b, size_t dim) {
  return g_impl->l2sq_i8(a, b, dim);
}

// Inner Product (Requirements 4.2, 4.5, 4.7)
float IpF32(const float* a, const float* b, size_t dim) {
  return g_impl->ip_f32(a, b, dim);
}

float IpF16(const uint16_t* a, const uint16_t* b, size_t dim) {
  return g_impl->ip_f16(a, b, dim);
}

float IpI8(const int8_t* a, const int8_t* b, size_t dim) {
  return g_impl->ip_i8(a, b, dim);
}

// Cosine Similarity (Requirement 4.3)
float CosineF32(const float* a, const float* b, size_t dim) {
  return g_impl->cosine_f32(a, b, dim);
}

float CosineF16(const uint16_t* a, const uint16_t* b, size_t dim) {
  return g_impl->cosine_f16(a, b, dim);
}

float CosineI8(const int8_t* a, const int8_t* b, size_t dim) {
  return g_impl->cosine_i8(a, b, dim);
}

// Batch Operations (Requirements 5.1, 5.2)
void L2SqF32Batch(const float* query, const float* const* vectors, size_t dim,
                  size_t count, float* results) {
  g_impl->l2sq_f32_batch(query, vectors, dim, count, results);
}

void IpF32Batch(const float* query, const float* const* vectors, size_t dim,
                size_t count, float* results) {
  g_impl->ip_f32_batch(query, vectors, dim, count, results);
}

}  // namespace metrics
}  // namespace valkey_search
