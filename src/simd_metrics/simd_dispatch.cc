/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/simd_metrics/simd_metrics.h"

#include <atomic>

#include "src/simd_metrics/cpu_caps.h"
#include "src/simd_metrics/simd_internal.h"

namespace {

// Global pointer to selected implementation vtable.
// Default to scalar - safe to use before metrics_init() is called.
std::atomic<const metrics_impl_t *> g_impl{&metrics_impl_scalar};

// Initialization state: 0=not started, 1=in progress, 2=complete
std::atomic<int> g_init_state{0};

const metrics_impl_t *select_best_impl(uint32_t caps) {
#if defined(__aarch64__)
  // Priority: SVE > NEON+dotprod > NEON baseline
  if (caps & CPU_CAP_SVE) {
    return &metrics_impl_sve;
  }
  if (caps & CPU_CAP_NEON_DOTPROD) {
    return &metrics_impl_neon_dotprod;
  }
  if (caps & CPU_CAP_NEON) {
    return &metrics_impl_neon;
  }
#elif defined(__x86_64__)
  // Priority: AVX512 > AVX2 > scalar
  if ((caps & CPU_CAP_AVX512F) && (caps & CPU_CAP_AVX512BW)) {
    return &metrics_impl_avx512;
  }
  if ((caps & CPU_CAP_AVX2) && (caps & CPU_CAP_FMA)) {
    return &metrics_impl_avx2;
  }
#endif

  // Fallback to scalar
  return &metrics_impl_scalar;
}

}  // namespace

extern "C" {

int metrics_init(void) {
  int expected = 0;

  // Fast path: already initialized
  if (g_init_state.load(std::memory_order_acquire) == 2) {
    return 0;
  }

  // Try to become the initializer
  if (!g_init_state.compare_exchange_strong(expected, 1,
                                            std::memory_order_acq_rel)) {
    // Another thread is initializing, spin until complete
    while (g_init_state.load(std::memory_order_acquire) != 2) {
      // Busy wait (should be very brief)
    }
    return 0;
  }

  // We are the initializer
  uint32_t caps = cpu_caps_detect();
  const metrics_impl_t *impl = select_best_impl(caps);
  g_impl.store(impl, std::memory_order_release);
  g_init_state.store(2, std::memory_order_release);

  return 0;
}

void metrics_fini(void) {
  g_impl.store(nullptr, std::memory_order_release);
  g_init_state.store(0, std::memory_order_release);
}

const char *metrics_impl_name(void) {
  const metrics_impl_t *impl = g_impl.load(std::memory_order_acquire);
  return impl ? impl->name : "uninitialized";
}

uint32_t metrics_impl_caps(void) {
  const metrics_impl_t *impl = g_impl.load(std::memory_order_acquire);
  return impl ? impl->caps : 0;
}

// ============================================
// Dispatch functions - single indirect call
// ============================================

__attribute__((hot)) float metrics_l2sq_f32(const float *a, const float *b,
                                            size_t dim) {
  return g_impl.load(std::memory_order_relaxed)->l2sq_f32(a, b, dim);
}

__attribute__((hot)) float metrics_l2sq_f16(const uint16_t *a,
                                            const uint16_t *b, size_t dim) {
  return g_impl.load(std::memory_order_relaxed)->l2sq_f16(a, b, dim);
}

__attribute__((hot)) float metrics_l2sq_i8(const int8_t *a, const int8_t *b,
                                           size_t dim) {
  return g_impl.load(std::memory_order_relaxed)->l2sq_i8(a, b, dim);
}

__attribute__((hot)) float metrics_ip_f32(const float *a, const float *b,
                                          size_t dim) {
  return g_impl.load(std::memory_order_relaxed)->ip_f32(a, b, dim);
}

__attribute__((hot)) float metrics_ip_f16(const uint16_t *a, const uint16_t *b,
                                          size_t dim) {
  return g_impl.load(std::memory_order_relaxed)->ip_f16(a, b, dim);
}

__attribute__((hot)) float metrics_ip_i8(const int8_t *a, const int8_t *b,
                                         size_t dim) {
  return g_impl.load(std::memory_order_relaxed)->ip_i8(a, b, dim);
}

__attribute__((hot)) float metrics_cosine_f32(const float *a, const float *b,
                                              size_t dim) {
  return g_impl.load(std::memory_order_relaxed)->cosine_f32(a, b, dim);
}

__attribute__((hot)) float metrics_cosine_f16(const uint16_t *a,
                                              const uint16_t *b, size_t dim) {
  return g_impl.load(std::memory_order_relaxed)->cosine_f16(a, b, dim);
}

__attribute__((hot)) float metrics_cosine_i8(const int8_t *a, const int8_t *b,
                                             size_t dim) {
  return g_impl.load(std::memory_order_relaxed)->cosine_i8(a, b, dim);
}

__attribute__((hot)) void metrics_l2sq_f32_batch(const float *query,
                                                 const float *const *vectors,
                                                 size_t dim, size_t count,
                                                 float *results) {
  g_impl.load(std::memory_order_relaxed)
      ->l2sq_f32_batch(query, vectors, dim, count, results);
}

__attribute__((hot)) void metrics_ip_f32_batch(const float *query,
                                               const float *const *vectors,
                                               size_t dim, size_t count,
                                               float *results) {
  g_impl.load(std::memory_order_relaxed)
      ->ip_f32_batch(query, vectors, dim, count, results);
}

// ============================================
// HNSW-compatible wrappers
// ============================================

__attribute__((hot)) float metrics_l2sq_f32_hnsw(const void *a, const void *b,
                                                 const void *dim_ptr) {
  size_t dim = *static_cast<const size_t *>(dim_ptr);
  return g_impl.load(std::memory_order_relaxed)
      ->l2sq_f32(static_cast<const float *>(a), static_cast<const float *>(b),
                 dim);
}

__attribute__((hot)) float metrics_ip_distance_f32_hnsw(const void *a,
                                                        const void *b,
                                                        const void *dim_ptr) {
  size_t dim = *static_cast<const size_t *>(dim_ptr);
  // Inner product distance = 1 - inner_product (for normalized vectors)
  return 1.0f - g_impl.load(std::memory_order_relaxed)
                    ->ip_f32(static_cast<const float *>(a),
                             static_cast<const float *>(b), dim);
}

}  // extern "C"
