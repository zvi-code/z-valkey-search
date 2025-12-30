/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * ARM NEON + dotprod implementation (armv8.2-a+dotprod).
 * Optimized for Graviton2 and newer with dot product instructions.
 */

#if defined(__aarch64__)

#include <arm_neon.h>
#include <cmath>
#include <cstddef>
#include <cstdint>

#include "src/simd_metrics/cpu_caps.h"
#include "src/simd_metrics/simd_internal.h"

namespace {

// Forward declarations from metrics_neon.cc (reused)
extern "C" float l2sq_f32_neon(const float *a, const float *b, size_t dim);
extern "C" float l2sq_f16_neon(const uint16_t *a, const uint16_t *b, size_t dim);
extern "C" float ip_f32_neon(const float *a, const float *b, size_t dim);
extern "C" float ip_f16_neon(const uint16_t *a, const uint16_t *b, size_t dim);
extern "C" float cosine_f32_neon(const float *a, const float *b, size_t dim);
extern "C" float cosine_f16_neon(const uint16_t *a, const uint16_t *b, size_t dim);
extern "C" void l2sq_f32_batch_neon(const float *query, const float *const *vectors,
                                    size_t dim, size_t count, float *results);
extern "C" void ip_f32_batch_neon(const float *query, const float *const *vectors,
                                  size_t dim, size_t count, float *results);

// ============================================
// L2 Squared Distance - INT8 with dotprod
// ============================================

float l2sq_i8_dotprod(const int8_t *a, const int8_t *b, size_t dim) {
  int32x4_t sum_vec = vdupq_n_s32(0);
  size_t i = 0;

  // Use SDOT instruction: 4x4 = 16 int8 multiplies -> 4 int32 accumulates
  for (; i + 16 <= dim; i += 16) {
    int8x16_t a_vec = vld1q_s8(a + i);
    int8x16_t b_vec = vld1q_s8(b + i);

    // Compute diff = a - b
    int8x16_t diff = vsubq_s8(a_vec, b_vec);

    // Use dot product to compute diff * diff
    sum_vec = vdotq_s32(sum_vec, diff, diff);
  }

  int32_t sum = vaddvq_s32(sum_vec);

  // Handle tail
  for (; i < dim; i++) {
    int32_t d = static_cast<int32_t>(a[i]) - static_cast<int32_t>(b[i]);
    sum += d * d;
  }

  return static_cast<float>(sum);
}

// ============================================
// Inner Product - INT8 with dotprod
// ============================================

float ip_i8_dotprod(const int8_t *a, const int8_t *b, size_t dim) {
  int32x4_t sum_vec = vdupq_n_s32(0);
  size_t i = 0;

  // SDOT: efficient dot product of 4x4 int8 values
  for (; i + 16 <= dim; i += 16) {
    int8x16_t a_vec = vld1q_s8(a + i);
    int8x16_t b_vec = vld1q_s8(b + i);
    sum_vec = vdotq_s32(sum_vec, a_vec, b_vec);
  }

  int32_t sum = vaddvq_s32(sum_vec);

  for (; i < dim; i++) {
    sum += static_cast<int32_t>(a[i]) * static_cast<int32_t>(b[i]);
  }

  return static_cast<float>(sum);
}

// ============================================
// Cosine Similarity - INT8 with dotprod
// ============================================

float cosine_i8_dotprod(const int8_t *a, const int8_t *b, size_t dim) {
  int32x4_t dot_vec = vdupq_n_s32(0);
  int32x4_t a2_vec = vdupq_n_s32(0);
  int32x4_t b2_vec = vdupq_n_s32(0);
  size_t i = 0;

  for (; i + 16 <= dim; i += 16) {
    int8x16_t a_vec = vld1q_s8(a + i);
    int8x16_t b_vec = vld1q_s8(b + i);

    dot_vec = vdotq_s32(dot_vec, a_vec, b_vec);
    a2_vec = vdotq_s32(a2_vec, a_vec, a_vec);
    b2_vec = vdotq_s32(b2_vec, b_vec, b_vec);
  }

  int64_t dot = vaddvq_s32(dot_vec);
  int64_t a2 = vaddvq_s32(a2_vec);
  int64_t b2 = vaddvq_s32(b2_vec);

  for (; i < dim; i++) {
    int32_t ai = static_cast<int32_t>(a[i]);
    int32_t bi = static_cast<int32_t>(b[i]);
    dot += ai * bi;
    a2 += ai * ai;
    b2 += bi * bi;
  }

  float denom =
      std::sqrt(static_cast<float>(a2)) * std::sqrt(static_cast<float>(b2));
  return (denom > 0.0f) ? (static_cast<float>(dot) / denom) : 0.0f;
}

}  // namespace

// We need to access functions from metrics_neon.cc
// Declare them as weak symbols that will be resolved at link time
extern "C" {

// Reuse FP32 implementations from NEON baseline (they're already optimal)
__attribute__((weak)) float l2sq_f32_neon(const float *a, const float *b, size_t dim);
__attribute__((weak)) float l2sq_f16_neon(const uint16_t *a, const uint16_t *b, size_t dim);
__attribute__((weak)) float ip_f32_neon(const float *a, const float *b, size_t dim);
__attribute__((weak)) float ip_f16_neon(const uint16_t *a, const uint16_t *b, size_t dim);
__attribute__((weak)) float cosine_f32_neon(const float *a, const float *b, size_t dim);
__attribute__((weak)) float cosine_f16_neon(const uint16_t *a, const uint16_t *b, size_t dim);
__attribute__((weak)) void l2sq_f32_batch_neon(const float *query, const float *const *vectors,
                                               size_t dim, size_t count, float *results);
__attribute__((weak)) void ip_f32_batch_neon(const float *query, const float *const *vectors,
                                             size_t dim, size_t count, float *results);

const metrics_impl_t metrics_impl_neon_dotprod = {
    .l2sq_f32 = l2sq_f32_neon,      // Reuse NEON
    .l2sq_f16 = l2sq_f16_neon,      // Reuse NEON
    .l2sq_i8 = l2sq_i8_dotprod,     // Optimized with dotprod
    .ip_f32 = ip_f32_neon,          // Reuse NEON
    .ip_f16 = ip_f16_neon,          // Reuse NEON
    .ip_i8 = ip_i8_dotprod,         // Optimized with dotprod
    .cosine_f32 = cosine_f32_neon,  // Reuse NEON
    .cosine_f16 = cosine_f16_neon,  // Reuse NEON
    .cosine_i8 = cosine_i8_dotprod, // Optimized with dotprod
    .l2sq_f32_batch = l2sq_f32_batch_neon,
    .ip_f32_batch = ip_f32_batch_neon,
    .name = "neon_dotprod",
    .caps = CPU_CAP_NEON | CPU_CAP_NEON_DOTPROD,
};

}  // extern "C"

#endif  // __aarch64__
