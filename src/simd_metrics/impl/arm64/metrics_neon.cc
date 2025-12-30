/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * ARM NEON baseline implementation (armv8-a).
 * Works on all AArch64 processors.
 */

#if defined(__aarch64__)

#include <arm_neon.h>
#include <cmath>
#include <cstddef>
#include <cstdint>

#include "src/simd_metrics/cpu_caps.h"
#include "src/simd_metrics/simd_internal.h"

namespace {

// ============================================
// L2 Squared Distance
// ============================================

float l2sq_f32_neon(const float *a, const float *b, size_t dim) {
  float32x4_t sum_vec = vdupq_n_f32(0.0f);
  size_t i = 0;

  // Process 4 floats at a time
  for (; i + 4 <= dim; i += 4) {
    float32x4_t a_vec = vld1q_f32(a + i);
    float32x4_t b_vec = vld1q_f32(b + i);
    float32x4_t diff = vsubq_f32(a_vec, b_vec);
    sum_vec = vfmaq_f32(sum_vec, diff, diff);
  }

  // Horizontal sum
  float sum = vaddvq_f32(sum_vec);

  // Handle tail
  for (; i < dim; i++) {
    float d = a[i] - b[i];
    sum += d * d;
  }

  return sum;
}

float l2sq_f16_neon(const uint16_t *a, const uint16_t *b, size_t dim) {
  float32x4_t sum_vec = vdupq_n_f32(0.0f);
  size_t i = 0;

  // Process 4 FP16 values at a time (convert to FP32)
  for (; i + 4 <= dim; i += 4) {
    float16x4_t a_f16 =
        vld1_f16(reinterpret_cast<const float16_t *>(a + i));
    float16x4_t b_f16 =
        vld1_f16(reinterpret_cast<const float16_t *>(b + i));
    float32x4_t a_vec = vcvt_f32_f16(a_f16);
    float32x4_t b_vec = vcvt_f32_f16(b_f16);
    float32x4_t diff = vsubq_f32(a_vec, b_vec);
    sum_vec = vfmaq_f32(sum_vec, diff, diff);
  }

  float sum = vaddvq_f32(sum_vec);

  // Handle tail (scalar fallback with conversion)
  for (; i < dim; i++) {
    float16x4_t a_f16 = vdup_n_f16(*reinterpret_cast<const float16_t *>(a + i));
    float16x4_t b_f16 = vdup_n_f16(*reinterpret_cast<const float16_t *>(b + i));
    float fa = vgetq_lane_f32(vcvt_f32_f16(a_f16), 0);
    float fb = vgetq_lane_f32(vcvt_f32_f16(b_f16), 0);
    float d = fa - fb;
    sum += d * d;
  }

  return sum;
}

float l2sq_i8_neon(const int8_t *a, const int8_t *b, size_t dim) {
  int32x4_t sum_vec = vdupq_n_s32(0);
  size_t i = 0;

  // Process 8 int8 values at a time
  for (; i + 8 <= dim; i += 8) {
    int8x8_t a_vec = vld1_s8(a + i);
    int8x8_t b_vec = vld1_s8(b + i);
    int16x8_t diff = vsubl_s8(a_vec, b_vec);

    // Square and accumulate
    int16x4_t diff_lo = vget_low_s16(diff);
    int16x4_t diff_hi = vget_high_s16(diff);
    sum_vec = vmlal_s16(sum_vec, diff_lo, diff_lo);
    sum_vec = vmlal_s16(sum_vec, diff_hi, diff_hi);
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
// Inner Product
// ============================================

float ip_f32_neon(const float *a, const float *b, size_t dim) {
  float32x4_t sum_vec = vdupq_n_f32(0.0f);
  size_t i = 0;

  for (; i + 4 <= dim; i += 4) {
    float32x4_t a_vec = vld1q_f32(a + i);
    float32x4_t b_vec = vld1q_f32(b + i);
    sum_vec = vfmaq_f32(sum_vec, a_vec, b_vec);
  }

  float sum = vaddvq_f32(sum_vec);

  for (; i < dim; i++) {
    sum += a[i] * b[i];
  }

  return sum;
}

float ip_f16_neon(const uint16_t *a, const uint16_t *b, size_t dim) {
  float32x4_t sum_vec = vdupq_n_f32(0.0f);
  size_t i = 0;

  for (; i + 4 <= dim; i += 4) {
    float16x4_t a_f16 =
        vld1_f16(reinterpret_cast<const float16_t *>(a + i));
    float16x4_t b_f16 =
        vld1_f16(reinterpret_cast<const float16_t *>(b + i));
    float32x4_t a_vec = vcvt_f32_f16(a_f16);
    float32x4_t b_vec = vcvt_f32_f16(b_f16);
    sum_vec = vfmaq_f32(sum_vec, a_vec, b_vec);
  }

  float sum = vaddvq_f32(sum_vec);

  for (; i < dim; i++) {
    float16x4_t a_f16 = vdup_n_f16(*reinterpret_cast<const float16_t *>(a + i));
    float16x4_t b_f16 = vdup_n_f16(*reinterpret_cast<const float16_t *>(b + i));
    float fa = vgetq_lane_f32(vcvt_f32_f16(a_f16), 0);
    float fb = vgetq_lane_f32(vcvt_f32_f16(b_f16), 0);
    sum += fa * fb;
  }

  return sum;
}

float ip_i8_neon(const int8_t *a, const int8_t *b, size_t dim) {
  int32x4_t sum_vec = vdupq_n_s32(0);
  size_t i = 0;

  for (; i + 8 <= dim; i += 8) {
    int8x8_t a_vec = vld1_s8(a + i);
    int8x8_t b_vec = vld1_s8(b + i);
    int16x8_t prod = vmull_s8(a_vec, b_vec);
    sum_vec = vaddw_s16(sum_vec, vget_low_s16(prod));
    sum_vec = vaddw_s16(sum_vec, vget_high_s16(prod));
  }

  int32_t sum = vaddvq_s32(sum_vec);

  for (; i < dim; i++) {
    sum += static_cast<int32_t>(a[i]) * static_cast<int32_t>(b[i]);
  }

  return static_cast<float>(sum);
}

// ============================================
// Cosine Similarity
// ============================================

float cosine_f32_neon(const float *a, const float *b, size_t dim) {
  float32x4_t dot_vec = vdupq_n_f32(0.0f);
  float32x4_t a2_vec = vdupq_n_f32(0.0f);
  float32x4_t b2_vec = vdupq_n_f32(0.0f);
  size_t i = 0;

  for (; i + 4 <= dim; i += 4) {
    float32x4_t a_vec = vld1q_f32(a + i);
    float32x4_t b_vec = vld1q_f32(b + i);
    dot_vec = vfmaq_f32(dot_vec, a_vec, b_vec);
    a2_vec = vfmaq_f32(a2_vec, a_vec, a_vec);
    b2_vec = vfmaq_f32(b2_vec, b_vec, b_vec);
  }

  float dot = vaddvq_f32(dot_vec);
  float a2 = vaddvq_f32(a2_vec);
  float b2 = vaddvq_f32(b2_vec);

  for (; i < dim; i++) {
    dot += a[i] * b[i];
    a2 += a[i] * a[i];
    b2 += b[i] * b[i];
  }

  float denom = std::sqrt(a2) * std::sqrt(b2);
  return (denom > 0.0f) ? (dot / denom) : 0.0f;
}

float cosine_f16_neon(const uint16_t *a, const uint16_t *b, size_t dim) {
  float32x4_t dot_vec = vdupq_n_f32(0.0f);
  float32x4_t a2_vec = vdupq_n_f32(0.0f);
  float32x4_t b2_vec = vdupq_n_f32(0.0f);
  size_t i = 0;

  for (; i + 4 <= dim; i += 4) {
    float16x4_t a_f16 =
        vld1_f16(reinterpret_cast<const float16_t *>(a + i));
    float16x4_t b_f16 =
        vld1_f16(reinterpret_cast<const float16_t *>(b + i));
    float32x4_t a_vec = vcvt_f32_f16(a_f16);
    float32x4_t b_vec = vcvt_f32_f16(b_f16);
    dot_vec = vfmaq_f32(dot_vec, a_vec, b_vec);
    a2_vec = vfmaq_f32(a2_vec, a_vec, a_vec);
    b2_vec = vfmaq_f32(b2_vec, b_vec, b_vec);
  }

  float dot = vaddvq_f32(dot_vec);
  float a2 = vaddvq_f32(a2_vec);
  float b2 = vaddvq_f32(b2_vec);

  for (; i < dim; i++) {
    float16x4_t a_f16 = vdup_n_f16(*reinterpret_cast<const float16_t *>(a + i));
    float16x4_t b_f16 = vdup_n_f16(*reinterpret_cast<const float16_t *>(b + i));
    float fa = vgetq_lane_f32(vcvt_f32_f16(a_f16), 0);
    float fb = vgetq_lane_f32(vcvt_f32_f16(b_f16), 0);
    dot += fa * fb;
    a2 += fa * fa;
    b2 += fb * fb;
  }

  float denom = std::sqrt(a2) * std::sqrt(b2);
  return (denom > 0.0f) ? (dot / denom) : 0.0f;
}

float cosine_i8_neon(const int8_t *a, const int8_t *b, size_t dim) {
  int32x4_t dot_vec = vdupq_n_s32(0);
  int32x4_t a2_vec = vdupq_n_s32(0);
  int32x4_t b2_vec = vdupq_n_s32(0);
  size_t i = 0;

  for (; i + 8 <= dim; i += 8) {
    int8x8_t a_vec = vld1_s8(a + i);
    int8x8_t b_vec = vld1_s8(b + i);

    int16x8_t prod = vmull_s8(a_vec, b_vec);
    int16x8_t a2_tmp = vmull_s8(a_vec, a_vec);
    int16x8_t b2_tmp = vmull_s8(b_vec, b_vec);

    dot_vec = vaddw_s16(dot_vec, vget_low_s16(prod));
    dot_vec = vaddw_s16(dot_vec, vget_high_s16(prod));
    a2_vec = vaddw_s16(a2_vec, vget_low_s16(a2_tmp));
    a2_vec = vaddw_s16(a2_vec, vget_high_s16(a2_tmp));
    b2_vec = vaddw_s16(b2_vec, vget_low_s16(b2_tmp));
    b2_vec = vaddw_s16(b2_vec, vget_high_s16(b2_tmp));
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

// ============================================
// Batch Operations
// ============================================

void l2sq_f32_batch_neon(const float *query, const float *const *vectors,
                         size_t dim, size_t count, float *results) {
  for (size_t i = 0; i < count; i++) {
    if (i + 2 < count) {
      __builtin_prefetch(vectors[i + 2], 0, 1);
    }
    results[i] = l2sq_f32_neon(query, vectors[i], dim);
  }
}

void ip_f32_batch_neon(const float *query, const float *const *vectors,
                       size_t dim, size_t count, float *results) {
  for (size_t i = 0; i < count; i++) {
    if (i + 2 < count) {
      __builtin_prefetch(vectors[i + 2], 0, 1);
    }
    results[i] = ip_f32_neon(query, vectors[i], dim);
  }
}

}  // namespace

extern "C" {

const metrics_impl_t metrics_impl_neon = {
    .l2sq_f32 = l2sq_f32_neon,
    .l2sq_f16 = l2sq_f16_neon,
    .l2sq_i8 = l2sq_i8_neon,
    .ip_f32 = ip_f32_neon,
    .ip_f16 = ip_f16_neon,
    .ip_i8 = ip_i8_neon,
    .cosine_f32 = cosine_f32_neon,
    .cosine_f16 = cosine_f16_neon,
    .cosine_i8 = cosine_i8_neon,
    .l2sq_f32_batch = l2sq_f32_batch_neon,
    .ip_f32_batch = ip_f32_batch_neon,
    .name = "neon",
    .caps = CPU_CAP_NEON,
};

}  // extern "C"

#endif  // __aarch64__
