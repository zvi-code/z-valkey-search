/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * ARM SVE implementation (armv8.2-a+sve).
 * Compile: -O3 -march=armv8.2-a+sve
 * Target: Graviton3 (256-bit SVE)
 */

#if defined(__aarch64__) && defined(__ARM_FEATURE_SVE)

#include <arm_sve.h>
#include <cmath>
#include <cstddef>
#include <cstdint>

#include "src/simd_metrics/cpu_caps.h"
#include "src/simd_metrics/simd_internal.h"

namespace {

// ============================================
// F32 Implementations
// ============================================

float l2sq_f32_sve(const float *a, const float *b, size_t dim) {
  svfloat32_t sum = svdup_f32(0.0f);
  size_t i = 0;

  while (i < dim) {
    svbool_t pg = svwhilelt_b32_u64(i, dim);
    svfloat32_t va = svld1_f32(pg, a + i);
    svfloat32_t vb = svld1_f32(pg, b + i);
    svfloat32_t diff = svsub_f32_x(pg, va, vb);
    sum = svmla_f32_x(pg, sum, diff, diff);
    i += svcntw();
  }

  return svaddv_f32(svptrue_b32(), sum);
}

float ip_f32_sve(const float *a, const float *b, size_t dim) {
  svfloat32_t sum = svdup_f32(0.0f);
  size_t i = 0;

  while (i < dim) {
    svbool_t pg = svwhilelt_b32_u64(i, dim);
    svfloat32_t va = svld1_f32(pg, a + i);
    svfloat32_t vb = svld1_f32(pg, b + i);
    sum = svmla_f32_x(pg, sum, va, vb);
    i += svcntw();
  }

  return svaddv_f32(svptrue_b32(), sum);
}

float cosine_f32_sve(const float *a, const float *b, size_t dim) {
  svfloat32_t dot_sum = svdup_f32(0.0f);
  svfloat32_t norm_a_sum = svdup_f32(0.0f);
  svfloat32_t norm_b_sum = svdup_f32(0.0f);
  size_t i = 0;

  while (i < dim) {
    svbool_t pg = svwhilelt_b32_u64(i, dim);
    svfloat32_t va = svld1_f32(pg, a + i);
    svfloat32_t vb = svld1_f32(pg, b + i);

    dot_sum = svmla_f32_x(pg, dot_sum, va, vb);
    norm_a_sum = svmla_f32_x(pg, norm_a_sum, va, va);
    norm_b_sum = svmla_f32_x(pg, norm_b_sum, vb, vb);

    i += svcntw();
  }

  float dot = svaddv_f32(svptrue_b32(), dot_sum);
  float norm_a = svaddv_f32(svptrue_b32(), norm_a_sum);
  float norm_b = svaddv_f32(svptrue_b32(), norm_b_sum);

  float denom = std::sqrt(norm_a) * std::sqrt(norm_b);
  return (denom > 0.0f) ? (dot / denom) : 0.0f;
}

// ============================================
// F16 Implementations
// Compute in FP16, accumulate in FP32 for precision
// ============================================

float l2sq_f16_sve(const uint16_t *a, const uint16_t *b, size_t dim) {
  svfloat32_t sum = svdup_f32(0.0f);
  size_t i = 0;

  // Process at FP32 width, loading FP16 and widening
  while (i < dim) {
    svbool_t pg32 = svwhilelt_b32_u64(i, dim);
    svbool_t pg16 = svwhilelt_b16_u64(i, dim);

    // Load FP16, convert to FP32
    svfloat16_t va_f16 =
        svld1_f16(pg16, reinterpret_cast<const float16_t *>(a + i));
    svfloat16_t vb_f16 =
        svld1_f16(pg16, reinterpret_cast<const float16_t *>(b + i));

    // Convert lower half to FP32
    svfloat32_t va = svcvt_f32_f16_z(pg32, va_f16);
    svfloat32_t vb = svcvt_f32_f16_z(pg32, vb_f16);

    svfloat32_t diff = svsub_f32_x(pg32, va, vb);
    sum = svmla_f32_x(pg32, sum, diff, diff);

    i += svcntw();
  }

  return svaddv_f32(svptrue_b32(), sum);
}

float ip_f16_sve(const uint16_t *a, const uint16_t *b, size_t dim) {
  svfloat32_t sum = svdup_f32(0.0f);
  size_t i = 0;

  while (i < dim) {
    svbool_t pg32 = svwhilelt_b32_u64(i, dim);
    svbool_t pg16 = svwhilelt_b16_u64(i, dim);

    svfloat16_t va_f16 =
        svld1_f16(pg16, reinterpret_cast<const float16_t *>(a + i));
    svfloat16_t vb_f16 =
        svld1_f16(pg16, reinterpret_cast<const float16_t *>(b + i));

    svfloat32_t va = svcvt_f32_f16_z(pg32, va_f16);
    svfloat32_t vb = svcvt_f32_f16_z(pg32, vb_f16);

    sum = svmla_f32_x(pg32, sum, va, vb);

    i += svcntw();
  }

  return svaddv_f32(svptrue_b32(), sum);
}

float cosine_f16_sve(const uint16_t *a, const uint16_t *b, size_t dim) {
  svfloat32_t dot_sum = svdup_f32(0.0f);
  svfloat32_t norm_a_sum = svdup_f32(0.0f);
  svfloat32_t norm_b_sum = svdup_f32(0.0f);
  size_t i = 0;

  while (i < dim) {
    svbool_t pg32 = svwhilelt_b32_u64(i, dim);
    svbool_t pg16 = svwhilelt_b16_u64(i, dim);

    svfloat16_t va_f16 =
        svld1_f16(pg16, reinterpret_cast<const float16_t *>(a + i));
    svfloat16_t vb_f16 =
        svld1_f16(pg16, reinterpret_cast<const float16_t *>(b + i));

    svfloat32_t va = svcvt_f32_f16_z(pg32, va_f16);
    svfloat32_t vb = svcvt_f32_f16_z(pg32, vb_f16);

    dot_sum = svmla_f32_x(pg32, dot_sum, va, vb);
    norm_a_sum = svmla_f32_x(pg32, norm_a_sum, va, va);
    norm_b_sum = svmla_f32_x(pg32, norm_b_sum, vb, vb);

    i += svcntw();
  }

  float dot = svaddv_f32(svptrue_b32(), dot_sum);
  float norm_a = svaddv_f32(svptrue_b32(), norm_a_sum);
  float norm_b = svaddv_f32(svptrue_b32(), norm_b_sum);

  float denom = std::sqrt(norm_a) * std::sqrt(norm_b);
  return (denom > 0.0f) ? (dot / denom) : 0.0f;
}

// ============================================
// I8 Implementations
// Use SVE SDOT for dot product acceleration
// ============================================

float l2sq_i8_sve(const int8_t *a, const int8_t *b, size_t dim) {
  svint32_t sum = svdup_s32(0);
  size_t i = 0;

  // SVE doesn't have direct squared-difference, compute manually
  // diff = a - b, then accumulate diff * diff
  while (i < dim) {
    svbool_t pg = svwhilelt_b8_u64(i, dim);

    svint8_t va = svld1_s8(pg, a + i);
    svint8_t vb = svld1_s8(pg, b + i);

    // Widen to 16-bit for subtraction to avoid overflow
    svint16_t va_lo = svunpklo_s16(va);
    svint16_t va_hi = svunpkhi_s16(va);
    svint16_t vb_lo = svunpklo_s16(vb);
    svint16_t vb_hi = svunpkhi_s16(vb);

    svint16_t diff_lo = svsub_s16_x(svptrue_b16(), va_lo, vb_lo);
    svint16_t diff_hi = svsub_s16_x(svptrue_b16(), va_hi, vb_hi);

    // Widen to 32-bit for multiplication
    svint32_t diff_lo_lo = svunpklo_s32(diff_lo);
    svint32_t diff_lo_hi = svunpkhi_s32(diff_lo);
    svint32_t diff_hi_lo = svunpklo_s32(diff_hi);
    svint32_t diff_hi_hi = svunpkhi_s32(diff_hi);

    // Accumulate squares
    sum = svmla_s32_x(svptrue_b32(), sum, diff_lo_lo, diff_lo_lo);
    sum = svmla_s32_x(svptrue_b32(), sum, diff_lo_hi, diff_lo_hi);
    sum = svmla_s32_x(svptrue_b32(), sum, diff_hi_lo, diff_hi_lo);
    sum = svmla_s32_x(svptrue_b32(), sum, diff_hi_hi, diff_hi_hi);

    i += svcntb();
  }

  return static_cast<float>(svaddv_s32(svptrue_b32(), sum));
}

float ip_i8_sve(const int8_t *a, const int8_t *b, size_t dim) {
  svint32_t sum = svdup_s32(0);
  size_t i = 0;

  // Use SDOT: signed dot product of 8-bit elements, accumulating to 32-bit
  while (i < dim) {
    svbool_t pg = svwhilelt_b8_u64(i, dim);

    svint8_t va = svld1_s8(pg, a + i);
    svint8_t vb = svld1_s8(pg, b + i);

    // SDOT: dot product of groups of 4 int8 values into int32
    sum = svdot_s32(sum, va, vb);

    i += svcntb();
  }

  return static_cast<float>(svaddv_s32(svptrue_b32(), sum));
}

float cosine_i8_sve(const int8_t *a, const int8_t *b, size_t dim) {
  svint32_t dot_sum = svdup_s32(0);
  svint32_t norm_a_sum = svdup_s32(0);
  svint32_t norm_b_sum = svdup_s32(0);
  size_t i = 0;

  while (i < dim) {
    svbool_t pg = svwhilelt_b8_u64(i, dim);

    svint8_t va = svld1_s8(pg, a + i);
    svint8_t vb = svld1_s8(pg, b + i);

    dot_sum = svdot_s32(dot_sum, va, vb);
    norm_a_sum = svdot_s32(norm_a_sum, va, va);
    norm_b_sum = svdot_s32(norm_b_sum, vb, vb);

    i += svcntb();
  }

  float dot = static_cast<float>(svaddv_s32(svptrue_b32(), dot_sum));
  float norm_a = static_cast<float>(svaddv_s32(svptrue_b32(), norm_a_sum));
  float norm_b = static_cast<float>(svaddv_s32(svptrue_b32(), norm_b_sum));

  float denom = std::sqrt(norm_a) * std::sqrt(norm_b);
  return (denom > 0.0f) ? (dot / denom) : 0.0f;
}

// ============================================
// Batch Operations
// Prefetch tuned for Graviton3 memory subsystem
// ============================================

void l2sq_f32_batch_sve(const float *query, const float *const *vectors,
                        size_t dim, size_t count, float *results) {
  // Graviton3: L1 = 64KB, L2 = 1MB per core
  // Prefetch distance tuned for typical vector sizes (768-1536 dims)
  const size_t prefetch_dist = 4;

  for (size_t i = 0; i < count; i++) {
    // Prefetch future vectors into L1
    if (i + prefetch_dist < count) {
      // Prefetch first cache line of future vector
      svprfw(svptrue_b32(), vectors[i + prefetch_dist], SV_PLDL1STRM);

      // For large vectors, prefetch additional cache lines
      if (dim > 64) {
        svprfw(svptrue_b32(), vectors[i + prefetch_dist] + 16, SV_PLDL1STRM);
      }
      if (dim > 128) {
        svprfw(svptrue_b32(), vectors[i + prefetch_dist] + 32, SV_PLDL1STRM);
      }
    }

    results[i] = l2sq_f32_sve(query, vectors[i], dim);
  }
}

void ip_f32_batch_sve(const float *query, const float *const *vectors,
                      size_t dim, size_t count, float *results) {
  const size_t prefetch_dist = 4;

  for (size_t i = 0; i < count; i++) {
    if (i + prefetch_dist < count) {
      svprfw(svptrue_b32(), vectors[i + prefetch_dist], SV_PLDL1STRM);

      if (dim > 64) {
        svprfw(svptrue_b32(), vectors[i + prefetch_dist] + 16, SV_PLDL1STRM);
      }
      if (dim > 128) {
        svprfw(svptrue_b32(), vectors[i + prefetch_dist] + 32, SV_PLDL1STRM);
      }
    }

    results[i] = ip_f32_sve(query, vectors[i], dim);
  }
}

}  // namespace

extern "C" {

const metrics_impl_t metrics_impl_sve = {
    .l2sq_f32 = l2sq_f32_sve,
    .l2sq_f16 = l2sq_f16_sve,
    .l2sq_i8 = l2sq_i8_sve,
    .ip_f32 = ip_f32_sve,
    .ip_f16 = ip_f16_sve,
    .ip_i8 = ip_i8_sve,
    .cosine_f32 = cosine_f32_sve,
    .cosine_f16 = cosine_f16_sve,
    .cosine_i8 = cosine_i8_sve,
    .l2sq_f32_batch = l2sq_f32_batch_sve,
    .ip_f32_batch = ip_f32_batch_sve,
    .name = "sve",
    .caps = CPU_CAP_NEON | CPU_CAP_SVE,
};

}  // extern "C"

#else  // No SVE support at compile time

#include "src/simd_metrics/cpu_caps.h"
#include "src/simd_metrics/simd_internal.h"

extern "C" {

// SVE not available - stub with null pointers (won't be selected at runtime)
const metrics_impl_t metrics_impl_sve = {
    .l2sq_f32 = nullptr,
    .l2sq_f16 = nullptr,
    .l2sq_i8 = nullptr,
    .ip_f32 = nullptr,
    .ip_f16 = nullptr,
    .ip_i8 = nullptr,
    .cosine_f32 = nullptr,
    .cosine_f16 = nullptr,
    .cosine_i8 = nullptr,
    .l2sq_f32_batch = nullptr,
    .ip_f32_batch = nullptr,
    .name = "sve_stub",
    .caps = CPU_CAP_SVE,
};

}  // extern "C"

#endif  // __aarch64__ && __ARM_FEATURE_SVE
