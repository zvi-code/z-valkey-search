/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * x86-64 AVX2 implementation.
 * Requires: -mavx2 -mfma -mf16c
 */

#if defined(__x86_64__)

#include <immintrin.h>
#include <cmath>
#include <cstddef>
#include <cstdint>

#include "src/simd_metrics/cpu_caps.h"
#include "src/simd_metrics/simd_internal.h"

namespace {

// ============================================
// L2 Squared Distance
// ============================================

float l2sq_f32_avx2(const float *a, const float *b, size_t dim) {
  __m256 sum = _mm256_setzero_ps();
  size_t i = 0;

  // Process 8 floats at a time
  for (; i + 8 <= dim; i += 8) {
    __m256 va = _mm256_loadu_ps(a + i);
    __m256 vb = _mm256_loadu_ps(b + i);
    __m256 diff = _mm256_sub_ps(va, vb);
    sum = _mm256_fmadd_ps(diff, diff, sum);
  }

  // Horizontal sum
  __m128 hi = _mm256_extractf128_ps(sum, 1);
  __m128 lo = _mm256_castps256_ps128(sum);
  __m128 sum128 = _mm_add_ps(lo, hi);
  sum128 = _mm_hadd_ps(sum128, sum128);
  sum128 = _mm_hadd_ps(sum128, sum128);
  float result = _mm_cvtss_f32(sum128);

  // Handle tail
  for (; i < dim; i++) {
    float d = a[i] - b[i];
    result += d * d;
  }

  return result;
}

float l2sq_f16_avx2(const uint16_t *a, const uint16_t *b, size_t dim) {
  __m256 sum = _mm256_setzero_ps();
  size_t i = 0;

  // Process 8 FP16 values at a time using F16C
  for (; i + 8 <= dim; i += 8) {
    __m128i a_f16 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(a + i));
    __m128i b_f16 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(b + i));
    __m256 va = _mm256_cvtph_ps(a_f16);
    __m256 vb = _mm256_cvtph_ps(b_f16);
    __m256 diff = _mm256_sub_ps(va, vb);
    sum = _mm256_fmadd_ps(diff, diff, sum);
  }

  __m128 hi = _mm256_extractf128_ps(sum, 1);
  __m128 lo = _mm256_castps256_ps128(sum);
  __m128 sum128 = _mm_add_ps(lo, hi);
  sum128 = _mm_hadd_ps(sum128, sum128);
  sum128 = _mm_hadd_ps(sum128, sum128);
  float result = _mm_cvtss_f32(sum128);

  // Handle tail
  for (; i < dim; i++) {
    __m128i a_f16 = _mm_set1_epi16(a[i]);
    __m128i b_f16 = _mm_set1_epi16(b[i]);
    float fa = _mm_cvtss_f32(_mm256_castps256_ps128(_mm256_cvtph_ps(a_f16)));
    float fb = _mm_cvtss_f32(_mm256_castps256_ps128(_mm256_cvtph_ps(b_f16)));
    float d = fa - fb;
    result += d * d;
  }

  return result;
}

float l2sq_i8_avx2(const int8_t *a, const int8_t *b, size_t dim) {
  __m256i sum = _mm256_setzero_si256();
  size_t i = 0;

  // Process 32 int8 values at a time
  for (; i + 32 <= dim; i += 32) {
    __m256i va = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(a + i));
    __m256i vb = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(b + i));

    // Compute diff (saturating subtract)
    __m256i diff = _mm256_sub_epi8(va, vb);

    // Sign-extend to 16-bit and square
    __m256i lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(diff));
    __m256i hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(diff, 1));

    __m256i sq_lo = _mm256_madd_epi16(lo, lo);
    __m256i sq_hi = _mm256_madd_epi16(hi, hi);

    sum = _mm256_add_epi32(sum, sq_lo);
    sum = _mm256_add_epi32(sum, sq_hi);
  }

  // Horizontal sum
  __m128i hi128 = _mm256_extracti128_si256(sum, 1);
  __m128i lo128 = _mm256_castsi256_si128(sum);
  __m128i sum128 = _mm_add_epi32(lo128, hi128);
  sum128 = _mm_hadd_epi32(sum128, sum128);
  sum128 = _mm_hadd_epi32(sum128, sum128);
  int32_t result = _mm_cvtsi128_si32(sum128);

  // Handle tail
  for (; i < dim; i++) {
    int32_t d = static_cast<int32_t>(a[i]) - static_cast<int32_t>(b[i]);
    result += d * d;
  }

  return static_cast<float>(result);
}

// ============================================
// Inner Product
// ============================================

float ip_f32_avx2(const float *a, const float *b, size_t dim) {
  __m256 sum = _mm256_setzero_ps();
  size_t i = 0;

  for (; i + 8 <= dim; i += 8) {
    __m256 va = _mm256_loadu_ps(a + i);
    __m256 vb = _mm256_loadu_ps(b + i);
    sum = _mm256_fmadd_ps(va, vb, sum);
  }

  __m128 hi = _mm256_extractf128_ps(sum, 1);
  __m128 lo = _mm256_castps256_ps128(sum);
  __m128 sum128 = _mm_add_ps(lo, hi);
  sum128 = _mm_hadd_ps(sum128, sum128);
  sum128 = _mm_hadd_ps(sum128, sum128);
  float result = _mm_cvtss_f32(sum128);

  for (; i < dim; i++) {
    result += a[i] * b[i];
  }

  return result;
}

float ip_f16_avx2(const uint16_t *a, const uint16_t *b, size_t dim) {
  __m256 sum = _mm256_setzero_ps();
  size_t i = 0;

  for (; i + 8 <= dim; i += 8) {
    __m128i a_f16 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(a + i));
    __m128i b_f16 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(b + i));
    __m256 va = _mm256_cvtph_ps(a_f16);
    __m256 vb = _mm256_cvtph_ps(b_f16);
    sum = _mm256_fmadd_ps(va, vb, sum);
  }

  __m128 hi = _mm256_extractf128_ps(sum, 1);
  __m128 lo = _mm256_castps256_ps128(sum);
  __m128 sum128 = _mm_add_ps(lo, hi);
  sum128 = _mm_hadd_ps(sum128, sum128);
  sum128 = _mm_hadd_ps(sum128, sum128);
  float result = _mm_cvtss_f32(sum128);

  for (; i < dim; i++) {
    __m128i a_f16 = _mm_set1_epi16(a[i]);
    __m128i b_f16 = _mm_set1_epi16(b[i]);
    float fa = _mm_cvtss_f32(_mm256_castps256_ps128(_mm256_cvtph_ps(a_f16)));
    float fb = _mm_cvtss_f32(_mm256_castps256_ps128(_mm256_cvtph_ps(b_f16)));
    result += fa * fb;
  }

  return result;
}

float ip_i8_avx2(const int8_t *a, const int8_t *b, size_t dim) {
  __m256i sum = _mm256_setzero_si256();
  size_t i = 0;

  for (; i + 32 <= dim; i += 32) {
    __m256i va = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(a + i));
    __m256i vb = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(b + i));

    // Sign-extend to 16-bit and multiply
    __m256i a_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(va));
    __m256i a_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(va, 1));
    __m256i b_lo = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(vb));
    __m256i b_hi = _mm256_cvtepi8_epi16(_mm256_extracti128_si256(vb, 1));

    __m256i prod_lo = _mm256_madd_epi16(a_lo, b_lo);
    __m256i prod_hi = _mm256_madd_epi16(a_hi, b_hi);

    sum = _mm256_add_epi32(sum, prod_lo);
    sum = _mm256_add_epi32(sum, prod_hi);
  }

  __m128i hi128 = _mm256_extracti128_si256(sum, 1);
  __m128i lo128 = _mm256_castsi256_si128(sum);
  __m128i sum128 = _mm_add_epi32(lo128, hi128);
  sum128 = _mm_hadd_epi32(sum128, sum128);
  sum128 = _mm_hadd_epi32(sum128, sum128);
  int32_t result = _mm_cvtsi128_si32(sum128);

  for (; i < dim; i++) {
    result += static_cast<int32_t>(a[i]) * static_cast<int32_t>(b[i]);
  }

  return static_cast<float>(result);
}

// ============================================
// Cosine Similarity
// ============================================

float cosine_f32_avx2(const float *a, const float *b, size_t dim) {
  __m256 dot_sum = _mm256_setzero_ps();
  __m256 a2_sum = _mm256_setzero_ps();
  __m256 b2_sum = _mm256_setzero_ps();
  size_t i = 0;

  for (; i + 8 <= dim; i += 8) {
    __m256 va = _mm256_loadu_ps(a + i);
    __m256 vb = _mm256_loadu_ps(b + i);
    dot_sum = _mm256_fmadd_ps(va, vb, dot_sum);
    a2_sum = _mm256_fmadd_ps(va, va, a2_sum);
    b2_sum = _mm256_fmadd_ps(vb, vb, b2_sum);
  }

  // Horizontal sums
  auto hsum = [](__m256 v) {
    __m128 hi = _mm256_extractf128_ps(v, 1);
    __m128 lo = _mm256_castps256_ps128(v);
    __m128 sum128 = _mm_add_ps(lo, hi);
    sum128 = _mm_hadd_ps(sum128, sum128);
    sum128 = _mm_hadd_ps(sum128, sum128);
    return _mm_cvtss_f32(sum128);
  };

  float dot = hsum(dot_sum);
  float a2 = hsum(a2_sum);
  float b2 = hsum(b2_sum);

  for (; i < dim; i++) {
    dot += a[i] * b[i];
    a2 += a[i] * a[i];
    b2 += b[i] * b[i];
  }

  float denom = std::sqrt(a2) * std::sqrt(b2);
  return (denom > 0.0f) ? (dot / denom) : 0.0f;
}

float cosine_f16_avx2(const uint16_t *a, const uint16_t *b, size_t dim) {
  __m256 dot_sum = _mm256_setzero_ps();
  __m256 a2_sum = _mm256_setzero_ps();
  __m256 b2_sum = _mm256_setzero_ps();
  size_t i = 0;

  for (; i + 8 <= dim; i += 8) {
    __m128i a_f16 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(a + i));
    __m128i b_f16 = _mm_loadu_si128(reinterpret_cast<const __m128i *>(b + i));
    __m256 va = _mm256_cvtph_ps(a_f16);
    __m256 vb = _mm256_cvtph_ps(b_f16);
    dot_sum = _mm256_fmadd_ps(va, vb, dot_sum);
    a2_sum = _mm256_fmadd_ps(va, va, a2_sum);
    b2_sum = _mm256_fmadd_ps(vb, vb, b2_sum);
  }

  auto hsum = [](__m256 v) {
    __m128 hi = _mm256_extractf128_ps(v, 1);
    __m128 lo = _mm256_castps256_ps128(v);
    __m128 sum128 = _mm_add_ps(lo, hi);
    sum128 = _mm_hadd_ps(sum128, sum128);
    sum128 = _mm_hadd_ps(sum128, sum128);
    return _mm_cvtss_f32(sum128);
  };

  float dot = hsum(dot_sum);
  float a2 = hsum(a2_sum);
  float b2 = hsum(b2_sum);

  for (; i < dim; i++) {
    __m128i a_f16 = _mm_set1_epi16(a[i]);
    __m128i b_f16 = _mm_set1_epi16(b[i]);
    float fa = _mm_cvtss_f32(_mm256_castps256_ps128(_mm256_cvtph_ps(a_f16)));
    float fb = _mm_cvtss_f32(_mm256_castps256_ps128(_mm256_cvtph_ps(b_f16)));
    dot += fa * fb;
    a2 += fa * fa;
    b2 += fb * fb;
  }

  float denom = std::sqrt(a2) * std::sqrt(b2);
  return (denom > 0.0f) ? (dot / denom) : 0.0f;
}

float cosine_i8_avx2(const int8_t *a, const int8_t *b, size_t dim) {
  // Use scalar for simplicity - INT8 cosine is rare
  int64_t dot = 0, a2 = 0, b2 = 0;
  for (size_t i = 0; i < dim; i++) {
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
// Batch Operations with prefetch
// ============================================

void l2sq_f32_batch_avx2(const float *query, const float *const *vectors,
                         size_t dim, size_t count, float *results) {
  for (size_t i = 0; i < count; i++) {
    if (i + 4 < count) {
      _mm_prefetch(reinterpret_cast<const char *>(vectors[i + 4]), _MM_HINT_T0);
    }
    results[i] = l2sq_f32_avx2(query, vectors[i], dim);
  }
}

void ip_f32_batch_avx2(const float *query, const float *const *vectors,
                       size_t dim, size_t count, float *results) {
  for (size_t i = 0; i < count; i++) {
    if (i + 4 < count) {
      _mm_prefetch(reinterpret_cast<const char *>(vectors[i + 4]), _MM_HINT_T0);
    }
    results[i] = ip_f32_avx2(query, vectors[i], dim);
  }
}

}  // namespace

extern "C" {

const metrics_impl_t metrics_impl_avx2 = {
    .l2sq_f32 = l2sq_f32_avx2,
    .l2sq_f16 = l2sq_f16_avx2,
    .l2sq_i8 = l2sq_i8_avx2,
    .ip_f32 = ip_f32_avx2,
    .ip_f16 = ip_f16_avx2,
    .ip_i8 = ip_i8_avx2,
    .cosine_f32 = cosine_f32_avx2,
    .cosine_f16 = cosine_f16_avx2,
    .cosine_i8 = cosine_i8_avx2,
    .l2sq_f32_batch = l2sq_f32_batch_avx2,
    .ip_f32_batch = ip_f32_batch_avx2,
    .name = "avx2",
    .caps = CPU_CAP_AVX2 | CPU_CAP_FMA | CPU_CAP_F16C,
};

}  // extern "C"

#endif  // __x86_64__
