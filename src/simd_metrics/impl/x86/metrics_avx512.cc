/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * x86-64 AVX-512 implementation.
 * Requires: -mavx512f -mavx512bw -mavx512vl
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

float l2sq_f32_avx512(const float *a, const float *b, size_t dim) {
  __m512 sum = _mm512_setzero_ps();
  size_t i = 0;

  // Process 16 floats at a time
  for (; i + 16 <= dim; i += 16) {
    __m512 va = _mm512_loadu_ps(a + i);
    __m512 vb = _mm512_loadu_ps(b + i);
    __m512 diff = _mm512_sub_ps(va, vb);
    sum = _mm512_fmadd_ps(diff, diff, sum);
  }

  float result = _mm512_reduce_add_ps(sum);

  // Handle tail (use AVX2 for remaining)
  for (; i < dim; i++) {
    float d = a[i] - b[i];
    result += d * d;
  }

  return result;
}

float l2sq_f16_avx512(const uint16_t *a, const uint16_t *b, size_t dim) {
  __m512 sum = _mm512_setzero_ps();
  size_t i = 0;

  // Process 16 FP16 values at a time
  for (; i + 16 <= dim; i += 16) {
    __m256i a_f16 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(a + i));
    __m256i b_f16 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(b + i));
    __m512 va = _mm512_cvtph_ps(a_f16);
    __m512 vb = _mm512_cvtph_ps(b_f16);
    __m512 diff = _mm512_sub_ps(va, vb);
    sum = _mm512_fmadd_ps(diff, diff, sum);
  }

  float result = _mm512_reduce_add_ps(sum);

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

float l2sq_i8_avx512(const int8_t *a, const int8_t *b, size_t dim) {
  __m512i sum = _mm512_setzero_si512();
  size_t i = 0;

  // Process 64 int8 values at a time
  for (; i + 64 <= dim; i += 64) {
    __m512i va = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(a + i));
    __m512i vb = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(b + i));

    // Compute diff
    __m512i diff = _mm512_sub_epi8(va, vb);

    // Split into 16-bit, square, and accumulate
    __m512i lo = _mm512_cvtepi8_epi16(_mm512_extracti64x4_epi64(diff, 0));
    __m512i hi = _mm512_cvtepi8_epi16(_mm512_extracti64x4_epi64(diff, 1));

    // Use vpdpwssd-like operation: madd and accumulate
    __m512i sq_lo = _mm512_madd_epi16(lo, lo);
    __m512i sq_hi = _mm512_madd_epi16(hi, hi);

    sum = _mm512_add_epi32(sum, sq_lo);
    sum = _mm512_add_epi32(sum, sq_hi);
  }

  int32_t result = _mm512_reduce_add_epi32(sum);

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

float ip_f32_avx512(const float *a, const float *b, size_t dim) {
  __m512 sum = _mm512_setzero_ps();
  size_t i = 0;

  for (; i + 16 <= dim; i += 16) {
    __m512 va = _mm512_loadu_ps(a + i);
    __m512 vb = _mm512_loadu_ps(b + i);
    sum = _mm512_fmadd_ps(va, vb, sum);
  }

  float result = _mm512_reduce_add_ps(sum);

  for (; i < dim; i++) {
    result += a[i] * b[i];
  }

  return result;
}

float ip_f16_avx512(const uint16_t *a, const uint16_t *b, size_t dim) {
  __m512 sum = _mm512_setzero_ps();
  size_t i = 0;

  for (; i + 16 <= dim; i += 16) {
    __m256i a_f16 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(a + i));
    __m256i b_f16 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(b + i));
    __m512 va = _mm512_cvtph_ps(a_f16);
    __m512 vb = _mm512_cvtph_ps(b_f16);
    sum = _mm512_fmadd_ps(va, vb, sum);
  }

  float result = _mm512_reduce_add_ps(sum);

  for (; i < dim; i++) {
    __m128i a_f16 = _mm_set1_epi16(a[i]);
    __m128i b_f16 = _mm_set1_epi16(b[i]);
    float fa = _mm_cvtss_f32(_mm256_castps256_ps128(_mm256_cvtph_ps(a_f16)));
    float fb = _mm_cvtss_f32(_mm256_castps256_ps128(_mm256_cvtph_ps(b_f16)));
    result += fa * fb;
  }

  return result;
}

float ip_i8_avx512(const int8_t *a, const int8_t *b, size_t dim) {
  __m512i sum = _mm512_setzero_si512();
  size_t i = 0;

  for (; i + 64 <= dim; i += 64) {
    __m512i va = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(a + i));
    __m512i vb = _mm512_loadu_si512(reinterpret_cast<const __m512i *>(b + i));

    __m512i a_lo = _mm512_cvtepi8_epi16(_mm512_extracti64x4_epi64(va, 0));
    __m512i a_hi = _mm512_cvtepi8_epi16(_mm512_extracti64x4_epi64(va, 1));
    __m512i b_lo = _mm512_cvtepi8_epi16(_mm512_extracti64x4_epi64(vb, 0));
    __m512i b_hi = _mm512_cvtepi8_epi16(_mm512_extracti64x4_epi64(vb, 1));

    __m512i prod_lo = _mm512_madd_epi16(a_lo, b_lo);
    __m512i prod_hi = _mm512_madd_epi16(a_hi, b_hi);

    sum = _mm512_add_epi32(sum, prod_lo);
    sum = _mm512_add_epi32(sum, prod_hi);
  }

  int32_t result = _mm512_reduce_add_epi32(sum);

  for (; i < dim; i++) {
    result += static_cast<int32_t>(a[i]) * static_cast<int32_t>(b[i]);
  }

  return static_cast<float>(result);
}

// ============================================
// Cosine Similarity
// ============================================

float cosine_f32_avx512(const float *a, const float *b, size_t dim) {
  __m512 dot_sum = _mm512_setzero_ps();
  __m512 a2_sum = _mm512_setzero_ps();
  __m512 b2_sum = _mm512_setzero_ps();
  size_t i = 0;

  for (; i + 16 <= dim; i += 16) {
    __m512 va = _mm512_loadu_ps(a + i);
    __m512 vb = _mm512_loadu_ps(b + i);
    dot_sum = _mm512_fmadd_ps(va, vb, dot_sum);
    a2_sum = _mm512_fmadd_ps(va, va, a2_sum);
    b2_sum = _mm512_fmadd_ps(vb, vb, b2_sum);
  }

  float dot = _mm512_reduce_add_ps(dot_sum);
  float a2 = _mm512_reduce_add_ps(a2_sum);
  float b2 = _mm512_reduce_add_ps(b2_sum);

  for (; i < dim; i++) {
    dot += a[i] * b[i];
    a2 += a[i] * a[i];
    b2 += b[i] * b[i];
  }

  float denom = std::sqrt(a2) * std::sqrt(b2);
  return (denom > 0.0f) ? (dot / denom) : 0.0f;
}

float cosine_f16_avx512(const uint16_t *a, const uint16_t *b, size_t dim) {
  __m512 dot_sum = _mm512_setzero_ps();
  __m512 a2_sum = _mm512_setzero_ps();
  __m512 b2_sum = _mm512_setzero_ps();
  size_t i = 0;

  for (; i + 16 <= dim; i += 16) {
    __m256i a_f16 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(a + i));
    __m256i b_f16 = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(b + i));
    __m512 va = _mm512_cvtph_ps(a_f16);
    __m512 vb = _mm512_cvtph_ps(b_f16);
    dot_sum = _mm512_fmadd_ps(va, vb, dot_sum);
    a2_sum = _mm512_fmadd_ps(va, va, a2_sum);
    b2_sum = _mm512_fmadd_ps(vb, vb, b2_sum);
  }

  float dot = _mm512_reduce_add_ps(dot_sum);
  float a2 = _mm512_reduce_add_ps(a2_sum);
  float b2 = _mm512_reduce_add_ps(b2_sum);

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

float cosine_i8_avx512(const int8_t *a, const int8_t *b, size_t dim) {
  // Use scalar for simplicity
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
// Batch Operations with AVX-512 scatter-gather potential
// ============================================

void l2sq_f32_batch_avx512(const float *query, const float *const *vectors,
                           size_t dim, size_t count, float *results) {
  // AVX-512 prefetch is more aggressive
  for (size_t i = 0; i < count; i++) {
    if (i + 8 < count) {
      _mm_prefetch(reinterpret_cast<const char *>(vectors[i + 8]), _MM_HINT_T0);
    }
    results[i] = l2sq_f32_avx512(query, vectors[i], dim);
  }
}

void ip_f32_batch_avx512(const float *query, const float *const *vectors,
                         size_t dim, size_t count, float *results) {
  for (size_t i = 0; i < count; i++) {
    if (i + 8 < count) {
      _mm_prefetch(reinterpret_cast<const char *>(vectors[i + 8]), _MM_HINT_T0);
    }
    results[i] = ip_f32_avx512(query, vectors[i], dim);
  }
}

}  // namespace

extern "C" {

const metrics_impl_t metrics_impl_avx512 = {
    .l2sq_f32 = l2sq_f32_avx512,
    .l2sq_f16 = l2sq_f16_avx512,
    .l2sq_i8 = l2sq_i8_avx512,
    .ip_f32 = ip_f32_avx512,
    .ip_f16 = ip_f16_avx512,
    .ip_i8 = ip_i8_avx512,
    .cosine_f32 = cosine_f32_avx512,
    .cosine_f16 = cosine_f16_avx512,
    .cosine_i8 = cosine_i8_avx512,
    .l2sq_f32_batch = l2sq_f32_batch_avx512,
    .ip_f32_batch = ip_f32_batch_avx512,
    .name = "avx512",
    .caps = CPU_CAP_AVX512F | CPU_CAP_AVX512BW | CPU_CAP_AVX512VL,
};

}  // extern "C"

#endif  // __x86_64__
