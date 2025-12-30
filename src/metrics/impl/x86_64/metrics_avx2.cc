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

// AVX2 + FMA implementation of similarity metrics
// Compiled with: -mavx2 -mfma
// Requirements: 4.1, 4.2, 4.3, 9.1, 9.2

#if defined(__x86_64__) || defined(_M_X64)

#include <immintrin.h>

#include <cmath>
#include <cstring>

#include "src/metrics/cpu_caps.h"
#include "src/metrics/metrics_internal.h"

namespace valkey_search {
namespace metrics {
namespace internal {

namespace {

// Helper: horizontal sum of 8 floats in __m256
inline float HorizontalSum(__m256 v) {
  // Sum pairs: [a0+a4, a1+a5, a2+a6, a3+a7, ...]
  __m128 hi = _mm256_extractf128_ps(v, 1);
  __m128 lo = _mm256_castps256_ps128(v);
  __m128 sum128 = _mm_add_ps(lo, hi);
  // Now sum128 = [a0+a4, a1+a5, a2+a6, a3+a7]
  // Shuffle and add
  __m128 shuf = _mm_movehdup_ps(sum128);  // [a1+a5, a1+a5, a3+a7, a3+a7]
  __m128 sums = _mm_add_ps(sum128, shuf);
  shuf = _mm_movehl_ps(shuf, sums);
  sums = _mm_add_ss(sums, shuf);
  return _mm_cvtss_f32(sums);
}

// Helper function to convert IEEE 754 half-precision (float16) to float32
inline float F16ToF32(uint16_t h) {
  uint32_t sign = (h & 0x8000) << 16;
  uint32_t exponent = (h >> 10) & 0x1F;
  uint32_t mantissa = h & 0x03FF;

  if (exponent == 0) {
    if (mantissa == 0) {
      uint32_t result = sign;
      float f;
      std::memcpy(&f, &result, sizeof(f));
      return f;
    }
    while ((mantissa & 0x0400) == 0) {
      mantissa <<= 1;
      exponent--;
    }
    exponent++;
    mantissa &= ~0x0400;
    exponent += 127 - 15;
    uint32_t result = sign | (exponent << 23) | (mantissa << 13);
    float f;
    std::memcpy(&f, &result, sizeof(f));
    return f;
  } else if (exponent == 31) {
    uint32_t result = sign | 0x7F800000 | (mantissa << 13);
    float f;
    std::memcpy(&f, &result, sizeof(f));
    return f;
  }

  exponent += 127 - 15;
  uint32_t result = sign | (exponent << 23) | (mantissa << 13);
  float f;
  std::memcpy(&f, &result, sizeof(f));
  return f;
}

// L2 Squared Distance - Float32
// Requirements: 4.1, 9.1, 9.2
float Avx2L2SqF32(const float* a, const float* b, size_t dim) {
  __m256 sum = _mm256_setzero_ps();

  // Process 8 floats at a time
  size_t i = 0;
  for (; i + 8 <= dim; i += 8) {
    __m256 va = _mm256_loadu_ps(a + i);
    __m256 vb = _mm256_loadu_ps(b + i);
    __m256 diff = _mm256_sub_ps(va, vb);
    // FMA: sum = diff * diff + sum
    sum = _mm256_fmadd_ps(diff, diff, sum);
  }

  float result = HorizontalSum(sum);

  // Handle tail elements
  for (; i < dim; ++i) {
    float diff = a[i] - b[i];
    result += diff * diff;
  }

  return result;
}

// L2 Squared Distance - Float16
// Requirements: 4.4, 9.1, 9.2
float Avx2L2SqF16(const uint16_t* a, const uint16_t* b, size_t dim) {
  // AVX2 doesn't have native F16 support, so we convert and use F32 path
  // Process 8 elements at a time by converting to F32
  __m256 sum = _mm256_setzero_ps();

  size_t i = 0;
  for (; i + 8 <= dim; i += 8) {
    // Convert 8 F16 values to F32
    alignas(32) float fa[8], fb[8];
    for (size_t j = 0; j < 8; ++j) {
      fa[j] = F16ToF32(a[i + j]);
      fb[j] = F16ToF32(b[i + j]);
    }
    __m256 va = _mm256_load_ps(fa);
    __m256 vb = _mm256_load_ps(fb);
    __m256 diff = _mm256_sub_ps(va, vb);
    sum = _mm256_fmadd_ps(diff, diff, sum);
  }

  float result = HorizontalSum(sum);

  // Handle tail elements
  for (; i < dim; ++i) {
    float fa = F16ToF32(a[i]);
    float fb = F16ToF32(b[i]);
    float diff = fa - fb;
    result += diff * diff;
  }

  return result;
}

// L2 Squared Distance - Int8
// Requirements: 4.6, 9.1, 9.2
float Avx2L2SqI8(const int8_t* a, const int8_t* b, size_t dim) {
  // Use 32-bit integer accumulator to avoid overflow
  __m256i sum = _mm256_setzero_si256();

  size_t i = 0;
  // Process 16 int8 elements at a time
  for (; i + 16 <= dim; i += 16) {
    // Load 16 int8 values
    __m128i va8 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(a + i));
    __m128i vb8 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(b + i));

    // Sign-extend to 16-bit
    __m256i va16 = _mm256_cvtepi8_epi16(va8);
    __m256i vb16 = _mm256_cvtepi8_epi16(vb8);

    // Compute difference
    __m256i diff16 = _mm256_sub_epi16(va16, vb16);

    // Square: multiply low and high parts separately
    __m256i diff_lo = _mm256_mullo_epi16(diff16, diff16);

    // Sign-extend to 32-bit and accumulate
    __m128i diff_lo_128 = _mm256_castsi256_si128(diff_lo);
    __m128i diff_hi_128 = _mm256_extracti128_si256(diff_lo, 1);

    __m256i diff32_lo = _mm256_cvtepi16_epi32(diff_lo_128);
    __m256i diff32_hi = _mm256_cvtepi16_epi32(diff_hi_128);

    sum = _mm256_add_epi32(sum, diff32_lo);
    sum = _mm256_add_epi32(sum, diff32_hi);
  }

  // Horizontal sum of 8 int32 values
  __m128i sum_hi = _mm256_extracti128_si256(sum, 1);
  __m128i sum_lo = _mm256_castsi256_si128(sum);
  __m128i sum128 = _mm_add_epi32(sum_lo, sum_hi);
  sum128 = _mm_hadd_epi32(sum128, sum128);
  sum128 = _mm_hadd_epi32(sum128, sum128);
  int32_t result = _mm_cvtsi128_si32(sum128);

  // Handle tail elements
  for (; i < dim; ++i) {
    int32_t diff = static_cast<int32_t>(a[i]) - static_cast<int32_t>(b[i]);
    result += diff * diff;
  }

  return static_cast<float>(result);
}

// Inner Product - Float32
// Requirements: 4.2, 9.1, 9.2
float Avx2IpF32(const float* a, const float* b, size_t dim) {
  __m256 sum = _mm256_setzero_ps();

  // Process 8 floats at a time
  size_t i = 0;
  for (; i + 8 <= dim; i += 8) {
    __m256 va = _mm256_loadu_ps(a + i);
    __m256 vb = _mm256_loadu_ps(b + i);
    // FMA: sum = va * vb + sum
    sum = _mm256_fmadd_ps(va, vb, sum);
  }

  float result = HorizontalSum(sum);

  // Handle tail elements
  for (; i < dim; ++i) {
    result += a[i] * b[i];
  }

  return result;
}

// Inner Product - Float16
// Requirements: 4.5, 9.1, 9.2
float Avx2IpF16(const uint16_t* a, const uint16_t* b, size_t dim) {
  __m256 sum = _mm256_setzero_ps();

  size_t i = 0;
  for (; i + 8 <= dim; i += 8) {
    alignas(32) float fa[8], fb[8];
    for (size_t j = 0; j < 8; ++j) {
      fa[j] = F16ToF32(a[i + j]);
      fb[j] = F16ToF32(b[i + j]);
    }
    __m256 va = _mm256_load_ps(fa);
    __m256 vb = _mm256_load_ps(fb);
    sum = _mm256_fmadd_ps(va, vb, sum);
  }

  float result = HorizontalSum(sum);

  for (; i < dim; ++i) {
    result += F16ToF32(a[i]) * F16ToF32(b[i]);
  }

  return result;
}

// Inner Product - Int8
// Requirements: 4.7, 9.1, 9.2
float Avx2IpI8(const int8_t* a, const int8_t* b, size_t dim) {
  __m256i sum = _mm256_setzero_si256();

  size_t i = 0;
  for (; i + 16 <= dim; i += 16) {
    __m128i va8 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(a + i));
    __m128i vb8 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(b + i));

    // Sign-extend to 16-bit
    __m256i va16 = _mm256_cvtepi8_epi16(va8);
    __m256i vb16 = _mm256_cvtepi8_epi16(vb8);

    // Multiply
    __m256i prod16 = _mm256_mullo_epi16(va16, vb16);

    // Sign-extend to 32-bit and accumulate
    __m128i prod_lo_128 = _mm256_castsi256_si128(prod16);
    __m128i prod_hi_128 = _mm256_extracti128_si256(prod16, 1);

    __m256i prod32_lo = _mm256_cvtepi16_epi32(prod_lo_128);
    __m256i prod32_hi = _mm256_cvtepi16_epi32(prod_hi_128);

    sum = _mm256_add_epi32(sum, prod32_lo);
    sum = _mm256_add_epi32(sum, prod32_hi);
  }

  // Horizontal sum
  __m128i sum_hi = _mm256_extracti128_si256(sum, 1);
  __m128i sum_lo = _mm256_castsi256_si128(sum);
  __m128i sum128 = _mm_add_epi32(sum_lo, sum_hi);
  sum128 = _mm_hadd_epi32(sum128, sum128);
  sum128 = _mm_hadd_epi32(sum128, sum128);
  int32_t result = _mm_cvtsi128_si32(sum128);

  for (; i < dim; ++i) {
    result += static_cast<int32_t>(a[i]) * static_cast<int32_t>(b[i]);
  }

  return static_cast<float>(result);
}

// Cosine Similarity - Float32
// Requirements: 4.3, 9.1, 9.2
float Avx2CosineF32(const float* a, const float* b, size_t dim) {
  __m256 dot_sum = _mm256_setzero_ps();
  __m256 norm_a_sum = _mm256_setzero_ps();
  __m256 norm_b_sum = _mm256_setzero_ps();

  size_t i = 0;
  for (; i + 8 <= dim; i += 8) {
    __m256 va = _mm256_loadu_ps(a + i);
    __m256 vb = _mm256_loadu_ps(b + i);

    // dot = a * b
    dot_sum = _mm256_fmadd_ps(va, vb, dot_sum);
    // norm_a = a * a
    norm_a_sum = _mm256_fmadd_ps(va, va, norm_a_sum);
    // norm_b = b * b
    norm_b_sum = _mm256_fmadd_ps(vb, vb, norm_b_sum);
  }

  float dot = HorizontalSum(dot_sum);
  float norm_a = HorizontalSum(norm_a_sum);
  float norm_b = HorizontalSum(norm_b_sum);

  // Handle tail elements
  for (; i < dim; ++i) {
    dot += a[i] * b[i];
    norm_a += a[i] * a[i];
    norm_b += b[i] * b[i];
  }

  float denom = std::sqrt(norm_a) * std::sqrt(norm_b);
  return denom > 0.0f ? dot / denom : 0.0f;
}

// Cosine Similarity - Float16
// Requirements: 4.4, 9.1, 9.2
float Avx2CosineF16(const uint16_t* a, const uint16_t* b, size_t dim) {
  __m256 dot_sum = _mm256_setzero_ps();
  __m256 norm_a_sum = _mm256_setzero_ps();
  __m256 norm_b_sum = _mm256_setzero_ps();

  size_t i = 0;
  for (; i + 8 <= dim; i += 8) {
    alignas(32) float fa[8], fb[8];
    for (size_t j = 0; j < 8; ++j) {
      fa[j] = F16ToF32(a[i + j]);
      fb[j] = F16ToF32(b[i + j]);
    }
    __m256 va = _mm256_load_ps(fa);
    __m256 vb = _mm256_load_ps(fb);

    dot_sum = _mm256_fmadd_ps(va, vb, dot_sum);
    norm_a_sum = _mm256_fmadd_ps(va, va, norm_a_sum);
    norm_b_sum = _mm256_fmadd_ps(vb, vb, norm_b_sum);
  }

  float dot = HorizontalSum(dot_sum);
  float norm_a = HorizontalSum(norm_a_sum);
  float norm_b = HorizontalSum(norm_b_sum);

  for (; i < dim; ++i) {
    float fa = F16ToF32(a[i]);
    float fb = F16ToF32(b[i]);
    dot += fa * fb;
    norm_a += fa * fa;
    norm_b += fb * fb;
  }

  float denom = std::sqrt(norm_a) * std::sqrt(norm_b);
  return denom > 0.0f ? dot / denom : 0.0f;
}

// Cosine Similarity - Int8
// Requirements: 4.6, 4.7, 9.1, 9.2
float Avx2CosineI8(const int8_t* a, const int8_t* b, size_t dim) {
  __m256i dot_sum = _mm256_setzero_si256();
  __m256i norm_a_sum = _mm256_setzero_si256();
  __m256i norm_b_sum = _mm256_setzero_si256();

  size_t i = 0;
  for (; i + 16 <= dim; i += 16) {
    __m128i va8 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(a + i));
    __m128i vb8 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(b + i));

    __m256i va16 = _mm256_cvtepi8_epi16(va8);
    __m256i vb16 = _mm256_cvtepi8_epi16(vb8);

    // Compute products
    __m256i dot16 = _mm256_mullo_epi16(va16, vb16);
    __m256i norm_a16 = _mm256_mullo_epi16(va16, va16);
    __m256i norm_b16 = _mm256_mullo_epi16(vb16, vb16);

    // Sign-extend to 32-bit and accumulate
    __m128i dot_lo = _mm256_castsi256_si128(dot16);
    __m128i dot_hi = _mm256_extracti128_si256(dot16, 1);
    dot_sum = _mm256_add_epi32(dot_sum, _mm256_cvtepi16_epi32(dot_lo));
    dot_sum = _mm256_add_epi32(dot_sum, _mm256_cvtepi16_epi32(dot_hi));

    __m128i na_lo = _mm256_castsi256_si128(norm_a16);
    __m128i na_hi = _mm256_extracti128_si256(norm_a16, 1);
    norm_a_sum = _mm256_add_epi32(norm_a_sum, _mm256_cvtepi16_epi32(na_lo));
    norm_a_sum = _mm256_add_epi32(norm_a_sum, _mm256_cvtepi16_epi32(na_hi));

    __m128i nb_lo = _mm256_castsi256_si128(norm_b16);
    __m128i nb_hi = _mm256_extracti128_si256(norm_b16, 1);
    norm_b_sum = _mm256_add_epi32(norm_b_sum, _mm256_cvtepi16_epi32(nb_lo));
    norm_b_sum = _mm256_add_epi32(norm_b_sum, _mm256_cvtepi16_epi32(nb_hi));
  }

  // Horizontal sums
  auto hsum_epi32 = [](__m256i v) -> int64_t {
    __m128i hi = _mm256_extracti128_si256(v, 1);
    __m128i lo = _mm256_castsi256_si128(v);
    __m128i sum128 = _mm_add_epi32(lo, hi);
    sum128 = _mm_hadd_epi32(sum128, sum128);
    sum128 = _mm_hadd_epi32(sum128, sum128);
    return _mm_cvtsi128_si32(sum128);
  };

  int64_t dot = hsum_epi32(dot_sum);
  int64_t norm_a = hsum_epi32(norm_a_sum);
  int64_t norm_b = hsum_epi32(norm_b_sum);

  for (; i < dim; ++i) {
    int32_t ai = static_cast<int32_t>(a[i]);
    int32_t bi = static_cast<int32_t>(b[i]);
    dot += ai * bi;
    norm_a += ai * ai;
    norm_b += bi * bi;
  }

  double denom =
      std::sqrt(static_cast<double>(norm_a)) * std::sqrt(static_cast<double>(norm_b));
  return denom > 0.0 ? static_cast<float>(static_cast<double>(dot) / denom) : 0.0f;
}

// Batch operations with prefetching
// Requirements: 5.1, 5.2, 5.3
void Avx2L2SqF32Batch(const float* query, const float* const* vectors,
                      size_t dim, size_t count, float* results) {
  constexpr size_t kPrefetchDistance = 4;

  for (size_t i = 0; i < count; ++i) {
    if (i + kPrefetchDistance < count) {
      __builtin_prefetch(vectors[i + kPrefetchDistance], 0, 0);
    }
    results[i] = Avx2L2SqF32(query, vectors[i], dim);
  }
}

void Avx2IpF32Batch(const float* query, const float* const* vectors,
                    size_t dim, size_t count, float* results) {
  constexpr size_t kPrefetchDistance = 4;

  for (size_t i = 0; i < count; ++i) {
    if (i + kPrefetchDistance < count) {
      __builtin_prefetch(vectors[i + kPrefetchDistance], 0, 0);
    }
    results[i] = Avx2IpF32(query, vectors[i], dim);
  }
}

}  // namespace

// AVX2 vtable instance
// Requirements: 3.2
const MetricsImpl kMetricsImplAvx2 = {
    .l2sq_f32 = Avx2L2SqF32,
    .l2sq_f16 = Avx2L2SqF16,
    .l2sq_i8 = Avx2L2SqI8,
    .ip_f32 = Avx2IpF32,
    .ip_f16 = Avx2IpF16,
    .ip_i8 = Avx2IpI8,
    .cosine_f32 = Avx2CosineF32,
    .cosine_f16 = Avx2CosineF16,
    .cosine_i8 = Avx2CosineI8,
    .l2sq_f32_batch = Avx2L2SqF32Batch,
    .ip_f32_batch = Avx2IpF32Batch,
    .name = "avx2",
    .caps = kCpuCapAvx2 | kCpuCapFma,
};

}  // namespace internal
}  // namespace metrics
}  // namespace valkey_search

#endif  // __x86_64__ || _M_X64
