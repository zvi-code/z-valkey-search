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

// AVX512 implementation of similarity metrics
// Compiled with: -mavx512f -mavx512dq -mavx512bw -mavx512vl
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
// Uses _mm512 intrinsics for 512-bit SIMD (16 floats at a time)
// Uses masked operations for tail handling
float Avx512L2SqF32(const float* a, const float* b, size_t dim) {
  __m512 sum = _mm512_setzero_ps();

  // Process 16 floats at a time
  size_t i = 0;
  for (; i + 16 <= dim; i += 16) {
    __m512 va = _mm512_loadu_ps(a + i);
    __m512 vb = _mm512_loadu_ps(b + i);
    __m512 diff = _mm512_sub_ps(va, vb);
    // FMA: sum = diff * diff + sum
    sum = _mm512_fmadd_ps(diff, diff, sum);
  }

  // Handle tail elements using masked operations
  size_t remaining = dim - i;
  if (remaining > 0) {
    // Create mask for remaining elements
    __mmask16 mask = (__mmask16)((1U << remaining) - 1);
    __m512 va = _mm512_maskz_loadu_ps(mask, a + i);
    __m512 vb = _mm512_maskz_loadu_ps(mask, b + i);
    __m512 diff = _mm512_sub_ps(va, vb);
    sum = _mm512_fmadd_ps(diff, diff, sum);
  }

  // Horizontal sum of 16 floats
  return _mm512_reduce_add_ps(sum);
}

// L2 Squared Distance - Float16
// Requirements: 4.4, 9.1, 9.2
float Avx512L2SqF16(const uint16_t* a, const uint16_t* b, size_t dim) {
  // AVX512 doesn't have native F16 support in base instruction set
  // Convert and use F32 path
  __m512 sum = _mm512_setzero_ps();

  size_t i = 0;
  for (; i + 16 <= dim; i += 16) {
    // Convert 16 F16 values to F32
    alignas(64) float fa[16], fb[16];
    for (size_t j = 0; j < 16; ++j) {
      fa[j] = F16ToF32(a[i + j]);
      fb[j] = F16ToF32(b[i + j]);
    }
    __m512 va = _mm512_load_ps(fa);
    __m512 vb = _mm512_load_ps(fb);
    __m512 diff = _mm512_sub_ps(va, vb);
    sum = _mm512_fmadd_ps(diff, diff, sum);
  }

  float result = _mm512_reduce_add_ps(sum);

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
float Avx512L2SqI8(const int8_t* a, const int8_t* b, size_t dim) {
  // Use 32-bit integer accumulator to avoid overflow
  __m512i sum = _mm512_setzero_si512();

  size_t i = 0;
  // Process 32 int8 elements at a time
  for (; i + 32 <= dim; i += 32) {
    // Load 32 int8 values
    __m256i va8 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i));
    __m256i vb8 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + i));

    // Sign-extend to 16-bit
    __m512i va16 = _mm512_cvtepi8_epi16(va8);
    __m512i vb16 = _mm512_cvtepi8_epi16(vb8);

    // Compute difference
    __m512i diff16 = _mm512_sub_epi16(va16, vb16);

    // Square: multiply low parts
    __m512i diff_sq = _mm512_mullo_epi16(diff16, diff16);

    // Sign-extend to 32-bit and accumulate
    // Process low 16 elements
    __m256i diff_sq_lo = _mm512_castsi512_si256(diff_sq);
    __m256i diff_sq_hi = _mm512_extracti64x4_epi64(diff_sq, 1);

    __m512i diff32_lo = _mm512_cvtepi16_epi32(diff_sq_lo);
    __m512i diff32_hi = _mm512_cvtepi16_epi32(diff_sq_hi);

    sum = _mm512_add_epi32(sum, diff32_lo);
    sum = _mm512_add_epi32(sum, diff32_hi);
  }

  // Horizontal sum of 16 int32 values
  int32_t result = _mm512_reduce_add_epi32(sum);

  // Handle tail elements
  for (; i < dim; ++i) {
    int32_t diff = static_cast<int32_t>(a[i]) - static_cast<int32_t>(b[i]);
    result += diff * diff;
  }

  return static_cast<float>(result);
}

// Inner Product - Float32
// Requirements: 4.2, 9.1, 9.2
float Avx512IpF32(const float* a, const float* b, size_t dim) {
  __m512 sum = _mm512_setzero_ps();

  // Process 16 floats at a time
  size_t i = 0;
  for (; i + 16 <= dim; i += 16) {
    __m512 va = _mm512_loadu_ps(a + i);
    __m512 vb = _mm512_loadu_ps(b + i);
    // FMA: sum = va * vb + sum
    sum = _mm512_fmadd_ps(va, vb, sum);
  }

  // Handle tail elements using masked operations
  size_t remaining = dim - i;
  if (remaining > 0) {
    __mmask16 mask = (__mmask16)((1U << remaining) - 1);
    __m512 va = _mm512_maskz_loadu_ps(mask, a + i);
    __m512 vb = _mm512_maskz_loadu_ps(mask, b + i);
    sum = _mm512_fmadd_ps(va, vb, sum);
  }

  return _mm512_reduce_add_ps(sum);
}

// Inner Product - Float16
// Requirements: 4.5, 9.1, 9.2
float Avx512IpF16(const uint16_t* a, const uint16_t* b, size_t dim) {
  __m512 sum = _mm512_setzero_ps();

  size_t i = 0;
  for (; i + 16 <= dim; i += 16) {
    alignas(64) float fa[16], fb[16];
    for (size_t j = 0; j < 16; ++j) {
      fa[j] = F16ToF32(a[i + j]);
      fb[j] = F16ToF32(b[i + j]);
    }
    __m512 va = _mm512_load_ps(fa);
    __m512 vb = _mm512_load_ps(fb);
    sum = _mm512_fmadd_ps(va, vb, sum);
  }

  float result = _mm512_reduce_add_ps(sum);

  for (; i < dim; ++i) {
    result += F16ToF32(a[i]) * F16ToF32(b[i]);
  }

  return result;
}

// Inner Product - Int8
// Requirements: 4.7, 9.1, 9.2
float Avx512IpI8(const int8_t* a, const int8_t* b, size_t dim) {
  __m512i sum = _mm512_setzero_si512();

  size_t i = 0;
  for (; i + 32 <= dim; i += 32) {
    __m256i va8 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i));
    __m256i vb8 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + i));

    // Sign-extend to 16-bit
    __m512i va16 = _mm512_cvtepi8_epi16(va8);
    __m512i vb16 = _mm512_cvtepi8_epi16(vb8);

    // Multiply
    __m512i prod16 = _mm512_mullo_epi16(va16, vb16);

    // Sign-extend to 32-bit and accumulate
    __m256i prod_lo = _mm512_castsi512_si256(prod16);
    __m256i prod_hi = _mm512_extracti64x4_epi64(prod16, 1);

    __m512i prod32_lo = _mm512_cvtepi16_epi32(prod_lo);
    __m512i prod32_hi = _mm512_cvtepi16_epi32(prod_hi);

    sum = _mm512_add_epi32(sum, prod32_lo);
    sum = _mm512_add_epi32(sum, prod32_hi);
  }

  // Horizontal sum
  int32_t result = _mm512_reduce_add_epi32(sum);

  for (; i < dim; ++i) {
    result += static_cast<int32_t>(a[i]) * static_cast<int32_t>(b[i]);
  }

  return static_cast<float>(result);
}

// Cosine Similarity - Float32
// Requirements: 4.3, 9.1, 9.2
float Avx512CosineF32(const float* a, const float* b, size_t dim) {
  __m512 dot_sum = _mm512_setzero_ps();
  __m512 norm_a_sum = _mm512_setzero_ps();
  __m512 norm_b_sum = _mm512_setzero_ps();

  size_t i = 0;
  for (; i + 16 <= dim; i += 16) {
    __m512 va = _mm512_loadu_ps(a + i);
    __m512 vb = _mm512_loadu_ps(b + i);

    // dot = a * b
    dot_sum = _mm512_fmadd_ps(va, vb, dot_sum);
    // norm_a = a * a
    norm_a_sum = _mm512_fmadd_ps(va, va, norm_a_sum);
    // norm_b = b * b
    norm_b_sum = _mm512_fmadd_ps(vb, vb, norm_b_sum);
  }

  // Handle tail elements using masked operations
  size_t remaining = dim - i;
  if (remaining > 0) {
    __mmask16 mask = (__mmask16)((1U << remaining) - 1);
    __m512 va = _mm512_maskz_loadu_ps(mask, a + i);
    __m512 vb = _mm512_maskz_loadu_ps(mask, b + i);

    dot_sum = _mm512_fmadd_ps(va, vb, dot_sum);
    norm_a_sum = _mm512_fmadd_ps(va, va, norm_a_sum);
    norm_b_sum = _mm512_fmadd_ps(vb, vb, norm_b_sum);
  }

  float dot = _mm512_reduce_add_ps(dot_sum);
  float norm_a = _mm512_reduce_add_ps(norm_a_sum);
  float norm_b = _mm512_reduce_add_ps(norm_b_sum);

  float denom = std::sqrt(norm_a) * std::sqrt(norm_b);
  return denom > 0.0f ? dot / denom : 0.0f;
}

// Cosine Similarity - Float16
// Requirements: 4.4, 9.1, 9.2
float Avx512CosineF16(const uint16_t* a, const uint16_t* b, size_t dim) {
  __m512 dot_sum = _mm512_setzero_ps();
  __m512 norm_a_sum = _mm512_setzero_ps();
  __m512 norm_b_sum = _mm512_setzero_ps();

  size_t i = 0;
  for (; i + 16 <= dim; i += 16) {
    alignas(64) float fa[16], fb[16];
    for (size_t j = 0; j < 16; ++j) {
      fa[j] = F16ToF32(a[i + j]);
      fb[j] = F16ToF32(b[i + j]);
    }
    __m512 va = _mm512_load_ps(fa);
    __m512 vb = _mm512_load_ps(fb);

    dot_sum = _mm512_fmadd_ps(va, vb, dot_sum);
    norm_a_sum = _mm512_fmadd_ps(va, va, norm_a_sum);
    norm_b_sum = _mm512_fmadd_ps(vb, vb, norm_b_sum);
  }

  float dot = _mm512_reduce_add_ps(dot_sum);
  float norm_a = _mm512_reduce_add_ps(norm_a_sum);
  float norm_b = _mm512_reduce_add_ps(norm_b_sum);

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
float Avx512CosineI8(const int8_t* a, const int8_t* b, size_t dim) {
  __m512i dot_sum = _mm512_setzero_si512();
  __m512i norm_a_sum = _mm512_setzero_si512();
  __m512i norm_b_sum = _mm512_setzero_si512();

  size_t i = 0;
  for (; i + 32 <= dim; i += 32) {
    __m256i va8 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i));
    __m256i vb8 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + i));

    __m512i va16 = _mm512_cvtepi8_epi16(va8);
    __m512i vb16 = _mm512_cvtepi8_epi16(vb8);

    // Compute products
    __m512i dot16 = _mm512_mullo_epi16(va16, vb16);
    __m512i norm_a16 = _mm512_mullo_epi16(va16, va16);
    __m512i norm_b16 = _mm512_mullo_epi16(vb16, vb16);

    // Sign-extend to 32-bit and accumulate
    __m256i dot_lo = _mm512_castsi512_si256(dot16);
    __m256i dot_hi = _mm512_extracti64x4_epi64(dot16, 1);
    dot_sum = _mm512_add_epi32(dot_sum, _mm512_cvtepi16_epi32(dot_lo));
    dot_sum = _mm512_add_epi32(dot_sum, _mm512_cvtepi16_epi32(dot_hi));

    __m256i na_lo = _mm512_castsi512_si256(norm_a16);
    __m256i na_hi = _mm512_extracti64x4_epi64(norm_a16, 1);
    norm_a_sum = _mm512_add_epi32(norm_a_sum, _mm512_cvtepi16_epi32(na_lo));
    norm_a_sum = _mm512_add_epi32(norm_a_sum, _mm512_cvtepi16_epi32(na_hi));

    __m256i nb_lo = _mm512_castsi512_si256(norm_b16);
    __m256i nb_hi = _mm512_extracti64x4_epi64(norm_b16, 1);
    norm_b_sum = _mm512_add_epi32(norm_b_sum, _mm512_cvtepi16_epi32(nb_lo));
    norm_b_sum = _mm512_add_epi32(norm_b_sum, _mm512_cvtepi16_epi32(nb_hi));
  }

  // Horizontal sums
  int64_t dot = _mm512_reduce_add_epi32(dot_sum);
  int64_t norm_a = _mm512_reduce_add_epi32(norm_a_sum);
  int64_t norm_b = _mm512_reduce_add_epi32(norm_b_sum);

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
void Avx512L2SqF32Batch(const float* query, const float* const* vectors,
                        size_t dim, size_t count, float* results) {
  // Prefetch distance for upcoming vectors (typically 2-4 cache lines ahead)
  constexpr size_t kPrefetchDistance = 4;

  for (size_t i = 0; i < count; ++i) {
    // Prefetch upcoming vector data
    if (i + kPrefetchDistance < count) {
      __builtin_prefetch(vectors[i + kPrefetchDistance], 0, 0);
    }
    results[i] = Avx512L2SqF32(query, vectors[i], dim);
  }
}

void Avx512IpF32Batch(const float* query, const float* const* vectors,
                      size_t dim, size_t count, float* results) {
  // Prefetch distance for upcoming vectors
  constexpr size_t kPrefetchDistance = 4;

  for (size_t i = 0; i < count; ++i) {
    // Prefetch upcoming vector data
    if (i + kPrefetchDistance < count) {
      __builtin_prefetch(vectors[i + kPrefetchDistance], 0, 0);
    }
    results[i] = Avx512IpF32(query, vectors[i], dim);
  }
}

}  // namespace

// AVX512 vtable instance
// Requirements: 3.1
const MetricsImpl kMetricsImplAvx512 = {
    .l2sq_f32 = Avx512L2SqF32,
    .l2sq_f16 = Avx512L2SqF16,
    .l2sq_i8 = Avx512L2SqI8,
    .ip_f32 = Avx512IpF32,
    .ip_f16 = Avx512IpF16,
    .ip_i8 = Avx512IpI8,
    .cosine_f32 = Avx512CosineF32,
    .cosine_f16 = Avx512CosineF16,
    .cosine_i8 = Avx512CosineI8,
    .l2sq_f32_batch = Avx512L2SqF32Batch,
    .ip_f32_batch = Avx512IpF32Batch,
    .name = "avx512",
    .caps = kCpuCapAvx512F | kCpuCapAvx512Dq | kCpuCapAvx512Bw | kCpuCapAvx512Vl,
};

}  // namespace internal
}  // namespace metrics
}  // namespace valkey_search

#endif  // __x86_64__ || _M_X64
