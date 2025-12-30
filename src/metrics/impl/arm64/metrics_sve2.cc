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

// ARM64 SVE2 implementation with SVE i8mm optimizations
// Compiled with: -march=armv9-a+sve2+i8mm
// Uses SVE2 intrinsics for f32 operations and SVE i8mm for optimized int8

#if defined(__aarch64__) || defined(_M_ARM64)

#include <arm_sve.h>

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

// =============================================================================
// L2 Squared Distance implementations using SVE2
// =============================================================================

float Sve2L2SqF32(const float* a, const float* b, size_t dim) {
  svfloat32_t sum_vec = svdup_f32(0.0f);

  // SVE2 loop with predication - handles tail automatically
  size_t i = 0;
  svbool_t pg = svwhilelt_b32(i, dim);

  while (svptest_first(svptrue_b32(), pg)) {
    svfloat32_t va = svld1_f32(pg, a + i);
    svfloat32_t vb = svld1_f32(pg, b + i);
    svfloat32_t diff = svsub_f32_x(pg, va, vb);
    sum_vec = svmla_f32_x(pg, sum_vec, diff, diff);  // sum += diff * diff

    i += svcntw();  // Increment by vector length (number of 32-bit elements)
    pg = svwhilelt_b32(i, dim);
  }

  // Horizontal sum reduction
  return svaddv_f32(svptrue_b32(), sum_vec);
}

float Sve2L2SqF16(const uint16_t* a, const uint16_t* b, size_t dim) {
  // SVE2 f16 support - fall back to scalar conversion with SVE accumulation
  svfloat32_t sum_vec = svdup_f32(0.0f);

  // Process in chunks, converting f16 to f32
  size_t i = 0;
  while (i < dim) {
    // Determine how many elements to process this iteration
    size_t remaining = dim - i;
    size_t chunk = remaining < 4 ? remaining : 4;

    float fa[4], fb[4];
    for (size_t j = 0; j < chunk; ++j) {
      fa[j] = F16ToF32(a[i + j]);
      fb[j] = F16ToF32(b[i + j]);
    }
    for (size_t j = chunk; j < 4; ++j) {
      fa[j] = 0.0f;
      fb[j] = 0.0f;
    }

    svbool_t pg = svwhilelt_b32(static_cast<size_t>(0), chunk);
    svfloat32_t va = svld1_f32(pg, fa);
    svfloat32_t vb = svld1_f32(pg, fb);
    svfloat32_t diff = svsub_f32_x(pg, va, vb);
    sum_vec = svmla_f32_x(pg, sum_vec, diff, diff);

    i += chunk;
  }

  return svaddv_f32(svptrue_b32(), sum_vec);
}

// L2 squared for int8 using SVE i8mm
// For L2 squared: sum((a[i] - b[i])^2) = sum(a[i]^2) - 2*sum(a[i]*b[i]) +
// sum(b[i]^2) We use svdot for the dot products
float Sve2L2SqI8(const int8_t* a, const int8_t* b, size_t dim) {
  svint32_t sum_aa = svdup_s32(0);
  svint32_t sum_ab = svdup_s32(0);
  svint32_t sum_bb = svdup_s32(0);

  // Process int8 elements using SVE dot product instructions
  size_t i = 0;
  svbool_t pg8 = svwhilelt_b8(i, dim);

  while (svptest_first(svptrue_b8(), pg8)) {
    // Load int8 values
    svint8_t va8 = svld1_s8(pg8, a + i);
    svint8_t vb8 = svld1_s8(pg8, b + i);

    // Use SVE dot product for a*a, a*b, b*b
    // svdot computes dot product of groups of 4 int8 elements
    sum_aa = svdot_s32(sum_aa, va8, va8);
    sum_ab = svdot_s32(sum_ab, va8, vb8);
    sum_bb = svdot_s32(sum_bb, vb8, vb8);

    i += svcntb();  // Increment by number of bytes
    pg8 = svwhilelt_b8(i, dim);
  }

  // Horizontal sum
  int32_t aa = svaddv_s32(svptrue_b32(), sum_aa);
  int32_t ab = svaddv_s32(svptrue_b32(), sum_ab);
  int32_t bb = svaddv_s32(svptrue_b32(), sum_bb);

  // L2^2 = sum(a^2) - 2*sum(a*b) + sum(b^2)
  return static_cast<float>(aa - 2 * ab + bb);
}

// =============================================================================
// Inner Product implementations using SVE2
// =============================================================================

float Sve2IpF32(const float* a, const float* b, size_t dim) {
  svfloat32_t sum_vec = svdup_f32(0.0f);

  size_t i = 0;
  svbool_t pg = svwhilelt_b32(i, dim);

  while (svptest_first(svptrue_b32(), pg)) {
    svfloat32_t va = svld1_f32(pg, a + i);
    svfloat32_t vb = svld1_f32(pg, b + i);
    sum_vec = svmla_f32_x(pg, sum_vec, va, vb);  // sum += a * b

    i += svcntw();
    pg = svwhilelt_b32(i, dim);
  }

  return svaddv_f32(svptrue_b32(), sum_vec);
}

float Sve2IpF16(const uint16_t* a, const uint16_t* b, size_t dim) {
  svfloat32_t sum_vec = svdup_f32(0.0f);

  size_t i = 0;
  while (i < dim) {
    size_t remaining = dim - i;
    size_t chunk = remaining < 4 ? remaining : 4;

    float fa[4], fb[4];
    for (size_t j = 0; j < chunk; ++j) {
      fa[j] = F16ToF32(a[i + j]);
      fb[j] = F16ToF32(b[i + j]);
    }
    for (size_t j = chunk; j < 4; ++j) {
      fa[j] = 0.0f;
      fb[j] = 0.0f;
    }

    svbool_t pg = svwhilelt_b32(static_cast<size_t>(0), chunk);
    svfloat32_t va = svld1_f32(pg, fa);
    svfloat32_t vb = svld1_f32(pg, fb);
    sum_vec = svmla_f32_x(pg, sum_vec, va, vb);

    i += chunk;
  }

  return svaddv_f32(svptrue_b32(), sum_vec);
}

// Inner product for int8 using SVE dot product
float Sve2IpI8(const int8_t* a, const int8_t* b, size_t dim) {
  svint32_t sum_vec = svdup_s32(0);

  size_t i = 0;
  svbool_t pg8 = svwhilelt_b8(i, dim);

  while (svptest_first(svptrue_b8(), pg8)) {
    svint8_t va8 = svld1_s8(pg8, a + i);
    svint8_t vb8 = svld1_s8(pg8, b + i);

    // Use SVE dot product for a*b
    sum_vec = svdot_s32(sum_vec, va8, vb8);

    i += svcntb();
    pg8 = svwhilelt_b8(i, dim);
  }

  return static_cast<float>(svaddv_s32(svptrue_b32(), sum_vec));
}

// =============================================================================
// Cosine Similarity implementations using SVE2
// =============================================================================

float Sve2CosineF32(const float* a, const float* b, size_t dim) {
  svfloat32_t dot_vec = svdup_f32(0.0f);
  svfloat32_t norm_a_vec = svdup_f32(0.0f);
  svfloat32_t norm_b_vec = svdup_f32(0.0f);

  size_t i = 0;
  svbool_t pg = svwhilelt_b32(i, dim);

  while (svptest_first(svptrue_b32(), pg)) {
    svfloat32_t va = svld1_f32(pg, a + i);
    svfloat32_t vb = svld1_f32(pg, b + i);

    dot_vec = svmla_f32_x(pg, dot_vec, va, vb);
    norm_a_vec = svmla_f32_x(pg, norm_a_vec, va, va);
    norm_b_vec = svmla_f32_x(pg, norm_b_vec, vb, vb);

    i += svcntw();
    pg = svwhilelt_b32(i, dim);
  }

  float dot = svaddv_f32(svptrue_b32(), dot_vec);
  float norm_a = svaddv_f32(svptrue_b32(), norm_a_vec);
  float norm_b = svaddv_f32(svptrue_b32(), norm_b_vec);

  float denom = std::sqrt(norm_a) * std::sqrt(norm_b);
  return denom > 0.0f ? dot / denom : 0.0f;
}

float Sve2CosineF16(const uint16_t* a, const uint16_t* b, size_t dim) {
  svfloat32_t dot_vec = svdup_f32(0.0f);
  svfloat32_t norm_a_vec = svdup_f32(0.0f);
  svfloat32_t norm_b_vec = svdup_f32(0.0f);

  size_t i = 0;
  while (i < dim) {
    size_t remaining = dim - i;
    size_t chunk = remaining < 4 ? remaining : 4;

    float fa[4], fb[4];
    for (size_t j = 0; j < chunk; ++j) {
      fa[j] = F16ToF32(a[i + j]);
      fb[j] = F16ToF32(b[i + j]);
    }
    for (size_t j = chunk; j < 4; ++j) {
      fa[j] = 0.0f;
      fb[j] = 0.0f;
    }

    svbool_t pg = svwhilelt_b32(static_cast<size_t>(0), chunk);
    svfloat32_t va = svld1_f32(pg, fa);
    svfloat32_t vb = svld1_f32(pg, fb);

    dot_vec = svmla_f32_x(pg, dot_vec, va, vb);
    norm_a_vec = svmla_f32_x(pg, norm_a_vec, va, va);
    norm_b_vec = svmla_f32_x(pg, norm_b_vec, vb, vb);

    i += chunk;
  }

  float dot = svaddv_f32(svptrue_b32(), dot_vec);
  float norm_a = svaddv_f32(svptrue_b32(), norm_a_vec);
  float norm_b = svaddv_f32(svptrue_b32(), norm_b_vec);

  float denom = std::sqrt(norm_a) * std::sqrt(norm_b);
  return denom > 0.0f ? dot / denom : 0.0f;
}

// Cosine similarity for int8 using SVE dot product
float Sve2CosineI8(const int8_t* a, const int8_t* b, size_t dim) {
  svint32_t dot_vec = svdup_s32(0);
  svint32_t norm_a_vec = svdup_s32(0);
  svint32_t norm_b_vec = svdup_s32(0);

  size_t i = 0;
  svbool_t pg8 = svwhilelt_b8(i, dim);

  while (svptest_first(svptrue_b8(), pg8)) {
    svint8_t va8 = svld1_s8(pg8, a + i);
    svint8_t vb8 = svld1_s8(pg8, b + i);

    // Use SVE dot product for a*b, a*a, b*b
    dot_vec = svdot_s32(dot_vec, va8, vb8);
    norm_a_vec = svdot_s32(norm_a_vec, va8, va8);
    norm_b_vec = svdot_s32(norm_b_vec, vb8, vb8);

    i += svcntb();
    pg8 = svwhilelt_b8(i, dim);
  }

  int64_t dot = svaddv_s32(svptrue_b32(), dot_vec);
  int64_t norm_a = svaddv_s32(svptrue_b32(), norm_a_vec);
  int64_t norm_b = svaddv_s32(svptrue_b32(), norm_b_vec);

  double denom = std::sqrt(static_cast<double>(norm_a)) *
                 std::sqrt(static_cast<double>(norm_b));
  return denom > 0.0 ? static_cast<float>(static_cast<double>(dot) / denom)
                     : 0.0f;
}

// =============================================================================
// Batch operations with prefetching
// =============================================================================

void Sve2L2SqF32Batch(const float* query, const float* const* vectors,
                      size_t dim, size_t count, float* results) {
  constexpr size_t kPrefetchDistance = 4;

  for (size_t i = 0; i < count; ++i) {
    if (i + kPrefetchDistance < count) {
      svprfb(svptrue_b8(), vectors[i + kPrefetchDistance], SV_PLDL1KEEP);
    }
    results[i] = Sve2L2SqF32(query, vectors[i], dim);
  }
}

void Sve2IpF32Batch(const float* query, const float* const* vectors, size_t dim,
                    size_t count, float* results) {
  constexpr size_t kPrefetchDistance = 4;

  for (size_t i = 0; i < count; ++i) {
    if (i + kPrefetchDistance < count) {
      svprfb(svptrue_b8(), vectors[i + kPrefetchDistance], SV_PLDL1KEEP);
    }
    results[i] = Sve2IpF32(query, vectors[i], dim);
  }
}

}  // namespace

// SVE2 vtable instance with SVE i8mm optimizations
const MetricsImpl kMetricsImplSve2 = {
    .l2sq_f32 = Sve2L2SqF32,
    .l2sq_f16 = Sve2L2SqF16,
    .l2sq_i8 = Sve2L2SqI8,
    .ip_f32 = Sve2IpF32,
    .ip_f16 = Sve2IpF16,
    .ip_i8 = Sve2IpI8,
    .cosine_f32 = Sve2CosineF32,
    .cosine_f16 = Sve2CosineF16,
    .cosine_i8 = Sve2CosineI8,
    .l2sq_f32_batch = Sve2L2SqF32Batch,
    .ip_f32_batch = Sve2IpF32Batch,
    .name = "sve2",
    .caps = kCpuCapNeon | kCpuCapSve | kCpuCapSve2 | kCpuCapSveI8mm,
};

}  // namespace internal
}  // namespace metrics
}  // namespace valkey_search

#endif  // __aarch64__ || _M_ARM64
