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

// ARM64 NEON+dotprod implementation
// Compiled with: -march=armv8.2-a+dotprod
// Uses vdotq_s32 for optimized int8 dot product operations
// Requirements: 4.6, 4.7, 9.1, 9.2

#if defined(__aarch64__) || defined(_M_ARM64)

#include <arm_neon.h>

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
// L2 Squared Distance implementations
// F32 and F16 use standard NEON (dotprod doesn't help with floating point)
// =============================================================================

float NeonDotprodL2SqF32(const float* a, const float* b, size_t dim) {
  float32x4_t sum_vec = vdupq_n_f32(0.0f);

  // Process 4 floats at a time using NEON
  size_t i = 0;
  for (; i + 4 <= dim; i += 4) {
    float32x4_t va = vld1q_f32(a + i);
    float32x4_t vb = vld1q_f32(b + i);
    float32x4_t diff = vsubq_f32(va, vb);
    sum_vec = vmlaq_f32(sum_vec, diff, diff);  // sum += diff * diff
  }

  // Horizontal sum of the vector
  float sum = vaddvq_f32(sum_vec);

  // Handle tail elements
  for (; i < dim; ++i) {
    float diff = a[i] - b[i];
    sum += diff * diff;
  }

  return sum;
}

float NeonDotprodL2SqF16(const uint16_t* a, const uint16_t* b, size_t dim) {
  float32x4_t sum_vec = vdupq_n_f32(0.0f);

  // Process 4 elements at a time
  size_t i = 0;
  for (; i + 4 <= dim; i += 4) {
    // Load and convert f16 to f32
    float32x4_t va = {F16ToF32(a[i]), F16ToF32(a[i + 1]), F16ToF32(a[i + 2]),
                      F16ToF32(a[i + 3])};
    float32x4_t vb = {F16ToF32(b[i]), F16ToF32(b[i + 1]), F16ToF32(b[i + 2]),
                      F16ToF32(b[i + 3])};
    float32x4_t diff = vsubq_f32(va, vb);
    sum_vec = vmlaq_f32(sum_vec, diff, diff);
  }

  float sum = vaddvq_f32(sum_vec);

  // Handle tail elements
  for (; i < dim; ++i) {
    float fa = F16ToF32(a[i]);
    float fb = F16ToF32(b[i]);
    float diff = fa - fb;
    sum += diff * diff;
  }

  return sum;
}

// L2 squared for int8 using vdotq_s32 (dotprod extension)
// For L2 squared: sum((a[i] - b[i])^2) = sum(a[i]^2) - 2*sum(a[i]*b[i]) +
// sum(b[i]^2)
float NeonDotprodL2SqI8(const int8_t* a, const int8_t* b, size_t dim) {
  int32x4_t sum_aa = vdupq_n_s32(0);
  int32x4_t sum_ab = vdupq_n_s32(0);
  int32x4_t sum_bb = vdupq_n_s32(0);

  // Process 16 int8 elements at a time using vdotq_s32
  // vdotq_s32 computes dot product of 4 int8 elements and accumulates to int32
  size_t i = 0;
  for (; i + 16 <= dim; i += 16) {
    int8x16_t va = vld1q_s8(a + i);
    int8x16_t vb = vld1q_s8(b + i);

    // Compute a*a, a*b, b*b using dot product instructions
    sum_aa = vdotq_s32(sum_aa, va, va);
    sum_ab = vdotq_s32(sum_ab, va, vb);
    sum_bb = vdotq_s32(sum_bb, vb, vb);
  }

  // Horizontal sum
  int32_t aa = vaddvq_s32(sum_aa);
  int32_t ab = vaddvq_s32(sum_ab);
  int32_t bb = vaddvq_s32(sum_bb);

  // Handle tail elements
  for (; i < dim; ++i) {
    int32_t ai = static_cast<int32_t>(a[i]);
    int32_t bi = static_cast<int32_t>(b[i]);
    aa += ai * ai;
    ab += ai * bi;
    bb += bi * bi;
  }

  // L2^2 = sum(a^2) - 2*sum(a*b) + sum(b^2)
  return static_cast<float>(aa - 2 * ab + bb);
}

// =============================================================================
// Inner Product implementations
// =============================================================================

float NeonDotprodIpF32(const float* a, const float* b, size_t dim) {
  float32x4_t sum_vec = vdupq_n_f32(0.0f);

  // Process 4 floats at a time
  size_t i = 0;
  for (; i + 4 <= dim; i += 4) {
    float32x4_t va = vld1q_f32(a + i);
    float32x4_t vb = vld1q_f32(b + i);
    sum_vec = vmlaq_f32(sum_vec, va, vb);  // sum += a * b
  }

  // Horizontal sum
  float sum = vaddvq_f32(sum_vec);

  // Handle tail elements
  for (; i < dim; ++i) {
    sum += a[i] * b[i];
  }

  return sum;
}

float NeonDotprodIpF16(const uint16_t* a, const uint16_t* b, size_t dim) {
  float32x4_t sum_vec = vdupq_n_f32(0.0f);

  size_t i = 0;
  for (; i + 4 <= dim; i += 4) {
    float32x4_t va = {F16ToF32(a[i]), F16ToF32(a[i + 1]), F16ToF32(a[i + 2]),
                      F16ToF32(a[i + 3])};
    float32x4_t vb = {F16ToF32(b[i]), F16ToF32(b[i + 1]), F16ToF32(b[i + 2]),
                      F16ToF32(b[i + 3])};
    sum_vec = vmlaq_f32(sum_vec, va, vb);
  }

  float sum = vaddvq_f32(sum_vec);

  for (; i < dim; ++i) {
    sum += F16ToF32(a[i]) * F16ToF32(b[i]);
  }

  return sum;
}

// Inner product for int8 using vdotq_s32 (dotprod extension)
float NeonDotprodIpI8(const int8_t* a, const int8_t* b, size_t dim) {
  int32x4_t sum_vec = vdupq_n_s32(0);

  // Process 16 int8 elements at a time using vdotq_s32
  size_t i = 0;
  for (; i + 16 <= dim; i += 16) {
    int8x16_t va = vld1q_s8(a + i);
    int8x16_t vb = vld1q_s8(b + i);
    sum_vec = vdotq_s32(sum_vec, va, vb);
  }

  int32_t sum = vaddvq_s32(sum_vec);

  // Handle tail elements
  for (; i < dim; ++i) {
    sum += static_cast<int32_t>(a[i]) * static_cast<int32_t>(b[i]);
  }

  return static_cast<float>(sum);
}

// =============================================================================
// Cosine Similarity implementations
// =============================================================================

float NeonDotprodCosineF32(const float* a, const float* b, size_t dim) {
  float32x4_t dot_vec = vdupq_n_f32(0.0f);
  float32x4_t norm_a_vec = vdupq_n_f32(0.0f);
  float32x4_t norm_b_vec = vdupq_n_f32(0.0f);

  size_t i = 0;
  for (; i + 4 <= dim; i += 4) {
    float32x4_t va = vld1q_f32(a + i);
    float32x4_t vb = vld1q_f32(b + i);

    dot_vec = vmlaq_f32(dot_vec, va, vb);
    norm_a_vec = vmlaq_f32(norm_a_vec, va, va);
    norm_b_vec = vmlaq_f32(norm_b_vec, vb, vb);
  }

  float dot = vaddvq_f32(dot_vec);
  float norm_a = vaddvq_f32(norm_a_vec);
  float norm_b = vaddvq_f32(norm_b_vec);

  // Handle tail elements
  for (; i < dim; ++i) {
    dot += a[i] * b[i];
    norm_a += a[i] * a[i];
    norm_b += b[i] * b[i];
  }

  float denom = std::sqrt(norm_a) * std::sqrt(norm_b);
  return denom > 0.0f ? dot / denom : 0.0f;
}

float NeonDotprodCosineF16(const uint16_t* a, const uint16_t* b, size_t dim) {
  float32x4_t dot_vec = vdupq_n_f32(0.0f);
  float32x4_t norm_a_vec = vdupq_n_f32(0.0f);
  float32x4_t norm_b_vec = vdupq_n_f32(0.0f);

  size_t i = 0;
  for (; i + 4 <= dim; i += 4) {
    float32x4_t va = {F16ToF32(a[i]), F16ToF32(a[i + 1]), F16ToF32(a[i + 2]),
                      F16ToF32(a[i + 3])};
    float32x4_t vb = {F16ToF32(b[i]), F16ToF32(b[i + 1]), F16ToF32(b[i + 2]),
                      F16ToF32(b[i + 3])};

    dot_vec = vmlaq_f32(dot_vec, va, vb);
    norm_a_vec = vmlaq_f32(norm_a_vec, va, va);
    norm_b_vec = vmlaq_f32(norm_b_vec, vb, vb);
  }

  float dot = vaddvq_f32(dot_vec);
  float norm_a = vaddvq_f32(norm_a_vec);
  float norm_b = vaddvq_f32(norm_b_vec);

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

// Cosine similarity for int8 using vdotq_s32 (dotprod extension)
float NeonDotprodCosineI8(const int8_t* a, const int8_t* b, size_t dim) {
  int32x4_t dot_vec = vdupq_n_s32(0);
  int32x4_t norm_a_vec = vdupq_n_s32(0);
  int32x4_t norm_b_vec = vdupq_n_s32(0);

  // Process 16 int8 elements at a time using vdotq_s32
  size_t i = 0;
  for (; i + 16 <= dim; i += 16) {
    int8x16_t va = vld1q_s8(a + i);
    int8x16_t vb = vld1q_s8(b + i);

    dot_vec = vdotq_s32(dot_vec, va, vb);
    norm_a_vec = vdotq_s32(norm_a_vec, va, va);
    norm_b_vec = vdotq_s32(norm_b_vec, vb, vb);
  }

  int64_t dot = vaddvq_s32(dot_vec);
  int64_t norm_a = vaddvq_s32(norm_a_vec);
  int64_t norm_b = vaddvq_s32(norm_b_vec);

  // Handle tail elements
  for (; i < dim; ++i) {
    int32_t ai = static_cast<int32_t>(a[i]);
    int32_t bi = static_cast<int32_t>(b[i]);
    dot += ai * bi;
    norm_a += ai * ai;
    norm_b += bi * bi;
  }

  double denom = std::sqrt(static_cast<double>(norm_a)) *
                 std::sqrt(static_cast<double>(norm_b));
  return denom > 0.0 ? static_cast<float>(static_cast<double>(dot) / denom)
                     : 0.0f;
}

// =============================================================================
// Batch operations with prefetching
// Requirements: 5.1, 5.2, 5.3
// =============================================================================

void NeonDotprodL2SqF32Batch(const float* query, const float* const* vectors,
                             size_t dim, size_t count, float* results) {
  // Prefetch distance for upcoming vectors (typically 2-4 cache lines ahead)
  constexpr size_t kPrefetchDistance = 4;

  for (size_t i = 0; i < count; ++i) {
    // Prefetch upcoming vector data
    if (i + kPrefetchDistance < count) {
      __builtin_prefetch(vectors[i + kPrefetchDistance], 0, 0);
    }
    results[i] = NeonDotprodL2SqF32(query, vectors[i], dim);
  }
}

void NeonDotprodIpF32Batch(const float* query, const float* const* vectors,
                           size_t dim, size_t count, float* results) {
  // Prefetch distance for upcoming vectors
  constexpr size_t kPrefetchDistance = 4;

  for (size_t i = 0; i < count; ++i) {
    // Prefetch upcoming vector data
    if (i + kPrefetchDistance < count) {
      __builtin_prefetch(vectors[i + kPrefetchDistance], 0, 0);
    }
    results[i] = NeonDotprodIpF32(query, vectors[i], dim);
  }
}

}  // namespace

// NEON+dotprod vtable instance
// Uses NEON for F32/F16 operations and vdotq_s32 for optimized I8 operations
// Requirements: 3.7
const MetricsImpl kMetricsImplNeonDotprod = {
    .l2sq_f32 = NeonDotprodL2SqF32,
    .l2sq_f16 = NeonDotprodL2SqF16,
    .l2sq_i8 = NeonDotprodL2SqI8,
    .ip_f32 = NeonDotprodIpF32,
    .ip_f16 = NeonDotprodIpF16,
    .ip_i8 = NeonDotprodIpI8,
    .cosine_f32 = NeonDotprodCosineF32,
    .cosine_f16 = NeonDotprodCosineF16,
    .cosine_i8 = NeonDotprodCosineI8,
    .l2sq_f32_batch = NeonDotprodL2SqF32Batch,
    .ip_f32_batch = NeonDotprodIpF32Batch,
    .name = "neon_dotprod",
    .caps = kCpuCapNeon | kCpuCapNeonDotprod,
};

}  // namespace internal
}  // namespace metrics
}  // namespace valkey_search

#endif  // __aarch64__ || _M_ARM64
