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

// ARM64 NEON+i8mm implementation
// Compiled with: -march=armv8.2-a+dotprod+i8mm
// Uses vmmlaq_s32 for optimized int8 matrix multiply operations

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
// F32 and F16 use standard NEON (i8mm doesn't help with floating point)
// =============================================================================

float NeonI8mmL2SqF32(const float* a, const float* b, size_t dim) {
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

float NeonI8mmL2SqF16(const uint16_t* a, const uint16_t* b, size_t dim) {
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

// L2 squared for int8 using i8mm instructions
// For L2 squared: sum((a[i] - b[i])^2) = sum(a[i]^2) - 2*sum(a[i]*b[i]) +
// sum(b[i]^2) We use vdotq_s32 for the dot products (available with dotprod
// extension)
float NeonI8mmL2SqI8(const int8_t* a, const int8_t* b, size_t dim) {
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

float NeonI8mmIpF32(const float* a, const float* b, size_t dim) {
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

float NeonI8mmIpF16(const uint16_t* a, const uint16_t* b, size_t dim) {
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
float NeonI8mmIpI8(const int8_t* a, const int8_t* b, size_t dim) {
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

float NeonI8mmCosineF32(const float* a, const float* b, size_t dim) {
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

float NeonI8mmCosineF16(const uint16_t* a, const uint16_t* b, size_t dim) {
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

// Cosine similarity for int8 using vdotq_s32
float NeonI8mmCosineI8(const int8_t* a, const int8_t* b, size_t dim) {
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
// =============================================================================

void NeonI8mmL2SqF32Batch(const float* query, const float* const* vectors,
                          size_t dim, size_t count, float* results) {
  constexpr size_t kPrefetchDistance = 4;

  for (size_t i = 0; i < count; ++i) {
    if (i + kPrefetchDistance < count) {
      __builtin_prefetch(vectors[i + kPrefetchDistance], 0, 0);
    }
    results[i] = NeonI8mmL2SqF32(query, vectors[i], dim);
  }
}

void NeonI8mmIpF32Batch(const float* query, const float* const* vectors,
                        size_t dim, size_t count, float* results) {
  constexpr size_t kPrefetchDistance = 4;

  for (size_t i = 0; i < count; ++i) {
    if (i + kPrefetchDistance < count) {
      __builtin_prefetch(vectors[i + kPrefetchDistance], 0, 0);
    }
    results[i] = NeonI8mmIpF32(query, vectors[i], dim);
  }
}

}  // namespace

// NEON+i8mm vtable instance
// Uses NEON for F32/F16 operations and vdotq_s32 for optimized I8 operations
const MetricsImpl kMetricsImplNeonI8mm = {
    .l2sq_f32 = NeonI8mmL2SqF32,
    .l2sq_f16 = NeonI8mmL2SqF16,
    .l2sq_i8 = NeonI8mmL2SqI8,
    .ip_f32 = NeonI8mmIpF32,
    .ip_f16 = NeonI8mmIpF16,
    .ip_i8 = NeonI8mmIpI8,
    .cosine_f32 = NeonI8mmCosineF32,
    .cosine_f16 = NeonI8mmCosineF16,
    .cosine_i8 = NeonI8mmCosineI8,
    .l2sq_f32_batch = NeonI8mmL2SqF32Batch,
    .ip_f32_batch = NeonI8mmIpF32Batch,
    .name = "neon_i8mm",
    .caps = kCpuCapNeon | kCpuCapNeonDotprod | kCpuCapNeonI8mm,
};

}  // namespace internal
}  // namespace metrics
}  // namespace valkey_search

#endif  // __aarch64__ || _M_ARM64
