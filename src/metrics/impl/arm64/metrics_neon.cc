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

// ARM64 NEON baseline implementation
// Compiled with: -march=armv8-a

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
// =============================================================================

float NeonL2SqF32(const float* a, const float* b, size_t dim) {
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

float NeonL2SqF16(const uint16_t* a, const uint16_t* b, size_t dim) {
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

float NeonL2SqI8(const int8_t* a, const int8_t* b, size_t dim) {
  int32x4_t sum_vec = vdupq_n_s32(0);

  // Process 8 int8 elements at a time
  size_t i = 0;
  for (; i + 8 <= dim; i += 8) {
    // Load 8 int8 values
    int8x8_t va = vld1_s8(a + i);
    int8x8_t vb = vld1_s8(b + i);

    // Widen to int16 and compute difference
    int16x8_t va_wide = vmovl_s8(va);
    int16x8_t vb_wide = vmovl_s8(vb);
    int16x8_t diff = vsubq_s16(va_wide, vb_wide);

    // Square and accumulate (split into low and high parts)
    int16x4_t diff_low = vget_low_s16(diff);
    int16x4_t diff_high = vget_high_s16(diff);

    sum_vec = vmlal_s16(sum_vec, diff_low, diff_low);
    sum_vec = vmlal_s16(sum_vec, diff_high, diff_high);
  }

  // Horizontal sum
  int32_t sum = vaddvq_s32(sum_vec);

  // Handle tail elements
  for (; i < dim; ++i) {
    int32_t diff = static_cast<int32_t>(a[i]) - static_cast<int32_t>(b[i]);
    sum += diff * diff;
  }

  return static_cast<float>(sum);
}

// =============================================================================
// Inner Product implementations
// =============================================================================

float NeonIpF32(const float* a, const float* b, size_t dim) {
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

float NeonIpF16(const uint16_t* a, const uint16_t* b, size_t dim) {
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

float NeonIpI8(const int8_t* a, const int8_t* b, size_t dim) {
  int32x4_t sum_vec = vdupq_n_s32(0);

  size_t i = 0;
  for (; i + 8 <= dim; i += 8) {
    int8x8_t va = vld1_s8(a + i);
    int8x8_t vb = vld1_s8(b + i);

    int16x8_t va_wide = vmovl_s8(va);
    int16x8_t vb_wide = vmovl_s8(vb);

    int16x4_t va_low = vget_low_s16(va_wide);
    int16x4_t va_high = vget_high_s16(va_wide);
    int16x4_t vb_low = vget_low_s16(vb_wide);
    int16x4_t vb_high = vget_high_s16(vb_wide);

    sum_vec = vmlal_s16(sum_vec, va_low, vb_low);
    sum_vec = vmlal_s16(sum_vec, va_high, vb_high);
  }

  int32_t sum = vaddvq_s32(sum_vec);

  for (; i < dim; ++i) {
    sum += static_cast<int32_t>(a[i]) * static_cast<int32_t>(b[i]);
  }

  return static_cast<float>(sum);
}

// =============================================================================
// Cosine Similarity implementations
// =============================================================================

float NeonCosineF32(const float* a, const float* b, size_t dim) {
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

float NeonCosineF16(const uint16_t* a, const uint16_t* b, size_t dim) {
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

float NeonCosineI8(const int8_t* a, const int8_t* b, size_t dim) {
  int32x4_t dot_vec = vdupq_n_s32(0);
  int32x4_t norm_a_vec = vdupq_n_s32(0);
  int32x4_t norm_b_vec = vdupq_n_s32(0);

  size_t i = 0;
  for (; i + 8 <= dim; i += 8) {
    int8x8_t va = vld1_s8(a + i);
    int8x8_t vb = vld1_s8(b + i);

    int16x8_t va_wide = vmovl_s8(va);
    int16x8_t vb_wide = vmovl_s8(vb);

    int16x4_t va_low = vget_low_s16(va_wide);
    int16x4_t va_high = vget_high_s16(va_wide);
    int16x4_t vb_low = vget_low_s16(vb_wide);
    int16x4_t vb_high = vget_high_s16(vb_wide);

    dot_vec = vmlal_s16(dot_vec, va_low, vb_low);
    dot_vec = vmlal_s16(dot_vec, va_high, vb_high);
    norm_a_vec = vmlal_s16(norm_a_vec, va_low, va_low);
    norm_a_vec = vmlal_s16(norm_a_vec, va_high, va_high);
    norm_b_vec = vmlal_s16(norm_b_vec, vb_low, vb_low);
    norm_b_vec = vmlal_s16(norm_b_vec, vb_high, vb_high);
  }

  int64_t dot = vaddvq_s32(dot_vec);
  int64_t norm_a = vaddvq_s32(norm_a_vec);
  int64_t norm_b = vaddvq_s32(norm_b_vec);

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

void NeonL2SqF32Batch(const float* query, const float* const* vectors,
                      size_t dim, size_t count, float* results) {
  constexpr size_t kPrefetchDistance = 4;

  for (size_t i = 0; i < count; ++i) {
    if (i + kPrefetchDistance < count) {
      __builtin_prefetch(vectors[i + kPrefetchDistance], 0, 0);
    }
    results[i] = NeonL2SqF32(query, vectors[i], dim);
  }
}

void NeonIpF32Batch(const float* query, const float* const* vectors,
                    size_t dim, size_t count, float* results) {
  constexpr size_t kPrefetchDistance = 4;

  for (size_t i = 0; i < count; ++i) {
    if (i + kPrefetchDistance < count) {
      __builtin_prefetch(vectors[i + kPrefetchDistance], 0, 0);
    }
    results[i] = NeonIpF32(query, vectors[i], dim);
  }
}

}  // namespace

// NEON vtable instance
const MetricsImpl kMetricsImplNeon = {
    .l2sq_f32 = NeonL2SqF32,
    .l2sq_f16 = NeonL2SqF16,
    .l2sq_i8 = NeonL2SqI8,
    .ip_f32 = NeonIpF32,
    .ip_f16 = NeonIpF16,
    .ip_i8 = NeonIpI8,
    .cosine_f32 = NeonCosineF32,
    .cosine_f16 = NeonCosineF16,
    .cosine_i8 = NeonCosineI8,
    .l2sq_f32_batch = NeonL2SqF32Batch,
    .ip_f32_batch = NeonIpF32Batch,
    .name = "neon",
    .caps = kCpuCapNeon,
};

}  // namespace internal
}  // namespace metrics
}  // namespace valkey_search

#endif  // __aarch64__ || _M_ARM64
