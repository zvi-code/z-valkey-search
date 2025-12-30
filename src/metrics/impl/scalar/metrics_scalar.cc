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

#include "src/metrics/metrics_internal.h"

#include <cmath>
#include <cstring>

namespace valkey_search {
namespace metrics {
namespace internal {

namespace {

// Helper function to convert IEEE 754 half-precision (float16) to float32
// Format: 1 sign bit, 5 exponent bits, 10 mantissa bits
inline float F16ToF32(uint16_t h) {
  uint32_t sign = (h & 0x8000) << 16;
  uint32_t exponent = (h >> 10) & 0x1F;
  uint32_t mantissa = h & 0x03FF;

  if (exponent == 0) {
    // Zero or denormalized number
    if (mantissa == 0) {
      // Zero
      uint32_t result = sign;
      float f;
      std::memcpy(&f, &result, sizeof(f));
      return f;
    }
    // Denormalized: convert to normalized
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
    // Inf or NaN
    uint32_t result = sign | 0x7F800000 | (mantissa << 13);
    float f;
    std::memcpy(&f, &result, sizeof(f));
    return f;
  }

  // Normalized number
  exponent += 127 - 15;
  uint32_t result = sign | (exponent << 23) | (mantissa << 13);
  float f;
  std::memcpy(&f, &result, sizeof(f));
  return f;
}

// L2 Squared Distance implementations
float ScalarL2SqF32(const float* a, const float* b, size_t dim) {
  float sum = 0.0f;
  for (size_t i = 0; i < dim; ++i) {
    float diff = a[i] - b[i];
    sum += diff * diff;
  }
  return sum;
}

float ScalarL2SqF16(const uint16_t* a, const uint16_t* b, size_t dim) {
  float sum = 0.0f;
  for (size_t i = 0; i < dim; ++i) {
    float fa = F16ToF32(a[i]);
    float fb = F16ToF32(b[i]);
    float diff = fa - fb;
    sum += diff * diff;
  }
  return sum;
}

float ScalarL2SqI8(const int8_t* a, const int8_t* b, size_t dim) {
  // Use int32 accumulator to avoid overflow
  int32_t sum = 0;
  for (size_t i = 0; i < dim; ++i) {
    int32_t diff = static_cast<int32_t>(a[i]) - static_cast<int32_t>(b[i]);
    sum += diff * diff;
  }
  return static_cast<float>(sum);
}

// Inner Product implementations
float ScalarIpF32(const float* a, const float* b, size_t dim) {
  float sum = 0.0f;
  for (size_t i = 0; i < dim; ++i) {
    sum += a[i] * b[i];
  }
  return sum;
}

float ScalarIpF16(const uint16_t* a, const uint16_t* b, size_t dim) {
  float sum = 0.0f;
  for (size_t i = 0; i < dim; ++i) {
    float fa = F16ToF32(a[i]);
    float fb = F16ToF32(b[i]);
    sum += fa * fb;
  }
  return sum;
}

float ScalarIpI8(const int8_t* a, const int8_t* b, size_t dim) {
  // Use int32 accumulator to avoid overflow
  int32_t sum = 0;
  for (size_t i = 0; i < dim; ++i) {
    sum += static_cast<int32_t>(a[i]) * static_cast<int32_t>(b[i]);
  }
  return static_cast<float>(sum);
}

// Cosine Similarity implementations
float ScalarCosineF32(const float* a, const float* b, size_t dim) {
  float dot = 0.0f;
  float norm_a = 0.0f;
  float norm_b = 0.0f;
  for (size_t i = 0; i < dim; ++i) {
    dot += a[i] * b[i];
    norm_a += a[i] * a[i];
    norm_b += b[i] * b[i];
  }
  float denom = std::sqrt(norm_a) * std::sqrt(norm_b);
  return denom > 0.0f ? dot / denom : 0.0f;
}

float ScalarCosineF16(const uint16_t* a, const uint16_t* b, size_t dim) {
  float dot = 0.0f;
  float norm_a = 0.0f;
  float norm_b = 0.0f;
  for (size_t i = 0; i < dim; ++i) {
    float fa = F16ToF32(a[i]);
    float fb = F16ToF32(b[i]);
    dot += fa * fb;
    norm_a += fa * fa;
    norm_b += fb * fb;
  }
  float denom = std::sqrt(norm_a) * std::sqrt(norm_b);
  return denom > 0.0f ? dot / denom : 0.0f;
}

float ScalarCosineI8(const int8_t* a, const int8_t* b, size_t dim) {
  // Use int32/int64 accumulators to avoid overflow
  int64_t dot = 0;
  int64_t norm_a = 0;
  int64_t norm_b = 0;
  for (size_t i = 0; i < dim; ++i) {
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

// Batch operations with prefetching
void ScalarL2SqF32Batch(const float* query, const float* const* vectors,
                        size_t dim, size_t count, float* results) {
  // Prefetch distance for upcoming vectors (typically 2-4 cache lines ahead)
  constexpr size_t kPrefetchDistance = 4;

  for (size_t i = 0; i < count; ++i) {
    // Prefetch upcoming vector data
    if (i + kPrefetchDistance < count) {
      // Prefetch the pointer and first cache line of the vector
      __builtin_prefetch(vectors[i + kPrefetchDistance], 0, 0);
    }
    results[i] = ScalarL2SqF32(query, vectors[i], dim);
  }
}

void ScalarIpF32Batch(const float* query, const float* const* vectors,
                      size_t dim, size_t count, float* results) {
  // Prefetch distance for upcoming vectors
  constexpr size_t kPrefetchDistance = 4;

  for (size_t i = 0; i < count; ++i) {
    // Prefetch upcoming vector data
    if (i + kPrefetchDistance < count) {
      __builtin_prefetch(vectors[i + kPrefetchDistance], 0, 0);
    }
    results[i] = ScalarIpF32(query, vectors[i], dim);
  }
}

}  // namespace

// Scalar vtable instance
const MetricsImpl kMetricsImplScalar = {
    .l2sq_f32 = ScalarL2SqF32,
    .l2sq_f16 = ScalarL2SqF16,
    .l2sq_i8 = ScalarL2SqI8,
    .ip_f32 = ScalarIpF32,
    .ip_f16 = ScalarIpF16,
    .ip_i8 = ScalarIpI8,
    .cosine_f32 = ScalarCosineF32,
    .cosine_f16 = ScalarCosineF16,
    .cosine_i8 = ScalarCosineI8,
    .l2sq_f32_batch = ScalarL2SqF32Batch,
    .ip_f32_batch = ScalarIpF32Batch,
    .name = "scalar",
    .caps = 0,
};

}  // namespace internal
}  // namespace metrics
}  // namespace valkey_search
