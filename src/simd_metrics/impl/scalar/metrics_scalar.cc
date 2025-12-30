/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * Scalar (non-SIMD) baseline implementations.
 * Used as fallback when no SIMD is available.
 */

#include <cmath>
#include <cstddef>
#include <cstdint>

#include "src/simd_metrics/cpu_caps.h"
#include "src/simd_metrics/simd_internal.h"

namespace {

// ============================================
// FP16 conversion helpers (software)
// ============================================

inline float fp16_to_f32(uint16_t h) {
  uint32_t sign = (h & 0x8000) << 16;
  uint32_t exp = (h >> 10) & 0x1F;
  uint32_t mant = h & 0x3FF;

  if (exp == 0) {
    // Denormalized or zero
    if (mant == 0) {
      uint32_t result = sign;
      return *reinterpret_cast<float *>(&result);
    }
    // Denormalized - convert to normalized
    exp = 1;
    while (!(mant & 0x400)) {
      mant <<= 1;
      exp--;
    }
    mant &= 0x3FF;
    exp = 127 - 15 + exp;
  } else if (exp == 31) {
    // Inf or NaN
    exp = 255;
  } else {
    // Normalized
    exp = exp - 15 + 127;
  }

  uint32_t result = sign | (exp << 23) | (mant << 13);
  return *reinterpret_cast<float *>(&result);
}

// ============================================
// L2 Squared Distance
// ============================================

float l2sq_f32_scalar(const float *a, const float *b, size_t dim) {
  float sum = 0.0f;
  for (size_t i = 0; i < dim; i++) {
    float d = a[i] - b[i];
    sum += d * d;
  }
  return sum;
}

float l2sq_f16_scalar(const uint16_t *a, const uint16_t *b, size_t dim) {
  float sum = 0.0f;
  for (size_t i = 0; i < dim; i++) {
    float fa = fp16_to_f32(a[i]);
    float fb = fp16_to_f32(b[i]);
    float d = fa - fb;
    sum += d * d;
  }
  return sum;
}

float l2sq_i8_scalar(const int8_t *a, const int8_t *b, size_t dim) {
  int32_t sum = 0;
  for (size_t i = 0; i < dim; i++) {
    int32_t d = static_cast<int32_t>(a[i]) - static_cast<int32_t>(b[i]);
    sum += d * d;
  }
  return static_cast<float>(sum);
}

// ============================================
// Inner Product
// ============================================

float ip_f32_scalar(const float *a, const float *b, size_t dim) {
  float sum = 0.0f;
  for (size_t i = 0; i < dim; i++) {
    sum += a[i] * b[i];
  }
  return sum;
}

float ip_f16_scalar(const uint16_t *a, const uint16_t *b, size_t dim) {
  float sum = 0.0f;
  for (size_t i = 0; i < dim; i++) {
    float fa = fp16_to_f32(a[i]);
    float fb = fp16_to_f32(b[i]);
    sum += fa * fb;
  }
  return sum;
}

float ip_i8_scalar(const int8_t *a, const int8_t *b, size_t dim) {
  int32_t sum = 0;
  for (size_t i = 0; i < dim; i++) {
    sum += static_cast<int32_t>(a[i]) * static_cast<int32_t>(b[i]);
  }
  return static_cast<float>(sum);
}

// ============================================
// Cosine Similarity
// ============================================

float cosine_f32_scalar(const float *a, const float *b, size_t dim) {
  float dot = 0.0f, norm_a = 0.0f, norm_b = 0.0f;
  for (size_t i = 0; i < dim; i++) {
    dot += a[i] * b[i];
    norm_a += a[i] * a[i];
    norm_b += b[i] * b[i];
  }
  float denom = std::sqrt(norm_a) * std::sqrt(norm_b);
  return (denom > 0.0f) ? (dot / denom) : 0.0f;
}

float cosine_f16_scalar(const uint16_t *a, const uint16_t *b, size_t dim) {
  float dot = 0.0f, norm_a = 0.0f, norm_b = 0.0f;
  for (size_t i = 0; i < dim; i++) {
    float fa = fp16_to_f32(a[i]);
    float fb = fp16_to_f32(b[i]);
    dot += fa * fb;
    norm_a += fa * fa;
    norm_b += fb * fb;
  }
  float denom = std::sqrt(norm_a) * std::sqrt(norm_b);
  return (denom > 0.0f) ? (dot / denom) : 0.0f;
}

float cosine_i8_scalar(const int8_t *a, const int8_t *b, size_t dim) {
  int64_t dot = 0, norm_a = 0, norm_b = 0;
  for (size_t i = 0; i < dim; i++) {
    int32_t ai = static_cast<int32_t>(a[i]);
    int32_t bi = static_cast<int32_t>(b[i]);
    dot += ai * bi;
    norm_a += ai * ai;
    norm_b += bi * bi;
  }
  float denom =
      std::sqrt(static_cast<float>(norm_a)) * std::sqrt(static_cast<float>(norm_b));
  return (denom > 0.0f) ? (static_cast<float>(dot) / denom) : 0.0f;
}

// ============================================
// Batch Operations (with prefetch)
// ============================================

void l2sq_f32_batch_scalar(const float *query, const float *const *vectors,
                           size_t dim, size_t count, float *results) {
  for (size_t i = 0; i < count; i++) {
    // Prefetch next vector
    if (i + 1 < count) {
      __builtin_prefetch(vectors[i + 1], 0, 1);
    }
    results[i] = l2sq_f32_scalar(query, vectors[i], dim);
  }
}

void ip_f32_batch_scalar(const float *query, const float *const *vectors,
                         size_t dim, size_t count, float *results) {
  for (size_t i = 0; i < count; i++) {
    // Prefetch next vector
    if (i + 1 < count) {
      __builtin_prefetch(vectors[i + 1], 0, 1);
    }
    results[i] = ip_f32_scalar(query, vectors[i], dim);
  }
}

}  // namespace

extern "C" {

const metrics_impl_t metrics_impl_scalar = {
    .l2sq_f32 = l2sq_f32_scalar,
    .l2sq_f16 = l2sq_f16_scalar,
    .l2sq_i8 = l2sq_i8_scalar,
    .ip_f32 = ip_f32_scalar,
    .ip_f16 = ip_f16_scalar,
    .ip_i8 = ip_i8_scalar,
    .cosine_f32 = cosine_f32_scalar,
    .cosine_f16 = cosine_f16_scalar,
    .cosine_i8 = cosine_i8_scalar,
    .l2sq_f32_batch = l2sq_f32_batch_scalar,
    .ip_f32_batch = ip_f32_batch_scalar,
    .name = "scalar",
    .caps = CPU_CAP_NONE,
};

}  // extern "C"
