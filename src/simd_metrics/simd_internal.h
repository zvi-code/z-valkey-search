/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEY_SEARCH_METRICS_INTERNAL_H_
#define VALKEY_SEARCH_METRICS_INTERNAL_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Function pointer types for metric implementations
typedef float (*metrics_fn_f32_t)(const float *, const float *, size_t);
typedef float (*metrics_fn_f16_t)(const uint16_t *, const uint16_t *, size_t);
typedef float (*metrics_fn_i8_t)(const int8_t *, const int8_t *, size_t);

// Batch operation function pointer (for prefetch optimization)
typedef void (*metrics_batch_fn_f32_t)(const float *, const float *const *,
                                       size_t, size_t, float *);

// Vtable structure containing all metric implementations
typedef struct metrics_impl {
  // L2 squared distance
  metrics_fn_f32_t l2sq_f32;
  metrics_fn_f16_t l2sq_f16;
  metrics_fn_i8_t l2sq_i8;

  // Inner product (dot product)
  metrics_fn_f32_t ip_f32;
  metrics_fn_f16_t ip_f16;
  metrics_fn_i8_t ip_i8;

  // Cosine similarity
  metrics_fn_f32_t cosine_f32;
  metrics_fn_f16_t cosine_f16;
  metrics_fn_i8_t cosine_i8;

  // Batch operations with prefetch
  metrics_batch_fn_f32_t l2sq_f32_batch;
  metrics_batch_fn_f32_t ip_f32_batch;

  // Metadata
  const char *name;
  uint32_t caps;
} metrics_impl_t;

// Platform implementations (defined in impl/ subdirectories)
extern const metrics_impl_t metrics_impl_scalar;

#if defined(__aarch64__)
extern const metrics_impl_t metrics_impl_neon;
extern const metrics_impl_t metrics_impl_neon_dotprod;
extern const metrics_impl_t metrics_impl_sve;
#endif

#if defined(__x86_64__)
extern const metrics_impl_t metrics_impl_avx2;
extern const metrics_impl_t metrics_impl_avx512;
#endif

#ifdef __cplusplus
}
#endif

#endif  // VALKEY_SEARCH_METRICS_INTERNAL_H_
