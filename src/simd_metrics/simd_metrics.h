/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 *
 * High-performance SIMD-accelerated distance metrics.
 *
 * This module provides optimized implementations for vector similarity
 * computations used in HNSW and other vector search algorithms.
 *
 * Features:
 * - One-time capability detection at module load
 * - Single indirect call in hot path (no per-call branches)
 * - Cross-compile support (build on Graviton2, run optimally on Graviton4)
 * - Safe for dlopen()-loaded modules
 */

#ifndef VALKEY_SEARCH_SIMD_METRICS_H_
#define VALKEY_SEARCH_SIMD_METRICS_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================
// Lifecycle
// ============================================

// Initialize the metrics subsystem. Must be called before any metric functions.
// Safe to call multiple times (idempotent). Thread-safe.
// Returns 0 on success, non-zero on failure.
int metrics_init(void);

// Cleanup the metrics subsystem. Safe to call multiple times.
void metrics_fini(void);

// ============================================
// Debug / Logging
// ============================================

// Get the name of the currently selected implementation (e.g., "sve", "neon")
const char *metrics_impl_name(void);

// Get the capability bitmask of the current implementation
uint32_t metrics_impl_caps(void);

// ============================================
// L2 Squared Distance
// ============================================

// Compute squared Euclidean distance: sum((a[i] - b[i])^2)
float metrics_l2sq_f32(const float *a, const float *b, size_t dim);
float metrics_l2sq_f16(const uint16_t *a, const uint16_t *b, size_t dim);
float metrics_l2sq_i8(const int8_t *a, const int8_t *b, size_t dim);

// ============================================
// Inner Product (Dot Product)
// ============================================

// Compute inner product: sum(a[i] * b[i])
float metrics_ip_f32(const float *a, const float *b, size_t dim);
float metrics_ip_f16(const uint16_t *a, const uint16_t *b, size_t dim);
float metrics_ip_i8(const int8_t *a, const int8_t *b, size_t dim);

// ============================================
// Cosine Similarity
// ============================================

// Compute cosine similarity: dot(a,b) / (norm(a) * norm(b))
float metrics_cosine_f32(const float *a, const float *b, size_t dim);
float metrics_cosine_f16(const uint16_t *a, const uint16_t *b, size_t dim);
float metrics_cosine_i8(const int8_t *a, const int8_t *b, size_t dim);

// ============================================
// Batch Operations (with prefetch optimization)
// ============================================

// Compute L2 squared distance from query to multiple vectors
void metrics_l2sq_f32_batch(const float *query, const float *const *vectors,
                            size_t dim, size_t count, float *results);

// Compute inner product from query to multiple vectors
void metrics_ip_f32_batch(const float *query, const float *const *vectors,
                          size_t dim, size_t count, float *results);

// ============================================
// HNSW-compatible wrappers
// ============================================

// These match the DISTFUNC signature: float (*)(const void*, const void*, const
// void*)
float metrics_l2sq_f32_hnsw(const void *a, const void *b, const void *dim_ptr);
float metrics_ip_distance_f32_hnsw(const void *a, const void *b,
                                   const void *dim_ptr);

#ifdef __cplusplus
}
#endif

#endif  // VALKEY_SEARCH_SIMD_METRICS_H_
