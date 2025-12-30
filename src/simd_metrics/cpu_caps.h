/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#ifndef VALKEY_SEARCH_METRICS_CPU_CAPS_H_
#define VALKEY_SEARCH_METRICS_CPU_CAPS_H_

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  CPU_CAP_NONE = 0,

  // ARM64 baseline
  CPU_CAP_NEON = (1 << 0),

  // ARM64 extensions
  CPU_CAP_NEON_DOTPROD = (1 << 1),
  CPU_CAP_NEON_I8MM = (1 << 2),
  CPU_CAP_NEON_FP16 = (1 << 3),
  CPU_CAP_NEON_BF16 = (1 << 4),

  // SVE
  CPU_CAP_SVE = (1 << 5),
  CPU_CAP_SVE2 = (1 << 6),
  CPU_CAP_SVE_I8MM = (1 << 7),
  CPU_CAP_SVE_BF16 = (1 << 8),

  // x86_64
  CPU_CAP_SSE42 = (1 << 16),
  CPU_CAP_AVX = (1 << 17),
  CPU_CAP_AVX2 = (1 << 18),
  CPU_CAP_FMA = (1 << 19),
  CPU_CAP_AVX512F = (1 << 20),
  CPU_CAP_AVX512BW = (1 << 21),
  CPU_CAP_AVX512VNNI = (1 << 22),
} cpu_cap_t;

// Detect CPU capabilities at runtime (cached after first call)
uint32_t cpu_caps_detect(void);

// Convert capabilities bitmask to human-readable string
const char *cpu_caps_to_string(uint32_t caps);

// Helper to check for a specific capability
static inline bool cpu_has_cap(cpu_cap_t cap) {
  return (cpu_caps_detect() & cap) != 0;
}

#ifdef __cplusplus
}
#endif

#endif  // VALKEY_SEARCH_METRICS_CPU_CAPS_H_
