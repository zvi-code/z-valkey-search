# Valkey-Search Similarity Metrics Dispatch: Design Document

## Overview

This design replaces SimSIMD's lazy per-call dispatch with one-time vtable resolution at module load. A single pointer to the optimal implementation struct is selected based on runtime capability detection, eliminating per-call branching overhead.

## Design Principles

1. Single capability detection at module load
2. One pointer dereference + indirect call in hot path (no branches)
3. Clean separation between architecture-specific implementations
4. Cross-compile on Graviton2, run optimally on Graviton4
5. Safe operation within `dlopen()`-loaded module context

---

## Current State Analysis

### SimSIMD Dispatch Pattern

```c
SIMSIMD_METRIC_DECLARATION(l2sq, f32, f32)

// Expands to:
void simsimd_l2sq_f32(const float* a, const float* b, size_t n, float* result) {
    static simsimd_metric_punned_t metric = 0;
    if (metric == 0) {                              // Branch every call
        simsimd_find_metric_punned(..., &metric);   // Capability lookup
        if (!metric) { *result = NaN; return; }
    }
    metric(a, b, n, result);                        // Indirect call
}
```

### Problems

| Issue | Impact |
|-------|--------|
| Per-call branch | Pipeline stall, misprediction on first call |
| Static variable load | Memory access per call |
| ~30 duplicate dispatch stubs | Code bloat |
| Mixed concerns | Capability detection interleaved with metric logic |

---

## Architecture

### Directory Structure

```
valkey-search/
└── src/
    └── metrics/
        ├── metrics.h                 # Public API
        ├── metrics_internal.h        # Vtable definition
        ├── metrics_dispatch.c        # Init + thin dispatch wrappers
        ├── cpu_caps.h
        ├── cpu_caps.c
        ├── impl/
        │   ├── scalar/
        │   │   └── metrics_scalar.c
        │   └── arm64/
        │       ├── metrics_neon.c            # -march=armv8-a
        │       ├── metrics_neon_dotprod.c    # -march=armv8.2-a+dotprod
        │       ├── metrics_neon_i8mm.c       # -march=armv8.2-a+dotprod+i8mm
        │       ├── metrics_sve.c             # -march=armv8.2-a+sve
        │       └── metrics_sve2.c            # -march=armv9-a+sve2+i8mm
        └── tests/
            ├── test_metrics.c
            └── bench_metrics.c
```

### Runtime Flow

```
Module Load (OnLoad)
    │
    └── metrics_init()
            │
            ├── cpu_caps_detect()
            │       │
            │       └── getauxval(AT_HWCAP/AT_HWCAP2)
            │
            └── g_impl = &metrics_impl_sve2   ← One-time selection
                         (or _sve, _neon_i8mm, _neon_dotprod, _neon)

HNSW Hot Path
    │
    └── metrics_l2sq_f32(a, b, dim)
            │
            └── g_impl->l2sq_f32(a, b, dim)   ← Single indirect call
```

---

## API

### Public Interface

```c
// ============================================
// metrics.h
// ============================================
#pragma once

#include <stddef.h>
#include <stdint.h>

// Lifecycle
int metrics_init(void);
void metrics_fini(void);

// Debug/logging
const char* metrics_impl_name(void);
uint32_t metrics_impl_caps(void);

// L2 squared distance
float metrics_l2sq_f32(const float* a, const float* b, size_t dim);
float metrics_l2sq_f16(const uint16_t* a, const uint16_t* b, size_t dim);
float metrics_l2sq_i8(const int8_t* a, const int8_t* b, size_t dim);

// Inner product
float metrics_ip_f32(const float* a, const float* b, size_t dim);
float metrics_ip_f16(const uint16_t* a, const uint16_t* b, size_t dim);
float metrics_ip_i8(const int8_t* a, const int8_t* b, size_t dim);

// Cosine similarity
float metrics_cosine_f32(const float* a, const float* b, size_t dim);
float metrics_cosine_f16(const uint16_t* a, const uint16_t* b, size_t dim);
float metrics_cosine_i8(const int8_t* a, const int8_t* b, size_t dim);

// Batch operations
void metrics_l2sq_f32_batch(
    const float* query,
    const float* const* vectors,
    size_t dim,
    size_t count,
    float* results
);

void metrics_ip_f32_batch(
    const float* query,
    const float* const* vectors,
    size_t dim,
    size_t count,
    float* results
);
```

### Internal Vtable

```c
// ============================================
// metrics_internal.h
// ============================================
#pragma once

#include <stddef.h>
#include <stdint.h>

typedef float (*metrics_fn_f32_t)(const float*, const float*, size_t);
typedef float (*metrics_fn_f16_t)(const uint16_t*, const uint16_t*, size_t);
typedef float (*metrics_fn_i8_t)(const int8_t*, const int8_t*, size_t);

typedef void (*metrics_batch_fn_f32_t)(
    const float*,
    const float* const*,
    size_t,
    size_t,
    float*
);

typedef struct metrics_impl {
    // L2 squared
    metrics_fn_f32_t l2sq_f32;
    metrics_fn_f16_t l2sq_f16;
    metrics_fn_i8_t  l2sq_i8;

    // Inner product
    metrics_fn_f32_t ip_f32;
    metrics_fn_f16_t ip_f16;
    metrics_fn_i8_t  ip_i8;

    // Cosine
    metrics_fn_f32_t cosine_f32;
    metrics_fn_f16_t cosine_f16;
    metrics_fn_i8_t  cosine_i8;

    // Batch
    metrics_batch_fn_f32_t l2sq_f32_batch;
    metrics_batch_fn_f32_t ip_f32_batch;

    // Metadata
    const char* name;
    uint32_t    caps;
} metrics_impl_t;

// Platform implementations
extern const metrics_impl_t metrics_impl_scalar;

#if defined(__aarch64__)
extern const metrics_impl_t metrics_impl_neon;
extern const metrics_impl_t metrics_impl_neon_dotprod;
extern const metrics_impl_t metrics_impl_neon_i8mm;
extern const metrics_impl_t metrics_impl_sve;
extern const metrics_impl_t metrics_impl_sve2;
#endif
```

---

## CPU Capability Detection

```c
// ============================================
// cpu_caps.h
// ============================================
#pragma once

#include <stdint.h>
#include <stdbool.h>

typedef enum {
    CPU_CAP_NONE         = 0,

    // ARM64 baseline
    CPU_CAP_NEON         = (1 << 0),

    // ARM64 extensions
    CPU_CAP_NEON_DOTPROD = (1 << 1),
    CPU_CAP_NEON_I8MM    = (1 << 2),
    CPU_CAP_NEON_FP16    = (1 << 3),
    CPU_CAP_NEON_BF16    = (1 << 4),

    // SVE
    CPU_CAP_SVE          = (1 << 5),
    CPU_CAP_SVE2         = (1 << 6),
    CPU_CAP_SVE_I8MM     = (1 << 7),
    CPU_CAP_SVE_BF16     = (1 << 8),
} cpu_cap_t;

uint32_t cpu_caps_detect(void);
const char* cpu_caps_to_string(uint32_t caps);

static inline bool cpu_has_cap(cpu_cap_t cap) {
    return (cpu_caps_detect() & cap) != 0;
}
```

```c
// ============================================
// cpu_caps.c
// ============================================
#include "cpu_caps.h"
#include <stdatomic.h>

#if defined(__aarch64__) && defined(__linux__)
#include <sys/auxv.h>
#include <asm/hwcap.h>
#endif

static atomic_uint g_caps = 0;
static atomic_bool g_detected = false;

uint32_t cpu_caps_detect(void) {
    if (atomic_load_explicit(&g_detected, memory_order_acquire)) {
        return atomic_load_explicit(&g_caps, memory_order_relaxed);
    }

    uint32_t caps = CPU_CAP_NONE;

#if defined(__aarch64__) && defined(__linux__)
    unsigned long hwcap = getauxval(AT_HWCAP);
    unsigned long hwcap2 = getauxval(AT_HWCAP2);

    caps |= CPU_CAP_NEON;

    if (hwcap & HWCAP_ASIMDDP)   caps |= CPU_CAP_NEON_DOTPROD;
    if (hwcap & HWCAP_FPHP)     caps |= CPU_CAP_NEON_FP16;
    if (hwcap2 & HWCAP2_BF16)   caps |= CPU_CAP_NEON_BF16;
    if (hwcap2 & HWCAP2_I8MM)   caps |= CPU_CAP_NEON_I8MM;

    if (hwcap & HWCAP_SVE) {
        caps |= CPU_CAP_SVE;
        if (hwcap2 & HWCAP2_SVE2)    caps |= CPU_CAP_SVE2;
        if (hwcap2 & HWCAP2_SVEI8MM) caps |= CPU_CAP_SVE_I8MM;
        if (hwcap2 & HWCAP2_SVEBF16) caps |= CPU_CAP_SVE_BF16;
    }

#elif defined(__aarch64__) && defined(__APPLE__)
    caps |= CPU_CAP_NEON | CPU_CAP_NEON_FP16 | CPU_CAP_NEON_DOTPROD;
#endif

    atomic_store_explicit(&g_caps, caps, memory_order_relaxed);
    atomic_store_explicit(&g_detected, true, memory_order_release);

    return caps;
}

const char* cpu_caps_to_string(uint32_t caps) {
    static _Thread_local char buf[256];
    buf[0] = '\0';
    char* p = buf;

#define APPEND(flag, name) \
    if (caps & flag) { \
        if (p != buf) *p++ = ','; \
        p += snprintf(p, sizeof(buf) - (p - buf), "%s", name); \
    }

    APPEND(CPU_CAP_NEON, "neon")
    APPEND(CPU_CAP_NEON_DOTPROD, "dotprod")
    APPEND(CPU_CAP_NEON_I8MM, "i8mm")
    APPEND(CPU_CAP_NEON_FP16, "fp16")
    APPEND(CPU_CAP_NEON_BF16, "bf16")
    APPEND(CPU_CAP_SVE, "sve")
    APPEND(CPU_CAP_SVE2, "sve2")

#undef APPEND

    return buf;
}
```

---

## Dispatch Logic

```c
// ============================================
// metrics_dispatch.c
// ============================================
#include "metrics.h"
#include "metrics_internal.h"
#include "cpu_caps.h"
#include <stdatomic.h>

static const metrics_impl_t* g_impl = NULL;
static atomic_int g_init_state = 0;

int metrics_init(void) {
    int expected = 0;

    if (atomic_load_explicit(&g_init_state, memory_order_acquire) == 2) {
        return 0;
    }

    if (!atomic_compare_exchange_strong(&g_init_state, &expected, 1)) {
        while (atomic_load_explicit(&g_init_state, memory_order_acquire) != 2) {
            // spin
        }
        return 0;
    }

    uint32_t caps = cpu_caps_detect();

#if defined(__aarch64__)
    if ((caps & CPU_CAP_SVE2) && (caps & CPU_CAP_SVE_I8MM)) {
        g_impl = &metrics_impl_sve2;
    } else if ((caps & CPU_CAP_SVE) && (caps & CPU_CAP_NEON_I8MM)) {
        g_impl = &metrics_impl_sve;
    } else if (caps & CPU_CAP_NEON_I8MM) {
        g_impl = &metrics_impl_neon_i8mm;
    } else if (caps & CPU_CAP_NEON_DOTPROD) {
        g_impl = &metrics_impl_neon_dotprod;
    } else {
        g_impl = &metrics_impl_neon;
    }
#else
    g_impl = &metrics_impl_scalar;
#endif

    atomic_store_explicit(&g_init_state, 2, memory_order_release);
    return 0;
}

void metrics_fini(void) {
    g_impl = NULL;
    atomic_store_explicit(&g_init_state, 0, memory_order_release);
}

const char* metrics_impl_name(void) {
    return g_impl ? g_impl->name : "uninitialized";
}

uint32_t metrics_impl_caps(void) {
    return g_impl ? g_impl->caps : 0;
}

// Dispatch — single indirect call, no branches
float metrics_l2sq_f32(const float* a, const float* b, size_t dim) {
    return g_impl->l2sq_f32(a, b, dim);
}

float metrics_l2sq_f16(const uint16_t* a, const uint16_t* b, size_t dim) {
    return g_impl->l2sq_f16(a, b, dim);
}

float metrics_l2sq_i8(const int8_t* a, const int8_t* b, size_t dim) {
    return g_impl->l2sq_i8(a, b, dim);
}

float metrics_ip_f32(const float* a, const float* b, size_t dim) {
    return g_impl->ip_f32(a, b, dim);
}

float metrics_ip_f16(const uint16_t* a, const uint16_t* b, size_t dim) {
    return g_impl->ip_f16(a, b, dim);
}

float metrics_ip_i8(const int8_t* a, const int8_t* b, size_t dim) {
    return g_impl->ip_i8(a, b, dim);
}

float metrics_cosine_f32(const float* a, const float* b, size_t dim) {
    return g_impl->cosine_f32(a, b, dim);
}

float metrics_cosine_f16(const uint16_t* a, const uint16_t* b, size_t dim) {
    return g_impl->cosine_f16(a, b, dim);
}

float metrics_cosine_i8(const int8_t* a, const int8_t* b, size_t dim) {
    return g_impl->cosine_i8(a, b, dim);
}

void metrics_l2sq_f32_batch(
    const float* query,
    const float* const* vectors,
    size_t dim,
    size_t count,
    float* results
) {
    g_impl->l2sq_f32_batch(query, vectors, dim, count, results);
}

void metrics_ip_f32_batch(
    const float* query,
    const float* const* vectors,
    size_t dim,
    size_t count,
    float* results
) {
    g_impl->ip_f32_batch(query, vectors, dim, count, results);
}
```

---

## Implementation Examples

### Scalar Baseline

```c
// ============================================
// impl/scalar/metrics_scalar.c
// Compile: -O3
// ============================================
#include "metrics_internal.h"
#include <math.h>

static float l2sq_f32(const float* a, const float* b, size_t dim) {
    float sum = 0.0f;
    for (size_t i = 0; i < dim; i++) {
        float d = a[i] - b[i];
        sum += d * d;
    }
    return sum;
}

static float ip_f32(const float* a, const float* b, size_t dim) {
    float sum = 0.0f;
    for (size_t i = 0; i < dim; i++) {
        sum += a[i] * b[i];
    }
    return sum;
}

static float cosine_f32(const float* a, const float* b, size_t dim) {
    float dot = 0.0f, norm_a = 0.0f, norm_b = 0.0f;
    for (size_t i = 0; i < dim; i++) {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    float denom = sqrtf(norm_a) * sqrtf(norm_b);
    return (denom > 0.0f) ? (dot / denom) : 0.0f;
}

static void l2sq_f32_batch(
    const float* query,
    const float* const* vectors,
    size_t dim,
    size_t count,
    float* results
) {
    for (size_t i = 0; i < count; i++) {
        if (i + 1 < count) {
            __builtin_prefetch(vectors[i + 1], 0, 1);
        }
        results[i] = l2sq_f32(query, vectors[i], dim);
    }
}

static void ip_f32_batch(
    const float* query,
    const float* const* vectors,
    size_t dim,
    size_t count,
    float* results
) {
    for (size_t i = 0; i < count; i++) {
        if (i + 1 < count) {
            __builtin_prefetch(vectors[i + 1], 0, 1);
        }
        results[i] = ip_f32(query, vectors[i], dim);
    }
}

// Stub implementations for types not optimized in scalar
static float l2sq_f16(const uint16_t* a, const uint16_t* b, size_t dim) {
    // FP16 → FP32 conversion, compute, return
    float sum = 0.0f;
    for (size_t i = 0; i < dim; i++) {
        float fa = fp16_to_f32(a[i]);
        float fb = fp16_to_f32(b[i]);
        float d = fa - fb;
        sum += d * d;
    }
    return sum;
}

static float l2sq_i8(const int8_t* a, const int8_t* b, size_t dim) {
    int32_t sum = 0;
    for (size_t i = 0; i < dim; i++) {
        int32_t d = (int32_t)a[i] - (int32_t)b[i];
        sum += d * d;
    }
    return (float)sum;
}

// ... remaining stubs

const metrics_impl_t metrics_impl_scalar = {
    .l2sq_f32       = l2sq_f32,
    .l2sq_f16       = l2sq_f16,
    .l2sq_i8        = l2sq_i8,
    .ip_f32         = ip_f32,
    .ip_f16         = ip_f16,
    .ip_i8          = ip_i8,
    .cosine_f32     = cosine_f32,
    .cosine_f16     = cosine_f16,
    .cosine_i8      = cosine_i8,
    .l2sq_f32_batch = l2sq_f32_batch,
    .ip_f32_batch   = ip_f32_batch,
    .name           = "scalar",
    .caps           = CPU_CAP_NONE,
};
```

### SVE Implementation (Graviton3)

```c
// ============================================
// impl/arm64/metrics_sve.c
// Compile: -O3 -march=armv8.2-a+sve
// ============================================
#include "metrics_internal.h"
#include <arm_sve.h>

static float l2sq_f32(const float* a, const float* b, size_t dim) {
    svfloat32_t sum = svdup_f32(0.0f);
    size_t i = 0;

    while (i < dim) {
        svbool_t pg = svwhilelt_b32(i, dim);
        svfloat32_t va = svld1_f32(pg, a + i);
        svfloat32_t vb = svld1_f32(pg, b + i);
        svfloat32_t diff = svsub_f32_x(pg, va, vb);
        sum = svmla_f32_x(pg, sum, diff, diff);
        i += svcntw();
    }

    return svaddv_f32(svptrue_b32(), sum);
}

static float ip_f32(const float* a, const float* b, size_t dim) {
    svfloat32_t sum = svdup_f32(0.0f);
    size_t i = 0;

    while (i < dim) {
        svbool_t pg = svwhilelt_b32(i, dim);
        svfloat32_t va = svld1_f32(pg, a + i);
        svfloat32_t vb = svld1_f32(pg, b + i);
        sum = svmla_f32_x(pg, sum, va, vb);
        i += svcntw();
    }

    return svaddv_f32(svptrue_b32(), sum);
}

static float cosine_f32(const float* a, const float* b, size_t dim) {
    svfloat32_t dot_sum = svdup_f32(0.0f);
    svfloat32_t norm_a_sum = svdup_f32(0.0f);
    svfloat32_t norm_b_sum = svdup_f32(0.0f);
    size_t i = 0;

    while (i < dim) {
        svbool_t pg = svwhilelt_b32(i, dim);
        svfloat32_t va = svld1_f32(pg, a + i);
        svfloat32_t vb = svld1_f32(pg, b + i);

        dot_sum = svmla_f32_x(pg, dot_sum, va, vb);
        norm_a_sum = svmla_f32_x(pg, norm_a_sum, va, va);
        norm_b_sum = svmla_f32_x(pg, norm_b_sum, vb, vb);

        i += svcntw();
    }

    float dot = svaddv_f32(svptrue_b32(), dot_sum);
    float norm_a = svaddv_f32(svptrue_b32(), norm_a_sum);
    float norm_b = svaddv_f32(svptrue_b32(), norm_b_sum);

    float denom = sqrtf(norm_a) * sqrtf(norm_b);
    return (denom > 0.0f) ? (dot / denom) : 0.0f;
}

static void l2sq_f32_batch(
    const float* query,
    const float* const* vectors,
    size_t dim,
    size_t count,
    float* results
) {
    const size_t prefetch_dist = 4;

    for (size_t i = 0; i < count; i++) {
        if (i + prefetch_dist < count) {
            svprfw(svptrue_b32(), vectors[i + prefetch_dist], SV_PLDL1STRM);
        }
        results[i] = l2sq_f32(query, vectors[i], dim);
    }
}

static void ip_f32_batch(
    const float* query,
    const float* const* vectors,
    size_t dim,
    size_t count,
    float* results
) {
    const size_t prefetch_dist = 4;

    for (size_t i = 0; i < count; i++) {
        if (i + prefetch_dist < count) {
            svprfw(svptrue_b32(), vectors[i + prefetch_dist], SV_PLDL1STRM);
        }
        results[i] = ip_f32(query, vectors[i], dim);
    }
}

// ... l2sq_f16, l2sq_i8, etc.

const metrics_impl_t metrics_impl_sve = {
    .l2sq_f32       = l2sq_f32,
    .l2sq_f16       = l2sq_f16,
    .l2sq_i8        = l2sq_i8,
    .ip_f32         = ip_f32,
    .ip_f16         = ip_f16,
    .ip_i8          = ip_i8,
    .cosine_f32     = cosine_f32,
    .cosine_f16     = cosine_f16,
    .cosine_i8      = cosine_i8,
    .l2sq_f32_batch = l2sq_f32_batch,
    .ip_f32_batch   = ip_f32_batch,
    .name           = "sve",
    .caps           = CPU_CAP_NEON | CPU_CAP_SVE,
};
```

### NEON + i8mm Implementation (Graviton3 INT8)

```c
// ============================================
// impl/arm64/metrics_neon_i8mm.c
// Compile: -O3 -march=armv8.2-a+dotprod+i8mm
// ============================================
#include "metrics_internal.h"
#include <arm_neon.h>

static float ip_i8(const int8_t* a, const int8_t* b, size_t dim) {
    int32x4_t sum = vdupq_n_s32(0);
    size_t i = 0;

    // i8mm: SMMLA processes 8x8 → 4x32 accumulate
    for (; i + 16 <= dim; i += 16) {
        int8x16_t va = vld1q_s8(a + i);
        int8x16_t vb = vld1q_s8(b + i);
        sum = vmmlaq_s32(sum, va, vb);
    }

    // Tail with dotprod
    for (; i + 8 <= dim; i += 8) {
        int8x8_t va = vld1_s8(a + i);
        int8x8_t vb = vld1_s8(b + i);
        sum = vdot_s32(vget_low_s32(sum), va, vb);
    }

    int32_t result = vaddvq_s32(sum);

    for (; i < dim; i++) {
        result += (int32_t)a[i] * (int32_t)b[i];
    }

    return (float)result;
}

// ... other functions reuse neon_dotprod implementations for f32

const metrics_impl_t metrics_impl_neon_i8mm = {
    .l2sq_f32       = l2sq_f32_neon,    // Reuse from neon_dotprod
    .l2sq_f16       = l2sq_f16_neon,
    .l2sq_i8        = l2sq_i8,          // i8mm version
    .ip_f32         = ip_f32_neon,
    .ip_f16         = ip_f16_neon,
    .ip_i8          = ip_i8,            // i8mm version
    .cosine_f32     = cosine_f32_neon,
    .cosine_f16     = cosine_f16_neon,
    .cosine_i8      = cosine_i8,
    .l2sq_f32_batch = l2sq_f32_batch_neon,
    .ip_f32_batch   = ip_f32_batch_neon,
    .name           = "neon_i8mm",
    .caps           = CPU_CAP_NEON | CPU_CAP_NEON_DOTPROD | CPU_CAP_NEON_I8MM,
};
```

---

## Build System

```cmake
# ============================================
# src/metrics/CMakeLists.txt
# ============================================

set(METRICS_BASE_FLAGS "-O3 -fPIC -Wall -Wextra")

set(METRICS_SOURCES
    cpu_caps.c
    metrics_dispatch.c
    impl/scalar/metrics_scalar.c
)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
    list(APPEND METRICS_SOURCES
        impl/arm64/metrics_neon.c
        impl/arm64/metrics_neon_dotprod.c
        impl/arm64/metrics_neon_i8mm.c
        impl/arm64/metrics_sve.c
        impl/arm64/metrics_sve2.c
    )

    # Explicit -march flags for cross-compilation
    # Build on Graviton2, target Graviton4
    set_source_files_properties(impl/arm64/metrics_neon.c
        PROPERTIES COMPILE_FLAGS "${METRICS_BASE_FLAGS} -march=armv8-a")

    set_source_files_properties(impl/arm64/metrics_neon_dotprod.c
        PROPERTIES COMPILE_FLAGS "${METRICS_BASE_FLAGS} -march=armv8.2-a+dotprod")

    set_source_files_properties(impl/arm64/metrics_neon_i8mm.c
        PROPERTIES COMPILE_FLAGS "${METRICS_BASE_FLAGS} -march=armv8.2-a+dotprod+i8mm")

    set_source_files_properties(impl/arm64/metrics_sve.c
        PROPERTIES COMPILE_FLAGS "${METRICS_BASE_FLAGS} -march=armv8.2-a+sve")

    set_source_files_properties(impl/arm64/metrics_sve2.c
        PROPERTIES COMPILE_FLAGS "${METRICS_BASE_FLAGS} -march=armv9-a+sve2+i8mm")
endif()

add_library(valkey_metrics STATIC ${METRICS_SOURCES})
target_include_directories(valkey_metrics PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
```

---

## Module Integration

```c
// ============================================
// module.c
// ============================================
#include "metrics.h"

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "search", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (metrics_init() != 0) {
        RedisModule_Log(ctx, "warning", "Failed to initialize metrics");
        return REDISMODULE_ERR;
    }

    RedisModule_Log(ctx, "notice", "Metrics: impl=%s caps=[%s]",
        metrics_impl_name(),
        cpu_caps_to_string(metrics_impl_caps()));

    // ... register commands

    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx) {
    metrics_fini();
    return REDISMODULE_OK;
}
```

```c
// ============================================
// index/hnsw/hnsw.c
// ============================================
#include "metrics.h"

// Before:
// simsimd_l2sq_f32(a, b, dim, &dist);

// After:
float dist = metrics_l2sq_f32(a, b, dim);
```

---

## Testing

### Correctness

```c
// tests/test_metrics.c

static float ref_l2sq_f32(const float* a, const float* b, size_t dim) {
    double sum = 0.0;
    for (size_t i = 0; i < dim; i++) {
        double d = (double)a[i] - (double)b[i];
        sum += d * d;
    }
    return (float)sum;
}

void test_impl_correctness(const metrics_impl_t* impl) {
    const size_t dims[] = {1, 3, 4, 7, 8, 15, 16, 31, 32,
                           63, 64, 127, 128, 256, 768, 1536};

    for (size_t d = 0; d < sizeof(dims)/sizeof(dims[0]); d++) {
        size_t dim = dims[d];
        float* a = aligned_alloc(64, dim * sizeof(float));
        float* b = aligned_alloc(64, dim * sizeof(float));

        for (size_t i = 0; i < dim; i++) {
            a[i] = (float)rand() / RAND_MAX * 2.0f - 1.0f;
            b[i] = (float)rand() / RAND_MAX * 2.0f - 1.0f;
        }

        float expected = ref_l2sq_f32(a, b, dim);
        float actual = impl->l2sq_f32(a, b, dim);

        float rel_err = fabsf(actual - expected) / fmaxf(fabsf(expected), 1e-6f);
        assert(rel_err < 1e-5f);

        free(a);
        free(b);
    }

    printf("[PASS] %s\n", impl->name);
}
```

### Benchmark

```c
// tests/bench_metrics.c

#define ITERATIONS 1000000

void bench(const char* name, size_t dim) {
    float* a = aligned_alloc(64, dim * sizeof(float));
    float* b = aligned_alloc(64, dim * sizeof(float));

    for (size_t i = 0; i < dim; i++) {
        a[i] = (float)i / dim;
        b[i] = (float)(dim - i) / dim;
    }

    volatile float result;
    struct timespec t0, t1;

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < ITERATIONS; i++) {
        result = metrics_l2sq_f32(a, b, dim);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    double ns = ((t1.tv_sec - t0.tv_sec) * 1e9 + (t1.tv_nsec - t0.tv_nsec)) / ITERATIONS;

    printf("%s dim=%zu: %.1f ns/op\n", name, dim, ns);

    free(a);
    free(b);
}

int main(void) {
    metrics_init();
    printf("impl: %s\n\n", metrics_impl_name());

    bench("l2sq_f32", 128);
    bench("l2sq_f32", 768);
    bench("l2sq_f32", 1536);

    return 0;
}
```

---

## Migration Path

| Phase | Action |
|-------|--------|
| 1 | Add `src/metrics/` alongside existing SimSIMD integration |
| 2 | Build both, validate correctness against SimSIMD |
| 3 | Benchmark A/B comparison |
| 4 | Switch HNSW to new API |
| 5 | Remove SimSIMD dependency |

---

## Summary

| Aspect | SimSIMD | This Design |
|--------|---------|-------------|
| Per-call overhead | Branch + static load + indirect | Indirect call only |
| Init timing | Lazy (first call per metric) | Once at module load |
| Cross-compile | Pragma per function | Per-file `-march` flags |
| Code organization | Macro-generated stubs | Explicit vtable + separate TUs |
| Module safety | N/A | Safe for `dlopen()` lifecycle |