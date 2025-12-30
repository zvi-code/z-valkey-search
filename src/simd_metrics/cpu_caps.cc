/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "src/simd_metrics/cpu_caps.h"

#include <atomic>
#include <cstdio>
#include <cstring>

#if defined(__aarch64__) && defined(__linux__)
#include <sys/auxv.h>
#include <asm/hwcap.h>
#endif

#if defined(__x86_64__)
#include <cpuid.h>
#endif

namespace {

std::atomic<uint32_t> g_caps{0};
std::atomic<bool> g_detected{false};

uint32_t detect_caps_impl() {
  uint32_t caps = CPU_CAP_NONE;

#if defined(__aarch64__) && defined(__linux__)
  unsigned long hwcap = getauxval(AT_HWCAP);
  unsigned long hwcap2 = getauxval(AT_HWCAP2);

  // NEON is always available on AArch64
  caps |= CPU_CAP_NEON;

  // Check for ASIMD dot product (Graviton2+)
  if (hwcap & HWCAP_ASIMDDP) caps |= CPU_CAP_NEON_DOTPROD;

  // Check for FP16 support
  if (hwcap & HWCAP_FPHP) caps |= CPU_CAP_NEON_FP16;

  // Check for BF16 support (Graviton3+)
#ifdef HWCAP2_BF16
  if (hwcap2 & HWCAP2_BF16) caps |= CPU_CAP_NEON_BF16;
#endif

  // Check for I8MM support (Graviton3+)
#ifdef HWCAP2_I8MM
  if (hwcap2 & HWCAP2_I8MM) caps |= CPU_CAP_NEON_I8MM;
#endif

  // Check for SVE support (Graviton3+)
  if (hwcap & HWCAP_SVE) {
    caps |= CPU_CAP_SVE;

#ifdef HWCAP2_SVE2
    if (hwcap2 & HWCAP2_SVE2) caps |= CPU_CAP_SVE2;
#endif
#ifdef HWCAP2_SVEI8MM
    if (hwcap2 & HWCAP2_SVEI8MM) caps |= CPU_CAP_SVE_I8MM;
#endif
#ifdef HWCAP2_SVEBF16
    if (hwcap2 & HWCAP2_SVEBF16) caps |= CPU_CAP_SVE_BF16;
#endif
  }

#elif defined(__aarch64__) && defined(__APPLE__)
  // Apple Silicon always has NEON, FP16, and dotprod
  caps |= CPU_CAP_NEON | CPU_CAP_NEON_FP16 | CPU_CAP_NEON_DOTPROD;

#elif defined(__x86_64__)
  unsigned int eax, ebx, ecx, edx;

  // Check for basic features (leaf 1)
  if (__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
    if (ecx & bit_SSE4_2) caps |= CPU_CAP_SSE42;
    if (ecx & bit_AVX) caps |= CPU_CAP_AVX;
    if (ecx & bit_FMA) caps |= CPU_CAP_FMA;
  }

  // Check for extended features (leaf 7)
  if (__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx)) {
    if (ebx & bit_AVX2) caps |= CPU_CAP_AVX2;
    if (ebx & bit_AVX512F) caps |= CPU_CAP_AVX512F;
    if (ebx & bit_AVX512BW) caps |= CPU_CAP_AVX512BW;
    // AVX512_VNNI is in ECX bit 11
    if (ecx & (1 << 11)) caps |= CPU_CAP_AVX512VNNI;
  }
#endif

  return caps;
}

}  // namespace

extern "C" {

uint32_t cpu_caps_detect(void) {
  if (g_detected.load(std::memory_order_acquire)) {
    return g_caps.load(std::memory_order_relaxed);
  }

  uint32_t caps = detect_caps_impl();
  g_caps.store(caps, std::memory_order_relaxed);
  g_detected.store(true, std::memory_order_release);

  return caps;
}

const char *cpu_caps_to_string(uint32_t caps) {
  static thread_local char buf[512];
  buf[0] = '\0';
  char *p = buf;
  size_t remaining = sizeof(buf);

#define APPEND(flag, name)                                       \
  if (caps & flag) {                                             \
    int written =                                                \
        snprintf(p, remaining, "%s%s", (p != buf) ? "," : "", name); \
    if (written > 0 && static_cast<size_t>(written) < remaining) { \
      p += written;                                              \
      remaining -= written;                                      \
    }                                                            \
  }

  // ARM64
  APPEND(CPU_CAP_NEON, "neon")
  APPEND(CPU_CAP_NEON_DOTPROD, "dotprod")
  APPEND(CPU_CAP_NEON_I8MM, "i8mm")
  APPEND(CPU_CAP_NEON_FP16, "fp16")
  APPEND(CPU_CAP_NEON_BF16, "bf16")
  APPEND(CPU_CAP_SVE, "sve")
  APPEND(CPU_CAP_SVE2, "sve2")
  APPEND(CPU_CAP_SVE_I8MM, "sve_i8mm")
  APPEND(CPU_CAP_SVE_BF16, "sve_bf16")

  // x86_64
  APPEND(CPU_CAP_SSE42, "sse4.2")
  APPEND(CPU_CAP_AVX, "avx")
  APPEND(CPU_CAP_AVX2, "avx2")
  APPEND(CPU_CAP_FMA, "fma")
  APPEND(CPU_CAP_AVX512F, "avx512f")
  APPEND(CPU_CAP_AVX512BW, "avx512bw")
  APPEND(CPU_CAP_AVX512VNNI, "avx512vnni")

#undef APPEND

  if (buf[0] == '\0') {
    return "none";
  }

  return buf;
}

}  // extern "C"
