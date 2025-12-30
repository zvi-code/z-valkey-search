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

#include "cpu_caps.h"

#include <atomic>
#include <cstring>

// Platform-specific includes
#if defined(__x86_64__) || defined(_M_X64)
#include <cpuid.h>
#endif

#if defined(__aarch64__) || defined(_M_ARM64)
#if defined(__linux__)
#include <sys/auxv.h>
// Linux ARM64 HWCAP bits
#ifndef HWCAP_ASIMD
#define HWCAP_ASIMD (1 << 1)
#endif
#ifndef HWCAP_ASIMDDP
#define HWCAP_ASIMDDP (1 << 20)
#endif
#ifndef HWCAP_SVE
#define HWCAP_SVE (1 << 22)
#endif
#ifndef HWCAP2_I8MM
#define HWCAP2_I8MM (1 << 13)
#endif
#ifndef HWCAP2_SVE2
#define HWCAP2_SVE2 (1 << 1)
#endif
#ifndef HWCAP2_SVEI8MM
#define HWCAP2_SVEI8MM (1 << 9)
#endif
#ifndef HWCAP2_BF16
#define HWCAP2_BF16 (1 << 14)
#endif
#ifndef HWCAP2_SVEBF16
#define HWCAP2_SVEBF16 (1 << 12)
#endif
#ifndef HWCAP_FPHP
#define HWCAP_FPHP (1 << 9)
#endif
#elif defined(__APPLE__)
#include <sys/sysctl.h>
#include <sys/types.h>
#endif
#endif

namespace valkey_search {
namespace metrics {

namespace {

// Atomic state for caching detected capabilities
// 0 = not detected, 1 = detection in progress, 2 = detected
std::atomic<int> g_caps_state{0};
std::atomic<uint32_t> g_cached_caps{kCpuCapNone};

// Thread-local buffer for CpuCapsToString result
// Maximum size: all capability names + commas + null terminator
constexpr size_t kCapsStringBufferSize = 512;
thread_local char g_caps_string_buffer[kCapsStringBufferSize];

#if defined(__x86_64__) || defined(_M_X64)

uint32_t DetectX86Caps() {
  uint32_t caps = kCpuCapNone;

  // Check CPUID support and get max function
  unsigned int eax, ebx, ecx, edx;

  // Get max basic CPUID function
  if (!__get_cpuid(0, &eax, &ebx, &ecx, &edx)) {
    return caps;
  }
  unsigned int max_basic = eax;

  if (max_basic < 1) {
    return caps;
  }

  // CPUID function 1: Feature flags
  if (__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
    // ECX bit 19: SSE4.1
    // ECX bit 20: SSE4.2
    if (ecx & (1 << 20)) {
      caps |= kCpuCapSse42;
    }
    // ECX bit 28: AVX
    if (ecx & (1 << 28)) {
      caps |= kCpuCapAvx;
    }
    // ECX bit 12: FMA
    if (ecx & (1 << 12)) {
      caps |= kCpuCapFma;
    }
  }

  // CPUID function 7: Extended features
  if (max_basic >= 7) {
    if (__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx)) {
      // EBX bit 5: AVX2
      if (ebx & (1 << 5)) {
        caps |= kCpuCapAvx2;
      }
      // EBX bit 16: AVX512F
      if (ebx & (1 << 16)) {
        caps |= kCpuCapAvx512F;
      }
      // EBX bit 17: AVX512DQ
      if (ebx & (1 << 17)) {
        caps |= kCpuCapAvx512Dq;
      }
      // EBX bit 30: AVX512BW
      if (ebx & (1 << 30)) {
        caps |= kCpuCapAvx512Bw;
      }
      // EBX bit 31: AVX512VL
      if (ebx & (1 << 31)) {
        caps |= kCpuCapAvx512Vl;
      }
      // ECX bit 11: AVX512VNNI
      if (ecx & (1 << 11)) {
        caps |= kCpuCapAvx512Vnni;
      }
    }
  }

  return caps;
}

#endif  // x86_64

#if defined(__aarch64__) || defined(_M_ARM64)

#if defined(__linux__)

uint32_t DetectArm64CapsLinux() {
  uint32_t caps = kCpuCapNone;

  unsigned long hwcap = getauxval(AT_HWCAP);
  unsigned long hwcap2 = getauxval(AT_HWCAP2);

  // NEON (ASIMD) - baseline for ARM64
  if (hwcap & HWCAP_ASIMD) {
    caps |= kCpuCapNeon;
  }

  // Dot product (ASIMDDP) - Armv8.2
  if (hwcap & HWCAP_ASIMDDP) {
    caps |= kCpuCapNeonDotprod;
  }

  // FP16 support
  if (hwcap & HWCAP_FPHP) {
    caps |= kCpuCapNeonFp16;
  }

  // I8MM - Int8 matrix multiply (Armv8.6)
  if (hwcap2 & HWCAP2_I8MM) {
    caps |= kCpuCapNeonI8mm;
  }

  // BF16 - Brain float16
  if (hwcap2 & HWCAP2_BF16) {
    caps |= kCpuCapNeonBf16;
  }

  // SVE - Scalable Vector Extension
  if (hwcap & HWCAP_SVE) {
    caps |= kCpuCapSve;
  }

  // SVE2 - SVE version 2
  if (hwcap2 & HWCAP2_SVE2) {
    caps |= kCpuCapSve2;
  }

  // SVE I8MM
  if (hwcap2 & HWCAP2_SVEI8MM) {
    caps |= kCpuCapSveI8mm;
  }

  // SVE BF16
  if (hwcap2 & HWCAP2_SVEBF16) {
    caps |= kCpuCapSveBf16;
  }

  return caps;
}

#elif defined(__APPLE__)

// Helper to check sysctl feature
bool SysctlHasFeature(const char* name) {
  int value = 0;
  size_t size = sizeof(value);
  if (sysctlbyname(name, &value, &size, nullptr, 0) == 0) {
    return value != 0;
  }
  return false;
}

uint32_t DetectArm64CapsMacOS() {
  uint32_t caps = kCpuCapNone;

  // NEON is always available on Apple Silicon
  caps |= kCpuCapNeon;

  // Check for specific features via sysctl
  // Apple Silicon (M1+) supports these features
  if (SysctlHasFeature("hw.optional.arm.FEAT_DotProd")) {
    caps |= kCpuCapNeonDotprod;
  }

  if (SysctlHasFeature("hw.optional.arm.FEAT_FP16")) {
    caps |= kCpuCapNeonFp16;
  }

  if (SysctlHasFeature("hw.optional.arm.FEAT_I8MM")) {
    caps |= kCpuCapNeonI8mm;
  }

  if (SysctlHasFeature("hw.optional.arm.FEAT_BF16")) {
    caps |= kCpuCapNeonBf16;
  }

  // Note: Apple Silicon does not support SVE/SVE2
  // SVE is primarily a server/HPC feature

  return caps;
}

#endif  // __APPLE__

#endif  // aarch64

uint32_t DetectCapsImpl() {
#if defined(__x86_64__) || defined(_M_X64)
  return DetectX86Caps();
#elif defined(__aarch64__) || defined(_M_ARM64)
#if defined(__linux__)
  return DetectArm64CapsLinux();
#elif defined(__APPLE__)
  return DetectArm64CapsMacOS();
#else
  return kCpuCapNone;
#endif
#else
  return kCpuCapNone;
#endif
}

}  // namespace

uint32_t CpuCapsDetect() {
  // Fast path: already detected
  if (g_caps_state.load(std::memory_order_acquire) == 2) {
    return g_cached_caps.load(std::memory_order_relaxed);
  }

  // Try to be the thread that performs detection
  int expected = 0;
  if (g_caps_state.compare_exchange_strong(expected, 1,
                                           std::memory_order_acq_rel)) {
    // We won the race, perform detection
    uint32_t caps = DetectCapsImpl();
    g_cached_caps.store(caps, std::memory_order_relaxed);
    g_caps_state.store(2, std::memory_order_release);
    return caps;
  }

  // Another thread is detecting, spin until done
  while (g_caps_state.load(std::memory_order_acquire) != 2) {
    // Spin
  }

  return g_cached_caps.load(std::memory_order_relaxed);
}

const char* CpuCapsToString(uint32_t caps) {
  if (caps == kCpuCapNone) {
    g_caps_string_buffer[0] = '\0';
    return g_caps_string_buffer;
  }

  char* ptr = g_caps_string_buffer;
  char* end = g_caps_string_buffer + kCapsStringBufferSize - 1;
  bool first = true;

  auto append_cap = [&](const char* name) {
    if (ptr >= end) return;
    if (!first) {
      *ptr++ = ',';
    }
    first = false;
    size_t len = strlen(name);
    size_t remaining = end - ptr;
    if (len > remaining) len = remaining;
    memcpy(ptr, name, len);
    ptr += len;
  };

  // x86_64 capabilities
  if (caps & kCpuCapSse42) append_cap("sse42");
  if (caps & kCpuCapAvx) append_cap("avx");
  if (caps & kCpuCapAvx2) append_cap("avx2");
  if (caps & kCpuCapFma) append_cap("fma");
  if (caps & kCpuCapAvx512F) append_cap("avx512f");
  if (caps & kCpuCapAvx512Dq) append_cap("avx512dq");
  if (caps & kCpuCapAvx512Bw) append_cap("avx512bw");
  if (caps & kCpuCapAvx512Vl) append_cap("avx512vl");
  if (caps & kCpuCapAvx512Vnni) append_cap("avx512vnni");

  // ARM64 capabilities
  if (caps & kCpuCapNeon) append_cap("neon");
  if (caps & kCpuCapNeonDotprod) append_cap("neon_dotprod");
  if (caps & kCpuCapNeonI8mm) append_cap("neon_i8mm");
  if (caps & kCpuCapNeonFp16) append_cap("neon_fp16");
  if (caps & kCpuCapNeonBf16) append_cap("neon_bf16");
  if (caps & kCpuCapSve) append_cap("sve");
  if (caps & kCpuCapSve2) append_cap("sve2");
  if (caps & kCpuCapSveI8mm) append_cap("sve_i8mm");
  if (caps & kCpuCapSveBf16) append_cap("sve_bf16");

  *ptr = '\0';
  return g_caps_string_buffer;
}

}  // namespace metrics
}  // namespace valkey_search
