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

#pragma once

#include <cstdint>

namespace valkey_search {
namespace metrics {

// CPU capability flags as a bitmask
// x86_64 capabilities use bits 0-15
// ARM64 capabilities use bits 16-31
enum CpuCap : uint32_t {
  kCpuCapNone = 0,

  // x86_64 capabilities (bits 0-15)
  kCpuCapSse42 = (1 << 0),
  kCpuCapAvx = (1 << 1),
  kCpuCapAvx2 = (1 << 2),
  kCpuCapFma = (1 << 3),
  kCpuCapAvx512F = (1 << 4),
  kCpuCapAvx512Dq = (1 << 5),
  kCpuCapAvx512Bw = (1 << 6),
  kCpuCapAvx512Vl = (1 << 7),
  kCpuCapAvx512Vnni = (1 << 8),

  // ARM64 baseline (bit 16)
  kCpuCapNeon = (1 << 16),

  // ARM64 extensions (bits 17-20)
  kCpuCapNeonDotprod = (1 << 17),
  kCpuCapNeonI8mm = (1 << 18),
  kCpuCapNeonFp16 = (1 << 19),
  kCpuCapNeonBf16 = (1 << 20),

  // ARM64 SVE (bits 21-24)
  kCpuCapSve = (1 << 21),
  kCpuCapSve2 = (1 << 22),
  kCpuCapSveI8mm = (1 << 23),
  kCpuCapSveBf16 = (1 << 24),
};

// Detect CPU capabilities at runtime.
// Results are cached after first detection.
// Returns a bitmask of CpuCap flags.
uint32_t CpuCapsDetect();

// Convert capability bitmask to human-readable string.
// Returns a comma-separated list of capability names.
// The returned string is valid until the next call to CpuCapsToString.
const char* CpuCapsToString(uint32_t caps);

// Check if a specific capability is available.
inline bool CpuHasCap(CpuCap cap) { return (CpuCapsDetect() & cap) != 0; }

}  // namespace metrics
}  // namespace valkey_search
