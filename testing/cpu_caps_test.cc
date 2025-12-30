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

#include "src/metrics/cpu_caps.h"

#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace valkey_search {
namespace metrics {
namespace {

// Feature: simsimd-metrics-refactor, Property 2: Capability Detection Consistency
// Validates: Requirements 2.4, 2.5
//
// Property: For any number of calls to CpuCapsDetect(), the function SHALL
// return the same bitmask value, demonstrating that capabilities are cached
// after first detection.
TEST(CpuCapsTest, PropertyCapabilityDetectionConsistency) {
  // Get the first result
  uint32_t first_result = CpuCapsDetect();

  // Call CpuCapsDetect() 100+ times and verify consistency
  constexpr int kIterations = 100;
  for (int i = 0; i < kIterations; ++i) {
    uint32_t result = CpuCapsDetect();
    EXPECT_EQ(result, first_result)
        << "CpuCapsDetect() returned different value on iteration " << i;
  }
}

// Test that CpuCapsDetect() returns consistent results across multiple threads
TEST(CpuCapsTest, PropertyCapabilityDetectionConsistencyMultiThreaded) {
  constexpr int kNumThreads = 8;
  constexpr int kIterationsPerThread = 100;

  // Get the expected result first
  uint32_t expected = CpuCapsDetect();

  std::vector<std::thread> threads;
  std::atomic<int> failures{0};

  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&]() {
      for (int i = 0; i < kIterationsPerThread; ++i) {
        uint32_t result = CpuCapsDetect();
        if (result != expected) {
          failures.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_EQ(failures.load(), 0)
      << "CpuCapsDetect() returned inconsistent results across threads";
}

// Test that CpuHasCap() is consistent with CpuCapsDetect()
TEST(CpuCapsTest, CpuHasCapConsistency) {
  uint32_t caps = CpuCapsDetect();

  // Check each capability flag
  EXPECT_EQ(CpuHasCap(kCpuCapSse42), (caps & kCpuCapSse42) != 0);
  EXPECT_EQ(CpuHasCap(kCpuCapAvx), (caps & kCpuCapAvx) != 0);
  EXPECT_EQ(CpuHasCap(kCpuCapAvx2), (caps & kCpuCapAvx2) != 0);
  EXPECT_EQ(CpuHasCap(kCpuCapFma), (caps & kCpuCapFma) != 0);
  EXPECT_EQ(CpuHasCap(kCpuCapAvx512F), (caps & kCpuCapAvx512F) != 0);
  EXPECT_EQ(CpuHasCap(kCpuCapAvx512Dq), (caps & kCpuCapAvx512Dq) != 0);
  EXPECT_EQ(CpuHasCap(kCpuCapAvx512Bw), (caps & kCpuCapAvx512Bw) != 0);
  EXPECT_EQ(CpuHasCap(kCpuCapAvx512Vl), (caps & kCpuCapAvx512Vl) != 0);
  EXPECT_EQ(CpuHasCap(kCpuCapAvx512Vnni), (caps & kCpuCapAvx512Vnni) != 0);
  EXPECT_EQ(CpuHasCap(kCpuCapNeon), (caps & kCpuCapNeon) != 0);
  EXPECT_EQ(CpuHasCap(kCpuCapNeonDotprod), (caps & kCpuCapNeonDotprod) != 0);
  EXPECT_EQ(CpuHasCap(kCpuCapNeonI8mm), (caps & kCpuCapNeonI8mm) != 0);
  EXPECT_EQ(CpuHasCap(kCpuCapNeonFp16), (caps & kCpuCapNeonFp16) != 0);
  EXPECT_EQ(CpuHasCap(kCpuCapNeonBf16), (caps & kCpuCapNeonBf16) != 0);
  EXPECT_EQ(CpuHasCap(kCpuCapSve), (caps & kCpuCapSve) != 0);
  EXPECT_EQ(CpuHasCap(kCpuCapSve2), (caps & kCpuCapSve2) != 0);
  EXPECT_EQ(CpuHasCap(kCpuCapSveI8mm), (caps & kCpuCapSveI8mm) != 0);
  EXPECT_EQ(CpuHasCap(kCpuCapSveBf16), (caps & kCpuCapSveBf16) != 0);
}

// Feature: simsimd-metrics-refactor, Property 8: Capability String Completeness
// Validates: Requirements 6.3
//
// Property: For any capability bitmask with N bits set, CpuCapsToString(caps)
// SHALL return a string containing exactly N capability names separated by commas.
TEST(CpuCapsTest, PropertyCapabilityStringCompleteness) {
  // Test with no capabilities
  const char* empty_str = CpuCapsToString(kCpuCapNone);
  EXPECT_STREQ(empty_str, "");

  // Test with single capabilities
  EXPECT_STREQ(CpuCapsToString(kCpuCapSse42), "sse42");
  EXPECT_STREQ(CpuCapsToString(kCpuCapAvx), "avx");
  EXPECT_STREQ(CpuCapsToString(kCpuCapAvx2), "avx2");
  EXPECT_STREQ(CpuCapsToString(kCpuCapFma), "fma");
  EXPECT_STREQ(CpuCapsToString(kCpuCapNeon), "neon");
  EXPECT_STREQ(CpuCapsToString(kCpuCapSve), "sve");
  EXPECT_STREQ(CpuCapsToString(kCpuCapSve2), "sve2");

  // Test with multiple capabilities - count commas
  uint32_t multi_caps = kCpuCapSse42 | kCpuCapAvx | kCpuCapAvx2 | kCpuCapFma;
  const char* multi_str = CpuCapsToString(multi_caps);
  std::string str(multi_str);

  // Count the number of commas (should be N-1 for N capabilities)
  int comma_count = 0;
  for (char c : str) {
    if (c == ',') comma_count++;
  }
  EXPECT_EQ(comma_count, 3);  // 4 capabilities = 3 commas

  // Verify all capability names are present
  EXPECT_NE(str.find("sse42"), std::string::npos);
  EXPECT_NE(str.find("avx"), std::string::npos);
  EXPECT_NE(str.find("avx2"), std::string::npos);
  EXPECT_NE(str.find("fma"), std::string::npos);
}

// Test CpuCapsToString with ARM64 capabilities
TEST(CpuCapsTest, CpuCapsToStringArm64) {
  uint32_t arm_caps = kCpuCapNeon | kCpuCapNeonDotprod | kCpuCapSve;
  const char* str = CpuCapsToString(arm_caps);
  std::string result(str);

  EXPECT_NE(result.find("neon"), std::string::npos);
  EXPECT_NE(result.find("neon_dotprod"), std::string::npos);
  EXPECT_NE(result.find("sve"), std::string::npos);

  // Count commas
  int comma_count = 0;
  for (char c : result) {
    if (c == ',') comma_count++;
  }
  EXPECT_EQ(comma_count, 2);  // 3 capabilities = 2 commas
}

// Test that detected capabilities are valid (platform-specific)
TEST(CpuCapsTest, DetectedCapabilitiesAreValid) {
  uint32_t caps = CpuCapsDetect();

#if defined(__x86_64__) || defined(_M_X64)
  // On x86_64, we should not have ARM capabilities
  EXPECT_EQ(caps & kCpuCapNeon, 0u);
  EXPECT_EQ(caps & kCpuCapNeonDotprod, 0u);
  EXPECT_EQ(caps & kCpuCapNeonI8mm, 0u);
  EXPECT_EQ(caps & kCpuCapSve, 0u);
  EXPECT_EQ(caps & kCpuCapSve2, 0u);
#elif defined(__aarch64__) || defined(_M_ARM64)
  // On ARM64, we should not have x86 capabilities
  EXPECT_EQ(caps & kCpuCapSse42, 0u);
  EXPECT_EQ(caps & kCpuCapAvx, 0u);
  EXPECT_EQ(caps & kCpuCapAvx2, 0u);
  EXPECT_EQ(caps & kCpuCapFma, 0u);
  EXPECT_EQ(caps & kCpuCapAvx512F, 0u);
  // On ARM64, NEON should always be available
  EXPECT_NE(caps & kCpuCapNeon, 0u);
#endif
}

}  // namespace
}  // namespace metrics
}  // namespace valkey_search
