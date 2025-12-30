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

#include "src/metrics/metrics.h"
#include "src/metrics/cpu_caps.h"

#include <atomic>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace valkey_search {
namespace metrics {
namespace {

// Feature: simsimd-metrics-refactor, Property 1: Initialization Idempotence
// Validates: Requirements 1.1, 1.2, 1.3
//
// Property: For any sequence of Init() calls (including concurrent calls from
// multiple threads), the metrics system SHALL be in a valid initialized state
// after the first successful call, and subsequent calls SHALL return success
// without changing the selected implementation.
class MetricsDispatchPropertyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Ensure clean state before each test
    Fini();
  }

  void TearDown() override { Fini(); }
};

// Test that Init() is idempotent - calling it multiple times returns success
// and doesn't change the implementation.
TEST_F(MetricsDispatchPropertyTest, PropertyInitializationIdempotence) {
  constexpr int kIterations = 100;

  // First initialization
  ASSERT_EQ(Init(), 0) << "First Init() should succeed";

  // Capture the implementation after first init
  const char* first_impl_name = ImplName();
  uint32_t first_impl_caps = ImplCaps();

  // Verify we got a valid implementation
  ASSERT_NE(first_impl_name, nullptr);
  ASSERT_STRNE(first_impl_name, "uninitialized");

  // Call Init() many more times - should all succeed and not change impl
  for (int i = 0; i < kIterations; ++i) {
    EXPECT_EQ(Init(), 0) << "Init() call " << i << " should succeed";
    EXPECT_STREQ(ImplName(), first_impl_name)
        << "Implementation name should not change after call " << i;
    EXPECT_EQ(ImplCaps(), first_impl_caps)
        << "Implementation caps should not change after call " << i;
  }
}

// Test that concurrent Init() calls are thread-safe and all succeed
TEST_F(MetricsDispatchPropertyTest,
       PropertyInitializationIdempotenceMultiThreaded) {
  constexpr int kNumThreads = 8;
  constexpr int kIterationsPerThread = 100;

  std::atomic<int> failures{0};
  std::atomic<int> threads_ready{0};
  std::atomic<bool> start_flag{false};

  // Storage for results from each thread
  std::vector<std::string> impl_names(kNumThreads);
  std::vector<uint32_t> impl_caps(kNumThreads);

  std::vector<std::thread> threads;
  for (int t = 0; t < kNumThreads; ++t) {
    threads.emplace_back([&, t]() {
      // Signal ready and wait for start
      threads_ready.fetch_add(1, std::memory_order_release);
      while (!start_flag.load(std::memory_order_acquire)) {
        // Spin wait
      }

      // All threads call Init() concurrently
      for (int i = 0; i < kIterationsPerThread; ++i) {
        int result = Init();
        if (result != 0) {
          failures.fetch_add(1, std::memory_order_relaxed);
        }
      }

      // Record the implementation we see
      impl_names[t] = ImplName();
      impl_caps[t] = ImplCaps();
    });
  }

  // Wait for all threads to be ready
  while (threads_ready.load(std::memory_order_acquire) < kNumThreads) {
    // Spin wait
  }

  // Start all threads simultaneously
  start_flag.store(true, std::memory_order_release);

  // Wait for all threads to complete
  for (auto& thread : threads) {
    thread.join();
  }

  // Verify no failures
  EXPECT_EQ(failures.load(), 0) << "All Init() calls should succeed";

  // Verify all threads see the same implementation
  for (int t = 1; t < kNumThreads; ++t) {
    EXPECT_EQ(impl_names[t], impl_names[0])
        << "Thread " << t << " saw different implementation name";
    EXPECT_EQ(impl_caps[t], impl_caps[0])
        << "Thread " << t << " saw different implementation caps";
  }

  // Verify the implementation is valid
  EXPECT_NE(impl_names[0], "uninitialized");
}

// Test that Fini() followed by Init() re-initializes correctly
TEST_F(MetricsDispatchPropertyTest, InitFiniCycle) {
  constexpr int kCycles = 10;

  for (int cycle = 0; cycle < kCycles; ++cycle) {
    // Initialize
    ASSERT_EQ(Init(), 0) << "Init() should succeed in cycle " << cycle;

    // Verify initialized
    const char* impl_name = ImplName();
    EXPECT_NE(impl_name, nullptr);
    EXPECT_STRNE(impl_name, "uninitialized")
        << "Should be initialized in cycle " << cycle;

    // Finalize
    Fini();

    // Verify uninitialized
    EXPECT_STREQ(ImplName(), "uninitialized")
        << "Should be uninitialized after Fini() in cycle " << cycle;
    EXPECT_EQ(ImplCaps(), 0u)
        << "Caps should be 0 after Fini() in cycle " << cycle;
  }
}

// Feature: simsimd-metrics-refactor, Property 7: Introspection Consistency
// Validates: Requirements 6.1, 6.2
//
// Property: For any initialized metrics system, ImplName() SHALL return a
// non-empty string from the set {"scalar", "avx2", "avx512", "neon",
// "neon_dotprod", "neon_i8mm", "sve", "sve2"}, and ImplCaps() SHALL return a
// bitmask consistent with the implementation's required capabilities.
TEST_F(MetricsDispatchPropertyTest, PropertyIntrospectionConsistency) {
  // Initialize the system
  ASSERT_EQ(Init(), 0);

  // Get implementation info
  const char* impl_name = ImplName();
  uint32_t impl_caps = ImplCaps();

  // Verify name is from the valid set
  std::set<std::string> valid_names = {"scalar",       "avx2",    "avx512",
                                       "neon",         "neon_dotprod",
                                       "neon_i8mm",    "sve",     "sve2"};
  EXPECT_TRUE(valid_names.count(impl_name) > 0)
      << "Implementation name '" << impl_name << "' is not in valid set";

  // Verify caps are consistent with the implementation name
  if (std::string(impl_name) == "scalar") {
    EXPECT_EQ(impl_caps, 0u) << "Scalar should have no capability flags";
  } else if (std::string(impl_name) == "avx2") {
    EXPECT_NE(impl_caps & kCpuCapAvx2, 0u) << "AVX2 impl should have AVX2 cap";
    EXPECT_NE(impl_caps & kCpuCapFma, 0u) << "AVX2 impl should have FMA cap";
  } else if (std::string(impl_name) == "avx512") {
    EXPECT_NE(impl_caps & kCpuCapAvx512F, 0u)
        << "AVX512 impl should have AVX512F cap";
    EXPECT_NE(impl_caps & kCpuCapAvx512Dq, 0u)
        << "AVX512 impl should have AVX512DQ cap";
  } else if (std::string(impl_name) == "neon") {
    EXPECT_NE(impl_caps & kCpuCapNeon, 0u) << "NEON impl should have NEON cap";
  } else if (std::string(impl_name) == "neon_dotprod") {
    EXPECT_NE(impl_caps & kCpuCapNeon, 0u)
        << "NEON dotprod impl should have NEON cap";
    EXPECT_NE(impl_caps & kCpuCapNeonDotprod, 0u)
        << "NEON dotprod impl should have dotprod cap";
  } else if (std::string(impl_name) == "neon_i8mm") {
    EXPECT_NE(impl_caps & kCpuCapNeon, 0u)
        << "NEON i8mm impl should have NEON cap";
    EXPECT_NE(impl_caps & kCpuCapNeonI8mm, 0u)
        << "NEON i8mm impl should have i8mm cap";
  } else if (std::string(impl_name) == "sve") {
    EXPECT_NE(impl_caps & kCpuCapSve, 0u) << "SVE impl should have SVE cap";
  } else if (std::string(impl_name) == "sve2") {
    EXPECT_NE(impl_caps & kCpuCapSve2, 0u) << "SVE2 impl should have SVE2 cap";
  }
}

// Test that introspection is consistent across multiple calls
TEST_F(MetricsDispatchPropertyTest, IntrospectionConsistencyMultipleCalls) {
  constexpr int kIterations = 100;

  ASSERT_EQ(Init(), 0);

  const char* first_name = ImplName();
  uint32_t first_caps = ImplCaps();

  for (int i = 0; i < kIterations; ++i) {
    EXPECT_STREQ(ImplName(), first_name)
        << "ImplName() should be consistent on call " << i;
    EXPECT_EQ(ImplCaps(), first_caps)
        << "ImplCaps() should be consistent on call " << i;
  }
}

// Test that uninitialized state returns correct values
TEST_F(MetricsDispatchPropertyTest, UninitializedIntrospection) {
  // Don't call Init()
  EXPECT_STREQ(ImplName(), "uninitialized");
  EXPECT_EQ(ImplCaps(), 0u);
}

}  // namespace
}  // namespace metrics
}  // namespace valkey_search
