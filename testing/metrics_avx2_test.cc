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

// Feature: simsimd-metrics-refactor, Property 9: Dimension Invariance (AVX2)
// Validates: Requirements 9.2, 9.3
//
// This test verifies that the AVX2 implementation produces correct results
// for all dimension sizes, including non-power-of-two dimensions that require
// tail handling.

#include "src/metrics/cpu_caps.h"
#include "src/metrics/metrics.h"
#include "src/metrics/metrics_internal.h"

#include <cmath>
#include <random>
#include <vector>

#include "gtest/gtest.h"

namespace valkey_search {
namespace metrics {
namespace {

// Test fixture for AVX2 metrics property tests
class MetricsAvx2PropertyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Initialize the metrics system
    ASSERT_EQ(Init(), 0);
    // Seed random number generator for reproducibility
    rng_.seed(42);

    // Check if AVX2 is available
    uint32_t caps = CpuCapsDetect();
    has_avx2_ = (caps & kCpuCapAvx2) && (caps & kCpuCapFma);
  }

  void TearDown() override { Fini(); }

  // Generate random float vector with values in [-1.0, 1.0]
  std::vector<float> GenerateRandomF32Vector(size_t dim) {
    std::vector<float> vec(dim);
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);
    for (size_t i = 0; i < dim; ++i) {
      vec[i] = dist(rng_);
    }
    return vec;
  }

  // Generate random int8 vector
  std::vector<int8_t> GenerateRandomI8Vector(size_t dim) {
    std::vector<int8_t> vec(dim);
    std::uniform_int_distribution<int> dist(-128, 127);
    for (size_t i = 0; i < dim; ++i) {
      vec[i] = static_cast<int8_t>(dist(rng_));
    }
    return vec;
  }

  // Reference implementation for L2 squared distance (double precision)
  float ReferenceL2SqF32(const float* a, const float* b, size_t dim) {
    double sum = 0.0;
    for (size_t i = 0; i < dim; ++i) {
      double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]);
      sum += diff * diff;
    }
    return static_cast<float>(sum);
  }

  // Reference implementation for inner product (double precision)
  float ReferenceIpF32(const float* a, const float* b, size_t dim) {
    double sum = 0.0;
    for (size_t i = 0; i < dim; ++i) {
      sum += static_cast<double>(a[i]) * static_cast<double>(b[i]);
    }
    return static_cast<float>(sum);
  }

  // Reference implementation for cosine similarity (double precision)
  float ReferenceCosineF32(const float* a, const float* b, size_t dim) {
    double dot = 0.0;
    double norm_a = 0.0;
    double norm_b = 0.0;
    for (size_t i = 0; i < dim; ++i) {
      dot += static_cast<double>(a[i]) * static_cast<double>(b[i]);
      norm_a += static_cast<double>(a[i]) * static_cast<double>(a[i]);
      norm_b += static_cast<double>(b[i]) * static_cast<double>(b[i]);
    }
    double denom = std::sqrt(norm_a) * std::sqrt(norm_b);
    return denom > 0.0 ? static_cast<float>(dot / denom) : 0.0f;
  }

  // Reference implementation for L2 squared distance (int8)
  float ReferenceL2SqI8(const int8_t* a, const int8_t* b, size_t dim) {
    int64_t sum = 0;
    for (size_t i = 0; i < dim; ++i) {
      int32_t diff = static_cast<int32_t>(a[i]) - static_cast<int32_t>(b[i]);
      sum += diff * diff;
    }
    return static_cast<float>(sum);
  }

  // Reference implementation for inner product (int8)
  float ReferenceIpI8(const int8_t* a, const int8_t* b, size_t dim) {
    int64_t sum = 0;
    for (size_t i = 0; i < dim; ++i) {
      sum += static_cast<int32_t>(a[i]) * static_cast<int32_t>(b[i]);
    }
    return static_cast<float>(sum);
  }

  // Check if two floats are approximately equal within relative tolerance
  bool ApproxEqual(float a, float b, float rel_tol = 1e-4f) {
    if (a == b) return true;
    float abs_diff = std::abs(a - b);
    float max_val = std::max(std::abs(a), std::abs(b));
    if (max_val == 0.0f) return abs_diff < rel_tol;
    return abs_diff / max_val < rel_tol;
  }

  std::mt19937 rng_;
  bool has_avx2_ = false;
};

// Feature: simsimd-metrics-refactor, Property 9: Dimension Invariance (AVX2)
// Validates: Requirements 9.2, 9.3
//
// Property: For any dimension size (including 1, 3, 7, 15, 16, 31, 32, 63, 64,
// 127, 128, 256, 768, 1536), all metric functions SHALL produce correct results
// matching the reference implementation.
TEST_F(MetricsAvx2PropertyTest, PropertyDimensionInvarianceL2SqF32) {
  if (!has_avx2_) {
    GTEST_SKIP() << "AVX2 not available on this CPU";
  }

  constexpr int kIterations = 100;
  // Test various dimensions including edge cases for AVX2 (8-element vectors)
  // and non-aligned dimensions that require tail handling
  std::vector<size_t> dimensions = {
      1,   2,   3,   4,   5,   6,   7,    8,    9,    10,   15,  16,
      17,  23,  24,  31,  32,  33,  63,   64,   65,   127,  128, 129,
      255, 256, 257, 511, 512, 513, 768,  1023, 1024, 1025, 1536};

  for (int iter = 0; iter < kIterations; ++iter) {
    for (size_t dim : dimensions) {
      auto a = GenerateRandomF32Vector(dim);
      auto b = GenerateRandomF32Vector(dim);

      float result = L2SqF32(a.data(), b.data(), dim);
      float expected = ReferenceL2SqF32(a.data(), b.data(), dim);

      EXPECT_TRUE(ApproxEqual(result, expected))
          << "L2SqF32 mismatch at iteration " << iter << ", dim=" << dim
          << ": got " << result << ", expected " << expected
          << ", diff=" << std::abs(result - expected);
    }
  }
}

TEST_F(MetricsAvx2PropertyTest, PropertyDimensionInvarianceIpF32) {
  if (!has_avx2_) {
    GTEST_SKIP() << "AVX2 not available on this CPU";
  }

  constexpr int kIterations = 100;
  std::vector<size_t> dimensions = {
      1,   2,   3,   4,   5,   6,   7,    8,    9,    10,   15,  16,
      17,  23,  24,  31,  32,  33,  63,   64,   65,   127,  128, 129,
      255, 256, 257, 511, 512, 513, 768,  1023, 1024, 1025, 1536};

  for (int iter = 0; iter < kIterations; ++iter) {
    for (size_t dim : dimensions) {
      auto a = GenerateRandomF32Vector(dim);
      auto b = GenerateRandomF32Vector(dim);

      float result = IpF32(a.data(), b.data(), dim);
      float expected = ReferenceIpF32(a.data(), b.data(), dim);

      EXPECT_TRUE(ApproxEqual(result, expected))
          << "IpF32 mismatch at iteration " << iter << ", dim=" << dim
          << ": got " << result << ", expected " << expected
          << ", diff=" << std::abs(result - expected);
    }
  }
}

TEST_F(MetricsAvx2PropertyTest, PropertyDimensionInvarianceCosineF32) {
  if (!has_avx2_) {
    GTEST_SKIP() << "AVX2 not available on this CPU";
  }

  constexpr int kIterations = 100;
  std::vector<size_t> dimensions = {
      1,   2,   3,   4,   5,   6,   7,    8,    9,    10,   15,  16,
      17,  23,  24,  31,  32,  33,  63,   64,   65,   127,  128, 129,
      255, 256, 257, 511, 512, 513, 768,  1023, 1024, 1025, 1536};

  for (int iter = 0; iter < kIterations; ++iter) {
    for (size_t dim : dimensions) {
      auto a = GenerateRandomF32Vector(dim);
      auto b = GenerateRandomF32Vector(dim);

      float result = CosineF32(a.data(), b.data(), dim);
      float expected = ReferenceCosineF32(a.data(), b.data(), dim);

      EXPECT_TRUE(ApproxEqual(result, expected))
          << "CosineF32 mismatch at iteration " << iter << ", dim=" << dim
          << ": got " << result << ", expected " << expected
          << ", diff=" << std::abs(result - expected);
    }
  }
}

TEST_F(MetricsAvx2PropertyTest, PropertyDimensionInvarianceL2SqI8) {
  if (!has_avx2_) {
    GTEST_SKIP() << "AVX2 not available on this CPU";
  }

  constexpr int kIterations = 100;
  // Int8 AVX2 processes 16 elements at a time
  std::vector<size_t> dimensions = {
      1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  15,   16,
      17,  23,  24,  31,  32,  33,  47,  48,  63,  64,  65,   127,
      128, 129, 255, 256, 257, 511, 512, 513, 768, 1024, 1536};

  for (int iter = 0; iter < kIterations; ++iter) {
    for (size_t dim : dimensions) {
      auto a = GenerateRandomI8Vector(dim);
      auto b = GenerateRandomI8Vector(dim);

      float result = L2SqI8(a.data(), b.data(), dim);
      float expected = ReferenceL2SqI8(a.data(), b.data(), dim);

      EXPECT_FLOAT_EQ(result, expected)
          << "L2SqI8 mismatch at iteration " << iter << ", dim=" << dim;
    }
  }
}

TEST_F(MetricsAvx2PropertyTest, PropertyDimensionInvarianceIpI8) {
  if (!has_avx2_) {
    GTEST_SKIP() << "AVX2 not available on this CPU";
  }

  constexpr int kIterations = 100;
  std::vector<size_t> dimensions = {
      1,   2,   3,   4,   5,   6,   7,   8,   9,   10,  15,   16,
      17,  23,  24,  31,  32,  33,  47,  48,  63,  64,  65,   127,
      128, 129, 255, 256, 257, 511, 512, 513, 768, 1024, 1536};

  for (int iter = 0; iter < kIterations; ++iter) {
    for (size_t dim : dimensions) {
      auto a = GenerateRandomI8Vector(dim);
      auto b = GenerateRandomI8Vector(dim);

      float result = IpI8(a.data(), b.data(), dim);
      float expected = ReferenceIpI8(a.data(), b.data(), dim);

      EXPECT_FLOAT_EQ(result, expected)
          << "IpI8 mismatch at iteration " << iter << ", dim=" << dim;
    }
  }
}

// Test that AVX2 implementation is selected when available
TEST_F(MetricsAvx2PropertyTest, Avx2ImplementationSelected) {
  if (!has_avx2_) {
    GTEST_SKIP() << "AVX2 not available on this CPU";
  }

  // On x86_64 with AVX2+FMA, the implementation should be "avx2" or "avx512"
  const char* impl_name = ImplName();
  uint32_t impl_caps = ImplCaps();

  // Either AVX2 or AVX512 should be selected (AVX512 takes priority if available)
  bool is_avx2_or_better =
      (std::string(impl_name) == "avx2" || std::string(impl_name) == "avx512");
  EXPECT_TRUE(is_avx2_or_better)
      << "Expected avx2 or avx512 implementation, got: " << impl_name;

  // Verify capabilities include AVX2
  EXPECT_TRUE(impl_caps & kCpuCapAvx2)
      << "Implementation should have AVX2 capability";
}

// Test batch operations with AVX2
TEST_F(MetricsAvx2PropertyTest, BatchOperationsCorrectness) {
  if (!has_avx2_) {
    GTEST_SKIP() << "AVX2 not available on this CPU";
  }

  constexpr int kIterations = 100;
  std::vector<size_t> dimensions = {1, 7, 8, 15, 16, 31, 32, 64, 128, 256, 768};
  std::vector<size_t> counts = {1, 2, 3, 4, 5, 8, 10, 16, 32};

  for (int iter = 0; iter < kIterations; ++iter) {
    for (size_t dim : dimensions) {
      for (size_t count : counts) {
        auto query = GenerateRandomF32Vector(dim);

        std::vector<std::vector<float>> target_vectors(count);
        std::vector<const float*> target_ptrs(count);
        for (size_t i = 0; i < count; ++i) {
          target_vectors[i] = GenerateRandomF32Vector(dim);
          target_ptrs[i] = target_vectors[i].data();
        }

        // Test L2Sq batch
        std::vector<float> batch_l2sq(count);
        L2SqF32Batch(query.data(), target_ptrs.data(), dim, count,
                     batch_l2sq.data());

        for (size_t i = 0; i < count; ++i) {
          float individual = L2SqF32(query.data(), target_ptrs[i], dim);
          EXPECT_TRUE(ApproxEqual(batch_l2sq[i], individual))
              << "L2SqF32Batch mismatch at iter=" << iter << ", dim=" << dim
              << ", count=" << count << ", i=" << i;
        }

        // Test IP batch
        std::vector<float> batch_ip(count);
        IpF32Batch(query.data(), target_ptrs.data(), dim, count,
                   batch_ip.data());

        for (size_t i = 0; i < count; ++i) {
          float individual = IpF32(query.data(), target_ptrs[i], dim);
          EXPECT_TRUE(ApproxEqual(batch_ip[i], individual))
              << "IpF32Batch mismatch at iter=" << iter << ", dim=" << dim
              << ", count=" << count << ", i=" << i;
        }
      }
    }
  }
}

// Test edge case: dimension = 0
TEST_F(MetricsAvx2PropertyTest, ZeroDimension) {
  if (!has_avx2_) {
    GTEST_SKIP() << "AVX2 not available on this CPU";
  }

  float dummy[1] = {1.0f};

  // Zero dimension should return 0 for all metrics
  EXPECT_FLOAT_EQ(L2SqF32(dummy, dummy, 0), 0.0f);
  EXPECT_FLOAT_EQ(IpF32(dummy, dummy, 0), 0.0f);
  EXPECT_FLOAT_EQ(CosineF32(dummy, dummy, 0), 0.0f);
}

// Test with identical vectors
TEST_F(MetricsAvx2PropertyTest, IdenticalVectors) {
  if (!has_avx2_) {
    GTEST_SKIP() << "AVX2 not available on this CPU";
  }

  constexpr int kIterations = 100;
  std::vector<size_t> dimensions = {1, 7, 8, 15, 16, 31, 32, 64, 128, 256, 768};

  for (int iter = 0; iter < kIterations; ++iter) {
    for (size_t dim : dimensions) {
      auto vec = GenerateRandomF32Vector(dim);

      // L2Sq of identical vectors should be 0
      float l2sq = L2SqF32(vec.data(), vec.data(), dim);
      EXPECT_NEAR(l2sq, 0.0f, 1e-5f)
          << "L2Sq of identical vectors should be 0, got " << l2sq;

      // Cosine of identical vectors should be 1
      float cosine = CosineF32(vec.data(), vec.data(), dim);
      EXPECT_TRUE(ApproxEqual(cosine, 1.0f))
          << "Cosine of identical vectors should be 1, got " << cosine;
    }
  }
}

}  // namespace
}  // namespace metrics
}  // namespace valkey_search
