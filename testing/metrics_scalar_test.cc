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
#include "src/metrics/metrics_internal.h"

#include <cmath>
#include <random>
#include <vector>

#include "gtest/gtest.h"

namespace valkey_search {
namespace metrics {
namespace {

// Test fixture for scalar metrics property tests
class MetricsScalarPropertyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Initialize the metrics system
    ASSERT_EQ(Init(), 0);
    // Seed random number generator for reproducibility
    rng_.seed(42);
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

  // Reference implementation for L2 squared distance
  float ReferenceL2SqF32(const float* a, const float* b, size_t dim) {
    double sum = 0.0;
    for (size_t i = 0; i < dim; ++i) {
      double diff = static_cast<double>(a[i]) - static_cast<double>(b[i]);
      sum += diff * diff;
    }
    return static_cast<float>(sum);
  }

  // Reference implementation for inner product
  float ReferenceIpF32(const float* a, const float* b, size_t dim) {
    double sum = 0.0;
    for (size_t i = 0; i < dim; ++i) {
      sum += static_cast<double>(a[i]) * static_cast<double>(b[i]);
    }
    return static_cast<float>(sum);
  }

  // Reference implementation for cosine similarity
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

  // Check if two floats are approximately equal within relative tolerance
  bool ApproxEqual(float a, float b, float rel_tol = 1e-4f) {
    if (a == b) return true;
    float abs_diff = std::abs(a - b);
    float max_val = std::max(std::abs(a), std::abs(b));
    if (max_val == 0.0f) return abs_diff < rel_tol;
    return abs_diff / max_val < rel_tol;
  }

  std::mt19937 rng_;
};

// Feature: simsimd-metrics-refactor, Property 3: L2 Squared Distance Correctness
// Validates: Requirements 4.1, 9.1, 9.2
//
// Property: For any pair of float32 vectors a and b of dimension dim,
// the result of L2SqF32(a, b, dim) SHALL equal the sum of squared differences:
// Σ(a[i] - b[i])² within 1e-5 relative error.
TEST_F(MetricsScalarPropertyTest, PropertyL2SqF32Correctness) {
  constexpr int kIterations = 100;
  std::vector<size_t> dimensions = {1, 3, 7, 15, 16, 31, 32, 64, 128, 256, 768, 1536};

  for (int iter = 0; iter < kIterations; ++iter) {
    for (size_t dim : dimensions) {
      auto a = GenerateRandomF32Vector(dim);
      auto b = GenerateRandomF32Vector(dim);

      float result = L2SqF32(a.data(), b.data(), dim);
      float expected = ReferenceL2SqF32(a.data(), b.data(), dim);

      EXPECT_TRUE(ApproxEqual(result, expected))
          << "L2SqF32 mismatch at iteration " << iter << ", dim=" << dim
          << ": got " << result << ", expected " << expected;
    }
  }
}


// Feature: simsimd-metrics-refactor, Property 4: Inner Product Correctness
// Validates: Requirements 4.2, 9.1, 9.2
//
// Property: For any pair of float32 vectors a and b of dimension dim,
// the result of IpF32(a, b, dim) SHALL equal the dot product:
// Σ(a[i] * b[i]) within 1e-5 relative error.
TEST_F(MetricsScalarPropertyTest, PropertyIpF32Correctness) {
  constexpr int kIterations = 100;
  std::vector<size_t> dimensions = {1, 3, 7, 15, 16, 31, 32, 64, 128, 256, 768, 1536};

  for (int iter = 0; iter < kIterations; ++iter) {
    for (size_t dim : dimensions) {
      auto a = GenerateRandomF32Vector(dim);
      auto b = GenerateRandomF32Vector(dim);

      float result = IpF32(a.data(), b.data(), dim);
      float expected = ReferenceIpF32(a.data(), b.data(), dim);

      EXPECT_TRUE(ApproxEqual(result, expected))
          << "IpF32 mismatch at iteration " << iter << ", dim=" << dim
          << ": got " << result << ", expected " << expected;
    }
  }
}

// Feature: simsimd-metrics-refactor, Property 5: Cosine Similarity Correctness
// Validates: Requirements 4.3, 9.1, 9.2
//
// Property: For any pair of non-zero float32 vectors a and b of dimension dim,
// the result of CosineF32(a, b, dim) SHALL equal dot(a,b) / (||a|| * ||b||)
// within 1e-5 relative error.
TEST_F(MetricsScalarPropertyTest, PropertyCosineF32Correctness) {
  constexpr int kIterations = 100;
  std::vector<size_t> dimensions = {1, 3, 7, 15, 16, 31, 32, 64, 128, 256, 768, 1536};

  for (int iter = 0; iter < kIterations; ++iter) {
    for (size_t dim : dimensions) {
      auto a = GenerateRandomF32Vector(dim);
      auto b = GenerateRandomF32Vector(dim);

      float result = CosineF32(a.data(), b.data(), dim);
      float expected = ReferenceCosineF32(a.data(), b.data(), dim);

      EXPECT_TRUE(ApproxEqual(result, expected))
          << "CosineF32 mismatch at iteration " << iter << ", dim=" << dim
          << ": got " << result << ", expected " << expected;
    }
  }
}

// Test L2 squared with zero vectors
TEST_F(MetricsScalarPropertyTest, L2SqF32ZeroVectors) {
  std::vector<float> zero(128, 0.0f);
  auto random = GenerateRandomF32Vector(128);

  // Zero vs zero should be 0
  EXPECT_FLOAT_EQ(L2SqF32(zero.data(), zero.data(), 128), 0.0f);

  // Zero vs random should equal sum of squares of random
  float expected = 0.0f;
  for (float v : random) {
    expected += v * v;
  }
  EXPECT_TRUE(ApproxEqual(L2SqF32(zero.data(), random.data(), 128), expected));
}

// Test inner product with zero vectors
TEST_F(MetricsScalarPropertyTest, IpF32ZeroVectors) {
  std::vector<float> zero(128, 0.0f);
  auto random = GenerateRandomF32Vector(128);

  // Zero dot anything should be 0
  EXPECT_FLOAT_EQ(IpF32(zero.data(), zero.data(), 128), 0.0f);
  EXPECT_FLOAT_EQ(IpF32(zero.data(), random.data(), 128), 0.0f);
  EXPECT_FLOAT_EQ(IpF32(random.data(), zero.data(), 128), 0.0f);
}

// Test cosine similarity with zero vectors
TEST_F(MetricsScalarPropertyTest, CosineF32ZeroVectors) {
  std::vector<float> zero(128, 0.0f);
  auto random = GenerateRandomF32Vector(128);

  // Cosine with zero vector should return 0 (guarded division)
  EXPECT_FLOAT_EQ(CosineF32(zero.data(), zero.data(), 128), 0.0f);
  EXPECT_FLOAT_EQ(CosineF32(zero.data(), random.data(), 128), 0.0f);
  EXPECT_FLOAT_EQ(CosineF32(random.data(), zero.data(), 128), 0.0f);
}

// Test cosine similarity with identical vectors (should be 1.0)
TEST_F(MetricsScalarPropertyTest, CosineF32IdenticalVectors) {
  constexpr int kIterations = 100;

  for (int iter = 0; iter < kIterations; ++iter) {
    size_t dim = 64 + (iter % 100);
    auto vec = GenerateRandomF32Vector(dim);

    float result = CosineF32(vec.data(), vec.data(), dim);
    EXPECT_TRUE(ApproxEqual(result, 1.0f))
        << "Cosine of identical vectors should be 1.0, got " << result;
  }
}

// Test L2 squared symmetry: L2Sq(a, b) == L2Sq(b, a)
TEST_F(MetricsScalarPropertyTest, L2SqF32Symmetry) {
  constexpr int kIterations = 100;

  for (int iter = 0; iter < kIterations; ++iter) {
    size_t dim = 64 + (iter % 100);
    auto a = GenerateRandomF32Vector(dim);
    auto b = GenerateRandomF32Vector(dim);

    float ab = L2SqF32(a.data(), b.data(), dim);
    float ba = L2SqF32(b.data(), a.data(), dim);

    EXPECT_FLOAT_EQ(ab, ba) << "L2Sq should be symmetric";
  }
}

// Test inner product symmetry: Ip(a, b) == Ip(b, a)
TEST_F(MetricsScalarPropertyTest, IpF32Symmetry) {
  constexpr int kIterations = 100;

  for (int iter = 0; iter < kIterations; ++iter) {
    size_t dim = 64 + (iter % 100);
    auto a = GenerateRandomF32Vector(dim);
    auto b = GenerateRandomF32Vector(dim);

    float ab = IpF32(a.data(), b.data(), dim);
    float ba = IpF32(b.data(), a.data(), dim);

    EXPECT_FLOAT_EQ(ab, ba) << "Inner product should be symmetric";
  }
}

// Test cosine similarity symmetry: Cosine(a, b) == Cosine(b, a)
TEST_F(MetricsScalarPropertyTest, CosineF32Symmetry) {
  constexpr int kIterations = 100;

  for (int iter = 0; iter < kIterations; ++iter) {
    size_t dim = 64 + (iter % 100);
    auto a = GenerateRandomF32Vector(dim);
    auto b = GenerateRandomF32Vector(dim);

    float ab = CosineF32(a.data(), b.data(), dim);
    float ba = CosineF32(b.data(), a.data(), dim);

    EXPECT_TRUE(ApproxEqual(ab, ba)) << "Cosine should be symmetric";
  }
}

// Test L2 squared non-negativity
TEST_F(MetricsScalarPropertyTest, L2SqF32NonNegative) {
  constexpr int kIterations = 100;

  for (int iter = 0; iter < kIterations; ++iter) {
    size_t dim = 64 + (iter % 100);
    auto a = GenerateRandomF32Vector(dim);
    auto b = GenerateRandomF32Vector(dim);

    float result = L2SqF32(a.data(), b.data(), dim);
    EXPECT_GE(result, 0.0f) << "L2 squared should be non-negative";
  }
}

// Test cosine similarity bounds: -1 <= cosine <= 1
TEST_F(MetricsScalarPropertyTest, CosineF32Bounds) {
  constexpr int kIterations = 100;

  for (int iter = 0; iter < kIterations; ++iter) {
    size_t dim = 64 + (iter % 100);
    auto a = GenerateRandomF32Vector(dim);
    auto b = GenerateRandomF32Vector(dim);

    float result = CosineF32(a.data(), b.data(), dim);
    EXPECT_GE(result, -1.0f - 1e-5f) << "Cosine should be >= -1";
    EXPECT_LE(result, 1.0f + 1e-5f) << "Cosine should be <= 1";
  }
}

// Feature: simsimd-metrics-refactor, Property 6: Batch Operation Equivalence
// Validates: Requirements 5.1, 5.2
//
// Property: For any query vector and array of target vectors, the batch
// operation L2SqF32Batch() SHALL produce results equivalent to calling
// L2SqF32() individually for each target vector.
TEST_F(MetricsScalarPropertyTest, PropertyBatchL2SqF32Equivalence) {
  constexpr int kIterations = 100;
  std::vector<size_t> dimensions = {1, 3, 7, 15, 16, 31, 32, 64, 128, 256, 768};
  std::vector<size_t> counts = {1, 2, 3, 4, 5, 8, 10, 16, 32};

  for (int iter = 0; iter < kIterations; ++iter) {
    for (size_t dim : dimensions) {
      for (size_t count : counts) {
        // Generate query vector
        auto query = GenerateRandomF32Vector(dim);

        // Generate target vectors
        std::vector<std::vector<float>> target_vectors(count);
        std::vector<const float*> target_ptrs(count);
        for (size_t i = 0; i < count; ++i) {
          target_vectors[i] = GenerateRandomF32Vector(dim);
          target_ptrs[i] = target_vectors[i].data();
        }

        // Compute batch results
        std::vector<float> batch_results(count);
        L2SqF32Batch(query.data(), target_ptrs.data(), dim, count,
                     batch_results.data());

        // Compute individual results and compare
        for (size_t i = 0; i < count; ++i) {
          float individual_result = L2SqF32(query.data(), target_ptrs[i], dim);
          EXPECT_TRUE(ApproxEqual(batch_results[i], individual_result))
              << "L2SqF32Batch mismatch at iteration " << iter << ", dim=" << dim
              << ", count=" << count << ", index=" << i
              << ": batch=" << batch_results[i]
              << ", individual=" << individual_result;
        }
      }
    }
  }
}

// Feature: simsimd-metrics-refactor, Property 6: Batch Operation Equivalence
// Validates: Requirements 5.1, 5.2
//
// Property: For any query vector and array of target vectors, the batch
// operation IpF32Batch() SHALL produce results equivalent to calling
// IpF32() individually for each target vector.
TEST_F(MetricsScalarPropertyTest, PropertyBatchIpF32Equivalence) {
  constexpr int kIterations = 100;
  std::vector<size_t> dimensions = {1, 3, 7, 15, 16, 31, 32, 64, 128, 256, 768};
  std::vector<size_t> counts = {1, 2, 3, 4, 5, 8, 10, 16, 32};

  for (int iter = 0; iter < kIterations; ++iter) {
    for (size_t dim : dimensions) {
      for (size_t count : counts) {
        // Generate query vector
        auto query = GenerateRandomF32Vector(dim);

        // Generate target vectors
        std::vector<std::vector<float>> target_vectors(count);
        std::vector<const float*> target_ptrs(count);
        for (size_t i = 0; i < count; ++i) {
          target_vectors[i] = GenerateRandomF32Vector(dim);
          target_ptrs[i] = target_vectors[i].data();
        }

        // Compute batch results
        std::vector<float> batch_results(count);
        IpF32Batch(query.data(), target_ptrs.data(), dim, count,
                   batch_results.data());

        // Compute individual results and compare
        for (size_t i = 0; i < count; ++i) {
          float individual_result = IpF32(query.data(), target_ptrs[i], dim);
          EXPECT_TRUE(ApproxEqual(batch_results[i], individual_result))
              << "IpF32Batch mismatch at iteration " << iter << ", dim=" << dim
              << ", count=" << count << ", index=" << i
              << ": batch=" << batch_results[i]
              << ", individual=" << individual_result;
        }
      }
    }
  }
}

// Test batch operations with empty count
TEST_F(MetricsScalarPropertyTest, BatchOperationsEmptyCount) {
  auto query = GenerateRandomF32Vector(64);
  std::vector<float> results(1, -1.0f);  // Initialize with sentinel value

  // Empty batch should not crash and should not modify results
  L2SqF32Batch(query.data(), nullptr, 64, 0, results.data());
  IpF32Batch(query.data(), nullptr, 64, 0, results.data());

  // Results should be unchanged (sentinel value)
  EXPECT_FLOAT_EQ(results[0], -1.0f);
}

// Test batch operations with single element
TEST_F(MetricsScalarPropertyTest, BatchOperationsSingleElement) {
  constexpr size_t kDim = 128;
  auto query = GenerateRandomF32Vector(kDim);
  auto target = GenerateRandomF32Vector(kDim);
  const float* target_ptr = target.data();

  float batch_l2sq_result;
  L2SqF32Batch(query.data(), &target_ptr, kDim, 1, &batch_l2sq_result);
  float individual_l2sq = L2SqF32(query.data(), target.data(), kDim);
  EXPECT_TRUE(ApproxEqual(batch_l2sq_result, individual_l2sq));

  float batch_ip_result;
  IpF32Batch(query.data(), &target_ptr, kDim, 1, &batch_ip_result);
  float individual_ip = IpF32(query.data(), target.data(), kDim);
  EXPECT_TRUE(ApproxEqual(batch_ip_result, individual_ip));
}

}  // namespace
}  // namespace metrics
}  // namespace valkey_search
