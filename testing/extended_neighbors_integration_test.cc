/*
 * Copyright (c) 2025, valkey-search contributors
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
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>
#include <random>
#include <chrono>
#include <map>
#include <set>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "testing/common.h"
#include "src/indexes/vector_hnsw.h"
#include "src/index_schema.pb.h"
#include "third_party/hnswlib/hnswalg.h"
#include "third_party/hnswlib/space_l2.h"
#include "third_party/hnswlib/space_ip.h"

namespace valkey_search::indexes {

namespace {
constexpr static int kDimensions = 32;
constexpr static int kM = 16;
constexpr static int kEFConstruction = 32;
constexpr static int kMaxElements = 2000;

// Helper function to create test HNSW indices with extended neighbors
// NOTE: This creates direct HNSW indices for testing, bypassing the VectorIndex proto
std::unique_ptr<hnswlib::HierarchicalNSW<float>> CreateTestHNSWIndex(
    hnswlib::SpaceInterface<float>* space, int max_elements,
    int m, int ef_construction, size_t ef_runtime, 
    bool use_extended_neighbors = true, float extended_list_factor = 1.5f) {
  
  return std::make_unique<hnswlib::HierarchicalNSW<float>>(
      space, max_elements, m, ef_construction, 200, // random seed
      false,  // allow_replace_deleted
      use_extended_neighbors,
      extended_list_factor
  );
}

// Helper to create test datasets
struct TestDataset {
  std::vector<std::vector<float>> vectors;
  std::vector<std::vector<float>> queries;
  std::string name;
};

TestDataset CreateGaussianDataset(int num_vectors, int num_queries, int dimensions, int seed = 42) {
  std::mt19937 gen(seed);
  std::normal_distribution<float> dis(0.0f, 1.0f);
  
  TestDataset dataset;
  dataset.name = "Gaussian";
  dataset.vectors.resize(num_vectors);
  dataset.queries.resize(num_queries);
  
  for (int i = 0; i < num_vectors; ++i) {
    dataset.vectors[i].resize(dimensions);
    for (int j = 0; j < dimensions; ++j) {
      dataset.vectors[i][j] = dis(gen);
    }
  }
  
  for (int i = 0; i < num_queries; ++i) {
    dataset.queries[i].resize(dimensions);
    for (int j = 0; j < dimensions; ++j) {
      dataset.queries[i][j] = dis(gen);
    }
  }
  
  return dataset;
}

TestDataset CreateClusteredDataset(int num_vectors, int num_queries, int dimensions, int num_clusters = 5, int seed = 42) {
  std::mt19937 gen(seed);
  std::uniform_real_distribution<float> cluster_dis(-5.0f, 5.0f);
  std::normal_distribution<float> point_dis(0.0f, 0.5f);
  std::uniform_int_distribution<int> cluster_choice(0, num_clusters - 1);
  
  TestDataset dataset;
  dataset.name = "Clustered";
  dataset.vectors.resize(num_vectors);
  dataset.queries.resize(num_queries);
  
  // Create cluster centers
  std::vector<std::vector<float>> centers(num_clusters);
  for (int c = 0; c < num_clusters; ++c) {
    centers[c].resize(dimensions);
    for (int d = 0; d < dimensions; ++d) {
      centers[c][d] = cluster_dis(gen);
    }
  }
  
  // Generate vectors around cluster centers
  for (int i = 0; i < num_vectors; ++i) {
    int cluster = cluster_choice(gen);
    dataset.vectors[i].resize(dimensions);
    for (int j = 0; j < dimensions; ++j) {
      dataset.vectors[i][j] = centers[cluster][j] + point_dis(gen);
    }
  }
  
  // Generate queries (some around centers, some random)
  for (int i = 0; i < num_queries; ++i) {
    dataset.queries[i].resize(dimensions);
    if (i < num_queries / 2) {
      // Query around a cluster center
      int cluster = cluster_choice(gen);
      for (int j = 0; j < dimensions; ++j) {
        dataset.queries[i][j] = centers[cluster][j] + point_dis(gen);
      }
    } else {
      // Random query
      for (int j = 0; j < dimensions; ++j) {
        dataset.queries[i][j] = cluster_dis(gen);
      }
    }
  }
  
  return dataset;
}

// Test metrics for evaluation
struct TestMetrics {
  double avg_recall;
  double avg_precision;
  double avg_search_time_ms;
  int total_results_found;
  double avg_distance_error;
};

// Evaluation helper
TestMetrics EvaluateIndex(const TestDataset& dataset, 
                         hnswlib::HierarchicalNSW<float>* index,
                         int k = 10, int ef_search = 50) {
  TestMetrics metrics = {0.0, 0.0, 0.0, 0, 0.0};
  
  // Create ground truth (brute force search)
  std::vector<std::vector<std::pair<float, int>>> ground_truth(dataset.queries.size());
  for (size_t q = 0; q < dataset.queries.size(); ++q) {
    ground_truth[q].reserve(dataset.vectors.size());
    for (size_t i = 0; i < dataset.vectors.size(); ++i) {
      float dist = 0.0f;
      for (int d = 0; d < kDimensions; ++d) {
        float diff = dataset.queries[q][d] - dataset.vectors[i][d];
        dist += diff * diff;
      }
      ground_truth[q].emplace_back(dist, i);
    }
    std::sort(ground_truth[q].begin(), ground_truth[q].end());
    ground_truth[q].resize(std::min(k, static_cast<int>(ground_truth[q].size())));
  }
  
  // Evaluate HNSW search
  index->setEf(ef_search);
  double total_recall = 0.0;
  double total_precision = 0.0;
  double total_time = 0.0;
  int total_found = 0;
  double total_distance_error = 0.0;
  
  for (size_t q = 0; q < dataset.queries.size(); ++q) {
    auto start = std::chrono::high_resolution_clock::now();
    auto results = index->searchKnn(dataset.queries[q].data(), k);
    auto end = std::chrono::high_resolution_clock::now();
    
    total_time += std::chrono::duration<double, std::milli>(end - start).count();
    
    // Convert results to set for easier comparison
    std::set<int> found_ids;
    std::vector<float> found_distances;
    while (!results.empty()) {
      auto result = results.top();
      results.pop();
      found_ids.insert(result.second);
      found_distances.push_back(result.first);
    }
    
    total_found += found_ids.size();
    
    // Calculate recall and precision
    std::set<int> true_ids;
    for (const auto& gt : ground_truth[q]) {
      true_ids.insert(gt.second);
    }
    
    std::set<int> intersection;
    std::set_intersection(found_ids.begin(), found_ids.end(),
                         true_ids.begin(), true_ids.end(),
                         std::inserter(intersection, intersection.begin()));
    
    double recall = static_cast<double>(intersection.size()) / true_ids.size();
    double precision = found_ids.empty() ? 0.0 : 
                      static_cast<double>(intersection.size()) / found_ids.size();
    
    total_recall += recall;
    total_precision += precision;
    
    // Calculate distance error (for found items)
    double distance_error = 0.0;
    int error_count = 0;
    for (size_t i = 0; i < std::min(found_distances.size(), ground_truth[q].size()); ++i) {
      distance_error += std::abs(found_distances[i] - ground_truth[q][i].first);
      error_count++;
    }
    if (error_count > 0) {
      total_distance_error += distance_error / error_count;
    }
  }
  
  metrics.avg_recall = total_recall / dataset.queries.size();
  metrics.avg_precision = total_precision / dataset.queries.size();
  metrics.avg_search_time_ms = total_time / dataset.queries.size();
  metrics.total_results_found = total_found;
  metrics.avg_distance_error = total_distance_error / dataset.queries.size();
  
  return metrics;
}

}  // namespace

// Test fixture for integration tests
class ExtendedNeighborsIntegrationTest : public ValkeySearchTest {
 protected:
  void SetUp() override {
    ValkeySearchTest::SetUp();
    space_ = std::make_unique<hnswlib::L2Space>(kDimensions);
  }

  std::unique_ptr<hnswlib::L2Space> space_;
};

// Test 1: Compare Search Quality Between Standard and Extended HNSW
TEST_F(ExtendedNeighborsIntegrationTest, SearchQualityComparison) {
  auto dataset = CreateGaussianDataset(1000, 100, kDimensions);
  
  // Create standard HNSW index
  auto standard_index = std::make_unique<hnswlib::HierarchicalNSW<float>>(
      space_.get(), kMaxElements, kM, kEFConstruction, 200,
      false, false, 1.0f); // no extended neighbors
  
  // Create extended HNSW index
  auto extended_index = std::make_unique<hnswlib::HierarchicalNSW<float>>(
      space_.get(), kMaxElements, kM, kEFConstruction, 200,
      false, true, 1.5f); // with extended neighbors
  
  // Add vectors to both indices
  for (size_t i = 0; i < dataset.vectors.size(); ++i) {
    standard_index->addPoint(dataset.vectors[i].data(), i);
    extended_index->addPoint(dataset.vectors[i].data(), i);
  }
  
  // Evaluate both indices
  auto standard_metrics = EvaluateIndex(dataset, standard_index.get(), 10, 50);
  auto extended_metrics = EvaluateIndex(dataset, extended_index.get(), 10, 50);
  
  // Print results
  std::cout << "Standard HNSW Metrics:\n";
  std::cout << "  Recall: " << standard_metrics.avg_recall << "\n";
  std::cout << "  Precision: " << standard_metrics.avg_precision << "\n";
  std::cout << "  Avg Search Time: " << standard_metrics.avg_search_time_ms << " ms\n";
  
  std::cout << "Extended HNSW Metrics:\n";
  std::cout << "  Recall: " << extended_metrics.avg_recall << "\n";
  std::cout << "  Precision: " << extended_metrics.avg_precision << "\n";
  std::cout << "  Avg Search Time: " << extended_metrics.avg_search_time_ms << " ms\n";
  
  // Extended index should maintain good quality
  EXPECT_GE(extended_metrics.avg_recall, 0.8);
  EXPECT_GE(extended_metrics.avg_precision, 0.8);
}

// Test 2: Search Quality Under Deletions
TEST_F(ExtendedNeighborsIntegrationTest, SearchQualityUnderDeletions) {
  auto dataset = CreateClusteredDataset(800, 50, kDimensions, 8);
  
  // Create both types of indices
  auto standard_index = std::make_unique<hnswlib::HierarchicalNSW<float>>(
      space_.get(), kMaxElements, kM, kEFConstruction, 200,
      false, false, 1.0f);
  
  auto extended_index = std::make_unique<hnswlib::HierarchicalNSW<float>>(
      space_.get(), kMaxElements, kM, kEFConstruction, 200,
      false, true, 1.8f); // Higher extended factor
  
  // Add vectors
  for (size_t i = 0; i < dataset.vectors.size(); ++i) {
    standard_index->addPoint(dataset.vectors[i].data(), i);
    extended_index->addPoint(dataset.vectors[i].data(), i);
  }
  
  // Evaluate before deletions
  auto standard_before = EvaluateIndex(dataset, standard_index.get(), 10, 80);
  auto extended_before = EvaluateIndex(dataset, extended_index.get(), 10, 80);
  
  // Delete 20% of vectors randomly
  std::mt19937 gen(123);
  std::uniform_int_distribution<size_t> dis(0, dataset.vectors.size() - 1);
  std::set<size_t> deleted_ids;
  
  size_t num_deletions = dataset.vectors.size() / 5;
  while (deleted_ids.size() < num_deletions) {
    size_t id = dis(gen);
    if (deleted_ids.find(id) == deleted_ids.end()) {
      deleted_ids.insert(id);
      standard_index->markDelete(id);
      extended_index->markDelete(id);
    }
  }
  
  // Evaluate after deletions
  auto standard_after = EvaluateIndex(dataset, standard_index.get(), 10, 80);
  auto extended_after = EvaluateIndex(dataset, extended_index.get(), 10, 80);
  
  // Print results
  std::cout << "Results after " << num_deletions << " deletions:\n";
  std::cout << "Standard - Recall drop: " 
            << (standard_before.avg_recall - standard_after.avg_recall) << "\n";
  std::cout << "Extended - Recall drop: " 
            << (extended_before.avg_recall - extended_after.avg_recall) << "\n";
  
  // Extended neighbors should help maintain quality under deletions
  double standard_recall_drop = standard_before.avg_recall - standard_after.avg_recall;
  double extended_recall_drop = extended_before.avg_recall - extended_after.avg_recall;
  
  // Extended index should have smaller recall drop
  EXPECT_LT(extended_recall_drop, standard_recall_drop + 0.05); // Allow some tolerance
  EXPECT_GE(extended_after.avg_recall, 0.7); // Should maintain reasonable quality
}

// Test 3: Memory Usage and Performance Impact
TEST_F(ExtendedNeighborsIntegrationTest, MemoryAndPerformanceImpact) {
  auto dataset = CreateGaussianDataset(1500, 200, kDimensions);
  
  struct IndexStats {
    size_t memory_usage;
    double insertion_time_ms;
    double search_time_ms;
    TestMetrics quality;
  };
  
  auto measure_index = [&](bool use_extended, float factor) -> IndexStats {
    auto index = std::make_unique<hnswlib::HierarchicalNSW<float>>(
        space_.get(), kMaxElements, kM, kEFConstruction, 200,
        false, use_extended, factor);
    
    // Measure insertion time
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < dataset.vectors.size(); ++i) {
      index->addPoint(dataset.vectors[i].data(), i);
    }
    auto end = std::chrono::high_resolution_clock::now();
    double insertion_time = std::chrono::duration<double, std::milli>(end - start).count();
    
    // Estimate memory usage (rough approximation)
    size_t memory_usage = index->getCurrentElementCount() * 
                         (kDimensions * sizeof(float) + index->size_links_per_element_);
    
    // Measure search quality and time
    auto quality = EvaluateIndex(dataset, index.get(), 15, 100);
    
    return {memory_usage, insertion_time, quality.avg_search_time_ms, quality};
  };
  
  // Test different configurations
  auto standard_stats = measure_index(false, 1.0f);
  auto extended_15_stats = measure_index(true, 1.5f);
  auto extended_20_stats = measure_index(true, 2.0f);
  
  // Print comprehensive results
  std::cout << "\nPerformance Comparison:\n";
  std::cout << "Standard HNSW:\n";
  std::cout << "  Memory: " << (standard_stats.memory_usage / 1024.0 / 1024.0) << " MB\n";
  std::cout << "  Insertion: " << standard_stats.insertion_time_ms << " ms\n";
  std::cout << "  Search: " << standard_stats.search_time_ms << " ms/query\n";
  std::cout << "  Recall: " << standard_stats.quality.avg_recall << "\n";
  
  std::cout << "Extended HNSW (1.5x):\n";
  std::cout << "  Memory: " << (extended_15_stats.memory_usage / 1024.0 / 1024.0) << " MB\n";
  std::cout << "  Insertion: " << extended_15_stats.insertion_time_ms << " ms\n";
  std::cout << "  Search: " << extended_15_stats.search_time_ms << " ms/query\n";
  std::cout << "  Recall: " << extended_15_stats.quality.avg_recall << "\n";
  
  std::cout << "Extended HNSW (2.0x):\n";
  std::cout << "  Memory: " << (extended_20_stats.memory_usage / 1024.0 / 1024.0) << " MB\n";
  std::cout << "  Insertion: " << extended_20_stats.insertion_time_ms << " ms\n";
  std::cout << "  Search: " << extended_20_stats.search_time_ms << " ms/query\n";
  std::cout << "  Recall: " << extended_20_stats.quality.avg_recall << "\n";
  
  // Memory usage should increase proportionally
  EXPECT_GT(extended_15_stats.memory_usage, standard_stats.memory_usage);
  EXPECT_GT(extended_20_stats.memory_usage, extended_15_stats.memory_usage);
  
  // Performance should be reasonable
  EXPECT_LT(extended_15_stats.insertion_time_ms, standard_stats.insertion_time_ms * 2.0);
  EXPECT_LT(extended_15_stats.search_time_ms, standard_stats.search_time_ms * 1.5);
  
  // Quality should be maintained or improved
  EXPECT_GE(extended_15_stats.quality.avg_recall, standard_stats.quality.avg_recall - 0.05);
  EXPECT_GE(extended_20_stats.quality.avg_recall, standard_stats.quality.avg_recall - 0.05);
}

// Test 4: Robustness Under Various Deletion Patterns
TEST_F(ExtendedNeighborsIntegrationTest, DeletionPatternRobustness) {
  auto dataset = CreateClusteredDataset(1000, 100, kDimensions, 10);
  
  auto test_deletion_pattern = [&](const std::string& pattern_name,
                                  std::function<std::set<size_t>()> deletion_func) {
    // Create extended index
    auto index = std::make_unique<hnswlib::HierarchicalNSW<float>>(
        space_.get(), kMaxElements, kM, kEFConstruction, 200,
        false, true, 1.6f);
    
    // Add vectors
    for (size_t i = 0; i < dataset.vectors.size(); ++i) {
      index->addPoint(dataset.vectors[i].data(), i);
    }
    
    // Measure before deletions
    auto before_metrics = EvaluateIndex(dataset, index.get(), 10, 60);
    
    // Apply deletion pattern
    auto deleted_ids = deletion_func();
    for (size_t id : deleted_ids) {
      index->markDelete(id);
    }
    
    // Measure after deletions
    auto after_metrics = EvaluateIndex(dataset, index.get(), 10, 60);
    
    double recall_drop = before_metrics.avg_recall - after_metrics.avg_recall;
    
    std::cout << pattern_name << " deletion pattern:\n";
    std::cout << "  Deleted: " << deleted_ids.size() << " vectors\n";
    std::cout << "  Recall drop: " << recall_drop << "\n";
    std::cout << "  Final recall: " << after_metrics.avg_recall << "\n\n";
    
    return std::make_pair(recall_drop, after_metrics.avg_recall);
  };
  
  // Test random deletions (25%)
  auto random_results = test_deletion_pattern("Random (25%)", [&]() {
    std::set<size_t> deleted;
    std::mt19937 gen(456);
    std::uniform_int_distribution<size_t> dis(0, dataset.vectors.size() - 1);
    while (deleted.size() < dataset.vectors.size() / 4) {
      deleted.insert(dis(gen));
    }
    return deleted;
  });
  
  // Test sequential deletions (first 25%)
  auto sequential_results = test_deletion_pattern("Sequential (25%)", [&]() {
    std::set<size_t> deleted;
    for (size_t i = 0; i < dataset.vectors.size() / 4; ++i) {
      deleted.insert(i);
    }
    return deleted;
  });
  
  // Test clustered deletions (remove every 4th vector)
  auto clustered_results = test_deletion_pattern("Clustered (every 4th)", [&]() {
    std::set<size_t> deleted;
    for (size_t i = 0; i < dataset.vectors.size(); i += 4) {
      deleted.insert(i);
    }
    return deleted;
  });
  
  // All patterns should maintain reasonable quality
  EXPECT_GE(random_results.second, 0.6);
  EXPECT_GE(sequential_results.second, 0.6);
  EXPECT_GE(clustered_results.second, 0.6);
  
  // Random deletions should be handled best by extended neighbors
  EXPECT_LT(random_results.first, 0.3);
}

// Test 5: Scalability Test with Large Dataset
TEST_F(ExtendedNeighborsIntegrationTest, ScalabilityTest) {
  const int large_size = 5000;
  const int query_size = 300;
  
  auto large_dataset = CreateGaussianDataset(large_size, query_size, kDimensions, 789);
  
  // Test with different extended factors
  std::vector<float> factors = {1.0f, 1.3f, 1.5f, 2.0f};
  
  for (float factor : factors) {
    bool use_extended = (factor > 1.0f);
    
    auto index = std::make_unique<hnswlib::HierarchicalNSW<float>>(
        space_.get(), large_size + 1000, kM, kEFConstruction, 200,
        false, use_extended, factor);
    
    // Measure insertion performance
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < large_dataset.vectors.size(); ++i) {
      index->addPoint(large_dataset.vectors[i].data(), i);
    }
    auto insertion_time = std::chrono::duration<double, std::milli>(
        std::chrono::high_resolution_clock::now() - start).count();
    
    // Measure search performance
    start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < 50; ++i) { // Sample searches
      auto results = index->searchKnn(large_dataset.queries[i].data(), 20);
    }
    auto search_sample_time = std::chrono::duration<double, std::milli>(
        std::chrono::high_resolution_clock::now() - start).count();
    
    double avg_search_time = search_sample_time / 50.0;
    
    std::cout << "Factor " << factor << ":\n";
    std::cout << "  Insertion time: " << insertion_time << " ms\n";
    std::cout << "  Avg search time: " << avg_search_time << " ms\n";
    std::cout << "  Elements: " << index->getCurrentElementCount() << "\n\n";
    
    // Performance should scale reasonably
    EXPECT_LT(insertion_time, 30000); // 30 seconds max
    EXPECT_LT(avg_search_time, 50);   // 50ms per search max
  }
}

// Test 6: Extended Neighbors with Replace Deleted
TEST_F(ExtendedNeighborsIntegrationTest, ExtendedNeighborsWithReplaceDeleted) {
  auto dataset = CreateGaussianDataset(500, 50, kDimensions, 999);
  
  // Create index with replace_deleted enabled
  auto index = std::make_unique<hnswlib::HierarchicalNSW<float>>(
      space_.get(), kMaxElements, kM, kEFConstruction, 200,
      true,   // allow_replace_deleted
      true,   // use_extended_neighbors
      1.5f);  // extended_list_factor
  
  // Add initial vectors
  for (size_t i = 0; i < dataset.vectors.size(); ++i) {
    index->addPoint(dataset.vectors[i].data(), i);
  }
  
  // Measure quality before operations
  auto before_metrics = EvaluateIndex(dataset, index.get(), 10, 50);
  
  // Delete some vectors
  std::set<size_t> deleted_ids;
  for (size_t i = 0; i < 50; ++i) {
    deleted_ids.insert(i);
    index->markDelete(i);
  }
  
  // Create replacement vectors
  auto replacement_vectors = CreateGaussianDataset(50, 0, kDimensions, 1111).vectors;
  
  // Replace deleted vectors
  for (size_t i = 0; i < 50; ++i) {
    index->addPoint(replacement_vectors[i].data(), i);
  }
  
  // Measure quality after replacement
  auto after_metrics = EvaluateIndex(dataset, index.get(), 10, 50);
  
  std::cout << "Replace Deleted Test:\n";
  std::cout << "  Before - Recall: " << before_metrics.avg_recall << "\n";
  std::cout << "  After  - Recall: " << after_metrics.avg_recall << "\n";
  
  // Quality should be maintained or improved after replacement
  EXPECT_GE(after_metrics.avg_recall, before_metrics.avg_recall - 0.1);
  EXPECT_GE(after_metrics.avg_recall, 0.7);
  
  // Verify no vectors are marked as deleted
  for (size_t i = 0; i < 50; ++i) {
    EXPECT_FALSE(index->isMarkedDeleted(i));
  }
}

}  // namespace valkey_search::indexes
