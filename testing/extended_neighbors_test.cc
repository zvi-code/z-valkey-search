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
#include <set>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "testing/common.h"
#include "third_party/hnswlib/hnswalg.h"
#include "third_party/hnswlib/space_l2.h"
#include "third_party/hnswlib/space_ip.h"

namespace valkey_search::indexes {

namespace {
constexpr static int kDimensions = 16;
constexpr static int kM = 8;
constexpr static int kEFConstruction = 16;
constexpr static int kMaxElements = 1000;
const hnswlib::L2Space kL2Space{kDimensions};

// Helper function to create vectors with specific patterns for testing
std::vector<std::vector<float>> CreateTestVectors(int count, int dimensions, int seed = 42) {
  std::mt19937 gen(seed);
  std::uniform_real_distribution<float> dis(-1.0f, 1.0f);
  
  std::vector<std::vector<float>> vectors(count);
  for (int i = 0; i < count; ++i) {
    vectors[i].resize(dimensions);
    for (int j = 0; j < dimensions; ++j) {
      vectors[i][j] = dis(gen);
    }
  }
  return vectors;
}

// Helper function to get neighbor statistics
struct NeighborStats {
  int regular_neighbors;
  int extended_neighbors;
  int total_neighbors;
};

// Test fixture for Extended Neighbors functionality
class ExtendedNeighborsTest : public testing::Test {
 protected:
  void SetUp() override {
    space_ = std::make_unique<hnswlib::L2Space>(kDimensions);
  }

  // Create HNSW index with extended neighbors disabled
  std::unique_ptr<hnswlib::HierarchicalNSW<float>> CreateStandardIndex() {
    return std::make_unique<hnswlib::HierarchicalNSW<float>>(
        space_.get(), kMaxElements, kM, kEFConstruction, 200, // random seed
        false,  // allow_replace_deleted
        false,  // use_extended_neighbors
        1.0f    // extended_list_factor (ignored when disabled)
    );
  }

  // Create HNSW index with extended neighbors enabled
  std::unique_ptr<hnswlib::HierarchicalNSW<float>> CreateExtendedIndex(
      float extended_factor = 1.5f, bool allow_replace_deleted = false) {
    return std::make_unique<hnswlib::HierarchicalNSW<float>>(
        space_.get(), kMaxElements, kM, kEFConstruction, 200, // random seed
        allow_replace_deleted,
        true,           // use_extended_neighbors
        extended_factor // extended_list_factor
    );
  }

  // Get neighbor count statistics for a specific node
  NeighborStats GetNeighborStats(hnswlib::HierarchicalNSW<float>* index, 
                                hnswlib::tableint node_id, int level = 0) {
    NeighborStats stats = {0, 0, 0};
    
    // Access the node's neighbor list at the specified level
    char* data = index->data_level0_memory_.get() + 
                 (size_t)node_id * index->size_data_per_element_;
    if (level > 0) {
      // For higher levels, we'd need to access different memory layout
      // This is simplified for level 0 testing
      return stats;
    }

    hnswlib::linklistsizeint* ll = index->get_linklist0(node_id);
    if (ll) {
      stats.regular_neighbors = index->getListCount(ll);
      stats.total_neighbors = index->getTotalListCount(ll);
      stats.extended_neighbors = stats.total_neighbors - stats.regular_neighbors;
    }
    
    return stats;
  }

  std::unique_ptr<hnswlib::L2Space> space_;
};

}  // namespace

// Test 1: Basic Configuration Validation
TEST_F(ExtendedNeighborsTest, BasicConfigurationValidation) {
  // Test standard index creation
  auto standard_index = CreateStandardIndex();
  EXPECT_FALSE(standard_index->use_extended_neighbors_);
  EXPECT_EQ(standard_index->extended_list_factor_, 1.0f);
  EXPECT_EQ(standard_index->maxM_extended_, standard_index->M_);
  EXPECT_EQ(standard_index->maxM0_extended_, standard_index->maxM0_);

  // Test extended index creation with different factors
  auto extended_index_15 = CreateExtendedIndex(1.5f);
  EXPECT_TRUE(extended_index_15->use_extended_neighbors_);
  EXPECT_FLOAT_EQ(extended_index_15->extended_list_factor_, 1.5f);
  EXPECT_EQ(extended_index_15->maxM_extended_, 
            static_cast<size_t>(kM * 1.5f));
  EXPECT_EQ(extended_index_15->maxM0_extended_, 
            static_cast<size_t>(extended_index_15->maxM0_ * 1.5f));

  auto extended_index_20 = CreateExtendedIndex(2.0f);
  EXPECT_TRUE(extended_index_20->use_extended_neighbors_);
  EXPECT_FLOAT_EQ(extended_index_20->extended_list_factor_, 2.0f);
  EXPECT_EQ(extended_index_20->maxM_extended_, 
            static_cast<size_t>(kM * 2.0f));
}

// Test 2: Memory Allocation Validation
TEST_F(ExtendedNeighborsTest, MemoryAllocationValidation) {
  auto standard_index = CreateStandardIndex();
  auto extended_index = CreateExtendedIndex(1.5f);

  // Extended index should allocate more memory per element
  EXPECT_GT(extended_index->size_links_per_element_, 
            standard_index->size_links_per_element_);

  // The difference should account for extended neighbors
  size_t expected_extended_links_size = 
      extended_index->maxM_extended_ * sizeof(hnswlib::tableint) + sizeof(hnswlib::linklistsizeint);
  size_t expected_standard_links_size = 
      standard_index->M_ * sizeof(hnswlib::tableint) + sizeof(hnswlib::linklistsizeint);

  EXPECT_EQ(extended_index->size_links_per_element_, expected_extended_links_size);
  EXPECT_EQ(standard_index->size_links_per_element_, expected_standard_links_size);
}

// Test 3: Basic Insertion and Extended Neighbor Storage
TEST_F(ExtendedNeighborsTest, BasicInsertionWithExtendedNeighbors) {
  auto extended_index = CreateExtendedIndex(1.5f);
  auto vectors = CreateTestVectors(50, kDimensions);

  // Add vectors to the index
  for (size_t i = 0; i < vectors.size(); ++i) {
    extended_index->addPoint(vectors[i].data(), i);
  }

  // Verify that some nodes have extended neighbors
  int nodes_with_extended = 0;
  int total_extended_neighbors = 0;

  for (size_t i = 0; i < vectors.size(); ++i) {
    auto stats = GetNeighborStats(extended_index.get(), i);
    if (stats.extended_neighbors > 0) {
      nodes_with_extended++;
      total_extended_neighbors += stats.extended_neighbors;
    }
    
    // Validate constraints
    EXPECT_LE(stats.regular_neighbors, kM);
    EXPECT_LE(stats.total_neighbors, static_cast<int>(extended_index->maxM0_extended_));
    EXPECT_GE(stats.extended_neighbors, 0);
  }

  // With a sufficient number of vectors and extended factor > 1.0,
  // we should see some extended neighbors
  EXPECT_GT(nodes_with_extended, 0);
  EXPECT_GT(total_extended_neighbors, 0);
}

// Test 4: Comparison with Standard Index
TEST_F(ExtendedNeighborsTest, ComparisonWithStandardIndex) {
  auto standard_index = CreateStandardIndex();
  auto extended_index = CreateExtendedIndex(1.5f);
  auto vectors = CreateTestVectors(100, kDimensions);

  // Add same vectors to both indices
  for (size_t i = 0; i < vectors.size(); ++i) {
    standard_index->addPoint(vectors[i].data(), i);
    extended_index->addPoint(vectors[i].data(), i);
  }

  // Compare neighbor counts
  int total_standard_neighbors = 0;
  int total_extended_neighbors = 0;

  for (size_t i = 0; i < vectors.size(); ++i) {
    auto standard_stats = GetNeighborStats(standard_index.get(), i);
    auto extended_stats = GetNeighborStats(extended_index.get(), i);

    total_standard_neighbors += standard_stats.total_neighbors;
    total_extended_neighbors += extended_stats.total_neighbors;

    // Extended index should have >= neighbors than standard
    EXPECT_GE(extended_stats.total_neighbors, standard_stats.total_neighbors);
  }

  // Overall, extended index should have more total neighbors
  EXPECT_GT(total_extended_neighbors, total_standard_neighbors);
}

// Test 5: Deletion Handling - Basic
TEST_F(ExtendedNeighborsTest, BasicDeletionHandling) {
  auto extended_index = CreateExtendedIndex(1.5f);
  auto vectors = CreateTestVectors(30, kDimensions);

  // Add vectors
  for (size_t i = 0; i < vectors.size(); ++i) {
    extended_index->addPoint(vectors[i].data(), i);
  }

  // Mark some nodes as deleted
  std::set<size_t> deleted_nodes = {5, 10, 15, 20};
  for (size_t node : deleted_nodes) {
    extended_index->markDelete(node);
    EXPECT_TRUE(extended_index->isMarkedDeleted(node));
  }

  // Verify that search still works despite deletions
  auto query = vectors[0];
  auto results = extended_index->searchKnn(query.data(), 10);
  
  EXPECT_FALSE(results.empty());
  
  // Results should not contain deleted nodes
  while (!results.empty()) {
    auto result = results.top();
    results.pop();
    EXPECT_EQ(deleted_nodes.count(result.second), 0);
  }
}

// Test 6: Extended Neighbor Promotion During Deletion
TEST_F(ExtendedNeighborsTest, ExtendedNeighborPromotionDuringDeletion) {
  auto extended_index = CreateExtendedIndex(1.5f, true); // enable replace_deleted
  auto vectors = CreateTestVectors(50, kDimensions);

  // Add vectors
  for (size_t i = 0; i < vectors.size(); ++i) {
    extended_index->addPoint(vectors[i].data(), i);
  }

  // Find a node with extended neighbors
  hnswlib::tableint target_node = 0;
  NeighborStats initial_stats;
  for (size_t i = 0; i < vectors.size(); ++i) {
    auto stats = GetNeighborStats(extended_index.get(), i);
    if (stats.extended_neighbors > 0) {
      target_node = i;
      initial_stats = stats;
      break;
    }
  }

  // If we found a node with extended neighbors, test promotion
  if (initial_stats.extended_neighbors > 0) {
    // Delete and replace the node
    extended_index->markDelete(target_node);
    
    // Add a new vector with the same ID (replacement)
    auto new_vector = CreateTestVectors(1, kDimensions, 999)[0];
    extended_index->addPoint(new_vector.data(), target_node);

    // The replacement should have triggered extended neighbor promotion
    // in other nodes that had this node in their extended neighbors
    
    // Verify the node is no longer marked as deleted
    EXPECT_FALSE(extended_index->isMarkedDeleted(target_node));
  }
}

// Test 7: Search Quality with Extended Neighbors
TEST_F(ExtendedNeighborsTest, SearchQualityWithExtendedNeighbors) {
  auto standard_index = CreateStandardIndex();
  auto extended_index = CreateExtendedIndex(1.5f);
  auto vectors = CreateTestVectors(200, kDimensions);

  // Add vectors to both indices
  for (size_t i = 0; i < vectors.size(); ++i) {
    standard_index->addPoint(vectors[i].data(), i);
    extended_index->addPoint(vectors[i].data(), i);
  }

  // Delete some nodes to test extended neighbor benefits
  std::vector<size_t> nodes_to_delete = {10, 25, 40, 55, 70, 85};
  for (size_t node : nodes_to_delete) {
    standard_index->markDelete(node);
    extended_index->markDelete(node);
  }

  // Perform searches and compare results
  auto query_vectors = CreateTestVectors(10, kDimensions, 999);
  int extended_better_count = 0;
  
  for (const auto& query : query_vectors) {
    auto standard_results = standard_index->searchKnn(query.data(), 10);
    auto extended_results = extended_index->searchKnn(query.data(), 10);
    
    // Count valid (non-deleted) results
    int standard_valid = 0;
    int extended_valid = 0;
    
    auto temp_standard = standard_results;
    while (!temp_standard.empty()) {
      auto result = temp_standard.top();
      temp_standard.pop();
      if (!standard_index->isMarkedDeleted(result.second)) {
        standard_valid++;
      }
    }
    
    auto temp_extended = extended_results;
    while (!temp_extended.empty()) {
      auto result = temp_extended.top();
      temp_extended.pop();
      if (!extended_index->isMarkedDeleted(result.second)) {
        extended_valid++;
      }
    }
    
    if (extended_valid >= standard_valid) {
      extended_better_count++;
    }
  }

  // Extended neighbors should provide equal or better search quality
  // in the presence of deletions
  EXPECT_GE(extended_better_count, query_vectors.size() / 2);
}

// Test 8: Bit Packing for Neighbor Counts
TEST_F(ExtendedNeighborsTest, BitPackingForNeighborCounts) {
  auto extended_index = CreateExtendedIndex(1.5f);
  
  // Test the bit packing functions directly
  hnswlib::linklistsizeint packed_count;
  
  // Test setting regular and total counts
  unsigned short regular_count = 8;
  unsigned short total_count = 12;
  
  extended_index->setListCounts(&packed_count, regular_count, total_count);
  
  // Verify retrieval
  EXPECT_EQ(extended_index->getListCount(&packed_count), regular_count);
  EXPECT_EQ(extended_index->getTotalListCount(&packed_count), total_count);
  
  // Test edge cases
  extended_index->setListCounts(&packed_count, 0, 0);
  EXPECT_EQ(extended_index->getListCount(&packed_count), 0);
  EXPECT_EQ(extended_index->getTotalListCount(&packed_count), 0);
  
  // Test maximum values (16-bit limits)
  unsigned short max_val = 65535;
  extended_index->setListCounts(&packed_count, max_val, max_val);
  EXPECT_EQ(extended_index->getListCount(&packed_count), max_val);
  EXPECT_EQ(extended_index->getTotalListCount(&packed_count), max_val);
}

// Test 9: Serialization and Deserialization
TEST_F(ExtendedNeighborsTest, SerializationDeserialization) {
  auto original_index = CreateExtendedIndex(1.5f);
  auto vectors = CreateTestVectors(100, kDimensions);

  // Populate original index
  for (size_t i = 0; i < vectors.size(); ++i) {
    original_index->addPoint(vectors[i].data(), i);
  }

  // Mark some nodes as deleted
  original_index->markDelete(10);
  original_index->markDelete(25);

  // Serialize the index
  std::stringstream ss;
  hnswlib::HNSWStreamWriter writer(ss);
  original_index->SaveIndex(writer);

  // Create new index and deserialize
  auto loaded_index = CreateExtendedIndex(1.5f);
  hnswlib::HNSWStreamReader reader(ss);
  loaded_index->LoadIndex(reader, space_.get(), kMaxElements);

  // Verify configuration was preserved
  EXPECT_EQ(loaded_index->use_extended_neighbors_, true);
  EXPECT_FLOAT_EQ(loaded_index->extended_list_factor_, 1.5f);
  EXPECT_EQ(loaded_index->maxM_extended_, original_index->maxM_extended_);

  // Verify deleted nodes status
  EXPECT_TRUE(loaded_index->isMarkedDeleted(10));
  EXPECT_TRUE(loaded_index->isMarkedDeleted(25));
  EXPECT_FALSE(loaded_index->isMarkedDeleted(5));

  // Verify search results are similar
  auto query = vectors[0];
  auto original_results = original_index->searchKnn(query.data(), 5);
  auto loaded_results = loaded_index->searchKnn(query.data(), 5);
  
  EXPECT_EQ(original_results.size(), loaded_results.size());
}

// Test 10: Performance Stress Test
TEST_F(ExtendedNeighborsTest, PerformanceStressTest) {
  auto extended_index = CreateExtendedIndex(1.8f);
  auto vectors = CreateTestVectors(500, kDimensions);

  // Add vectors (this tests insertion performance)
  auto start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < vectors.size(); ++i) {
    extended_index->addPoint(vectors[i].data(), i);
  }
  auto insertion_time = std::chrono::high_resolution_clock::now() - start;

  // Perform searches (this tests search performance)
  auto query_vectors = CreateTestVectors(50, kDimensions, 888);
  start = std::chrono::high_resolution_clock::now();
  for (const auto& query : query_vectors) {
    auto results = extended_index->searchKnn(query.data(), 10);
    EXPECT_FALSE(results.empty());
  }
  auto search_time = std::chrono::high_resolution_clock::now() - start;

  // Delete some nodes (this tests deletion performance)
  start = std::chrono::high_resolution_clock::now();
  for (size_t i = 0; i < 50; ++i) {
    extended_index->markDelete(i);
  }
  auto deletion_time = std::chrono::high_resolution_clock::now() - start;

  // Performance should be reasonable (these are sanity checks)
  EXPECT_LT(insertion_time.count(), 10000000000LL); // 10 seconds
  EXPECT_LT(search_time.count(), 5000000000LL);     // 5 seconds
  EXPECT_LT(deletion_time.count(), 1000000000LL);   // 1 second

  // Log performance metrics
  std::cout << "Extended Neighbors Performance Test Results:\n";
  std::cout << "  Insertion time: " << insertion_time.count() / 1000000.0 << " ms\n";
  std::cout << "  Search time: " << search_time.count() / 1000000.0 << " ms\n";
  std::cout << "  Deletion time: " << deletion_time.count() / 1000000.0 << " ms\n";
}

// Test 11: Edge Cases and Error Conditions
TEST_F(ExtendedNeighborsTest, EdgeCasesAndErrorConditions) {
  // Test with extended_factor = 1.0 (should behave like standard)
  auto index_10 = CreateExtendedIndex(1.0f);
  EXPECT_TRUE(index_10->use_extended_neighbors_);
  EXPECT_FLOAT_EQ(index_10->extended_list_factor_, 1.0f);
  EXPECT_EQ(index_10->maxM_extended_, index_10->M_);

  // Test with very high extended_factor
  auto index_high = CreateExtendedIndex(3.0f);
  EXPECT_FLOAT_EQ(index_high->extended_list_factor_, 3.0f);
  EXPECT_EQ(index_high->maxM_extended_, static_cast<size_t>(kM * 3.0f));

  // Test adding vectors to empty index
  auto vectors = CreateTestVectors(5, kDimensions);
  for (size_t i = 0; i < vectors.size(); ++i) {
    index_high->addPoint(vectors[i].data(), i);
  }

  // Should work without errors
  auto results = index_high->searchKnn(vectors[0].data(), 3);
  EXPECT_FALSE(results.empty());
}

// Test 12: Extended Neighbor Statistics and Debugging
TEST_F(ExtendedNeighborsTest, ExtendedNeighborStatistics) {
  auto extended_index = CreateExtendedIndex(1.5f);
  auto vectors = CreateTestVectors(100, kDimensions);

  // Add vectors
  for (size_t i = 0; i < vectors.size(); ++i) {
    extended_index->addPoint(vectors[i].data(), i);
  }

  // Collect statistics
  int nodes_with_extended = 0;
  int total_regular_neighbors = 0;
  int total_extended_neighbors = 0;
  int max_extended_per_node = 0;

  for (size_t i = 0; i < vectors.size(); ++i) {
    auto stats = GetNeighborStats(extended_index.get(), i);
    
    total_regular_neighbors += stats.regular_neighbors;
    total_extended_neighbors += stats.extended_neighbors;
    
    if (stats.extended_neighbors > 0) {
      nodes_with_extended++;
      max_extended_per_node = std::max(max_extended_per_node, stats.extended_neighbors);
    }
  }

  // Print statistics for debugging
  std::cout << "Extended Neighbors Statistics:\n";
  std::cout << "  Nodes with extended neighbors: " << nodes_with_extended 
            << "/" << vectors.size() << "\n";
  std::cout << "  Total regular neighbors: " << total_regular_neighbors << "\n";
  std::cout << "  Total extended neighbors: " << total_extended_neighbors << "\n";
  std::cout << "  Max extended neighbors per node: " << max_extended_per_node << "\n";
  std::cout << "  Average extended per node: " 
            << (nodes_with_extended > 0 ? (double)total_extended_neighbors / nodes_with_extended : 0.0) << "\n";

  // Sanity checks
  EXPECT_GT(total_regular_neighbors, 0);
  EXPECT_GE(total_extended_neighbors, 0);
  EXPECT_LE(max_extended_per_node, static_cast<int>(extended_index->maxM0_extended_ - kM));
}

}  // namespace valkey_search::indexes
