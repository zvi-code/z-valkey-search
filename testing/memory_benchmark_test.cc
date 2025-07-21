/*
 * Copyright (c) 2025, valkey-search contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "src/indexes/tag.h"
#include "src/utils/patricia_tree.h"
#include "src/utils/string_interning.h"
#include "src/index_schema.pb.h"
#include "testing/common.h"
#include "vmsdk/src/testing_infra/utils.h"

namespace valkey_search::testing {

// Memory measurement utilities
struct MemoryStats {
  size_t rss_kb = 0;
  size_t virt_kb = 0;
  size_t heap_kb = 0;
};

MemoryStats GetMemoryUsage() {
  MemoryStats stats;
  std::ifstream status("/proc/self/status");
  std::string line;
  
  while (std::getline(status, line)) {
    if (line.substr(0, 6) == "VmRSS:") {
      stats.rss_kb = std::stoul(line.substr(7));
    } else if (line.substr(0, 7) == "VmSize:") {
      stats.virt_kb = std::stoul(line.substr(8));
    }
  }
  return stats;
}

class MemoryBenchmarkTest : public vmsdk::ValkeyTest {
 public:
  void SetUp() override {
    vmsdk::ValkeyTest::SetUp();
  }

  void CreateTagIndex(char separator = ',', bool case_sensitive = false) {
    data_model::TagIndex tag_index_proto;
    tag_index_proto.set_separator(std::string(1, separator));
    tag_index_proto.set_case_sensitive(case_sensitive);
    tag_index_ = std::make_unique<IndexTester<indexes::Tag, data_model::TagIndex>>(
        tag_index_proto);
  }

  // Generate test data patterns
  std::vector<std::string> GenerateHierarchicalTags(int count, const std::string& prefix) {
    std::vector<std::string> tags;
    std::vector<std::string> departments = {"eng", "sales", "hr", "marketing"};
    std::vector<std::string> teams = {"backend", "frontend", "mobile", "devops"};
    std::vector<std::string> roles = {"lead", "senior", "junior", "intern"};
    
    for (int i = 0; i < count; ++i) {
      std::ostringstream tag;
      tag << prefix << ":" 
          << departments[i % departments.size()] << ":"
          << teams[i % teams.size()] << ":"
          << roles[i % roles.size()] << ":"
          << "user" << (i + 1000);
      tags.push_back(tag.str());
    }
    return tags;
  }

  std::vector<std::string> GenerateRandomTags(int count, int length) {
    std::vector<std::string> tags;
    std::random_device rd;
    std::mt19937 gen(42); // Fixed seed for reproducibility
    std::uniform_int_distribution<> dis('a', 'z');
    
    for (int i = 0; i < count; ++i) {
      std::string tag;
      tag.reserve(length);
      for (int j = 0; j < length; ++j) {
        tag += static_cast<char>(dis(gen));
      }
      tag += "_" + std::to_string(i); // Ensure uniqueness
      tags.push_back(tag);
    }
    return tags;
  }

  std::vector<std::string> GenerateClusteredTags(int count, int clusters) {
    std::vector<std::string> tags;
    std::vector<std::string> cluster_prefixes;
    
    // Generate cluster prefixes
    for (int c = 0; c < clusters; ++c) {
      cluster_prefixes.push_back("cluster" + std::to_string(c) + "_log_2024_");
    }
    
    for (int i = 0; i < count; ++i) {
      int cluster = i % clusters;
      std::string tag = cluster_prefixes[cluster] + "entry_" + std::to_string(i / clusters);
      tags.push_back(tag);
    }
    return tags;
  }

  std::vector<std::string> GenerateLongTags(int count, int base_length, const std::string& content_type) {
    std::vector<std::string> tags;
    for (int i = 0; i < count; ++i) {
      std::ostringstream tag;
      if (content_type == "metadata") {
        tag << "metadata:project:2024:quarter:q" << (i % 4 + 1)
            << ":customer:enterprise:region:north-america:priority:high:status:active"
            << ":created_by:user" << i << ":timestamp:" << (1640995200 + i * 3600);
        // Pad to desired length
        while (tag.str().length() < base_length) {
          tag << ":extra_field_" << tag.str().length();
        }
      } else if (content_type == "4kb") {
        tag << "large_tag_" << i << "_";
        while (tag.str().length() < base_length) {
          tag << "abcdefghijklmnopqrstuvwxyz0123456789";
        }
      }
      tags.push_back(tag.str().substr(0, base_length));
    }
    return tags;
  }

  struct BenchmarkResult {
    std::string scenario_name;
    int num_keys;
    int num_unique_tags;
    int avg_tags_per_key;
    int avg_tag_length;
    size_t memory_before_kb;
    size_t memory_after_kb;
    size_t memory_used_kb;
    double time_seconds;
    size_t raw_data_size_bytes;
    double overhead_factor;
  };

  BenchmarkResult RunScenario(const std::string& name,
                             const std::vector<std::string>& tag_patterns,
                             int keys_per_tag,
                             int tags_per_key) {
    CreateTagIndex();
    
    auto start_memory = GetMemoryUsage();
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Calculate raw data size
    size_t raw_data_size = 0;
    
    // Add records to the index
    int key_counter = 0;
    for (const auto& tag_pattern : tag_patterns) {
      for (int k = 0; k < keys_per_tag; ++k) {
        std::string key = "key" + std::to_string(++key_counter);
        
        // Build tag string for this key
        std::ostringstream tag_string;
        for (int t = 0; t < tags_per_key; ++t) {
          if (t > 0) tag_string << ",";
          if (t < tag_patterns.size()) {
            tag_string << tag_patterns[t];
          } else {
            tag_string << tag_patterns[t % tag_patterns.size()];
          }
        }
        
        auto result = tag_index_->AddRecord(key, tag_string.str());
        EXPECT_TRUE(result.ok()) << "Failed to add record: " << result.status();
        
        // Calculate raw data size
        raw_data_size += key.length() + tag_string.str().length();
      }
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto end_memory = GetMemoryUsage();
    
    double elapsed = std::chrono::duration<double>(end_time - start_time).count();
    
    BenchmarkResult result;
    result.scenario_name = name;
    result.num_keys = key_counter;
    result.num_unique_tags = tag_patterns.size();
    result.avg_tags_per_key = tags_per_key;
    result.avg_tag_length = tag_patterns.empty() ? 0 : 
        std::accumulate(tag_patterns.begin(), tag_patterns.end(), 0,
                       [](int sum, const std::string& tag) { return sum + tag.length(); }) / tag_patterns.size();
    result.memory_before_kb = start_memory.rss_kb;
    result.memory_after_kb = end_memory.rss_kb;
    result.memory_used_kb = end_memory.rss_kb - start_memory.rss_kb;
    result.time_seconds = elapsed;
    result.raw_data_size_bytes = raw_data_size;
    result.overhead_factor = static_cast<double>(result.memory_used_kb * 1024) / raw_data_size;
    
    return result;
  }

  void PrintResults(const std::vector<BenchmarkResult>& results) {
    std::cout << "\n=== Tag Index Memory Benchmark Results ===\n";
    std::cout << std::left << std::setw(20) << "Scenario"
              << std::setw(10) << "Keys" 
              << std::setw(12) << "UniqueTags"
              << std::setw(12) << "Tags/Key"
              << std::setw(12) << "TagLen"
              << std::setw(12) << "MemoryKB"
              << std::setw(12) << "RawDataKB"
              << std::setw(10) << "Overhead"
              << std::setw(10) << "Time(s)"
              << "\n";
    std::cout << std::string(120, '-') << "\n";
    
    for (const auto& result : results) {
      std::cout << std::left << std::setw(20) << result.scenario_name.substr(0, 19)
                << std::setw(10) << result.num_keys
                << std::setw(12) << result.num_unique_tags
                << std::setw(12) << result.avg_tags_per_key
                << std::setw(12) << result.avg_tag_length
                << std::setw(12) << result.memory_used_kb
                << std::setw(12) << (result.raw_data_size_bytes / 1024)
                << std::setw(10) << std::fixed << std::setprecision(2) << result.overhead_factor
                << std::setw(10) << std::fixed << std::setprecision(2) << result.time_seconds
                << "\n";
    }
    
    // Write CSV for further analysis
    std::ofstream csv("tag_memory_benchmark.csv");
    csv << "Scenario,Keys,UniqueTags,TagsPerKey,AvgTagLength,MemoryKB,RawDataKB,OverheadFactor,TimeSeconds\n";
    for (const auto& result : results) {
      csv << result.scenario_name << ","
          << result.num_keys << ","
          << result.num_unique_tags << ","
          << result.avg_tags_per_key << ","
          << result.avg_tag_length << ","
          << result.memory_used_kb << ","
          << (result.raw_data_size_bytes / 1024) << ","
          << result.overhead_factor << ","
          << result.time_seconds << "\n";
    }
    csv.close();
    std::cout << "\nDetailed results written to tag_memory_benchmark.csv\n";
  }

  std::unique_ptr<IndexTester<indexes::Tag, data_model::TagIndex>> tag_index_;
};

TEST_F(MemoryBenchmarkTest, ComprehensiveMemoryAnalysis) {
  std::vector<BenchmarkResult> results;
  
  // Scenario 1: Short tags with high sharing (hierarchical)
  {
    auto tags = GenerateHierarchicalTags(100, "org:acme");
    results.push_back(RunScenario("Short_Hierarchical", tags, 100, 5));
  }
  
  // Scenario 2: Medium tags with moderate sharing
  {
    auto tags = GenerateClusteredTags(1000, 10);
    results.push_back(RunScenario("Medium_Clustered", tags, 10, 10));
  }
  
  // Scenario 3: Long tags with low sharing
  {
    auto tags = GenerateLongTags(100, 100, "metadata");
    results.push_back(RunScenario("Long_Metadata", tags, 100, 20));
  }
  
  // Scenario 4: Random tags (worst case for Patricia tree)
  {
    auto tags = GenerateRandomTags(1000, 30);
    results.push_back(RunScenario("Random_NoSharing", tags, 10, 10));
  }
  
  // Scenario 5: Very long tags (4KB) with high sharing
  {
    auto tags = GenerateLongTags(10, 4096, "4kb");
    results.push_back(RunScenario("VeryLong_4KB", tags, 1000, 10));
  }
  
  // Scenario 6: Many short tags per key
  {
    auto tags = GenerateRandomTags(500, 10);
    results.push_back(RunScenario("ManyShort_Tags", tags, 20, 50));
  }
  
  // Scenario 7: Sparse tags (each tag used by few keys)
  {
    auto tags = GenerateRandomTags(5000, 25);
    results.push_back(RunScenario("Sparse_Tags", tags, 2, 8));
  }
  
  PrintResults(results);
}

// Test Patricia tree directly
TEST_F(MemoryBenchmarkTest, PatriciaTreeDirectTest) {
  using PatriciaTreeTest = PatriciaTree<InternedStringPtr, InternedStringPtrHash, InternedStringPtrEqual>;
  
  auto start_memory = GetMemoryUsage();
  
  PatriciaTreeTest tree(false); // case insensitive
  
  // Test with hierarchical data
  std::vector<std::string> hierarchical_tags = GenerateHierarchicalTags(10000, "org:company");
  
  for (int i = 0; i < hierarchical_tags.size(); ++i) {
    auto key = StringInternStore::Intern("key" + std::to_string(i));
    tree.AddKeyValue(hierarchical_tags[i], key);
  }
  
  auto end_memory = GetMemoryUsage();
  
  std::cout << "\nPatricia Tree Direct Test (10K hierarchical tags):\n";
  std::cout << "Memory used: " << (end_memory.rss_kb - start_memory.rss_kb) << " KB\n";
  
  // Test prefix matching
  auto prefix_iter = tree.PrefixMatcher("org:company:eng");
  int prefix_matches = 0;
  while (!prefix_iter.Done()) {
    prefix_matches++;
    prefix_iter.Next();
  }
  std::cout << "Prefix matches for 'org:company:eng': " << prefix_matches << "\n";
}

// Test string interning directly  
TEST_F(MemoryBenchmarkTest, StringInterningDirectTest) {
  auto start_memory = GetMemoryUsage();
  auto& intern_store = StringInternStore::Instance();
  
  // Test with various sharing patterns
  std::vector<InternedStringPtr> interned_strings;
  
  // High sharing scenario
  std::string base_string = "org:company:department:engineering:team:backend:user:";
  for (int i = 0; i < 10000; ++i) {
    std::string full_string = base_string + std::to_string(i);
    auto interned = StringInternStore::Intern(full_string);
    interned_strings.push_back(interned);
  }
  
  auto mid_memory = GetMemoryUsage();
  
  // Add duplicate strings (should reuse existing)
  for (int i = 0; i < 5000; ++i) {
    std::string full_string = base_string + std::to_string(i);
    auto interned = StringInternStore::Intern(full_string);
    interned_strings.push_back(interned);
  }
  
  auto end_memory = GetMemoryUsage();
  
  std::cout << "\nString Interning Direct Test:\n";
  std::cout << "Memory after 10K unique strings: " << (mid_memory.rss_kb - start_memory.rss_kb) << " KB\n";
  std::cout << "Memory after adding 5K duplicates: " << (end_memory.rss_kb - start_memory.rss_kb) << " KB\n";
  std::cout << "Store size: " << intern_store.Size() << " unique strings\n";
}

} // namespace valkey_search::testing