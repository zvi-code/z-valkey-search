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

  // Generate a key with fixed length (64 bytes)
  std::string GenerateFixedLengthKey(int id) {
    std::string key = "key_" + std::to_string(id);
    // Pad to 64 bytes
    while (key.length() < 64) {
      key += "_padding";
    }
    return key.substr(0, 64);  // Ensure exactly 64 bytes
  }
  
  // Generate tags with frequency distribution
  std::vector<std::pair<std::string, double>> GenerateTagsWithFrequency(
      int num_unique_tags, int avg_tag_length, const std::string& prefix = "tag") {
    std::vector<std::pair<std::string, double>> tags_with_freq;
    
    // Generate different frequency tiers
    int high_freq_count = std::max(1, num_unique_tags / 20);  // 5% are high frequency (50% of keys)
    int med_freq_count = std::max(1, num_unique_tags / 10);   // 10% are medium frequency (10% of keys)
    int low_freq_count = std::max(1, num_unique_tags / 5);    // 20% are low frequency (1-5% of keys)
    int rare_freq_count = num_unique_tags - high_freq_count - med_freq_count - low_freq_count; // Rest are rare (0.1% of keys)
    
    int tag_id = 0;
    
    // High frequency tags (50% of keys have them)
    for (int i = 0; i < high_freq_count; ++i) {
      std::string tag = prefix + "_high_freq_" + std::to_string(tag_id++);
      while (tag.length() < avg_tag_length) tag += "_pad";
      tags_with_freq.push_back({tag.substr(0, avg_tag_length), 0.5});
    }
    
    // Medium frequency tags (10% of keys)
    for (int i = 0; i < med_freq_count; ++i) {
      std::string tag = prefix + "_med_freq_" + std::to_string(tag_id++);
      while (tag.length() < avg_tag_length) tag += "_pad";
      tags_with_freq.push_back({tag.substr(0, avg_tag_length), 0.1});
    }
    
    // Low frequency tags (1-5% of keys)
    for (int i = 0; i < low_freq_count; ++i) {
      std::string tag = prefix + "_low_freq_" + std::to_string(tag_id++);
      while (tag.length() < avg_tag_length) tag += "_pad";
      double freq = 0.01 + (0.04 * i) / low_freq_count; // 1% to 5%
      tags_with_freq.push_back({tag.substr(0, avg_tag_length), freq});
    }
    
    // Rare frequency tags (0.1% of keys)
    for (int i = 0; i < rare_freq_count; ++i) {
      std::string tag = prefix + "_rare_" + std::to_string(tag_id++);
      while (tag.length() < avg_tag_length) tag += "_pad";
      tags_with_freq.push_back({tag.substr(0, avg_tag_length), 0.001});
    }
    
    return tags_with_freq;
  }
  
  BenchmarkResult RunAdvancedScenario(const std::string& name,
                                     const std::vector<std::pair<std::string, double>>& tags_with_freq,
                                     int total_keys,
                                     int avg_tags_per_key) {
    CreateTagIndex();
    
    auto start_memory = GetMemoryUsage();
    auto start_time = std::chrono::high_resolution_clock::now();
    
    size_t raw_data_size = 0;
    std::random_device rd;
    std::mt19937 gen(42); // Fixed seed for reproducibility
    
    // Add records to the index
    for (int i = 0; i < total_keys; ++i) {
      std::string key = GenerateFixedLengthKey(i);
      std::ostringstream tag_string;
      
      int tags_for_this_key = 0;
      bool first_tag = true;
      
      // For each tag, decide if this key should have it based on frequency
      for (const auto& [tag, frequency] : tags_with_freq) {
        std::uniform_real_distribution<> dis(0.0, 1.0);
        if (dis(gen) < frequency && tags_for_this_key < avg_tags_per_key * 2) {
          if (!first_tag) tag_string << ",";
          tag_string << tag;
          first_tag = false;
          tags_for_this_key++;
        }
      }
      
      // Ensure minimum tags per key
      while (tags_for_this_key < std::max(1, avg_tags_per_key / 2)) {
        if (!first_tag) tag_string << ",";
        // Add a random tag from the list
        std::uniform_int_distribution<> tag_dis(0, tags_with_freq.size() - 1);
        tag_string << tags_with_freq[tag_dis(gen)].first;
        first_tag = false;
        tags_for_this_key++;
      }
      
      auto result = tag_index_->AddRecord(key, tag_string.str());
      EXPECT_TRUE(result.ok()) << "Failed to add record: " << result.status();
      
      // Calculate raw data size
      raw_data_size += key.length() + tag_string.str().length();
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto end_memory = GetMemoryUsage();
    
    double elapsed = std::chrono::duration<double>(end_time - start_time).count();
    
    BenchmarkResult result;
    result.scenario_name = name;
    result.num_keys = total_keys;
    result.num_unique_tags = tags_with_freq.size();
    result.avg_tags_per_key = avg_tags_per_key;
    result.avg_tag_length = tags_with_freq.empty() ? 0 : 
        std::accumulate(tags_with_freq.begin(), tags_with_freq.end(), 0,
                       [](int sum, const auto& pair) { return sum + pair.first.length(); }) / tags_with_freq.size();
    result.memory_before_kb = start_memory.rss_kb;
    result.memory_after_kb = end_memory.rss_kb;
    result.memory_used_kb = end_memory.rss_kb - start_memory.rss_kb;
    result.time_seconds = elapsed;
    result.raw_data_size_bytes = raw_data_size;
    result.overhead_factor = static_cast<double>(result.memory_used_kb * 1024) / raw_data_size;
    
    return result;
  }
  
  // Legacy method for backward compatibility
  BenchmarkResult RunScenario(const std::string& name,
                             const std::vector<std::string>& tag_patterns,
                             int keys_per_tag,
                             int tags_per_key) {
    std::vector<std::pair<std::string, double>> tags_with_freq;
    for (const auto& tag : tag_patterns) {
      tags_with_freq.push_back({tag, 1.0 / tag_patterns.size()});
    }
    return RunAdvancedScenario(name, tags_with_freq, keys_per_tag * tag_patterns.size(), tags_per_key);
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
  
  const int TOTAL_KEYS = 10000000;  // 10M keys for all tests
  const int AVG_TAGS_PER_KEY = 8;  // Base tags per key
  
  std::cout << "\n=== Running Comprehensive Memory Analysis with " << TOTAL_KEYS << " keys ===\n";
  
  // ========== VARYING NUMBER OF UNIQUE TAGS ==========
  std::cout << "\n--- Testing Unique Tags Impact (Fixed: 10M keys, avg 8 tags/key, 32 byte tags) ---\n";
  
  std::vector<int> unique_tag_counts = {1000, 5000, 10000, 50000, 100000, 500000, 1000000};
  for (int unique_tags : unique_tag_counts) {
    auto tags = GenerateTagsWithFrequency(unique_tags, 32, "tag");
    results.push_back(RunAdvancedScenario("UniqueTags_" + std::to_string(unique_tags), 
                                         tags, TOTAL_KEYS, AVG_TAGS_PER_KEY));
  }
  
  // ========== VARYING TAG LENGTH ==========
  std::cout << "\n--- Testing Tag Length Impact (Fixed: 10M keys, 50K unique tags, avg 8 tags/key) ---\n";
  
  std::vector<int> tag_lengths = {8, 16, 32, 64, 128, 256, 512, 1024};
  for (int tag_length : tag_lengths) {
    auto tags = GenerateTagsWithFrequency(50000, tag_length, "len" + std::to_string(tag_length));
    results.push_back(RunAdvancedScenario("TagLen_" + std::to_string(tag_length), 
                                         tags, TOTAL_KEYS, AVG_TAGS_PER_KEY));
  }
  
  // ========== VARYING TAGS PER KEY ==========
  std::cout << "\n--- Testing Tags Per Key Impact (Fixed: 10M keys, 100K unique tags, 32 byte tags) ---\n";
  
  std::vector<int> tags_per_key_counts = {1, 3, 5, 8, 15, 25, 40, 60};
  for (int tags_per_key : tags_per_key_counts) {
    auto tags = GenerateTagsWithFrequency(100000, 32, "tpk" + std::to_string(tags_per_key));
    results.push_back(RunAdvancedScenario("TagsPerKey_" + std::to_string(tags_per_key), 
                                         tags, TOTAL_KEYS, tags_per_key));
  }
  
  // ========== FREQUENCY DISTRIBUTION SCENARIOS ==========
  std::cout << "\n--- Testing Frequency Distribution Impact (Fixed: 10M keys, avg 8 tags/key, 32 byte tags) ---\n";
  
  // High sharing scenario - fewer unique tags with more overlap
  {
    auto tags = GenerateTagsWithFrequency(10000, 32, "high_share");
    results.push_back(RunAdvancedScenario("FreqDist_HighSharing", tags, TOTAL_KEYS, AVG_TAGS_PER_KEY));
  }
  
  // Medium sharing scenario - balanced distribution  
  {
    auto tags = GenerateTagsWithFrequency(100000, 32, "med_share");
    results.push_back(RunAdvancedScenario("FreqDist_MediumSharing", tags, TOTAL_KEYS, AVG_TAGS_PER_KEY));
  }
  
  // Low sharing scenario - many unique tags with little overlap
  {
    auto tags = GenerateTagsWithFrequency(500000, 32, "low_share");
    results.push_back(RunAdvancedScenario("FreqDist_LowSharing", tags, TOTAL_KEYS, AVG_TAGS_PER_KEY));
  }
  
  // Extreme high frequency scenario - very few tags used by most keys
  {
    std::vector<std::pair<std::string, double>> extreme_high_freq;
    for (int i = 0; i < 100; ++i) {
      std::string tag = "extreme_freq_tag_" + std::to_string(i);
      while (tag.length() < 32) tag += "_pad";
      extreme_high_freq.push_back({tag.substr(0, 32), 0.8});  // 80% of keys have each tag
    }
    results.push_back(RunAdvancedScenario("FreqDist_ExtremeHighFreq", extreme_high_freq, TOTAL_KEYS, 10));
  }
  
  // Zipf-like distribution scenario - power law distribution
  {
    std::vector<std::pair<std::string, double>> zipf_tags;
    for (int i = 0; i < 50000; ++i) {
      std::string tag = "zipf_tag_" + std::to_string(i);
      while (tag.length() < 32) tag += "_pad";
      // Zipf distribution: frequency proportional to 1/rank
      double frequency = std::min(0.7, 1.0 / (i + 1));
      zipf_tags.push_back({tag.substr(0, 32), frequency});
    }
    results.push_back(RunAdvancedScenario("FreqDist_ZipfLike", zipf_tags, TOTAL_KEYS, AVG_TAGS_PER_KEY));
  }
  
  // ========== EXTREME SCENARIOS ==========
  std::cout << "\n--- Testing Extreme Scenarios ---\n";
  
  // Maximum unique tags scenario
  {
    auto tags = GenerateTagsWithFrequency(2000000, 16, "max_unique");
    results.push_back(RunAdvancedScenario("Extreme_MaxUnique", tags, TOTAL_KEYS, 5));
  }
  
  // Maximum tag length scenario
  {
    auto tags = GenerateTagsWithFrequency(10000, 2048, "max_len");
    results.push_back(RunAdvancedScenario("Extreme_MaxLength", tags, TOTAL_KEYS, 4));
  }
  
  // Maximum tags per key scenario
  {
    auto tags = GenerateTagsWithFrequency(50000, 24, "max_tpk");
    results.push_back(RunAdvancedScenario("Extreme_MaxTagsPerKey", tags, TOTAL_KEYS, 100));
  }
  
  // Minimum scenario - baseline
  {
    auto tags = GenerateTagsWithFrequency(100, 8, "min");
    results.push_back(RunAdvancedScenario("Extreme_Minimal", tags, TOTAL_KEYS, 1));
  }
  
  PrintResults(results);
}

// Smaller test for quick validation with 100K keys
TEST_F(MemoryBenchmarkTest, QuickMemoryValidation) {
  std::vector<BenchmarkResult> results;
  
  const int TOTAL_KEYS = 100000;  // 100K keys for quick tests
  const int AVG_TAGS_PER_KEY = 8;
  
  std::cout << "\n=== Quick Validation with " << TOTAL_KEYS << " keys ===\n";
  
  // Test varying unique tags
  std::vector<int> unique_tag_counts = {1000, 10000, 50000};
  for (int unique_tags : unique_tag_counts) {
    auto tags = GenerateTagsWithFrequency(unique_tags, 32, "tag");
    results.push_back(RunAdvancedScenario("Quick_UniqueTags_" + std::to_string(unique_tags), 
                                         tags, TOTAL_KEYS, AVG_TAGS_PER_KEY));
  }
  
  // Test varying tag lengths
  std::vector<int> tag_lengths = {16, 64, 256};
  for (int tag_length : tag_lengths) {
    auto tags = GenerateTagsWithFrequency(10000, tag_length, "len" + std::to_string(tag_length));
    results.push_back(RunAdvancedScenario("Quick_TagLen_" + std::to_string(tag_length), 
                                         tags, TOTAL_KEYS, AVG_TAGS_PER_KEY));
  }
  
  // Test frequency distributions
  {
    auto tags = GenerateTagsWithFrequency(5000, 32, "high_freq");
    results.push_back(RunAdvancedScenario("Quick_HighFreq", tags, TOTAL_KEYS, AVG_TAGS_PER_KEY));
  }
  
  {
    auto tags = GenerateTagsWithFrequency(50000, 32, "low_freq");
    results.push_back(RunAdvancedScenario("Quick_LowFreq", tags, TOTAL_KEYS, AVG_TAGS_PER_KEY));
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