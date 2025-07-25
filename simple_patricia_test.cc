/*
 * Simple Patricia tree and string interning memory test
 * Compile with: g++ -I./src -I./.build-release/.deps/install/include simple_patricia_test.cc -o simple_test
 */

#include <iostream>
#include <iomanip>
#include <vector>
#include <string>
#include <chrono>
#include <fstream>
#include <sstream>
#include <memory>
#include <random>
#include <unordered_map>

// Simple memory tracking
struct MemoryStats {
    size_t rss_kb = 0;
    size_t virt_kb = 0;
};

MemoryStats GetMemoryUsage() {
    MemoryStats stats;
    std::ifstream status("/proc/self/status");
    std::string line;
    
    while (std::getline(status, line)) {
        if (line.substr(0, 6) == "VmRSS:") {
            std::istringstream iss(line.substr(7));
            iss >> stats.rss_kb;
        } else if (line.substr(0, 7) == "VmSize:") {
            std::istringstream iss(line.substr(8));
            iss >> stats.virt_kb;
        }
    }
    return stats;
}

// Simplified Patricia Tree for testing
class SimplePatriciaTree {
private:
    struct Node {
        std::string prefix;
        std::unordered_map<char, std::unique_ptr<Node>> children;
        std::vector<std::string> values;  // Documents that match at this node
        
        Node(const std::string& p = "") : prefix(p) {}
    };
    
    std::unique_ptr<Node> root_;
    
public:
    SimplePatriciaTree() : root_(std::make_unique<Node>()) {}
    
    void AddKeyValue(const std::string& key, const std::string& value) {
        Node* current = root_.get();
        std::string remaining = key;
        
        while (!remaining.empty()) {
            char first_char = remaining[0];
            auto it = current->children.find(first_char);
            
            if (it == current->children.end()) {
                // Create new node
                auto new_node = std::make_unique<Node>(remaining);
                new_node->values.push_back(value);
                current->children[first_char] = std::move(new_node);
                return;
            }
            
            // Find common prefix
            Node* child = it->second.get();
            std::string& child_prefix = child->prefix;
            size_t common = 0;
            size_t max_len = std::min(remaining.length(), child_prefix.length());
            
            while (common < max_len && remaining[common] == child_prefix[common]) {
                common++;
            }
            
            if (common == child_prefix.length()) {
                // Move to child, continue with remaining
                current = child;
                remaining = remaining.substr(common);
            } else if (common == remaining.length()) {
                // Current key is prefix of child, need to split
                auto new_node = std::make_unique<Node>(child_prefix.substr(common));
                new_node->children = std::move(child->children);
                new_node->values = std::move(child->values);
                
                child->prefix = remaining;
                child->children.clear();
                child->values.clear();
                child->values.push_back(value);
                child->children[child_prefix[common]] = std::move(new_node);
                return;
            } else {
                // Need to split child node
                auto new_child = std::make_unique<Node>(child_prefix.substr(common));
                new_child->children = std::move(child->children);
                new_child->values = std::move(child->values);
                
                auto new_sibling = std::make_unique<Node>(remaining.substr(common));
                new_sibling->values.push_back(value);
                
                child->prefix = child_prefix.substr(0, common);
                child->children.clear();
                child->values.clear();
                child->children[child_prefix[common]] = std::move(new_child);
                child->children[remaining[common]] = std::move(new_sibling);
                return;
            }
        }
        
        // Add to current node
        current->values.push_back(value);
    }
    
    size_t GetNodeCount() const {
        return CountNodes(root_.get());
    }
    
    size_t GetTotalValues() const {
        return CountValues(root_.get());
    }
    
private:
    size_t CountNodes(const Node* node) const {
        if (!node) return 0;
        size_t count = 1;
        for (const auto& child : node->children) {
            count += CountNodes(child.second.get());
        }
        return count;
    }
    
    size_t CountValues(const Node* node) const {
        if (!node) return 0;
        size_t count = node->values.size();
        for (const auto& child : node->children) {
            count += CountValues(child.second.get());
        }
        return count;
    }
};

// Simple string interning
class SimpleStringIntern {
private:
    std::unordered_map<std::string, std::shared_ptr<std::string>> store_;
    
public:
    std::shared_ptr<std::string> Intern(const std::string& str) {
        auto it = store_.find(str);
        if (it != store_.end()) {
            return it->second;
        }
        auto interned = std::make_shared<std::string>(str);
        store_[str] = interned;
        return interned;
    }
    
    size_t Size() const { return store_.size(); }
    
    void Clear() { store_.clear(); }
};

// Test data generators
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
    std::mt19937 gen(42); // Fixed seed
    std::uniform_int_distribution<> dis('a', 'z');
    
    for (int i = 0; i < count; ++i) {
        std::string tag;
        tag.reserve(length);
        for (int j = 0; j < length; ++j) {
            tag += static_cast<char>(dis(gen));
        }
        tag += "_" + std::to_string(i);
        tags.push_back(tag);
    }
    return tags;
}

std::vector<std::string> GenerateLongTags(int count, int length) {
    std::vector<std::string> tags;
    for (int i = 0; i < count; ++i) {
        std::ostringstream tag;
        tag << "metadata:project:2024:quarter:q" << (i % 4 + 1)
            << ":customer:enterprise:region:north-america:priority:high:status:active"
            << ":created_by:user" << i << ":timestamp:" << (1640995200 + i * 3600);
        
        // Pad to desired length
        while (tag.str().length() < length) {
            tag << ":extra_field_" << tag.str().length();
        }
        tags.push_back(tag.str().substr(0, length));
    }
    return tags;
}

struct TestResult {
    std::string name;
    size_t memory_kb;
    size_t nodes;
    size_t values;
    size_t raw_data_kb;
    double overhead_factor;
    double time_seconds;
};

TestResult RunPatriciaTest(const std::string& name, const std::vector<std::string>& tags, int keys_per_tag) {
    auto start_memory = GetMemoryUsage();
    auto start_time = std::chrono::high_resolution_clock::now();
    
    SimplePatriciaTree tree;
    size_t raw_data_size = 0;
    
    // Add tags to tree
    for (size_t i = 0; i < tags.size(); ++i) {
        for (int k = 0; k < keys_per_tag; ++k) {
            std::string key = "key" + std::to_string(i * keys_per_tag + k);
            tree.AddKeyValue(tags[i], key);
            raw_data_size += tags[i].length() + key.length();
        }
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto end_memory = GetMemoryUsage();
    
    TestResult result;
    result.name = name;
    result.memory_kb = end_memory.rss_kb - start_memory.rss_kb;
    result.nodes = tree.GetNodeCount();
    result.values = tree.GetTotalValues();
    result.raw_data_kb = raw_data_size / 1024;
    result.overhead_factor = static_cast<double>(result.memory_kb) / result.raw_data_kb;
    result.time_seconds = std::chrono::duration<double>(end_time - start_time).count();
    
    return result;
}

TestResult RunStringInternTest(const std::string& name, const std::vector<std::string>& strings, int duplicates) {
    auto start_memory = GetMemoryUsage();
    auto start_time = std::chrono::high_resolution_clock::now();
    
    SimpleStringIntern intern;
    size_t raw_data_size = 0;
    
    // Add original strings
    std::vector<std::shared_ptr<std::string>> interned_strings;
    for (const auto& str : strings) {
        auto interned = intern.Intern(str);
        interned_strings.push_back(interned);
        raw_data_size += str.length();
    }
    
    // Add duplicates
    for (int d = 0; d < duplicates; ++d) {
        for (const auto& str : strings) {
            auto interned = intern.Intern(str);
            interned_strings.push_back(interned);
            raw_data_size += str.length(); // Count toward "raw" usage
        }
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto end_memory = GetMemoryUsage();
    
    TestResult result;
    result.name = name;
    result.memory_kb = end_memory.rss_kb - start_memory.rss_kb;
    result.nodes = intern.Size(); // Unique strings
    result.values = interned_strings.size(); // Total references
    result.raw_data_kb = raw_data_size / 1024;
    result.overhead_factor = static_cast<double>(result.memory_kb) / result.raw_data_kb;
    result.time_seconds = std::chrono::duration<double>(end_time - start_time).count();
    
    return result;
}

int main() {
    std::cout << "=== Simple Patricia Tree and String Interning Memory Tests ===\n\n";
    
    std::vector<TestResult> results;
    
    // Test 1: Hierarchical tags (good compression)
    {
        auto tags = GenerateHierarchicalTags(1000, "org:company");
        auto result = RunPatriciaTest("Hierarchical_1K", tags, 10);
        results.push_back(result);
    }
    
    // Test 2: Random tags (poor compression) 
    {
        auto tags = GenerateRandomTags(1000, 30);
        auto result = RunPatriciaTest("Random_1K", tags, 10);
        results.push_back(result);
    }
    
    // Test 3: Long tags
    {
        auto tags = GenerateLongTags(100, 200);
        auto result = RunPatriciaTest("Long_100", tags, 100);
        results.push_back(result);
    }
    
    // Test 4: Very long tags (simulating 4KB)
    {
        auto tags = GenerateLongTags(10, 1000);
        auto result = RunPatriciaTest("VeryLong_10", tags, 1000);
        results.push_back(result);
    }
    
    // String interning tests
    {
        auto strings = GenerateHierarchicalTags(5000, "org:shared");
        auto result = RunStringInternTest("StringIntern_Hier", strings, 3); // 3x duplication
        results.push_back(result);
    }
    
    {
        auto strings = GenerateRandomTags(5000, 50);
        auto result = RunStringInternTest("StringIntern_Random", strings, 1); // 1x duplication
        results.push_back(result);
    }
    
    // Print results
    std::cout << std::left;
    std::cout << std::setw(20) << "Test Name" 
              << std::setw(10) << "MemoryKB"
              << std::setw(8) << "Nodes"
              << std::setw(8) << "Values" 
              << std::setw(10) << "RawDataKB"
              << std::setw(10) << "Overhead"
              << std::setw(10) << "Time(s)"
              << "\n";
    std::cout << std::string(76, '-') << "\n";
    
    for (const auto& result : results) {
        std::cout << std::setw(20) << result.name.substr(0, 19)
                  << std::setw(10) << result.memory_kb
                  << std::setw(8) << result.nodes
                  << std::setw(8) << result.values
                  << std::setw(10) << result.raw_data_kb
                  << std::setw(10) << std::fixed << std::setprecision(2) << result.overhead_factor
                  << std::setw(10) << std::fixed << std::setprecision(3) << result.time_seconds
                  << "\n";
    }
    
    // Write CSV
    std::ofstream csv("simple_memory_test_results.csv");
    csv << "TestName,MemoryKB,Nodes,Values,RawDataKB,OverheadFactor,TimeSeconds\n";
    for (const auto& result : results) {
        csv << result.name << "," << result.memory_kb << "," << result.nodes << ","
            << result.values << "," << result.raw_data_kb << "," 
            << result.overhead_factor << "," << result.time_seconds << "\n";
    }
    csv.close();
    
    std::cout << "\nResults saved to simple_memory_test_results.csv\n";
    
    return 0;
}