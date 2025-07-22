#!/usr/bin/env python3
"""
Python runner for memory benchmark tests with enhanced analysis
"""

import subprocess
import os
import sys

# Optional dependencies for analysis
try:
    import pandas as pd
    import matplotlib.pyplot as plt
    ANALYSIS_AVAILABLE = True
except ImportError:
    ANALYSIS_AVAILABLE = False

def run_memory_benchmark():
    """Run the C++ memory benchmark and analyze results"""
    
    print("Building and running memory benchmark test...")
    
    # Build the test using the build.sh script
    build_cmd = ["./build.sh"]
    try:
        result = subprocess.run(build_cmd, capture_output=True, text=True, check=True)
        print("Build successful")
    except subprocess.CalledProcessError as e:
        print(f"Build failed: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        return
    
    # Run the test from the correct test directory
    # Use QuickMemoryValidation for faster testing, or ComprehensiveMemoryAnalysis for full analysis
    test_filter = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] in ["--quick", "--full"] else "--quick"
    if test_filter == "--quick":
        test_cmd = ["./.build-release/tests/memory_benchmark_test", "--gtest_filter=*QuickMemoryValidation*"]
        print("Running quick validation test (100K keys)")
    else:
        test_cmd = ["./.build-release/tests/memory_benchmark_test", "--gtest_filter=*ComprehensiveMemoryAnalysis*"]
        print("Running comprehensive analysis (10M keys) - this will take several minutes")
    try:
        result = subprocess.run(test_cmd, capture_output=True, text=True, check=True)
        print("Test execution successful")
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Test execution failed: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        return
    
    # Analyze CSV results if available
    if os.path.exists("tag_memory_benchmark.csv"):
        if ANALYSIS_AVAILABLE:
            analyze_results("tag_memory_benchmark.csv")
        else:
            print("Analysis libraries not available. CSV results can be found at tag_memory_benchmark.csv")
            # Show basic stats without pandas
            with open("tag_memory_benchmark.csv", "r") as f:
                lines = f.readlines()
                print(f"\nBasic results from CSV ({len(lines)-1} scenarios):")
                for line in lines:
                    print(line.strip())
    else:
        print("CSV results file not found")

def analyze_results(csv_file):
    """Analyze benchmark results and create visualizations"""
    
    print(f"\nAnalyzing results from {csv_file}...")
    
    try:
        df = pd.read_csv(csv_file)
        print(f"Loaded {len(df)} benchmark results")
        
        # Display summary
        print("\n=== Summary Statistics ===")
        print(df[['MemoryKB', 'RawDataKB', 'OverheadFactor']].describe())
        
        # Analyze results by category
        analyze_by_category(df)
        
        # Try to create visualizations
        try:
            create_visualizations(df)
        except Exception as viz_error:
            print(f"Visualization failed: {viz_error}")
            print("Continuing with text-based analysis...")
    
    except Exception as e:
        print(f"Error analyzing results: {e}")
        import traceback
        traceback.print_exc()

def analyze_by_category(df):
    """Analyze results by different test categories"""
    print("\n=== Analysis by Test Category ===")
    
    # Group by test type based on scenario name patterns
    categories = {
        'UniqueTags': df[df['Scenario'].str.contains('UniqueTags_', na=False)],
        'TagLength': df[df['Scenario'].str.contains('TagLen_', na=False)],
        'TagsPerKey': df[df['Scenario'].str.contains('TagsPerKey_', na=False)],
        'FreqDist': df[df['Scenario'].str.contains('FreqDist_', na=False)],
        'Extreme': df[df['Scenario'].str.contains('Extreme_', na=False)],
        'Quick': df[df['Scenario'].str.contains('Quick_', na=False)]
    }
    
    for category_name, category_df in categories.items():
        if len(category_df) > 0:
            print(f"\n--- {category_name} Analysis ---")
            
            if category_name == 'UniqueTags':
                # Extract unique tag counts from scenario names
                category_df = category_df.copy()
                category_df['UniqueTagCount'] = category_df['Scenario'].str.extract(r'UniqueTags_(\d+)').astype(int)
                category_df = category_df.sort_values('UniqueTagCount')
                print("Impact of Number of Unique Tags:")
                print("UniqueTags | MemoryKB | Overhead | Memory/UniqueTag")
                print("-" * 50)
                for _, row in category_df.iterrows():
                    unique_count = row['UniqueTagCount']
                    memory_kb = row['MemoryKB']
                    overhead = row['OverheadFactor']
                    memory_per_tag = memory_kb / unique_count if unique_count > 0 else 0
                    print(f"{unique_count:>9d} | {memory_kb:>7.0f} | {overhead:>7.2f}x | {memory_per_tag:>13.3f} KB")
                    
            elif category_name == 'TagLength':
                # Extract tag lengths from scenario names
                category_df = category_df.copy()
                category_df['TagLength'] = category_df['Scenario'].str.extract(r'TagLen_(\d+)').astype(int)
                category_df = category_df.sort_values('TagLength')
                print("Impact of Tag Length:")
                print("TagLength | MemoryKB | Overhead | Memory/Byte")
                print("-" * 45)
                for _, row in category_df.iterrows():
                    tag_len = row['TagLength']
                    memory_kb = row['MemoryKB']
                    overhead = row['OverheadFactor']
                    memory_per_byte = memory_kb / (tag_len * row['UniqueTags']) if tag_len > 0 else 0
                    print(f"{tag_len:>8d}B | {memory_kb:>7.0f} | {overhead:>7.2f}x | {memory_per_byte:>10.6f} KB")
                    
            elif category_name == 'TagsPerKey':
                # Extract tags per key from scenario names
                category_df = category_df.copy()
                category_df['ExtractedTPK'] = category_df['Scenario'].str.extract(r'TagsPerKey_(\d+)').astype(int)
                category_df = category_df.sort_values('ExtractedTPK')
                print("Impact of Tags Per Key:")
                print("Tags/Key | MemoryKB | Overhead | Memory/Tag/Key")
                print("-" * 48)
                for _, row in category_df.iterrows():
                    tpk = row['ExtractedTPK']
                    memory_kb = row['MemoryKB']
                    overhead = row['OverheadFactor']
                    keys = row['Keys']
                    memory_per_tag_key = memory_kb / (tpk * keys) if tpk > 0 and keys > 0 else 0
                    print(f"{tpk:>7d}  | {memory_kb:>7.0f} | {overhead:>7.2f}x | {memory_per_tag_key:>13.6f} KB")
            
            else:
                # Generic analysis for other categories
                print("Scenario | MemoryKB | Overhead | Efficiency")
                print("-" * 50)
                for _, row in category_df.iterrows():
                    scenario = row['Scenario'].replace(category_name + '_', '')[:15]
                    memory_kb = row['MemoryKB']
                    overhead = row['OverheadFactor']
                    efficiency = "Good" if overhead < 2 else "Fair" if overhead < 5 else "Poor"
                    print(f"{scenario:>14s} | {memory_kb:>7.0f} | {overhead:>7.2f}x | {efficiency:>9s}")

def create_visualizations(df):
    """Create visualizations with error handling"""
    # Create a simple, smaller visualization
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(12, 8))
    
    scenarios = df['Scenario'].tolist()
    memory_kb = df['MemoryKB'].tolist()
    raw_data_kb = df['RawDataKB'].tolist()
    overhead_factors = df['OverheadFactor'].tolist()
    tag_lengths = df['AvgTagLength'].tolist()
    
    # 1. Simple bar chart of memory usage
    x = range(len(scenarios))
    ax1.bar(x, memory_kb, alpha=0.7, label='Index Memory')
    ax1.set_xlabel('Scenario')
    ax1.set_ylabel('Memory (KB)')
    ax1.set_title('Index Memory Usage')
    ax1.set_xticks(x)
    ax1.set_xticklabels([s[:8] + '...' if len(s) > 8 else s for s in scenarios], rotation=45, ha='right')
    ax1.set_yscale('log')
    
    # 2. Overhead factors
    colors = ['red' if x > 2 else 'orange' if x > 1 else 'green' for x in overhead_factors]
    bars = ax2.bar(x, overhead_factors, color=colors, alpha=0.7)
    ax2.axhline(y=1, color='black', linestyle='--', alpha=0.5, label='1x baseline')
    ax2.set_xlabel('Scenario') 
    ax2.set_ylabel('Overhead Factor')
    ax2.set_title('Memory Overhead Factor')
    ax2.set_xticks(x)
    ax2.set_xticklabels([s[:8] + '...' if len(s) > 8 else s for s in scenarios], rotation=45, ha='right')
    ax2.legend()
    
    # 3. Tag length vs overhead
    ax3.scatter(tag_lengths, overhead_factors, s=60, alpha=0.7, c=colors)
    ax3.set_xlabel('Avg Tag Length')
    ax3.set_ylabel('Overhead Factor')
    ax3.set_title('Tag Length vs Overhead')
    ax3.grid(True, alpha=0.3)
    
    # 4. Raw data size vs overhead
    ax4.scatter(raw_data_kb, overhead_factors, s=60, alpha=0.7, c=colors)
    ax4.set_xlabel('Raw Data Size (KB)')
    ax4.set_ylabel('Overhead Factor')
    ax4.set_title('Data Size vs Overhead') 
    ax4.set_xscale('log')
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout(pad=1.5)
    plt.savefig('actual_memory_benchmark_results.png', dpi=100, bbox_inches='tight')
    plt.close()  # Close the figure to free memory
    print("Visualization saved as actual_memory_benchmark_results.png")

def create_simple_test():
    """Create a simpler test that doesn't require full build system"""
    
    simple_test = '''
#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <fstream>
#include <memory>
#include "src/utils/patricia_tree.h"
#include "src/utils/string_interning.h"

using namespace valkey_search;

struct MemoryStats {
    size_t rss_kb = 0;
};

MemoryStats GetMemoryUsage() {
    MemoryStats stats;
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.substr(0, 6) == "VmRSS:") {
            stats.rss_kb = std::stoul(line.substr(7));
            break;
        }
    }
    return stats;
}

int main() {
    std::cout << "Simple Patricia Tree Memory Test\\n";
    
    auto start_mem = GetMemoryUsage();
    std::cout << "Starting memory: " << start_mem.rss_kb << " KB\\n";
    
    // Test patricia tree with different patterns
    PatriciaTree<std::string> tree(false);
    
    // Pattern 1: Hierarchical (good compression)
    for (int i = 0; i < 1000; ++i) {
        std::string tag = "org:company:dept:eng:team:backend:user:" + std::to_string(i);
        tree.AddKeyValue(tag, "key" + std::to_string(i));
    }
    
    auto mid_mem = GetMemoryUsage();
    std::cout << "After 1K hierarchical tags: " << (mid_mem.rss_kb - start_mem.rss_kb) << " KB\\n";
    
    // Pattern 2: Random (poor compression)
    for (int i = 0; i < 1000; ++i) {
        std::string tag = "random_" + std::to_string(i * 12345 % 98765) + "_tag";
        tree.AddKeyValue(tag, "key" + std::to_string(i + 1000));
    }
    
    auto end_mem = GetMemoryUsage();
    std::cout << "After 1K random tags: " << (end_mem.rss_kb - start_mem.rss_kb) << " KB total\\n";
    
    return 0;
}
'''
    
    with open('simple_memory_test.cc', 'w') as f:
        f.write(simple_test)
    
    print("Created simple_memory_test.cc for manual compilation")

if __name__ == "__main__":
    # Check if we have necessary dependencies for analysis
    if not ANALYSIS_AVAILABLE:
        print("Note: pandas/matplotlib not available - running without advanced analysis")
        print("Install with: pip install pandas matplotlib for full analysis features")
    
    # Try to run full benchmark, fallback to simple test
    # if len(sys.argv) > 1 and sys.argv[1] == "--simple":
    #     create_simple_test()
    # else:
    run_memory_benchmark()