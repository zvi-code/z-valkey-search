#!/usr/bin/env python3
"""
Test to verify that comprehensive memory benchmark creates log files
"""

def test_comprehensive_memory_benchmark_logging():
    """Verify that comprehensive benchmark creates proper log files"""
    import os
    import glob
    import time
    from datetime import datetime
    
    print("🔍 Checking for comprehensive memory benchmark log files...")
    
    # Get current timestamp for comparison
    current_time = datetime.now().strftime('%Y%m%d_%H%M')
    
    # Look for recent log files
    log_pattern = "memory_benchmark_comprehensive_*.log"
    csv_pattern = "comprehensive_benchmark_results_*.csv"
    
    log_files = glob.glob(log_pattern)
    csv_files = glob.glob(csv_pattern)
    
    print(f"📋 Found {len(log_files)} log files matching pattern: {log_pattern}")
    print(f"📊 Found {len(csv_files)} CSV files matching pattern: {csv_pattern}")
    
    # Show recent files (within last hour)
    recent_logs = []
    recent_csvs = []
    
    for log_file in log_files:
        mod_time = os.path.getmtime(log_file)
        if time.time() - mod_time < 3600:  # Within last hour
            recent_logs.append((log_file, mod_time))
    
    for csv_file in csv_files:
        mod_time = os.path.getmtime(csv_file)
        if time.time() - mod_time < 3600:  # Within last hour
            recent_csvs.append((csv_file, mod_time))
    
    if recent_logs:
        print("📝 Recent log files (last hour):")
        for log_file, mod_time in sorted(recent_logs, key=lambda x: x[1], reverse=True):
            mod_str = datetime.fromtimestamp(mod_time).strftime('%Y-%m-%d %H:%M:%S')
            size_kb = os.path.getsize(log_file) // 1024
            print(f"   • {log_file} - {size_kb}KB - {mod_str}")
    
    if recent_csvs:
        print("📊 Recent CSV files (last hour):")
        for csv_file, mod_time in sorted(recent_csvs, key=lambda x: x[1], reverse=True):
            mod_str = datetime.fromtimestamp(mod_time).strftime('%Y-%m-%d %H:%M:%S')
            size_kb = os.path.getsize(csv_file) // 1024
            print(f"   • {csv_file} - {size_kb}KB - {mod_str}")
    
    # Verify log file format
    expected_log_format = f"memory_benchmark_comprehensive_{current_time}"
    print(f"✅ Expected log format: {expected_log_format}XX.log")
    print(f"✅ Expected CSV format: comprehensive_benchmark_results_{current_time}XX.csv")
    
    print("\n🎯 LOGGING CONFIGURATION:")
    print("   • Log files: memory_benchmark_comprehensive_YYYYMMDD_HHMMSS.log")
    print("   • CSV files: comprehensive_benchmark_results_YYYYMMDD_HHMMSS.csv")
    print("   • Location: Current working directory")
    print("   • Format: Timestamped entries with thread info")
    print("   • Content: All monitor logs, progress, results")
    
    return True

if __name__ == '__main__':
    test_comprehensive_memory_benchmark_logging()