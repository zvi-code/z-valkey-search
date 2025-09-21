import os
import sys
import time
import random
import string
import struct
import threading
from typing import Dict, List, Tuple, Optional
import logging

from valkey import Valkey, ResponseError
from valkey_search_test_case import ValkeySearchTestCaseBase

# Optional dependencies for analysis
try:
    import pandas as pd
    import matplotlib.pyplot as plt
    ANALYSIS_AVAILABLE = True
except ImportError:
    assert False, "Pandas and Matplotlib are required for analysis but not installed."


class ProgressMonitor:
    """Background thread to monitor and report progress during long operations"""
    
    def __init__(self, client: Valkey, operation_name: str):
        self.client = client
        self.operation_name = operation_name
        self.running = False
        self.thread = None
        self.start_time = time.time()
        self.last_memory = 0
        self.last_keys = 0
        self.messages = []  # Queue for status messages
        self.current_status = {}  # Current operation status
        self.lock = threading.Lock()  # Thread safety
        
    def start(self):
        """Start the monitoring thread"""
        self.running = True
        self.start_time = time.time()
        self.thread = threading.Thread(target=self._monitor, daemon=True)
        self.thread.start()
        self.log(f"📊 Started progress monitor for: {self.operation_name}")
        
    def stop(self):
        """Stop the monitoring thread"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=3)
        total_time = time.time() - self.start_time
        self.log(f"✅ {self.operation_name} completed in {total_time:.1f}s")
        
    def log(self, message: str):
        """Add a message to be printed by the monitoring thread"""
        with self.lock:
            self.messages.append(message)
            
    def update_status(self, status_dict: dict):
        """Update the current operation status"""
        with self.lock:
            self.current_status.update(status_dict)
            
    def _monitor(self):
        """Background monitoring loop"""
        last_report = time.time()
        client: Valkey = self.client
        logging.info(f"📊 Monitoring started for: {self.operation_name}")
        while self.running:
            try:
                current_time = time.time()
                elapsed = current_time - self.start_time
                
                # Print any queued messages immediately
                # with self.lock:
                while self.messages:
                    message = self.messages.pop(0)
                    logging.info(message)
                    sys.stdout.flush()
                
                # Report system stats every 5 seconds
                if current_time - last_report >= 5.0:
                    # Get memory info
                    memory_info = client.info("memory")
                    current_memory_kb = memory_info['used_memory'] // 1024
                    memory_delta = current_memory_kb - self.last_memory
                    
                    # Get key count
                    db_info = client.info("keyspace")
                    current_keys = 0
                    if 'db0' in db_info:
                        # Parse "keys=123,expires=0,avg_ttl=0" format
                        db0_info = db_info['db0']
                        if isinstance(db0_info, str) and 'keys=' in db0_info:
                            current_keys = int(db0_info.split('keys=')[1].split(',')[0])
                    
                    keys_delta = current_keys - self.last_keys
                    
                    # Build status report
                    status_parts = [
                        f"Time: {elapsed:.0f}s",
                        f"Memory: {current_memory_kb:,} KB (+{memory_delta:,})",
                        f"Keys: {current_keys:,} (+{keys_delta:,})"
                    ]
                    
                    # Add current operation status if available
                    # with self.lock:
                    if self.current_status:
                        for key, value in self.current_status.items():
                            status_parts.append(f"{key}: {value}")
                
                    logging.info(f"🔄 [{self.operation_name}] {' | '.join(status_parts)}")
                    sys.stdout.flush()
                    
                    self.last_memory = current_memory_kb
                    self.last_keys = current_keys
                    last_report = current_time
                    
            except Exception as e:
                logging.info(f"⚠️ Monitor error: {e}")
                sys.stdout.flush()
                
            time.sleep(1)  # Check every second for messages, report every 5 seconds


class TestMemoryBenchmark(ValkeySearchTestCaseBase):
    """Comprehensive Tag Index memory benchmarking using integration test framework"""
    
    def generate_fixed_length_key(self, id: int) -> str:
        """Generate a key with fixed length (64 bytes)"""
        key = f"key_{id}"
        while len(key) < 64:
            key += "_padding"
        return key[:64]
        
    def generate_tags_with_frequency(self, num_unique_tags: int, avg_tag_length: int, 
                                   prefix: str = "tag") -> List[Tuple[str, float]]:
        """Generate tags with different frequency distributions"""
        tags_with_freq = []
        
        # Generate different frequency tiers
        high_freq_count = max(1, num_unique_tags // 20)  # 5% are high frequency (50% of keys)
        med_freq_count = max(1, num_unique_tags // 10)   # 10% are medium frequency (10% of keys)
        low_freq_count = max(1, num_unique_tags // 5)    # 20% are low frequency (1-5% of keys)
        rare_freq_count = num_unique_tags - high_freq_count - med_freq_count - low_freq_count
        
        tag_id = 0
        
        # High frequency tags (50% of keys have them)
        for i in range(high_freq_count):
            tag = f"{prefix}_high_freq_{tag_id}"
            while len(tag) < avg_tag_length:
                tag += "_pad"
            tags_with_freq.append((tag[:avg_tag_length], 0.5))
            tag_id += 1
            
        # Medium frequency tags (10% of keys)
        for i in range(med_freq_count):
            tag = f"{prefix}_med_freq_{tag_id}"
            while len(tag) < avg_tag_length:
                tag += "_pad"
            tags_with_freq.append((tag[:avg_tag_length], 0.1))
            tag_id += 1
            
        # Low frequency tags (1-5% of keys)
        for i in range(low_freq_count):
            tag = f"{prefix}_low_freq_{tag_id}"
            while len(tag) < avg_tag_length:
                tag += "_pad"
            freq = 0.01 + (0.04 * i) / low_freq_count  # 1% to 5%
            tags_with_freq.append((tag[:avg_tag_length], freq))
            tag_id += 1
            
        # Rare frequency tags (0.1% of keys)
        for i in range(rare_freq_count):
            tag = f"{prefix}_rare_{tag_id}"
            while len(tag) < avg_tag_length:
                tag += "_pad"
            tags_with_freq.append((tag[:avg_tag_length], 0.001))
            tag_id += 1
            
        return tags_with_freq
        
    def create_index_with_minimal_vector(self, index_name: str):
        """Create index with minimal vector field to enable tag usage"""
        client: Valkey = self.server.get_new_client()
        # Create index with small vector (8 dimensions) and tag field
        cmd = [
            "FT.CREATE", index_name,
            "ON", "HASH",
            "SCHEMA",
            "tags", "TAG", "SEPARATOR", ",",
            "vector", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "8", "DISTANCE_METRIC", "COSINE"
        ]
        result = client.execute_command(*cmd)
        assert result == b"OK", f"Index creation failed: {result}"
        
    def run_isolated_memory_scenario(self, scenario_name: str, tags_with_freq: List[Tuple[str, float]],
                                    total_keys: int, avg_tags_per_key: int) -> Dict:
        """Run isolated memory benchmark - measuring valkey data vs. index memory separately"""
        client: Valkey = self.server.get_new_client()
        
        # Start background monitoring
        monitor = ProgressMonitor(client, f"{scenario_name}")
        monitor.start()
        monitor.log(f"Running isolated scenario: {scenario_name} ({total_keys:,} keys, {len(tags_with_freq):,} unique tags)")
        
        # Verify server connection
        if not self.verify_server_connection(client):
            monitor.stop()
            raise RuntimeError(f"Failed to connect to valkey server for scenario {scenario_name}")
        
        # Clean up any existing data
        client.flushall()
        time.sleep(1)  # Give time for cleanup
        
        # Step 1: Measure baseline memory (empty valkey server)
        baseline_memory = client.info("memory")
        baseline_used_memory = baseline_memory['used_memory']
        
        monitor.log(f"  Baseline memory: {baseline_used_memory // 1024:,} KB")
        
        # Create minimal vector for all keys (8 dimensions as requested)
        # Use binary representation for proper vector storage
        dummy_vector = struct.pack('<8f', *[0.1] * 8)  # 8-dimensional vector
            
        # Step 2: Generate and insert raw data (HASH fields only, no index)
        monitor.log(f"  Generating {total_keys:,} keys with tag data...")
        start_time = time.time()
        raw_data_size = 0
        keys_and_tags = []
        
        last_gen_time = start_time
        for i in range(total_keys):
            key = self.generate_fixed_length_key(i)
            
            # Select tags for this key based on frequency
            selected_tags = []
            for tag, frequency in tags_with_freq:
                if random.random() < frequency and len(selected_tags) < avg_tags_per_key * 2:
                    selected_tags.append(tag)
                    
            # Ensure minimum tags per key
            while len(selected_tags) < max(1, avg_tags_per_key // 2):
                tag, _ = random.choice(tags_with_freq)
                if tag not in selected_tags:
                    selected_tags.append(tag)
                    
            tag_string = ",".join(selected_tags[:avg_tags_per_key * 2])  # Limit max tags
            keys_and_tags.append((key, tag_string))
            raw_data_size += len(key) + len(tag_string) + 32  # 32 bytes for 8-dim float vector
            
            # Progress reporting during generation
            if (i + 1) % 50000 == 0 or (time.time() - last_gen_time >= 5.0):
                progress_pct = ((i + 1) / total_keys) * 100
                elapsed = time.time() - start_time
                keys_per_sec = (i + 1) / elapsed if elapsed > 0 else 0
                data_size_mb = raw_data_size / (1024 * 1024)
                
                # Update monitor status for continuous reporting
                monitor.update_status({
                    "Phase": "Generation",
                    "Progress": f"{i+1:,}/{total_keys:,} ({progress_pct:.1f}%)",
                    "Speed": f"{keys_per_sec:.0f} keys/sec",
                    "Data": f"{data_size_mb:.1f} MB"
                })
                last_gen_time = time.time()
        
        generation_time = time.time() - start_time
        monitor.log(f"  ✓ Data generation complete: {total_keys:,} keys in {generation_time:.1f}s "
                   f"({total_keys/generation_time:.0f} keys/sec)")
            
        # Insert raw data in batches with detailed progress tracking
        batch_size = 1000
        monitor.log(f"  Starting data ingestion: {total_keys:,} keys in batches of {batch_size:,}")
        
        insertion_start_time = time.time()
        last_progress_time = insertion_start_time
        
        for i in range(0, len(keys_and_tags), batch_size):
            batch = keys_and_tags[i:i+batch_size]
            batch_start = time.time()
            
            pipe = client.pipeline()
            for key, tag_string in batch:
                pipe.hset(key, mapping={"tags": tag_string, "vector": dummy_vector})
            pipe.execute()
            
            batch_time = time.time() - batch_start
            current_time = time.time()
            
            # Report progress every 3 seconds or every 5 batches
            batch_num = (i // batch_size + 1)
            if (current_time - last_progress_time >= 3.0) or (batch_num % 5 == 0):
                keys_inserted = i + len(batch)
                progress_pct = (keys_inserted / total_keys) * 100
                elapsed_time = current_time - insertion_start_time
                keys_per_sec = keys_inserted / elapsed_time if elapsed_time > 0 else 0
                
                eta_seconds = (total_keys - keys_inserted) / keys_per_sec if keys_per_sec > 0 else 0
                eta_str = f"{eta_seconds/60:.1f}m" if eta_seconds > 60 else f"{eta_seconds:.0f}s"
                
                # Update monitor status for continuous reporting
                monitor.update_status({
                    "Phase": "Insertion",
                    "Progress": f"{keys_inserted:,}/{total_keys:,} ({progress_pct:.1f}%)",
                    "Speed": f"{keys_per_sec:.0f} keys/sec",
                    "Batch": f"#{batch_num} ({batch_time*1000:.1f}ms)",
                    "ETA": eta_str
                })
                
                last_progress_time = current_time
                
        insertion_time = time.time() - insertion_start_time
        total_keys_inserted = len(keys_and_tags)
        monitor.log(f"  ✓ Data insertion complete: {total_keys_inserted:,} keys in {insertion_time:.1f}s "
                   f"({total_keys_inserted/insertion_time:.0f} keys/sec)")
                
        # Step 3: Measure memory after raw data insertion (CRITICAL: before index creation)
        data_only_memory = client.info("memory")
        data_only_used_memory = data_only_memory['used_memory']
        
        valkey_data_memory_kb = (data_only_used_memory - baseline_used_memory) // 1024
        logging.info(f"  Valkey data memory (no index): {valkey_data_memory_kb:,} KB")
        
        # Step 4: Create the index with minimal vector field
        index_name = f"idx_{scenario_name.lower()}"
        self.create_index_with_minimal_vector(index_name)
        
        # Wait for indexing to complete with progress monitoring
        monitor.log(f"  Creating index and waiting for indexing...")
        self.wait_for_indexing(client, index_name, total_keys, monitor)
        
        # Step 5: Measure final memory after index creation
        final_memory = client.info("memory")
        final_used_memory = final_memory['used_memory']
        
        total_memory_kb = (final_used_memory - baseline_used_memory) // 1024
        index_overhead_kb = (final_used_memory - data_only_used_memory) // 1024
        
        # Try to get search module memory details
        try:
            search_info = client.info("search")
            search_index_memory_kb = search_info.get('search_index_memory', 0) // 1024
            vector_memory_kb = search_info.get('search_index_vector_memory', 0) // 1024
        except (ResponseError, Exception):
            search_index_memory_kb = index_overhead_kb
            vector_memory_kb = 0
        
        # Calculate tag-specific index memory (subtract vector memory from total search memory)
        tag_index_memory_kb = max(0, search_index_memory_kb - vector_memory_kb)
        
        end_time = time.time()
        
        # Calculate metrics
        raw_data_kb = raw_data_size // 1024
        overhead_factor = tag_index_memory_kb / raw_data_kb if raw_data_kb > 0 else 0
        
        result = {
            'scenario_name': scenario_name,
            'total_keys': total_keys,
            'unique_tags': len(tags_with_freq),
            'avg_tags_per_key': avg_tags_per_key,
            'avg_tag_length': sum(len(tag) for tag, _ in tags_with_freq) // len(tags_with_freq) if tags_with_freq else 0,
            'raw_data_size_bytes': raw_data_size,
            'raw_data_kb': raw_data_kb,
            'baseline_memory_kb': baseline_used_memory // 1024,
            'valkey_data_memory_kb': valkey_data_memory_kb,
            'total_memory_kb': total_memory_kb,
            'index_overhead_kb': index_overhead_kb,
            'search_index_memory_kb': search_index_memory_kb,
            'tag_index_memory_kb': tag_index_memory_kb,
            'vector_memory_kb': vector_memory_kb,
            'overhead_factor': overhead_factor,
            'data_efficiency': valkey_data_memory_kb / raw_data_kb if raw_data_kb > 0 else 0,
            'time_seconds': end_time - start_time,
        }
        
        logging.info(f"  Completed in {end_time - start_time:.1f}s:")
        logging.info(f"    Valkey data: {valkey_data_memory_kb:,} KB ({result['data_efficiency']:.2f}x raw data)")
        logging.info(f"    Tag index: {tag_index_memory_kb:,} KB ({overhead_factor:.2f}x overhead)")
        logging.info(f"    Vector index: {vector_memory_kb:,} KB (minimal)")
        logging.info(f"    Total: {total_memory_kb:,} KB")
        
        # Clean up
        try:
            client.execute_command("FT.DROPINDEX", index_name)
        except:
            pass
            
        monitor.stop()
        return result

    def wait_for_indexing(self, client: Valkey, index_name: str, expected_docs: int, monitor: ProgressMonitor, timeout: int = 300):
        """Wait for indexing to complete with detailed progress monitoring"""
        start_time = time.time()
        last_report_time = start_time
        last_doc_count = 0
        
        monitor.log(f"    Starting search index creation for {expected_docs:,} documents...")
        
        while time.time() - start_time < timeout:
            try:
                # Get index info to check indexing status
                info = client.execute_command("FT.INFO", index_name)
                info_dict = {}
                for i in range(0, len(info), 2):
                    key = info[i].decode() if isinstance(info[i], bytes) else str(info[i])
                    value = info[i+1]
                    if isinstance(value, bytes):
                        try:
                            value = value.decode()
                        except:
                            pass
                    info_dict[key] = value
                
                num_docs = int(info_dict.get('num_docs', 0))
                indexing = info_dict.get('indexing', '0') == '1'
                
                # Get memory info
                memory_info = client.info("memory")
                current_memory_kb = memory_info['used_memory'] // 1024
                
                # Calculate indexing rate
                current_time = time.time()
                elapsed = current_time - start_time
                
                # Report progress every 5 seconds or when significant progress is made
                doc_progress = num_docs - last_doc_count
                time_since_report = current_time - last_report_time
                
                if time_since_report >= 5 or doc_progress >= expected_docs * 0.05:  # Every 5% progress
                    progress_pct = (num_docs / expected_docs * 100) if expected_docs > 0 else 0
                    docs_per_sec = num_docs / elapsed if elapsed > 0 else 0
                    
                    eta_seconds = (expected_docs - num_docs) / docs_per_sec if docs_per_sec > 0 and num_docs < expected_docs else 0
                    eta_str = f"{eta_seconds/60:.1f}m" if eta_seconds > 60 else f"{eta_seconds:.0f}s"
                    
                    status = "INDEXING" if indexing else "IDLE"
                    
                    logging.info(f"    Search Index: {num_docs:,}/{expected_docs:,} docs ({progress_pct:.1f}%) | "
                          f"{docs_per_sec:.0f} docs/sec | Memory: {current_memory_kb:,} KB | "
                          f"Status: {status} | ETA: {eta_str}")
                    sys.stdout.flush()  # Ensure immediate output
                    
                    last_report_time = current_time
                    last_doc_count = num_docs
                
                # Check if indexing is complete
                if num_docs >= expected_docs and not indexing:
                    total_time = time.time() - start_time
                    avg_docs_per_sec = num_docs / total_time if total_time > 0 else 0
                    logging.info(f"    ✓ Search indexing complete: {num_docs:,} docs indexed in {total_time:.1f}s "
                          f"(avg {avg_docs_per_sec:.0f} docs/sec)")
                    logging.info(f"    ✓ Final memory usage: {current_memory_kb:,} KB")
                    sys.stdout.flush()  # Ensure immediate output
                    return
                    
            except Exception as e:
                logging.info(f"    Warning: Could not check indexing status: {e}")
                sys.stdout.flush()  # Ensure immediate output
                
            time.sleep(1)  # Check every 1 second for more responsive monitoring
        
        logging.info(f"    ⚠ Warning: Indexing timeout after {timeout}s, proceeding anyway")
        sys.stdout.flush()  # Ensure immediate output

    def verify_server_connection(self, client: Valkey) -> bool:
        """Verify that the server is responding"""
        try:
            client.ping()
            return True
        except Exception as e:
            logging.info(f"❌ Server connection failed: {e}")
            return False

    def test_quick_memory_benchmark(self):
        """Test memory benchmark with smaller dataset (100K keys)"""
        logging.info("=== Running Quick Memory Benchmark (100K keys) ===")
        logging.info("This analysis measures valkey data memory separately from tag index overhead")
        logging.info("Using minimal 8-dimensional vectors to isolate tag index memory usage\n")
        
        total_keys = 1000
        results = []
        
        # Test different unique tag counts
        logging.info(f"\n--- Testing Unique Tags Impact (Fixed: {total_keys:,} keys, avg 8 tags/key, 32 byte tags) ---")
        for unique_tags in [10, 50, 100]:
            tags = self.generate_tags_with_frequency(unique_tags, 32, "tag")
            result = self.run_isolated_memory_scenario(f"UniqueTags_{unique_tags}", tags, total_keys, 8)
            results.append(result)
            
        # Print summary
        logging.info("\n" + "="*100)
        logging.info("QUICK MEMORY BENCHMARK RESULTS")
        logging.info("="*100)
        
        logging.info(f"{'Scenario':<20} {'Keys':>8} {'UniqTags':>8} {'ValkeyKB':>9} {'TagIdxKB':>9} {'VecKB':>7} {'Overhead':>8}")
        logging.info("-" * 100)
        
        for result in results:
            logging.info(f"{result['scenario_name']:<20} "
                  f"{result['total_keys']:>8,} "
                  f"{result['unique_tags']:>8,} "
                  f"{result['valkey_data_memory_kb']:>9,} "
                  f"{result['tag_index_memory_kb']:>9,} "
                  f"{result['vector_memory_kb']:>7,} "
                  f"{result['overhead_factor']:>8.2f}")
        
        logging.info(f"\nKey findings:")
        logging.info(f"- Valkey efficiently stores hash data with ~{results[0]['data_efficiency']:.1f}x compression")
        logging.info(f"- Tag index overhead ranges from {min(r['overhead_factor'] for r in results):.2f}x to {max(r['overhead_factor'] for r in results):.2f}x of raw data")
        logging.info(f"- Vector index uses minimal memory ({results[0]['vector_memory_kb']} KB) with 8 dimensions")

    def test_comprehensive_memory_benchmark(self):
        """Test comprehensive memory benchmark with medium dataset (300K keys)"""
        logging.info("=== Running Comprehensive Memory Benchmark (300K keys) ===")
        logging.info("This analysis measures valkey data memory separately from tag index overhead")
        logging.info("Using minimal 8-dimensional vectors to isolate tag index memory usage\n")
        
        total_keys = 100
        results = []
        
        # Test different parameters
        logging.info(f"\n--- Testing Unique Tags Impact ---")
        for unique_tags in [10, 50, 100]:
            tags = self.generate_tags_with_frequency(unique_tags, 32, "tag")
            result = self.run_isolated_memory_scenario(f"UniqueTags_{unique_tags}", tags, total_keys, 8)
            results.append(result)
            
        logging.info(f"\n--- Testing Tag Length Impact ---")
        for tag_length in [16, 32, 64, 128]:
            tags = self.generate_tags_with_frequency(20000, tag_length, f"len{tag_length}")
            result = self.run_isolated_memory_scenario(f"TagLen_{tag_length}", tags, total_keys, 8)
            results.append(result)
            
        logging.info(f"\n--- Testing Tags Per Key Impact ---")
        for tags_per_key in [4, 8, 16, 32]:
            tags = self.generate_tags_with_frequency(20000, 32, f"tpk{tags_per_key}")
            result = self.run_isolated_memory_scenario(f"TagsPerKey_{tags_per_key}", tags, total_keys, tags_per_key)
            results.append(result)
        
        # Print comprehensive summary
        logging.info("\n" + "="*120)
        logging.info("COMPREHENSIVE MEMORY BENCHMARK RESULTS")
        logging.info("="*120)
        
        logging.info(f"{'Scenario':<25} {'Keys':>8} {'UniqTags':>8} {'TagLen':>7} {'Tags/Key':>8} {'RawKB':>8} {'ValkeyKB':>9} {'TagIdxKB':>9} {'Overhead':>8}")
        logging.info("-" * 120)
        
        for result in results:
            logging.info(f"{result['scenario_name']:<25} "
                  f"{result['total_keys']:>8,} "
                  f"{result['unique_tags']:>8,} "
                  f"{result['avg_tag_length']:>7} "
                  f"{result['avg_tags_per_key']:>8} "
                  f"{result['raw_data_kb']:>8,} "
                  f"{result['valkey_data_memory_kb']:>9,} "
                  f"{result['tag_index_memory_kb']:>9,} "
                  f"{result['overhead_factor']:>8.2f}")
        
        # Save results to CSV
        csv_filename = "comprehensive_memory_benchmark.csv"
        headers = [
            'Scenario', 'Keys', 'UniqueTags', 'TagsPerKey', 'AvgTagLength', 
            'RawDataKB', 'BaselineMemoryKB', 'ValkeyDataMemoryKB', 'TotalMemoryKB', 
            'IndexOverheadKB', 'TagIndexMemoryKB', 'VectorMemoryKB',
            'OverheadFactor', 'DataEfficiency', 'TimeSeconds'
        ]
        
        with open(csv_filename, 'w') as f:
            f.write(','.join(headers) + '\n')
            for result in results:
                row = [
                    result['scenario_name'], result['total_keys'], result['unique_tags'],
                    result['avg_tags_per_key'], result['avg_tag_length'], result['raw_data_kb'],
                    result['baseline_memory_kb'], result['valkey_data_memory_kb'], result['total_memory_kb'],
                    result['index_overhead_kb'], result['tag_index_memory_kb'], result['vector_memory_kb'],
                    f"{result['overhead_factor']:.6f}", f"{result['data_efficiency']:.6f}", 
                    f"{result['time_seconds']:.6f}"
                ]
                f.write(','.join(str(x) for x in row) + '\n')
                
        logging.info(f"\nResults saved to {csv_filename}")
        logging.info("\nKey findings:")
        logging.info(f"- Data efficiency: ~{results[0]['data_efficiency']:.1f}x compression")
        logging.info(f"- Tag index overhead: {min(r['overhead_factor'] for r in results):.2f}x to {max(r['overhead_factor'] for r in results):.2f}x")
        logging.info(f"- Vector index minimal overhead: ~{results[0]['vector_memory_kb']} KB")