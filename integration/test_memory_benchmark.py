import os
import sys
import time
import random
import string
import struct
import threading
from typing import Dict, List, Tuple, Optional, Set
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from valkey import Valkey, ResponseError
from valkey_search_test_case import ValkeySearchTestCaseBase

# Optional dependencies for analysis
try:
    import pandas as pd
    import matplotlib.pyplot as plt
    ANALYSIS_AVAILABLE = True
except ImportError:
    assert False, "Pandas and Matplotlib are required for analysis but not installed."


class ClientPool:
    """Thread-indexed pool of Valkey clients for multi-threaded operations"""
    
    def __init__(self, server, pool_size: int):
        self.server = server
        self.pool_size = pool_size
        self.clients = []
        self.lock = threading.Lock()
        self.thread_local = threading.local()
        
        # Pre-create all clients
        for i in range(pool_size):
            client = self.server.get_new_client()
            self.clients.append(client)
    
    def get_client_for_thread(self, thread_index: int) -> Valkey:
        """Get a dedicated client for a specific thread index"""
        if thread_index >= self.pool_size:
            raise ValueError(f"Thread index {thread_index} exceeds pool size {self.pool_size}")
        return self.clients[thread_index]
    
    def get_client(self) -> Valkey:
        """Get a client - backward compatibility method that uses thread-local storage"""
        # Check if we already have a client assigned to this thread
        if hasattr(self.thread_local, 'client'):
            return self.thread_local.client
        
        # Assign a client based on thread ID hash for deterministic assignment
        import threading
        thread_id = threading.get_ident()
        client_index = thread_id % self.pool_size
        self.thread_local.client = self.clients[client_index]
        return self.thread_local.client
    
    def return_client(self, client: Valkey):
        """Return a client to the pool - no-op for thread-indexed clients"""
        # In thread-indexed mode, clients are not returned, they stay with their threads
        pass
    


class ProgressMonitor:
    """
    Background thread to monitor and report progress during long operations.
    
    The monitor queries Valkey directly for memory, key count, and index progress.
    Test methods can use:
    - set_index_name(name) to add a single index to monitor
    - set_index_names([name1, name2, ...]) to add multiple indexes to monitor
    - remove_index_name(name) to stop monitoring a specific index
    - clear_index_names() to stop monitoring all indexes
    
    The monitor will track all specified indexes and report their combined status.
    """
    
    def __init__(self, server, operation_name: str):
        self.server = server
        # self.monitor_client = None
        self.operation_name = operation_name
        self.running = False
        self.thread = None
        self.start_time = time.time()
        self.last_memory = 0
        self.last_keys = 0
        self.messages = []  # Queue for status messages
        self.current_status = {}  # Current operation status
        self.lock = threading.Lock()  # Thread safety
        self.active_index_names = set()  # Set of index names to monitor (set by test)
        
    def start(self):
        """Start the monitoring thread"""
        self.running = True
        self.start_time = time.time()
        # Get a dedicated client for monitoring
        # self.monitor_client = self.server.get_new_client()
        self.thread = threading.Thread(target=self._monitor, daemon=True)
        self.thread.start()
        self.log(f"📊 Started progress monitor for: {self.operation_name}")
        
    def stop(self):
        """Stop the monitoring thread"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=3)
        # Return the monitoring client to the pool
        # if self.monitor_client:
        #     self.client_pool.return_client(self.monitor_client)
        #     self.monitor_client = None
        total_time = time.time() - self.start_time
        self.log(f"✅ {self.operation_name} completed in {total_time:.1f}s")
        
    def log(self, message: str):
        """Add a message to be printed by the monitoring thread"""
        with self.lock:
            self.messages.append(message)
            
    def set_index_name(self, index_name: str):
        """Add an index name to monitor for indexing progress"""
        with self.lock:
            self.active_index_names.add(index_name)
    
    def set_index_names(self, index_names: List[str]):
        """Set multiple index names to monitor for indexing progress"""
        with self.lock:
            self.active_index_names.update(index_names)
    
    def remove_index_name(self, index_name: str):
        """Remove a specific index from monitoring"""
        with self.lock:
            self.active_index_names.discard(index_name)
    
    def clear_index_names(self):
        """Clear all index names (stop monitoring indexing progress)"""
        with self.lock:
            self.active_index_names.clear()
    
    def clear_index_name(self):
        """Backward compatibility: Clear all index names (stop monitoring indexing progress)"""
        self.clear_index_names()
    
    def get_monitored_indexes(self) -> List[str]:
        """Get the list of currently monitored index names"""
        with self.lock:
            return list(self.active_index_names)
    
    def update_status(self, status_dict: dict):
        """Update the current operation status"""
        with self.lock:
            self.current_status.update(status_dict)
            
    def _monitor(self):
        """Background monitoring loop"""
        last_report = time.time()
        client: Valkey = self.server.get_new_client()
        logging.info(f"📊 Monitoring started for: {self.operation_name}")
        logging.info(f"🔄 ZZZZZZZZZZZZZZZZZZ")
        while self.running:
            try:
                logging.info(f"🔄 ZZZZZZZZZZZZZZZZZZ")
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
                    
                    # Get key count from db info
                    db_info = client.info("keyspace")
                    logging.info(f"DB Info: {db_info}")
                    current_keys = 0
                    if 'db0' in db_info and isinstance(db_info['db0'], dict) and 'keys' in db_info['db0']:
                        current_keys = db_info['db0']['keys']
                    elif 'db0' in db_info and isinstance(db_info['db0'], str) and 'keys=' in db_info['db0']:
                        # Parse "keys=123,expires=0,avg_ttl=0" format
                        try:
                            current_keys = int(db_info['db0'].split('keys=')[1].split(',')[0])
                        except:
                            current_keys = 0
                    
                    # Try to get index information if available (during indexing phase)
                    index_info = {}
                    current_index_names = set()
                    with self.lock:
                        current_index_names = self.active_index_names.copy()
                    
                    if current_index_names:
                        for index_name in current_index_names:
                            try:
                                ft_info = client.execute_command("FT.INFO", index_name)
                                # Parse FT.INFO response for this index
                                index_data = {}
                                for i in range(0, len(ft_info), 2):
                                    if i + 1 < len(ft_info):
                                        key = ft_info[i].decode() if isinstance(ft_info[i], bytes) else str(ft_info[i])
                                        value = ft_info[i + 1]
                                        if isinstance(value, bytes):
                                            try:
                                                value = value.decode()
                                            except:
                                                pass
                                        index_data[key] = value
                                
                                # Store index info with index name as key
                                index_info[index_name] = index_data
                                
                            except Exception as e:
                                # Index might not exist yet or be accessible
                                pass
                    
                    keys_delta = current_keys - self.last_keys
                    
                    # Build status report
                    status_parts = [
                        f"Time: {elapsed:.0f}s",
                        f"Memory: {current_memory_kb:,} KB (+{memory_delta:,})",
                        f"Keys: {current_keys:,} (+{keys_delta:,})"
                    ]
                    
                    # Add index information if available
                    if index_info:
                        if len(index_info) == 1:
                            # Single index - show detailed info
                            index_name, info = next(iter(index_info.items()))
                            num_docs = info.get('num_docs', '0')
                            mutation_queue = info.get('mutation_queue_size', '0')
                            state = info.get('state', 'unknown')
                            status_parts.append(f"Index[{index_name}]: {num_docs} docs, queue: {mutation_queue}, state: {state}")
                        else:
                            # Multiple indexes - show summary
                            total_docs = sum(int(info.get('num_docs', '0')) for info in index_info.values())
                            total_queue = sum(int(info.get('mutation_queue_size', '0')) for info in index_info.values())
                            states = [info.get('state', 'unknown') for info in index_info.values()]
                            status_parts.append(f"Indexes[{len(index_info)}]: {total_docs} docs, queue: {total_queue}, states: {','.join(set(states))}")
                            
                            # Add individual index details if there are only a few
                            if len(index_info) <= 3:
                                for index_name, info in index_info.items():
                                    num_docs = info.get('num_docs', '0')
                                    state = info.get('state', 'unknown')
                                    status_parts.append(f"  {index_name}: {num_docs} docs, {state}")
                    
                    # Add current operation status from worker threads
                    with self.lock:
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
        # Use the main client from the pool (should be available)
        client = self.server.get_new_client()
        # try:
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
        
        # Calculate thread count and create client pool
        num_threads = min(8, max(2, total_keys // 50000))  # Scale threads based on data size, 2-8 threads
        
        # Get a main client from the pool for setup operations
        client = self.server.get_new_client()
        
        # try:
        # Start background monitoring using the client pool
        monitor = ProgressMonitor(self.server, f"{scenario_name}")
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
            
        # Insert raw data using multiple threads for faster performance
        batch_size = max(500, min(2000, total_keys // (num_threads * 10)))  # Optimize batch size for threading
        monitor.log(f"  Starting data ingestion: {total_keys:,} keys in batches of {batch_size:,} using {num_threads} threads")
        
        insertion_start_time = time.time()
        
        # Thread-safe counter for progress tracking
        class ThreadSafeCounter:
            def __init__(self, initial_value=0):
                self.value = initial_value
                self.lock = threading.Lock()
            
            def increment(self, delta=1):
                with self.lock:
                    self.value += delta
                    return self.value
            
            def get(self):
                with self.lock:
                    return self.value
            
        keys_processed = ThreadSafeCounter(0)
        
        def process_batch(batch_data, thread_id):
            """Process a batch of keys in a worker thread"""
            thread_client = None
            # try:
            # Get a client from the pool
            thread_client = self.server.get_new_client()
            
            batch_start = time.time()
            pipe = thread_client.pipeline()
            
            for key, tag_string in batch_data:
                pipe.hset(key, mapping={"tags": tag_string, "vector": dummy_vector})
            
            pipe.execute()
            batch_time = time.time() - batch_start
            
            # Update progress counter
            processed_count = keys_processed.increment(len(batch_data))
            
            # Only update monitor status occasionally to avoid contention
            if processed_count % (batch_size * 5) == 0 or processed_count >= total_keys:
                current_time = time.time()
                elapsed_time = current_time - insertion_start_time
                progress_pct = (processed_count / total_keys) * 100
                keys_per_sec = processed_count / elapsed_time if elapsed_time > 0 else 0
                
                eta_seconds = (total_keys - processed_count) / keys_per_sec if keys_per_sec > 0 else 0
                eta_str = f"{eta_seconds/60:.1f}m" if eta_seconds > 60 else f"{eta_seconds:.0f}s"
                
                # Update monitor with latest stats (monitor thread will query Valkey directly for index state)
                monitor.update_status({
                    "Phase": "Insertion",
                    "Progress": f"{processed_count:,}/{total_keys:,} ({progress_pct:.1f}%)",
                    "Speed": f"{keys_per_sec:.0f} keys/sec",
                    "Threads": f"{num_threads} active",
                    "ETA": eta_str
                })
            
            return len(batch_data), batch_time
            
        
        # Split keys_and_tags into batches for threading
        batches = []
        for i in range(0, len(keys_and_tags), batch_size):
            batch = keys_and_tags[i:i+batch_size]
            batches.append(batch)
        
        # Process batches using ThreadPoolExecutor
        completed_batches = 0
        total_batches = len(batches)
        
        with ThreadPoolExecutor(max_workers=num_threads, thread_name_prefix="ValKeyIngest") as executor:
            # Submit all batches
            future_to_batch = {}
            for batch_idx, batch in enumerate(batches):
                future = executor.submit(process_batch, batch, batch_idx)
                future_to_batch[future] = batch_idx
            
            # Process completed batches
            for future in as_completed(future_to_batch):
                batch_idx = future_to_batch[future]
                try:
                    batch_keys_processed, batch_time = future.result()
                    completed_batches += 1
                    
                    # Periodic progress logging (every 10% or 50 batches)
                    if completed_batches % max(1, total_batches // 10) == 0 or completed_batches % 50 == 0:
                        batch_progress_pct = (completed_batches / total_batches) * 100
                        current_keys = keys_processed.get()
                        monitor.log(f"    Batch progress: {completed_batches}/{total_batches} ({batch_progress_pct:.1f}%) | Keys: {current_keys:,}")
                        
                except Exception as e:
                    monitor.log(f"  ❌ Batch {batch_idx} failed: {e}")
                
        insertion_time = time.time() - insertion_start_time
        total_keys_inserted = keys_processed.get()
        monitor.log(f"  ✓ Data insertion complete: {total_keys_inserted:,} keys in {insertion_time:.1f}s "
                    f"({total_keys_inserted/insertion_time:.0f} keys/sec) using {num_threads} threads")
                
        # Step 3: Measure memory after raw data insertion (CRITICAL: before index creation)
        data_only_memory = client.info("memory")
        data_only_used_memory = data_only_memory['used_memory']
        
        valkey_data_memory_kb = (data_only_used_memory - baseline_used_memory) // 1024
        logging.info(f"  Valkey data memory (no index): {valkey_data_memory_kb:,} KB")
        
        # Step 4: Create the index with minimal vector field
        index_name = f"idx_{scenario_name.lower()}"
        monitor.log(f"  Creating index '{index_name}' and waiting for indexing...")
        monitor.set_index_name(index_name)  # Tell monitor which index to track
        self.create_index_with_minimal_vector(index_name)
        
        # Wait for indexing to complete with progress monitoring
        self.wait_for_indexing(client, index_name, total_keys, monitor)
        
        # Step 5: Measure final memory after index creation
        final_memory = client.info("memory")
        final_used_memory = final_memory['used_memory']
        
        total_memory_kb = (final_used_memory - baseline_used_memory) // 1024
        index_overhead_kb = (final_used_memory - data_only_used_memory) // 1024
        
        # Try to get search module memory details
        try:
            search_info = client.info("SEARCH")
            search_index_memory_kb = search_info.get('search_used_memory_bytes', 0) // 1024
            
            # Get specific index information using FT.INFO
            try:
                ft_info_result = client.execute_command("FT.INFO", index_name)
                # Parse the response to get index-specific metrics
                ft_info_dict = {}
                for i in range(0, len(ft_info_result), 2):
                    if i + 1 < len(ft_info_result):
                        key = ft_info_result[i].decode() if isinstance(ft_info_result[i], bytes) else str(ft_info_result[i])
                        value = ft_info_result[i + 1]
                        if isinstance(value, bytes):
                            value = value.decode()
                        ft_info_dict[key] = value
                
                # Log index state for debugging
                num_docs = ft_info_dict.get('num_docs', '0')
                mutation_queue_size = ft_info_dict.get('mutation_queue_size', '0')
                state = ft_info_dict.get('state', 'unknown')
                logging.info(f"    Index state: {state}, docs: {num_docs}, queue: {mutation_queue_size}")
                
            except Exception as e:
                logging.warning(f"    Could not get FT.INFO for {index_name}: {e}")
            
            # Note: There's no separate vector memory field in search info
            # We'll estimate based on minimal vector usage (8 dims * 4 bytes * total_keys)
            estimated_vector_memory_bytes = total_keys * 8 * 4  # 8 float32 values per vector
            vector_memory_kb = estimated_vector_memory_bytes // 1024
        except (ResponseError, Exception) as e:
            logging.warning(f"    Could not get search info: {e}")
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
        
        # Clear index monitoring
        monitor.clear_index_names()
        monitor.stop()
        return result

    def wait_for_indexing(self, client: Valkey, index_name: str, expected_docs: int, monitor: ProgressMonitor, timeout: int = 300):
        """Wait for indexing to complete with detailed progress monitoring"""
        start_time = time.time()
        last_report_time = start_time
        last_doc_count = 0
        
        monitor.log(f"    Starting search index creation for {expected_docs:,} documents...")
        
        while time.time() - start_time < timeout:
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
            mutation_queue_size = int(info_dict.get('mutation_queue_size', 0))
            backfill_in_progress = int(info_dict.get('backfill_in_progress', 0))
            backfill_complete_percent = float(info_dict.get('backfill_complete_percent', 0.0))
            state = info_dict.get('state', 'unknown')
            
            # Index is still processing if there are mutations in queue or backfill in progress
            indexing = mutation_queue_size > 0 or backfill_in_progress > 0
            
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
                
                status = f"State: {state}"
                if mutation_queue_size > 0:
                    status += f", Queue: {mutation_queue_size}"
                if backfill_in_progress > 0:
                    status += f", Backfill: {backfill_complete_percent:.1f}%"
                
                logging.info(f"    Search Index: {num_docs:,}/{expected_docs:,} docs ({progress_pct:.1f}%) | "
                        f"{docs_per_sec:.0f} docs/sec | Memory: {current_memory_kb:,} KB | "
                        f"{status} | ETA: {eta_str}")
                sys.stdout.flush()  # Ensure immediate output
                
                last_report_time = current_time
                last_doc_count = num_docs
            
            # Check if indexing is complete - all docs indexed and no pending operations
            if num_docs >= expected_docs and not indexing and state == "ready":
                total_time = time.time() - start_time
                avg_docs_per_sec = num_docs / total_time if total_time > 0 else 0
                logging.info(f"    ✓ Search indexing complete: {num_docs:,} docs indexed in {total_time:.1f}s "
                        f"(avg {avg_docs_per_sec:.0f} docs/sec)")
                logging.info(f"    ✓ Final memory usage: {current_memory_kb:,} KB")
                logging.info(f"    ✓ Index state: {state}, queue: {mutation_queue_size}, backfill: {backfill_complete_percent:.1f}%")
                sys.stdout.flush()  # Ensure immediate output
                return
                
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
        
        total_keys = 1000000
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

    def test_million_key_memory_tracking(self):
        """Test memory tracking with 1 million keys using multi-threading and client pool"""
        logging.info("=== Running Million Key Memory Tracking ===")
        logging.info("Testing large-scale memory usage patterns with parallel ingestion and monitoring\n")
        
        total_keys = 1000000
        num_threads = min(8, max(4, total_keys // 100000))  # 4-8 threads for 1M keys
        
        
        # Get main client for setup
        client = self.server.get_new_client()
        
        # try:
        # Start monitoring
        monitor = ProgressMonitor(self.server, "Million Key Memory Tracking")
        monitor.start()
        monitor.log(f"Processing {total_keys:,} keys with {num_threads} threads")
        
        # Verify connection
        if not self.verify_server_connection(client):
            monitor.stop()
            raise RuntimeError("Failed to connect to valkey server")
        
        # Clean up existing data
        client.flushall()
        time.sleep(1)
        
        # Measure baseline
        baseline_memory = client.info("memory")
        baseline_used = baseline_memory['used_memory']
        monitor.log(f"Baseline memory: {baseline_used // 1024:,} KB")
        
        # Generate test data - high variety tags for memory stress testing
        tags_with_freq = self.generate_tags_with_frequency(50000, 32, "stress_tag")
        monitor.log(f"Generated {len(tags_with_freq):,} unique tags with realistic frequency distribution")
        
        # Phase 1: Insert raw data with parallel threads
        monitor.log("Phase 1: Parallel data insertion")
        insertion_start = time.time()
        
        # Create all key-tag combinations
        dummy_vector = struct.pack('<8f', *[0.1] * 8)  # 8-dimensional vector
        keys_and_data = []
        
        for i in range(total_keys):
            key = self.generate_fixed_length_key(i)
            # Select tags based on frequency
            selected_tags = []
            for tag, frequency in tags_with_freq:
                if random.random() < frequency and len(selected_tags) < 16:
                    selected_tags.append(tag)
            
            # Ensure minimum tags
            while len(selected_tags) < 4:
                tag, _ = random.choice(tags_with_freq)
                if tag not in selected_tags:
                    selected_tags.append(tag)
            
            tag_string = ",".join(selected_tags[:12])  # Max 12 tags per key
            keys_and_data.append((key, tag_string))
        
        # Insert using thread pool
        batch_size = max(1000, total_keys // (num_threads * 20))
        monitor.log(f"Using batch size: {batch_size:,} keys per batch")
        
        # Thread-safe progress tracking
        class ProgressTracker:
            def __init__(self):
                self.processed = 0
                self.lock = threading.Lock()
            
            def update(self, count):
                with self.lock:
                    self.processed += count
                    return self.processed
            
            def get(self):
                with self.lock:
                    return self.processed
        
        progress = ProgressTracker()
        
        def insert_batch(batch_client, batch_data, batch_id):
            """Insert a batch of keys using a pooled client"""               
            pipe = batch_client.pipeline()
            
            for key, tag_string in batch_data:
                pipe.hset(key, mapping={"tags": tag_string, "vector": dummy_vector})
            
            pipe.execute()
            
            # Update progress
            processed = progress.update(len(batch_data))
            
            # Update monitor status occasionally
            if processed % (batch_size * 10) == 0 or processed >= total_keys:
                elapsed = time.time() - insertion_start
                rate = processed / elapsed if elapsed > 0 else 0
                progress_pct = (processed / total_keys) * 100
                
                monitor.update_status({
                    "Phase": "Data Insertion",
                    "Progress": f"{processed:,}/{total_keys:,} ({progress_pct:.1f}%)",
                    "Rate": f"{rate:.0f} keys/sec",
                    "Batches": f"{batch_id+1} threads active"
                })
            
            return len(batch_data)
                
        
        # Create batches and process in parallel
        batches = []
        for i in range(0, len(keys_and_data), batch_size):
            batch = keys_and_data[i:i+batch_size]
            batches.append(batch)
        
        # Execute all batches in parallel
        with ThreadPoolExecutor(max_workers=num_threads, thread_name_prefix="MillionKeyInsert") as executor:
            futures = []
            for batch_id, batch in enumerate(batches):
                batch_client = self.server.get_new_client()
                future = executor.submit(insert_batch, batch_client, batch, batch_id)
                futures.append(future)
            
            # Wait for all batches to complete
            total_inserted = 0
            for future in as_completed(futures):
                try:
                    inserted = future.result()
                    total_inserted += inserted
                except Exception as e:
                    monitor.log(f"Batch execution error: {e}")
        
        insertion_time = time.time() - insertion_start
        final_processed = progress.get()
        monitor.log(f"✓ Parallel insertion complete: {final_processed:,} keys in {insertion_time:.1f}s "
                    f"({final_processed/insertion_time:.0f} keys/sec)")
        
        # Phase 2: Measure data-only memory
        data_memory = client.info("memory")
        data_used = data_memory['used_memory']
        data_only_kb = (data_used - baseline_used) // 1024
        monitor.log(f"Data-only memory: {data_only_kb:,} KB")
        
        # Phase 3: Create index and monitor indexing progress
        index_name = "million_key_stress_index"
        monitor.log(f"Phase 2: Creating search index '{index_name}'")
        monitor.set_index_name(index_name)
        
        # Create index
        self.create_index_with_minimal_vector(index_name)
        
        # Wait for indexing with detailed monitoring
        self.wait_for_indexing(client, index_name, total_keys, monitor)
        
        # Phase 4: Final measurements and analysis
        final_memory = client.info("memory")
        final_used = final_memory['used_memory']
        total_memory_kb = (final_used - baseline_used) // 1024
        index_overhead_kb = (final_used - data_used) // 1024
        
        # Get search-specific memory details
        try:
            search_info = client.info("SEARCH")
            search_memory_kb = search_info.get('search_used_memory_bytes', 0) // 1024
        except:
            search_memory_kb = index_overhead_kb
        
        # Calculate key metrics
        overhead_ratio = index_overhead_kb / data_only_kb if data_only_kb > 0 else 0
        memory_per_key = total_memory_kb / total_keys if total_keys > 0 else 0
        
        # Final report
        total_time = time.time() - insertion_start
        monitor.log("✓ Million key memory tracking complete!")
        
        logging.info("\n" + "="*80)
        logging.info("MILLION KEY MEMORY TRACKING RESULTS")
        logging.info("="*80)
        logging.info(f"Total Keys:           {total_keys:,}")
        logging.info(f"Unique Tags:          {len(tags_with_freq):,}")
        logging.info(f"Processing Time:      {total_time:.1f}s")
        logging.info(f"Insertion Rate:       {total_keys/insertion_time:.0f} keys/sec")
        logging.info(f"Threads Used:         {num_threads}")
        logging.info("")
        logging.info("Memory Usage:")
        logging.info(f"  Baseline:           {baseline_used//1024:,} KB")
        logging.info(f"  Data Only:          {data_only_kb:,} KB")
        logging.info(f"  Search Index:       {index_overhead_kb:,} KB")
        logging.info(f"  Total:              {total_memory_kb:,} KB")
        logging.info(f"  Memory per Key:     {memory_per_key:.3f} KB")
        logging.info(f"  Index Overhead:     {overhead_ratio:.2f}x data size")
        logging.info("")
        logging.info("Key Findings:")
        logging.info(f"- Parallel insertion achieved {total_keys/insertion_time:.0f} keys/sec with {num_threads} threads")
        logging.info(f"- Search index overhead is {overhead_ratio:.2f}x the raw data size")
        logging.info(f"- Average memory per key: {memory_per_key:.3f} KB")
        logging.info(f"- Total memory efficiency: {(total_keys * 100) / total_memory_kb:.0f} keys per MB")
        
        # Cleanup
        try:
            client.execute_command("FT.DROPINDEX", index_name)
            monitor.log("Index cleanup complete")
        except:
            pass
        
        monitor.clear_index_names()
        monitor.stop()