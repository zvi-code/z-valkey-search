#!/usr/bin/env python3
"""
Comprehensive Tag Index memory benchmarking using hash_generator.py
for efficient data generation with various sharing patterns.
Maintains ALL original functionality while integrating new generator.
"""

import os
import sys
import time
import struct
import threading
import logging
from typing import Dict, List, Tuple, Optional, Iterator, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase

# Import our hash generator components
from hash_generator import (
    HashKeyGenerator, HashGeneratorConfig, IndexSchema, FieldSchema,
    FieldType, VectorFieldSchema, VectorAlgorithm, VectorMetric
)
from tags_builder import (
    TagsConfig, TagDistribution, TagSharingConfig, TagSharingMode
)
from string_generator import (
    LengthConfig, PrefixConfig, Distribution, StringType
)


@dataclass
class BenchmarkScenario:
    """Configuration for a benchmark scenario"""
    name: str
    total_keys: int
    tags_config: TagsConfig
    description: str


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
    
    def close_all(self):
        """Close all clients in the pool"""
        for client in self.clients:
            try:
                client.close()
            except:
                pass


class ThreadSafeCounter:
    """Thread-safe counter for progress tracking"""
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
        
        while self.running:
            try:
                current_time = time.time()
                elapsed = current_time - self.start_time
                
                # Print any queued messages immediately
                with self.lock:
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
    """Comprehensive Tag Index memory benchmarking using hash_generator"""
    
    def create_schema(self, index_name: str, vector_dim: int = 8) -> IndexSchema:
        """Create a minimal schema with tags and small vector"""
        return IndexSchema(
            index_name=index_name,
            prefix=["key:"],
            fields=[
                FieldSchema(name="tags", type=FieldType.TAG, separator=","),
                FieldSchema(
                    name="vector",
                    type=FieldType.VECTOR,
                    vector_config=VectorFieldSchema(
                        algorithm=VectorAlgorithm.FLAT,
                        dim=vector_dim,
                        distance_metric=VectorMetric.COSINE
                    )
                )
            ]
        )
    
    def verify_server_connection(self, client: Valkey) -> bool:
        """Verify that the server is responding"""
        try:
            client.ping()
            return True
        except Exception as e:
            logging.info(f"❌ Server connection failed: {e}")
            return False
    
    def wait_for_indexing(self, client: Valkey, index_name: str, expected_docs: int, 
                         monitor: ProgressMonitor, timeout: int = 300000):
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
    
    def run_benchmark_scenario(self, scenario: BenchmarkScenario) -> Dict:
        """Run a single benchmark scenario with full monitoring"""
        monitor = ProgressMonitor(self.server, scenario.name)
        monitor.start()
        
        # Get main client
        client = self.server.get_new_client()
        
        try:
            # Verify server connection
            if not self.verify_server_connection(client):
                monitor.stop()
                raise RuntimeError(f"Failed to connect to valkey server for scenario {scenario.name}")
            
            # Clean up any existing data
            client.flushall()
            time.sleep(1)
            
            # Measure baseline
            baseline_memory = client.info("memory")['used_memory']
            monitor.log(f"  Baseline memory: {baseline_memory // 1024:,} KB")
            
            # Create schema and generator
            index_name = f"idx_{scenario.name.lower().replace(' ', '_')}"
            schema = self.create_schema(index_name)
            
            config = HashGeneratorConfig(
                num_keys=scenario.total_keys,
                schema=schema,
                tags_config=scenario.tags_config,
                key_length=LengthConfig(avg=16, min=16, max=16),  # Fixed length keys
                batch_size=max(500, min(2000, scenario.total_keys // 20)),  # Optimize batch size
                seed=42
            )
            
            generator = HashKeyGenerator(config)
            
            # Calculate data generation and insertion
            monitor.log(f"  Generating {scenario.total_keys:,} keys with {scenario.description}")
            
            # Create client pool for parallel insertion
            num_threads = min(64, max(2, scenario.total_keys // 50000))
            client_pool = ClientPool(self.server, num_threads)
            
            # Insert data using generator with parallel processing
            insertion_start_time = time.time()
            keys_processed = ThreadSafeCounter(0)
            
            # Process batches with thread pool
            def process_batch(batch_data: List[Tuple[str, Dict[str, Any]]], thread_id: int) -> Tuple[int, float]:
                """Process a batch of keys in a worker thread"""
                thread_client = client_pool.get_client_for_thread(thread_id)
                
                batch_start = time.time()
                pipe = thread_client.pipeline(transaction=False)
                
                for key, fields in batch_data:
                    pipe.hset(key, mapping=fields)
                
                pipe.execute()
                batch_time = time.time() - batch_start
                
                # Update progress counter
                processed_count = keys_processed.increment(len(batch_data))
                
                # Update monitor status periodically
                if processed_count % (config.batch_size * 5) == 0 or processed_count >= scenario.total_keys:
                    current_time = time.time()
                    elapsed_time = current_time - insertion_start_time
                    progress_pct = (processed_count / scenario.total_keys) * 100
                    keys_per_sec = processed_count / elapsed_time if elapsed_time > 0 else 0
                    
                    eta_seconds = (scenario.total_keys - processed_count) / keys_per_sec if keys_per_sec > 0 else 0
                    eta_str = f"{eta_seconds/60:.1f}m" if eta_seconds > 60 else f"{eta_seconds:.0f}s"
                    
                    monitor.update_status({
                        "Phase": "Insertion",
                        "Progress": f"{processed_count:,}/{scenario.total_keys:,} ({progress_pct:.1f}%)",
                        "Speed": f"{keys_per_sec:.0f} keys/sec",
                        "Threads": f"{num_threads} active",
                        "ETA": eta_str
                    })
                
                return len(batch_data), batch_time
            
            # Execute parallel insertion
            monitor.log(f"  Starting data ingestion: {scenario.total_keys:,} keys using {num_threads} threads")
            
            with ThreadPoolExecutor(max_workers=num_threads, thread_name_prefix="ValKeyIngest") as executor:
                futures = []
                batch_id = 0
                
                for batch in generator:
                    future = executor.submit(process_batch, batch, batch_id % num_threads)
                    futures.append(future)
                    batch_id += 1
                
                # Wait for all batches to complete
                completed_batches = 0
                for future in as_completed(futures):
                    try:
                        batch_keys, batch_time = future.result()
                        completed_batches += 1
                    except Exception as e:
                        monitor.log(f"  ❌ Batch failed: {e}")
            
            insertion_time = time.time() - insertion_start_time
            total_keys_inserted = keys_processed.get()
            monitor.log(f"  ✓ Data insertion complete: {total_keys_inserted:,} keys in {insertion_time:.1f}s "
                       f"({total_keys_inserted/insertion_time:.0f} keys/sec) using {num_threads} threads")
            
            # Measure memory after data insertion
            data_memory = client.info("memory")['used_memory']
            data_memory_kb = (data_memory - baseline_memory) // 1024
            monitor.log(f"  Valkey data memory (no index): {data_memory_kb:,} KB")
            
            # Create index
            monitor.log(f"  Creating index '{index_name}' and waiting for indexing...")
            monitor.set_index_name(index_name)
            
            cmd = generator.generate_ft_create_command()
            client.execute_command(*cmd.split())
            
            # Wait for indexing
            self.wait_for_indexing(client, index_name, scenario.total_keys, monitor)
            
            # Measure final memory
            final_memory = client.info("memory")['used_memory']
            total_memory_kb = (final_memory - baseline_memory) // 1024
            index_overhead_kb = (final_memory - data_memory) // 1024
            
            # Get search module info
            try:
                search_info = client.info("SEARCH")
                search_memory_kb = search_info.get('search_used_memory_bytes', 0) // 1024
            except:
                search_memory_kb = index_overhead_kb
            
            # Calculate metrics
            vector_memory_kb = (scenario.total_keys * 8 * 4) // 1024  # 8 dim * 4 bytes
            tag_index_memory_kb = max(0, search_memory_kb - vector_memory_kb)
            
            result = {
                'scenario_name': scenario.name,
                'description': scenario.description,
                'total_keys': scenario.total_keys,
                'data_memory_kb': data_memory_kb,
                'total_memory_kb': total_memory_kb,
                'index_overhead_kb': index_overhead_kb,
                'tag_index_memory_kb': tag_index_memory_kb,
                'vector_memory_kb': vector_memory_kb,
                'insertion_time': insertion_time,
                'tags_config': str(scenario.tags_config.sharing.mode.value)
            }
            
            monitor.log(f"  Results: Data={data_memory_kb:,}KB, TagIndex={tag_index_memory_kb:,}KB, "
                       f"Vector={vector_memory_kb:,}KB, Total={total_memory_kb:,}KB")
            
            # Cleanup
            try:
                client.execute_command("FT.DROPINDEX", index_name)
            except:
                pass
            
            # Clear index monitoring
            monitor.clear_index_names()
            
        finally:
            # Clean up resources
            client_pool.close_all()
            monitor.stop()
        
        return result
    
    def create_comprehensive_scenarios(self, base_keys: int = 100000) -> List[BenchmarkScenario]:
        """Create comprehensive test scenarios"""
        scenarios = []
        
        # 1. Baseline: Unique tags (no sharing)
        scenarios.append(BenchmarkScenario(
            name="Baseline_Unique",
            total_keys=base_keys,
            tags_config=TagsConfig(
                num_keys=base_keys,
                tags_per_key=TagDistribution(avg=5, min=3, max=8),
                tag_length=LengthConfig(avg=20, min=10, max=30),
                sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
            ),
            description="Unique tags per key (no sharing)"
        ))
        
        # 2. Perfect overlap (all keys share same tags)
        scenarios.append(BenchmarkScenario(
            name="Perfect_Overlap",
            total_keys=base_keys,
            tags_config=TagsConfig(
                num_keys=base_keys,
                tags_per_key=TagDistribution(avg=5, min=5, max=5),
                tag_length=LengthConfig(avg=20, min=10, max=30),
                sharing=TagSharingConfig(mode=TagSharingMode.PERFECT_OVERLAP)
            ),
            description="All keys share same 5 tags"
        ))
        
        # 3. Shared pool with varying pool sizes
        for pool_size in [100, 1000, 10000]:
            scenarios.append(BenchmarkScenario(
                name=f"SharedPool_{pool_size}",
                total_keys=base_keys,
                tags_config=TagsConfig(
                    num_keys=base_keys,
                    tags_per_key=TagDistribution(avg=5, min=3, max=8),
                    tag_length=LengthConfig(avg=20, min=10, max=30),
                    sharing=TagSharingConfig(
                        mode=TagSharingMode.SHARED_POOL,
                        pool_size=pool_size,
                        reuse_probability=0.7
                    )
                ),
                description=f"Shared pool of {pool_size} tags"
            ))
        
        # 4. Group-based sharing (realistic scenarios)
        for group_size in [100, 1000, 10000]:
            scenarios.append(BenchmarkScenario(
                name=f"GroupBased_{group_size}",
                total_keys=base_keys,
                tags_config=TagsConfig(
                    num_keys=base_keys,
                    tags_per_key=TagDistribution(avg=5, min=3, max=8),
                    tag_length=LengthConfig(avg=20, min=10, max=30),
                    sharing=TagSharingConfig(
                        mode=TagSharingMode.GROUP_BASED,
                        keys_per_group=group_size,
                        tags_per_group=20
                    )
                ),
                description=f"Groups of {group_size} keys sharing 20 tags"
            ))
        
        # 5. Prefix sharing variations
        for prefix_ratio in [0.3, 0.5, 0.8]:
            scenarios.append(BenchmarkScenario(
                name=f"PrefixShare_{int(prefix_ratio*100)}pct",
                total_keys=base_keys,
                tags_config=TagsConfig(
                    num_keys=base_keys,
                    tags_per_key=TagDistribution(avg=5, min=3, max=8),
                    tag_length=LengthConfig(avg=20, min=15, max=25),
                    tag_prefix=PrefixConfig(
                        enabled=True,
                        min_shared=int(15 * prefix_ratio),
                        max_shared=int(20 * prefix_ratio),
                        share_probability=0.8,
                        prefix_pool_size=50
                    ),
                    sharing=TagSharingConfig(mode=TagSharingMode.SHARED_POOL, pool_size=1000)
                ),
                description=f"{int(prefix_ratio*100)}% prefix sharing"
            ))
        
        # 6. Tag count variations
        for avg_tags in [2, 5, 10, 20]:
            scenarios.append(BenchmarkScenario(
                name=f"TagCount_{avg_tags}",
                total_keys=base_keys,
                tags_config=TagsConfig(
                    num_keys=base_keys,
                    tags_per_key=TagDistribution(avg=avg_tags, min=max(1, avg_tags-2), max=avg_tags+2),
                    tag_length=LengthConfig(avg=20, min=10, max=30),
                    sharing=TagSharingConfig(mode=TagSharingMode.SHARED_POOL, pool_size=5000)
                ),
                description=f"Average {avg_tags} tags per key"
            ))
        
        # 7. Tag length variations
        for tag_len in [10, 20, 50, 100]:
            scenarios.append(BenchmarkScenario(
                name=f"TagLength_{tag_len}",
                total_keys=base_keys,
                tags_config=TagsConfig(
                    num_keys=base_keys,
                    tags_per_key=TagDistribution(avg=5, min=3, max=8),
                    tag_length=LengthConfig(avg=tag_len, min=tag_len-5, max=tag_len+5),
                    sharing=TagSharingConfig(mode=TagSharingMode.SHARED_POOL, pool_size=5000)
                ),
                description=f"Average tag length {tag_len} bytes"
            ))
        
        # 8. Distribution variations
        for dist in [Distribution.UNIFORM, Distribution.NORMAL, Distribution.ZIPF]:
            scenarios.append(BenchmarkScenario(
                name=f"Distribution_{dist.value}",
                total_keys=base_keys,
                tags_config=TagsConfig(
                    num_keys=base_keys,
                    tags_per_key=TagDistribution(avg=50, min=1, max=1000, distribution=dist),
                    tag_length=LengthConfig(avg=40, min=10, max=300),
                    sharing=TagSharingConfig(mode=TagSharingMode.SHARED_POOL, pool_size=5000)
                ),
                description=f"{dist.value} distribution of tags per key"
            ))
        
        return scenarios
    
    def test_comprehensive_memory_benchmark(self):
        """Run comprehensive memory benchmark with all sharing patterns"""
        logging.info("=== COMPREHENSIVE MEMORY BENCHMARK ===")
        logging.info("Testing various tag sharing patterns and configurations\n")
        
        # Create scenarios
        scenarios = self.create_comprehensive_scenarios(base_keys=10000000)
        results = []
        
        # Run each scenario
        for i, scenario in enumerate(scenarios):
            logging.info(f"\n--- Scenario {i+1}/{len(scenarios)}: {scenario.name} ---")
            try:
                result = self.run_benchmark_scenario(scenario)
                results.append(result)
            except Exception as e:
                logging.error(f"  ❌ Scenario failed: {e}")
                continue
        
        # Print summary table
        logging.info("\n" + "="*120)
        logging.info("COMPREHENSIVE BENCHMARK RESULTS SUMMARY")
        logging.info("="*120)
        
        logging.info(f"{'Scenario':<30} {'Keys':>8} {'DataKB':>10} {'TagIdxKB':>10} {'TotalKB':>10} {'Time(s)':>8} {'Mode':<15}")
        logging.info("-" * 120)
        
        for r in results:
            logging.info(f"{r['scenario_name']:<30} "
                        f"{r['total_keys']:>8,} "
                        f"{r['data_memory_kb']:>10,} "
                        f"{r['tag_index_memory_kb']:>10,} "
                        f"{r['total_memory_kb']:>10,} "
                        f"{r['insertion_time']:>8.1f} "
                        f"{r['tags_config']:<15}")
        
        # Analyze results by category
        logging.info("\n" + "="*80)
        logging.info("KEY FINDINGS")
        logging.info("="*80)
        
        # Find extremes
        baseline = next((r for r in results if 'Baseline' in r['scenario_name']), None)
        perfect = next((r for r in results if 'Perfect' in r['scenario_name']), None)
        
        if baseline and perfect:
            savings = (baseline['tag_index_memory_kb'] - perfect['tag_index_memory_kb']) / baseline['tag_index_memory_kb'] * 100
            logging.info(f"\nTag Sharing Impact:")
            logging.info(f"  - Unique tags (baseline): {baseline['tag_index_memory_kb']:,} KB")
            logging.info(f"  - Perfect overlap: {perfect['tag_index_memory_kb']:,} KB")
            logging.info(f"  - Memory savings: {savings:.1f}%")
        
        # Group-based analysis
        group_results = [r for r in results if 'GroupBased' in r['scenario_name']]
        if group_results:
            logging.info(f"\nGroup-based Sharing:")
            for r in sorted(group_results, key=lambda x: x['scenario_name']):
                logging.info(f"  - {r['description']}: {r['tag_index_memory_kb']:,} KB")
        
        # Save detailed results
        csv_filename = "comprehensive_benchmark_results.csv"
        with open(csv_filename, 'w') as f:
            headers = list(results[0].keys())
            f.write(','.join(headers) + '\n')
            for r in results:
                f.write(','.join(str(r.get(h, '')) for h in headers) + '\n')
        
        logging.info(f"\nDetailed results saved to {csv_filename}")
    
    def test_quick_memory_benchmark(self):
        """Quick benchmark with smaller dataset"""
        logging.info("=== QUICK MEMORY BENCHMARK (10K keys) ===")
        
        scenarios = [
            BenchmarkScenario(
                name="Quick_Unique",
                total_keys=10000,
                tags_config=TagsConfig(
                    num_keys=10000,
                    tags_per_key=TagDistribution(avg=5, min=3, max=8),
                    tag_length=LengthConfig(avg=20, min=10, max=30),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description="Unique tags baseline"
            ),
            BenchmarkScenario(
                name="Quick_SharedPool",
                total_keys=10000,
                tags_config=TagsConfig(
                    num_keys=10000,
                    tags_per_key=TagDistribution(avg=5, min=3, max=8),
                    tag_length=LengthConfig(avg=20, min=10, max=30),
                    sharing=TagSharingConfig(mode=TagSharingMode.SHARED_POOL, pool_size=500)
                ),
                description="Shared pool of 500 tags"
            ),
            BenchmarkScenario(
                name="Quick_Groups",
                total_keys=10000,
                tags_config=TagsConfig(
                    num_keys=10000,
                    tags_per_key=TagDistribution(avg=5, min=3, max=8),
                    tag_length=LengthConfig(avg=20, min=10, max=30),
                    sharing=TagSharingConfig(
                        mode=TagSharingMode.GROUP_BASED,
                        keys_per_group=100,
                        tags_per_group=20
                    )
                ),
                description="Groups of 100 keys"
            )
        ]
        
        results = []
        for scenario in scenarios:
            logging.info(f"\nRunning: {scenario.name}")
            result = self.run_benchmark_scenario(scenario)
            results.append(result)
        
        # Quick summary
        logging.info("\n" + "="*80)
        logging.info("QUICK BENCHMARK SUMMARY")
        logging.info("="*80)
        for r in results:
            logging.info(f"{r['scenario_name']:<20}: {r['tag_index_memory_kb']:>8,} KB tag index")