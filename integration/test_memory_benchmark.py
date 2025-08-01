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
import asyncio
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Iterator, Any, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from collections import Counter, defaultdict
import json
import numpy as np
import random
import csv

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logging.warning("psutil not available - memory validation will use fallback method")


from integration.utils.memory_evaluators import calculate_comprehensive_memory, estimate_memory_usage, validate_memory_requirements, verify_memory
from integration.utils.utils import DistributionCollector, ThreadSafeCounter, calculate_progress_stats, get_index_state_info


from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase, ValkeySearchClusterTestCase

# Suppress client logging
import contextlib

# Import our hash generator components
from integration.utils.hash_generator import (
    HashKeyGenerator, HashGeneratorConfig, IndexSchema, FieldSchema,
    FieldType, VectorFieldSchema, VectorAlgorithm, VectorMetric
)
from integration.utils.tags_builder import (
    TagsConfig, TagDistribution, TagSharingConfig, TagSharingMode
)
from integration.utils.string_generator import (
    LengthConfig, PrefixConfig, Distribution, StringType
)
from integration.utils.valkey_clients import (
    SilentValkeyClient, AsyncSilentValkeyClient, SilentClientPool, AsyncSilentClientPool,
    create_silent_client_from_server, create_silent_async_client_from_server, silence_valkey_loggers
)
from integration.utils.monitoring import (
    ProgressMonitor, get_memory_info_summary, log_memory_info, 
    get_key_count_from_db_info, parse_ft_info, safe_get
)
from integration.utils.workload_scenario import (
    WorkloadType, WorkloadOperation, WorkloadStage, WorkloadParser, BenchmarkScenario
)















class TestMemoryBenchmark(ValkeySearchTestCaseBase):
    """Comprehensive Tag Index memory benchmarking using hash_generator"""
    def get_results_header(self) -> List[str]:
                # Define fixed header order for consistent CSV structure
        return [
            'timestamp', 'scenario_name', 'description', 'total_keys', 'data_memory_kb', 'total_memory_kb',
            'index_overhead_kb', 'tag_index_memory_kb', 'estimated_tag_memory_kb',
            'estimated_total_memory_kb', 'estimated_vector_memory_kb', 'estimated_numeric_memory_kb',
            'search_used_memory_kb', 'search_index_reclaimable_memory_kb',
            # Add fields with names that match CSV headers
            'search_memory_kb', 'reclaimable_memory_kb', 'tag_memory_accuracy', 'total_memory_accuracy',
            'vector_memory_kb', 'vector_dims', 'numeric_memory_kb', 'numeric_fields_count',
            'insertion_time', 'insertion_time_sec', 'keys_per_second',
            'tags_config',
#             # Detailed scenario configuration - Add missing fields for CSV
            'vector_dim', 'vector_algorithm', 'vector_metric', 'hnsw_m', 'num_tag_fields', 'num_numeric_fields',                
#             # Tag configuration details
            'tag_avg_length', 'tag_prefix_sharing', 'tag_avg_per_key', 'tag_avg_keys_per_tag',
            'tag_unique_ratio', 'tag_reuse_factor', 'tag_sharing_mode',
#             # Numeric fields info
            'numeric_fields_names', 'numeric_fields_ranges',
#             # Additional CSV fields            
            'index_state', 'phase_time',
#             # Memory breakdown components
            'valkey_core_kb', 'key_interning_kb', 'search_module_overhead_kb', 'fragmentation_overhead_kb',
#             # Distribution statistics
            'unique_tags', 'tag_length_min', 'tag_length_max', 'tag_length_mean', 'tag_length_p95',
            'tags_per_key_min', 'tags_per_key_max', 'tags_per_key_mean',
            'tags_per_key_p95', 'keys_per_tag_min', 'keys_per_tag_max', 'keys_per_tag_mean',
            'keys_per_tag_p95', 'hashes_csv', 'tags_csv'
        ]
    async def run_async_insertion(self, scenario: BenchmarkScenario, generator, monitor: ProgressMonitor, 
                                  insertion_start_time: float, keys_processed: ThreadSafeCounter, 
                                  dist_collector: DistributionCollector, config) -> float:
        """Run async insertion for maximum performance"""
        
        # Create async client pool with optimal client-to-task ratio
        num_tasks = min(300, max(20, scenario.total_keys // config.batch_size))  # Many concurrent tasks
        num_clients = num_tasks * 10  # Fewer clients, shared among tasks
        async_client_pool = AsyncSilentClientPool(self.server, num_clients)
        await async_client_pool.initialize()
        
        monitor.log(f"🚀 Using I/O: {num_tasks} concurrent tasks sharing {num_clients} clients")
        
        async def process_batch_async(batch_data: List[Tuple[str, Dict[str, Any]]], task_id: int) -> Tuple[int, float]:
            """Process a batch of keys asynchronously"""
            async_client, semaphore = async_client_pool.get_client_for_task(task_id)
            
            # Use semaphore to limit concurrent operations per client
            async with semaphore:
                batch_start = time.time()
                async_pipe = async_client.pipeline(transaction=False)
                
                for key, fields in batch_data:
                    async_pipe.hset(key, mapping=fields)
                    
                    # Collect distribution statistics (thread-safe)
                    if 'tags' in fields:
                        dist_collector.process_key(key, fields['tags'])
                
                await async_pipe.execute()
                batch_time = time.time() - batch_start
            
            # Update progress counter
            processed_count = keys_processed.increment(len(batch_data))
            
            # Update monitor status periodically
            if processed_count % (config.batch_size * 10) == 0 or processed_count >= scenario.total_keys:
                current_time = time.time()
                elapsed_time = current_time - insertion_start_time
                progress_pct = (processed_count / scenario.total_keys) * 100
                keys_per_sec = processed_count / elapsed_time if elapsed_time > 0 else 0
                
                eta_seconds = (scenario.total_keys - processed_count) / keys_per_sec if keys_per_sec > 0 else 0
                eta_str = f"{eta_seconds/60:.1f}m" if eta_seconds > 60 else f"{eta_seconds:.0f}s"
                
                monitor.update_status({
                    "Phase": "run_async_insertion",
                    "Progress": f"{processed_count:,}/{scenario.total_keys:,} ({progress_pct:.1f}%)",
                    "Speed": f"{keys_per_sec:.0f} keys/sec",
                    "Tasks": f"{num_tasks} async",
                    "ETA": eta_str
                })
            
            return len(batch_data), batch_time
        
        # Create async tasks with proper client pool management
        active_tasks = {}  # task -> client_index mapping
        available_clients = set(range(num_tasks))  # Track which client indices are free
        generator_iter = iter(generator)
        generator_exhausted = False
        
        # Process batches in a streaming fashion
        while not generator_exhausted or active_tasks:
            # Submit new tasks if we have capacity and available clients
            while len(active_tasks) < num_tasks and not generator_exhausted and available_clients:
                try:
                    batch = next(generator_iter)
                    # Get a free client index
                    client_index = available_clients.pop()
                    task = asyncio.create_task(process_batch_async(batch, client_index))
                    active_tasks[task] = client_index
                except StopIteration:
                    generator_exhausted = True
                    break
            
            # Wait for at least one task to complete
            if active_tasks:
                done, pending = await asyncio.wait(
                    active_tasks.keys(), 
                    return_when=asyncio.FIRST_COMPLETED
                )
                # Process completed tasks and free up client indices
                for task in done:
                    client_index = active_tasks.pop(task)
                    available_clients.add(client_index)  # Return client to available pool
                    try:
                        await task  # This will raise any exceptions
                    except Exception as e:
                        monitor.log(f"❌ Async task failed: {e}")
                
                # Update active_tasks to only contain pending tasks
                active_tasks = {task: active_tasks[task] for task in pending}
        
        # All tasks are complete
        monitor.log(f"✅ All async tasks completed, performing final sync...")
        
        # CRITICAL FIX: Ensure all async operations are committed and visible
        # Create a sync client to verify data is committed
        sync_client = self.get_silent_client()
        
        # Force a small delay to ensure all async writes are committed
        await asyncio.sleep(0.1)
        
        # Verify key count is correct using sync client  
        db_info = sync_client.execute_command("info", "keyspace")
        current_keys = get_key_count_from_db_info(db_info)
        
        expected_keys = keys_processed.get()
        monitor.log(f"🔍 Post-async verification: Expected {expected_keys:,} keys, Found {current_keys:,} keys")
        
        if current_keys < expected_keys:
            monitor.log(f"⚠️  Warning: Key count mismatch detected. Waiting for sync completion...")
            # Wait up to 5 seconds for all keys to be visible
            for i in range(50):  # 50 * 0.1s = 5s max
                await asyncio.sleep(0.1)
                db_info = sync_client.execute_command("info", "keyspace")
                current_keys = get_key_count_from_db_info(db_info)
                
                if current_keys >= expected_keys:
                    monitor.log(f"✅ Sync complete: {current_keys:,} keys now visible after {(i+1)*0.1:.1f}s")
                    break
            else:
                monitor.log(f"⚠️  Still missing keys after 5s wait: {current_keys:,}/{expected_keys:,}")
        else:
            monitor.log(f"✅ All keys immediately visible: {current_keys:,}/{expected_keys:,}")
        
        sync_client.close()
        
        await async_client_pool.close_all()
        
        return time.time() - insertion_start_time

    def get_silent_client(self) -> SilentValkeyClient:
        """Create a new silent client that suppresses logging"""
        if hasattr(self.server, 'get_new_client'):
            # Temporarily suppress logging while getting connection params
            original_level = logging.getLogger().level
            logging.getLogger().setLevel(logging.CRITICAL)
            
            try:
                # Get a regular client first to extract connection params
                temp_client = self.server.get_new_client()
                connection_kwargs = temp_client.connection_pool.connection_kwargs
                temp_client.close()
            finally:
                # Restore logging level
                logging.getLogger().setLevel(original_level)
            
            # Create our silent client with the same params
            return SilentValkeyClient(**connection_kwargs)
        else:
            # Fallback: create with default params
            return SilentValkeyClient(host='localhost', port=6379, decode_responses=True)
    
    def setup_file_logging(self, test_name: str) -> str:
        """Set up file logging for the test with timestamps"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_filename = f"memory_benchmark_{test_name}_{timestamp}.log"
        
        # Get root logger
        root_logger = logging.getLogger()
        
        # Remove existing file handlers to avoid duplicates
        existing_file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
        for handler in existing_file_handlers:
            root_logger.removeHandler(handler)
            handler.close()
        
        # Create a file handler with thread-safe settings
        file_handler = logging.FileHandler(log_filename, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # Create formatter with timestamp and thread info
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', 
                                    datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(formatter)
        
        # Add handler to root logger
        root_logger.addHandler(file_handler)
        
        # Also update console output to include timestamps
        for handler in root_logger.handlers:
            if isinstance(handler, logging.StreamHandler) and handler != file_handler:
                handler.setFormatter(formatter)
        
        # Set root logger level to ensure all messages are captured
        root_logger.setLevel(logging.INFO)
        
        logging.info(f"Starting {test_name} - Log file: {log_filename}")
        return log_filename
    
    def append_to_csv(self, csv_filename: str, result: Dict, monitor: ProgressMonitor = None, write_header: bool = False):
        """Append a result to CSV file incrementally with consistent headers"""
        # Add timestamp to the result
        result['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Define fixed header order for consistent CSV structure
        result_header = self.get_results_header()
        # verify all required fields are present
        for header in result_header:
            assert header in result, f"Missing required header in result: {header}"

        for col in result:
            assert col in result_header, f"Unexpected column in result: {col}"
        # Check if file exists
        file_exists = os.path.exists(csv_filename)
        
        # Write to CSV with consistent structure
        with open(csv_filename, 'a', newline='', encoding='utf-8') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=result_header, extrasaction='ignore')

            # Write header if needed (new file or explicitly requested)
            if write_header or not file_exists:
                writer.writeheader()
            
            # Ensure all required fields have default values
            complete_row = {header: result.get(header, '') for header in result_header}
            
            # Write the data row  
            writer.writerow(complete_row)
        
        # Log through monitor if available, otherwise use direct logging
        if monitor:
            monitor.log(f"Results appended to {csv_filename}")
        else:
            logging.info(f"Results appended to {csv_filename}")
        
    def export_dataset_to_csv(self, client: Valkey, prefix: str = "key:", scenario_name: str = "", timestamp: str = "", monitor: ProgressMonitor = None) -> tuple[str, str]:
        """
        Export dataset to CSV files for analysis.
        
        Returns tuple of (hashes_csv_filename, tags_csv_filename)
        """
        if monitor:
            monitor.log("📁 Exporting dataset to CSV files...")
        
        if not timestamp:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Generate filenames
        hashes_csv = f"dataset_hashes_{scenario_name}_{timestamp}.csv"
        tags_csv = f"dataset_tags_{scenario_name}_{timestamp}.csv"
        
        # Data structures for analysis
        hash_data = []  # List of (key, vector_len, tags_len, tag_ids)
        tag_to_keys = {}  # tag -> set of keys
        tag_id_counter = 0
        tag_to_id = {}  # tag -> unique_id
        
        if monitor:
            monitor.log(f"   Scanning keys with prefix: {prefix}")
        
        # Use SCAN to iterate through all keys
        cursor = 0
        processed_keys = 0
        
        while True:
            cursor, keys = client.scan(cursor, match=f"{prefix}*", count=1000)
            
            if keys:
                # Use pipeline for efficient batch reading
                pipe = client.pipeline(transaction=False)
                
                # Queue all HGETALL commands
                for key in keys:
                    pipe.hgetall(key)
                
                # Execute and process results
                results = pipe.execute()
                
                for i, (key, fields) in enumerate(zip(keys, results)):
                    if fields:  # Make sure we got data
                        processed_keys += 1
                        
                        # Handle key name (bytes vs string)
                        if isinstance(key, bytes):
                            key_str = key.decode('utf-8', errors='replace')
                        else:
                            key_str = str(key)
                        
                        # Handle field keys (bytes vs string)
                        tags_key = b'tags' if isinstance(list(fields.keys())[0], bytes) else 'tags'
                        vector_key = b'vector' if isinstance(list(fields.keys())[0], bytes) else 'vector'
                        
                        # Get vector length
                        vector_len = 0
                        if vector_key in fields:
                            vector_value = fields[vector_key]
                            if isinstance(vector_value, bytes):
                                vector_len = len(vector_value)
                            else:
                                vector_len = len(str(vector_value))
                        
                        # Process tags
                        tags_len = 0
                        tag_ids = []
                        if tags_key in fields:
                            tag_value = fields[tags_key]
                            if isinstance(tag_value, bytes):
                                tag_value = tag_value.decode('utf-8', errors='replace')
                            else:
                                tag_value = str(tag_value)
                            
                            tags_len = len(tag_value)
                            
                            # Split tags and assign IDs
                            if tag_value.strip():
                                tags = [tag.strip() for tag in tag_value.split(',') if tag.strip()]
                                for tag in tags:
                                    # Assign unique ID to each tag
                                    if tag not in tag_to_id:
                                        tag_to_id[tag] = tag_id_counter
                                        tag_id_counter += 1
                                        tag_to_keys[tag] = set()
                                    
                                    tag_ids.append(tag_to_id[tag])
                                    tag_to_keys[tag].add(key_str)
                        
                        # Store hash data
                        hash_data.append({
                            'key': key_str,
                            'vector_len': vector_len,
                            'tags_len': tags_len,
                            'tag_ids': ','.join(map(str, sorted(tag_ids))) if tag_ids else ''
                        })
                
                # Update progress
                if monitor and processed_keys % 10000 == 0:
                    monitor.log(f"   Processed {processed_keys:,} keys...")
            
            # Check if we're done
            if cursor == 0:
                break
        
        if monitor:
            monitor.log(f"✅ Processed {processed_keys:,} keys, found {len(tag_to_id)} unique tags")
        
        # Write hashes CSV
        try:
            import csv
            with open(hashes_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['key', 'vector_len', 'tags_len', 'tag_ids'])
                writer.writeheader()
                writer.writerows(hash_data)
            
            if monitor:
                monitor.log(f"📄 Hashes CSV written: {hashes_csv} ({len(hash_data):,} rows)")
        
        except Exception as e:
            if monitor:
                monitor.log(f"❌ Failed to write hashes CSV: {e}")
            raise
        
        # Write tags CSV
        try:
            tag_data = []
            for tag, tag_id in tag_to_id.items():
                keys_for_tag = tag_to_keys[tag]
                tag_data.append({
                    'id': tag_id,
                    'tag': tag,
                    'len': len(tag),
                    'keys': ','.join(sorted(keys_for_tag))
                })
            
            # Sort by ID for consistent output
            tag_data.sort(key=lambda x: x['id'])
            
            with open(tags_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['id', 'tag', 'len', 'keys'])
                writer.writeheader()
                writer.writerows(tag_data)
            
            if monitor:
                monitor.log(f"📄 Tags CSV written: {tags_csv} ({len(tag_data):,} rows)")
        
        except Exception as e:
            if monitor:
                monitor.log(f"❌ Failed to write tags CSV: {e}")
            raise
        
        return hashes_csv, tags_csv
    
    def create_schema(self, index_name: str, vector_dim: int = 8, include_numeric: bool = True, 
                     vector_algorithm: VectorAlgorithm = VectorAlgorithm.FLAT,
                     vector_metric: VectorMetric = VectorMetric.COSINE,
                     numeric_fields: Dict[str, Tuple[float, float]] = None,
                     hnsw_m: int = 16) -> IndexSchema:
        """Create a schema with tags, vector, and optionally numeric fields"""
        from integration.utils.hash_generator import create_numeric_field
        
        fields = [
            FieldSchema(name="tags", type=FieldType.TAG, separator=","),
            FieldSchema(
                name="vector",
                type=FieldType.VECTOR,
                vector_config=VectorFieldSchema(
                    algorithm=vector_algorithm,
                    dim=vector_dim,
                    distance_metric=vector_metric,
                    m=hnsw_m if vector_algorithm == VectorAlgorithm.HNSW else None
                )
            )
        ]
        
        if include_numeric and numeric_fields:
            # Add numeric fields based on configuration
            for field_name, (min_val, max_val) in numeric_fields.items():
                distribution = "normal" if "score" in field_name else "uniform"
                fields.append(
                    create_numeric_field(field_name, min_val=min_val, max_val=max_val, distribution=distribution)
                )
        
        return IndexSchema(
            index_name=index_name,
            prefix=["key:"],
            fields=fields
        )
    
    def verify_server_connection(self, client: Valkey, monitor: ProgressMonitor = None) -> bool:
        """Verify that the server is responding"""
        try:
            client.ping()
            return True
        except Exception as e:
            error_msg = f"❌ Server connection failed: {e}"
            if monitor:
                monitor.log(error_msg)
            else:
                logging.info(error_msg)
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
            ft_info = client.execute_command("FT.INFO", index_name)
            info_dict = parse_ft_info(ft_info)
            index_state = get_index_state_info(info_dict)
            
            num_docs = index_state['num_docs']
            mutation_queue_size = index_state['mutation_queue_size']
            backfill_in_progress = index_state['backfill_in_progress']
            backfill_complete_percent = float(info_dict.get('backfill_complete_percent', 0.0))
            state = index_state['state']
            indexing = index_state['is_indexing']
            
            # Get memory info
            memory_summary = get_memory_info_summary(client)
            log_memory_info(monitor, memory_summary, "Index is still processing:")
            current_memory_kb = memory_summary['used_memory_kb']
            # Calculate indexing rate
            current_time = time.time()
            elapsed = current_time - start_time
            
            # Report progress every 5 seconds or when significant progress is made
            doc_progress = num_docs - last_doc_count
            time_since_report = current_time - last_report_time
            
            if time_since_report >= 5 or doc_progress >= expected_docs * 0.05:  # Every 5% progress
                progress_stats = calculate_progress_stats(num_docs, expected_docs, elapsed)
                docs_per_sec = progress_stats['items_per_sec']
                progress_pct = progress_stats['progress_pct']
                eta_str = progress_stats['eta_str']
                
                status = f"State: {state}"
                if mutation_queue_size > 0:
                    status += f", Queue: {mutation_queue_size}"
                if backfill_in_progress > 0:
                    status += f", Backfill: {backfill_complete_percent:.1f}%"
                
                monitor.log(f"    Search Index: {num_docs:,}/{expected_docs:,} docs ({progress_pct:.1f}%) | "
                        f"{docs_per_sec:.0f} docs/sec | Memory: {current_memory_kb:,} KB | "
                        f"{status} | ETA: {eta_str}")
                sys.stdout.flush()  # Ensure immediate output
                last_report_time = current_time
                last_doc_count = num_docs
            
            # Check if indexing is complete - all docs indexed and no pending operations
            if num_docs >= expected_docs and not indexing and state == "ready":
                total_time = time.time() - start_time
                avg_docs_per_sec = num_docs / total_time if total_time > 0 else 0
                monitor.log(f"    ✓ Search indexing complete: {num_docs:,} docs indexed in {total_time:.1f}s "
                        f"(avg {avg_docs_per_sec:.0f} docs/sec)")
                monitor.log(f"    ✓ Final memory usage: {current_memory_kb:,} KB")
                monitor.log(f"    ✓ Index state: {state}, queue: {mutation_queue_size}, backfill: {backfill_complete_percent:.1f}%")
                memory_summary = get_memory_info_summary(client)
                log_memory_info(monitor, memory_summary, "Indexing DONE:")               
                sys.stdout.flush()  # Ensure immediate output
                return
                
            time.sleep(1)  # Check every 1 second for more responsive monitoring

        monitor.log(f"    ⚠ Warning: Indexing timeout after {timeout}s, proceeding anyway")
        memory_summary = get_memory_info_summary(client)
        log_memory_info(monitor, memory_summary, "Indexing NOT DONE:")
        sys.stdout.flush()  # Ensure immediate output

    def run_benchmark_scenario(self, monitor_csv_filename: str, scenario: BenchmarkScenario, monitor: ProgressMonitor = None) -> Dict:
        """Run a single benchmark scenario with full monitoring"""
        index_config = scenario.get_index_config()
        
        
        # Use provided monitor or create a new one if none provided
        should_stop_monitor = False
        if monitor is None:
            monitor = ProgressMonitor(self.server, monitor_csv_filename, scenario.name, index_config)
            monitor.start()
            should_stop_monitor = True
        monitor.log("")
        monitor.log("┏" + "━" * 78 + "┓")
        monitor.log(f"┃ 🚀 STARTING SCENARIO: {scenario.name:<50} ┃")
        monitor.log(f"┃    {scenario.description:<65} ┃")
        monitor.log("┗" + "━" * 78 + "┛")
        monitor.log("")

        # Validate memory requirements before starting
        can_run, message = validate_memory_requirements(scenario)
        monitor.log(f"📋 Memory Validation: {message}")
        
        if not can_run:
            assert False, f"Scenario {scenario.name} should not run due to memory constraints"            
            # monitor.error(f"❌ Skipping scenario due to memory constraints")
            # return {
            #     'scenario_name': scenario.name,
            #     'description': scenario.description,
            #     'total_keys': scenario.total_keys,
            #     'skipped': True,
            #     'reason': message
            # }
        

        # try:
        # Get main client
        client = self.get_silent_client()
        client_pool = None  # Initialize to None to avoid undefined variable error
        
        # try:
        # Verify server connection
        if not self.verify_server_connection(client, monitor):
            if should_stop_monitor:
                monitor.stop()
            raise RuntimeError(f"Failed to connect to valkey server for scenario {scenario.name}")
        
        # Clean up any existing data
        client.flushall()
        time.sleep(1)
        memory_usage = { 'after_flush':{'used_memory': 0, 'used_memory_dataset': 0} ,
                            'before_insertion':{'used_memory': 0, 'used_memory_dataset': 0} ,
                            'after_insertion':{'used_memory': 0, 'used_memory_dataset': 0, 'dataset_valkey_size': 0} ,
                            'after_search_backfill':{'used_memory': 0, 'used_memory_dataset': 0, 'search_used_memory':0} }
        #get dataset size before insertion
        memory_info = client.execute_command("info", "memory")
        used_dataset_size = safe_get(memory_info, 'used_memory_dataset', 0)
        used_memory_size = safe_get(memory_info, 'used_memory', 0)
        monitor.log(f"📊 INFO used_memory_dataset: Before insertion: {used_dataset_size:,} bytes")
        monitor.log(f"📊 INFO used_memory: Before insertion: {used_memory_size:,} bytes")
        memory_usage['after_flush']['used_memory'] = used_memory_size
        memory_usage['after_flush']['used_memory_dataset'] = used_dataset_size
        # Get key count from db info
        db_info = client.execute_command("info","keyspace")
        current_keys = get_key_count_from_db_info(db_info)
        while current_keys > 0:
            assert False, "Database should be empty before starting scenario"
        assert current_keys == 0, f"Expected empty dataset before starting scenario, dataset size is not zero keys: {current_keys}"
            
            
        memory_summary = get_memory_info_summary(client)
        total_memory = memory_summary['used_memory']
        baseline_memory = memory_summary['used_memory']
        
        monitor.log("📊 BASELINE MEASUREMENTS")
        monitor.log("─" * 50)
        log_memory_info(monitor, memory_summary)
        monitor.log(f"📈 Baseline: {baseline_memory // 1024:,} KB")
        monitor.log("")
        
        # Log memory estimates
        estimates = estimate_memory_usage(scenario, monitor)

        
        # Create schema and generator
        index_name = f"idx_{scenario.name.lower().replace(' ', '_')}"
        
        # Use scenario configuration for vector and numeric fields
        schema = self.create_schema(
            index_name, 
            vector_dim=scenario.vector_dim,
            vector_algorithm=scenario.vector_algorithm,
            vector_metric=scenario.vector_metric,
            include_numeric=scenario.include_numeric,
            numeric_fields=scenario.numeric_fields,
            hnsw_m=scenario.hnsw_m
        )
        
        config = HashGeneratorConfig(
            num_keys=scenario.total_keys,
            schema=schema,
            tags_config=scenario.tags_config,
            key_length=LengthConfig(avg=8, min=8, max=8),  # Fixed length keys
            batch_size=10,  # Optimize batch size
            seed=42
        )
        
        generator = HashKeyGenerator(config)
        
        # Create client pool for parallel insertion
        num_threads = min(64, max(2, scenario.total_keys // config.batch_size))
        client_pool = SilentClientPool(self.server, num_threads)
        
        # Create distribution collector
        dist_collector = DistributionCollector()
        
        monitor.log("⚡ DATA INSERTION PHASE")
        monitor.log("─" * 50)  
        monitor.log(f"📝 Generating: {scenario.total_keys:,} keys")
        num_tasks = min(100, max(10, scenario.total_keys // config.batch_size))
        monitor.log(f"🚀 Mode: ASYNC I/O with {num_tasks} concurrent tasks")
        monitor.log(f"📦 Batch size: {config.batch_size:,}")
        monitor.log("")
        
        # Insert data using generator with parallel processing
        insertion_start_time = time.time()
        keys_processed = ThreadSafeCounter(0)       
        memory_info = client.execute_command("info", "memory")        
        used_dataset_size = safe_get(memory_info, 'used_memory_dataset', 0)
        used_memory_size = safe_get(memory_info, 'used_memory', 0)       
        memory_usage['before_insertion']['used_memory'] = used_memory_size
        memory_usage['before_insertion']['used_memory_dataset'] = used_dataset_size
        # Choose insertion method based on use_async parameter
        # Use async I/O for maximum performance
        monitor.log("🚀 Starting data ingestion")
        try:
            insertion_time = asyncio.run(
                self.run_async_insertion(scenario, generator, monitor, insertion_start_time, 
                                        keys_processed, dist_collector, config)
            )
        except Exception as e:
            assert False, f"Async insertion failed: {e}"                        
        
        total_keys_inserted = keys_processed.get()
        memory_summary = get_memory_info_summary(client)
        
        # Clear any ongoing status updates since insertion is complete
        monitor.update_status({})
        memory_info = client.execute_command("info", "memory")        
        used_dataset_size = safe_get(memory_info, 'used_memory_dataset', 0)
        used_memory_size = safe_get(memory_info, 'used_memory', 0)       
        memory_usage['after_insertion']['used_memory'] = used_memory_size
        memory_usage['after_insertion']['used_memory_dataset'] = used_dataset_size
        memory_usage['after_insertion']['used_memory_dataset'] = used_dataset_size
        monitor.log("✅ INSERTION COMPLETE")
        monitor.log("─" * 50)
        monitor.log(f"📊 Keys Inserted: {total_keys_inserted:,}")
        monitor.log(f"⏱️  Time Taken: {insertion_time:.1f}s")
        monitor.log(f"🚀 Speed: {total_keys_inserted/insertion_time:.0f} keys/sec")
        log_memory_info(monitor, memory_summary)
        monitor.log("")
        
        # Log distribution statistics
        dist_summary = dist_collector.get_summary()
        monitor.log("📈 DATA DISTRIBUTION STATS [based on inserted data]")
        monitor.log("─" * 50)
        monitor.log(f"🏷️  Tag Lengths: min={dist_summary['tag_lengths']['min']}, "
                    f"max={dist_summary['tag_lengths']['max']}, "
                    f"avg={dist_summary['tag_lengths']['mean']:.1f}, "
                    f"p95={dist_summary['tag_lengths']['p95']}")
        monitor.log(f"📝 Tags/Key: min={dist_summary['tags_per_key']['min']}, "
                    f"max={dist_summary['tags_per_key']['max']}, "
                    f"avg={dist_summary['tags_per_key']['mean']:.1f}, "
                    f"p95={dist_summary['tags_per_key']['p95']}")
        monitor.log(f"🔄 Tag Reuse: {dist_summary['tag_usage']['unique_tags']} unique tags, "
                    f"avg keys/tag={dist_summary['tag_usage']['mean_keys_per_tag']:.1f}, "
                    f"max={dist_summary['tag_usage']['max_keys_per_tag']}")
        monitor.log("")
        
        # Verify memory by reading all keys
        if scenario.total_keys <= 1000000:  # Only verify for datasets up to 1M keys to avoid timeout
            verify_memory(client, monitor=monitor)
        
        # Export dataset to CSV files for analysis
        hashes_csv = ""
        tags_csv = ""
        # if scenario.total_keys <= 100000:  # Only export for smaller datasets to avoid huge files
        #     monitor.log("📁 DATASET EXPORT")
        #     monitor.log("─" * 50)
        #     export_start = time.time()
            
        #     # Get timestamp from current scenario
        #     timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
        #     # Create a client without decode_responses for proper binary handling
        #     export_client = SilentValkeyClient(
        #         host=client.connection_pool.connection_kwargs['host'],
        #         port=client.connection_pool.connection_kwargs['port'],
        #         decode_responses=False  # Important for binary data
        #     )
        #     hashes_csv = ""
        #     tags_csv = ""
        #     # try:
        #     #     hashes_csv, tags_csv = self.export_dataset_to_csv(
        #     #         export_client, 
        #     #         prefix="key:",  # Assuming standard prefix
        #     #         scenario_name=scenario.name,
        #     #         timestamp=timestamp,
        #     #         monitor=monitor
        #     #     )
        #     #     export_time = time.time() - export_start
        #     #     monitor.log(f"⏱️  Export time: {export_time:.1f}s")
        #     # finally:
        #     #     assert export_client is not None
        #     #     export_client.close()

        #     monitor.log("")
        monitor.log("📊 INFO MEMORY + INFO MODULES")
        # Measure memory after data insertion
        data_memory_summary = get_memory_info_summary(client)
        data_memory = data_memory_summary['used_memory_dataset']
        data_memory_kb = data_memory // 1024
        monitor.log("=" * 50)
        monitor.log("🏗️  INDEX CREATION PHASE")
        monitor.log("─" * 50)
        monitor.log(f"📊 Data Memory (no index): {data_memory_kb:,} KB")
        monitor.log(f"🏷️  Index Name: {index_name}")
        
        
        monitor.set_index_name(index_name)
        
        cmd = generator.generate_ft_create_command()
        monitor.log(f"📜 FT.CREATE index with command: {cmd}")
        client.execute_command(*cmd.split())
        monitor.log("")
        monitor.log("=" * 50)
        
        # check index exists and print ft.info
        index_info = client.execute_command("FT.INFO", index_name)
        monitor.log(f"📑 FT.INFO: {index_info}")
        monitor.log(f"📊 waiting for backfill of: {index_name} ...")
        # Wait for indexing
        self.wait_for_indexing(client, index_name, scenario.total_keys, monitor)          
        monitor.log("📊 Indexing complete, measuring memory usage...")
        memory_info = client.execute_command("info", "memory")        
        used_dataset_size = safe_get(memory_info, 'used_memory_dataset', 0)
        used_memory_size = safe_get(memory_info, 'used_memory', 0)       
        memory_usage['after_search_backfill']['used_memory'] = used_memory_size
        memory_usage['after_search_backfill']['used_memory_dataset'] = used_dataset_size
        search_info = client.execute_command("info", "modules")
        memory_usage['after_search_backfill']['search_used_memory'] = safe_get(search_info, 'search_used_memory_bytes', 0)
        # Measure memory after index creation
        monitor.log("📊  INFO MEMORY ...")  
        # Measure final memory
        final_memory_summary = get_memory_info_summary(client)
        final_memory = final_memory_summary['used_memory_dataset']
        total_memory_kb = final_memory // 1024
        index_overhead_kb = (final_memory - data_memory) // 1024
        monitor.log("📊 > INFO MODULES ...") 
        # Get search module info
        search_info = client.execute_command("info", "modules")
        search_memory_kb = search_info['search_used_memory_bytes'] // 1024
        search_reclaimed_kb = search_info['search_index_reclaimable_memory'] // 1024
        monitor.log(f"search info: {search_info}")
        monitor.log(f"📊 Search Memory: {search_memory_kb:,} KB"
                    f" (reclaimable: {search_reclaimed_kb:,} KB)")
        monitor.log(f"📊 Total Memory: {total_memory_kb:,} KB")
        # Get distribution statistics first
        dist_stats = dist_collector.get_summary()
        
        # Get actual vector dimensions from data verification
        prefix = schema.prefix[0] if schema.prefix else "key:"  # Get prefix from schema
        verify_result = verify_memory(client, prefix, monitor)
        vector_dims = verify_result.get('vector_dims', scenario.vector_dim)  # Use scenario config as fallback
        
        # Calculate metrics with improved tag index memory estimation
        vector_memory_kb = (scenario.total_keys * vector_dims * 4) // 1024  # dims * 4 bytes
        
        # Use comprehensive memory analysis based on actual server state
        memory_info = client.info("memory")
        stats_dict = {
            'unique_tags': dist_stats['tag_usage']['unique_tags'],
            'avg_tag_length': dist_stats['tag_lengths']['mean'],
            'avg_tags_per_key': dist_stats['tags_per_key']['mean'],
            'avg_keys_per_tag': dist_stats['tag_usage']['mean_keys_per_tag']
        }
        memory_breakdown = calculate_comprehensive_memory(
            client, scenario, memory_info, search_info, stats_dict, baseline_memory, data_memory, monitor
        )
        # info_all = client.execute_command("info", "memory")
        # monitor.log(f"📊 Memory Info: {info_all}")
        # Extract components for comparison - use PRE-INGESTION estimates vs POST-INGESTION actuals
        pre_ingestion_estimates = estimates  # From earlier estimate_memory_usage call
        estimated_total_kb = pre_ingestion_estimates['total_memory'] // 1024
        estimated_tag_memory_kb = pre_ingestion_estimates['tag_index_memory'] // 1024
        estimated_vector_memory_kb = pre_ingestion_estimates['vector_index_memory'] // 1024
        
        # Use the comprehensive breakdown for proper tag memory calculation
        # The search_memory_kb includes ALL module memory (key interning, tag index, vector index, metadata, etc.)
        # So we can't just subtract vector memory - we need to use our comprehensive estimate
        tag_index_memory_kb = memory_breakdown['tag_index_memory_kb']
        
        # For accuracy calculation, we need to compare estimates vs actual deduced values
        # The v2 function deduces actual tag memory by working backwards from total search memory
        actual_tag_memory_kb = memory_breakdown['tag_index_memory_kb']
        
        # But for comparison with the PRE-INGESTION estimate, we need to use a different approach
        # The estimate comes from estimate_memory_usage, the actual comes from calculate_comprehensive_memory
        # These are fundamentally different: one predicts, one deduces from real data
        
        result = {
            'scenario_name': scenario.name,
            'description': scenario.description,
            'total_keys': scenario.total_keys,
            'data_memory_kb': data_memory_kb,
            'total_memory_kb': total_memory_kb,
            'index_overhead_kb': index_overhead_kb,
            'tag_index_memory_kb': tag_index_memory_kb,
            'estimated_tag_memory_kb': estimated_tag_memory_kb,
            'estimated_total_memory_kb': estimated_total_kb,
            'estimated_vector_memory_kb': estimated_vector_memory_kb,
            'estimated_numeric_memory_kb': memory_breakdown['numeric_index_memory_kb'],
            'search_used_memory_kb': search_memory_kb,  # Add missing field for fuzzy test
            'search_index_reclaimable_memory_kb': search_reclaimed_kb,
            # Add fields with names that match CSV headers
            'search_memory_kb': search_memory_kb,
            'reclaimable_memory_kb': search_reclaimed_kb,
            'tag_memory_accuracy': (estimated_tag_memory_kb / max(1, actual_tag_memory_kb)) if actual_tag_memory_kb > 0 else 0,
            'total_memory_accuracy': (estimated_total_kb / max(1, search_memory_kb)) if search_memory_kb > 0 else 0,
            'vector_memory_kb': vector_memory_kb,
            'vector_dims': vector_dims,
            'numeric_memory_kb': memory_breakdown['numeric_index_memory_kb'],
            'numeric_fields_count': verify_result.get('numeric_fields_count', 0),
            'insertion_time': insertion_time,
            'insertion_time_sec': insertion_time,  # Add for fuzzy test compatibility
            'keys_per_second': total_keys_inserted/insertion_time if insertion_time > 0 else 0,  # Add for fuzzy test
            'tags_config': str(scenario.tags_config.sharing.mode.value),
            
            # Detailed scenario configuration - Add missing fields for CSV
            'vector_dim': scenario.vector_dim,
            'vector_algorithm': scenario.vector_algorithm.name if hasattr(scenario.vector_algorithm, 'name') else str(scenario.vector_algorithm),
            'vector_metric': scenario.vector_metric.name if hasattr(scenario.vector_metric, 'name') else str(scenario.vector_metric),
            'hnsw_m': scenario.hnsw_m if (hasattr(scenario.vector_algorithm, 'name') and scenario.vector_algorithm.name == 'HNSW') or (str(scenario.vector_algorithm) == 'HNSW') else '',
            'num_tag_fields': 1,  # Always have tags field
            'num_numeric_fields': len(scenario.numeric_fields) if scenario.numeric_fields else 0,
            
            # Tag configuration details
            'tag_avg_length': scenario.tags_config.tag_length.avg if scenario.tags_config and scenario.tags_config.tag_length else 0,
            'tag_prefix_sharing': scenario.tags_config.tag_prefix.share_probability if scenario.tags_config and scenario.tags_config.tag_prefix else 0,
            'tag_avg_per_key': scenario.tags_config.tags_per_key.avg if scenario.tags_config and scenario.tags_config.tags_per_key else 0,
            'tag_avg_keys_per_tag': dist_stats['tag_usage']['mean_keys_per_tag'] if 'tag_usage' in dist_stats else 0,
            'tag_unique_ratio': (dist_stats['tag_usage']['unique_tags'] / max(1, total_keys_inserted * scenario.tags_config.tags_per_key.avg)) if scenario.tags_config and scenario.tags_config.tags_per_key and 'tag_usage' in dist_stats else 0,
            'tag_reuse_factor': (total_keys_inserted * scenario.tags_config.tags_per_key.avg / max(1, dist_stats['tag_usage']['unique_tags'])) if scenario.tags_config and scenario.tags_config.tags_per_key and 'tag_usage' in dist_stats else 1.0,
            'tag_sharing_mode': scenario.tags_config.sharing.mode.name if scenario.tags_config and scenario.tags_config.sharing and hasattr(scenario.tags_config.sharing.mode, 'name') else str(scenario.tags_config.sharing.mode) if scenario.tags_config and scenario.tags_config.sharing else 'unique',
            
            # Numeric fields info
            'numeric_fields_names': ', '.join(scenario.numeric_fields.keys()) if scenario.numeric_fields else '',
            'numeric_fields_ranges': str(scenario.numeric_fields) if scenario.numeric_fields else '',
            
            # Additional CSV fields
            'index_state': 'completed',  # Could be extracted from FT.INFO if needed
            'phase_time': insertion_time,
            
            # Memory breakdown components
            'valkey_core_kb': memory_breakdown['valkey_total_kb'],
            'key_interning_kb': memory_breakdown['key_interning_memory_kb'],
            'search_module_overhead_kb': memory_breakdown['search_module_overhead_kb'],
            'fragmentation_overhead_kb': memory_breakdown['allocator_fragmentation_kb'],
            # Distribution statistics
            'unique_tags': dist_stats['tag_usage']['unique_tags'],
            'tag_length_min': dist_stats['tag_lengths']['min'],
            'tag_length_max': dist_stats['tag_lengths']['max'],
            'tag_length_mean': dist_stats['tag_lengths']['mean'],
            'tag_length_p95': dist_stats['tag_lengths']['p95'],
            'tags_per_key_min': dist_stats['tags_per_key']['min'],
            'tags_per_key_max': dist_stats['tags_per_key']['max'],
            'tags_per_key_mean': dist_stats['tags_per_key']['mean'],
            'tags_per_key_p95': dist_stats['tags_per_key']['p95'],
            'keys_per_tag_min': dist_stats['tag_usage']['min_keys_per_tag'],
            'keys_per_tag_max': dist_stats['tag_usage']['max_keys_per_tag'],
            'keys_per_tag_mean': dist_stats['tag_usage']['mean_keys_per_tag'],
            'keys_per_tag_p95': dist_stats['tag_usage']['p95_keys_per_tag'],
            'hashes_csv': hashes_csv,
            'tags_csv': tags_csv
            
        }
        monitor.log("🎯 MEMORY ANALYSIS RESULTS")
        monitor.log("─" * 60)
        for key, value in memory_usage.items():
            for subkey, subvalue in value.items():
                if isinstance(subvalue, int):
                    subvalue = subvalue // 1024  # Convert to KB
                monitor.log(f"📊 {key} - {subkey}: {subvalue:,} KB")
        monitor.log("─" * 60)
        monitor.log("🎯 FINAL RESULTS")
        monitor.log("─" * 60)
        monitor.log(f"📁 Data Memory:          {data_memory_kb:,} KB")
        monitor.log(f"🔍 Search Module Total:  {search_memory_kb:,} KB (actual)")
        monitor.log(f"🧮 Search Module Est.:   {estimated_total_kb:,} KB ({(estimated_total_kb/max(1,search_memory_kb)*100):.1f}% accuracy)")
        monitor.log("")
        monitor.log("📊 COMPREHENSIVE MEMORY BREAKDOWN:")
        monitor.log(f"   🗄️  Valkey Core:       {memory_breakdown['valkey_total_kb']:,} KB")
        monitor.log(f"   🔗 Key Interning:     {memory_breakdown['key_interning_memory_kb']:,} KB") 
        monitor.log(f"   🏷️  Tag Index:         {estimated_tag_memory_kb:,} KB (est) vs {actual_tag_memory_kb:,} KB (actual)")
        monitor.log(f"   🎯 Vector Index:      {estimated_vector_memory_kb:,} KB (est) vs {vector_memory_kb:,} KB (calc)")
        if memory_breakdown['numeric_index_memory_kb'] > 0:
            monitor.log(f"   🔢 Numeric Index:     {memory_breakdown['numeric_index_memory_kb']:,} KB")
        monitor.log(f"   ⚙️  Module Overhead:   {memory_breakdown['search_module_overhead_kb']:,} KB")
        monitor.log(f"   🔀 Fragmentation:     {memory_breakdown['allocator_fragmentation_kb']:,} KB")
        monitor.log("─" * 60)
        monitor.log(f"📈 Index Overhead:       {index_overhead_kb:,} KB")
        monitor.log(f"💰 Total Memory:         {total_memory_kb:,} KB")
        monitor.log("")

        # Accuracy analysis
        # Search module accuracy: estimated search components vs actual search memory
        estimated_search_kb = (
            pre_ingestion_estimates['key_interning_memory'] + 
            pre_ingestion_estimates['tag_index_memory'] + 
            pre_ingestion_estimates['vector_index_memory'] + 
            pre_ingestion_estimates['numeric_index_memory'] + 
            pre_ingestion_estimates['search_module_overhead']
        ) // 1024
        search_accuracy = estimated_search_kb / max(1, search_memory_kb)
        
        # Overall accuracy: estimated total (Valkey + Search) vs actual total
        estimated_overall_kb = pre_ingestion_estimates['total_memory'] // 1024  # Includes Valkey + Search
        actual_overall_kb = memory_breakdown['valkey_total_kb'] + search_memory_kb  # Actual Valkey + Search
        overall_accuracy = estimated_overall_kb / max(1, actual_overall_kb)
        
        tag_accuracy = estimated_tag_memory_kb / max(1, actual_tag_memory_kb) if actual_tag_memory_kb > 0 else 0
        
        monitor.log("🎯 ESTIMATION ACCURACY:")
        monitor.log(f"   📊 Search Module: {search_accuracy:.2f}x ({estimated_search_kb:,} KB est vs {search_memory_kb:,} KB actual)")
        if monitor:
            monitor.log(f"      Search estimate breakdown: {pre_ingestion_estimates['tag_index_memory']//1024:,} tag + {pre_ingestion_estimates['vector_index_memory']//1024:,} vector + {pre_ingestion_estimates['key_interning_memory']//1024:,} keys + {pre_ingestion_estimates['numeric_index_memory']//1024:,} numeric + {pre_ingestion_estimates['search_module_overhead']//1024:,} overhead KB")
        if search_accuracy < 0.7:
            monitor.log(f"      ⚠️  Search: Underestimating - missing components?")
        elif search_accuracy > 1.5:
            monitor.log(f"      ⚠️  Search: Overestimating - double counting?")
        else:
            monitor.log(f"      ✅ Search: Good accuracy")
        
        monitor.log(f"   💰 Overall Total: {overall_accuracy:.2f}x ({estimated_overall_kb:,} KB est vs {actual_overall_kb:,} KB actual)")
        if overall_accuracy < 0.7:
            monitor.log(f"      ⚠️  Overall: Underestimating - missing components?")
        elif overall_accuracy > 1.5:
            monitor.log(f"      ⚠️  Overall: Overestimating - double counting?")
        else:
            monitor.log(f"      ✅ Overall: Good accuracy")
        
        if tag_accuracy > 0:
            if tag_accuracy < 0.5:
                monitor.log(f"   ⚠️  Tag: {tag_accuracy:.2f}x (underestimating) - complex sharing patterns?")
            elif tag_accuracy > 2.0:
                monitor.log(f"   ⚠️  Tag: {tag_accuracy:.2f}x (overestimating) - better sharing than expected?")
            else:
                monitor.log(f"   ✅ Tag: {tag_accuracy:.2f}x (good tag index accuracy)")
        monitor.log("")
        
        # Save detailed distribution data
        detailed_dists = dist_collector.get_detailed_distributions()
        dist_filename = f"distributions_{scenario.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(dist_filename, 'w') as f:
            json.dump({
                'scenario': scenario.name,
                'total_keys': scenario.total_keys,
                'summary': dist_stats,
                'detailed': detailed_dists
            }, f, indent=2)
        monitor.log(f"  Distribution details saved to {dist_filename}")
        monitor.log("─" * 60)
        monitor.log(f"🚀 EXECUTING STAGES: {len(scenario.workload_stages)} stages")
        # Execute workload stages if defined
        workload_results = {}
        if scenario.workload_stages:
            monitor.log("─" * 50)
            monitor.log(f"🔄 WORKLOAD EXECUTION PHASE scenario: {scenario.name}")
            workload_results = self.execute_workload_stages(
                client, scenario, index_name, generator, monitor, scenario.total_keys
            )
            
            # Add workload results to the main result
            result['workload_stages'] = workload_results.get('stages', [])
            result['workload_final_key_count'] = workload_results['final_key_count']
            result['workload_deleted_keys'] = workload_results['deleted_keys_count']
            
            # Get final memory state after workload
            final_workload_memory = get_memory_info_summary(client)
            result['workload_final_memory_kb'] = final_workload_memory['used_memory'] // 1024
        
        # Cleanup
        client.execute_command("FT.DROPINDEX", index_name)
        
        # Clear index monitoring and status
        monitor.clear_index_names()
        monitor.update_status({})  # Clear any lingering status updates

        # except Exception as e:
        #     # Handle any errors during the benchmark
        #     monitor.error(f"  ❌ Scenario failed: {e}")
        #     result = {
        #         'scenario_name': scenario.name,
        #         'description': scenario.description,
        #         'total_keys': scenario.total_keys,
        #         'skipped': True,
        #         'reason': f"Failed: {str(e)}",
                
        #         # Add basic configuration for failed scenarios too
        #         'vector_dim': scenario.vector_dim,
        #         'vector_algorithm': scenario.vector_algorithm.name if hasattr(scenario.vector_algorithm, 'name') else str(scenario.vector_algorithm),
        #         'hnsw_m': scenario.hnsw_m if (hasattr(scenario.vector_algorithm, 'name') and scenario.vector_algorithm.name == 'HNSW') or (str(scenario.vector_algorithm) == 'HNSW') else '',
        #         'num_tag_fields': 1,
        #         'num_numeric_fields': len(scenario.numeric_fields) if scenario.numeric_fields else 0,
        #         'tag_avg_length': scenario.tags_config.tag_length.avg if scenario.tags_config and scenario.tags_config.tag_length else 0,
        #         'tag_prefix_sharing': scenario.tags_config.tag_prefix.share_probability if scenario.tags_config and scenario.tags_config.tag_prefix else 0,
        #         'tag_avg_per_key': scenario.tags_config.tags_per_key.avg if scenario.tags_config and scenario.tags_config.tags_per_key else 0,
        #         'tag_sharing_mode': scenario.tags_config.sharing.mode.name if scenario.tags_config and scenario.tags_config.sharing and hasattr(scenario.tags_config.sharing.mode, 'name') else str(scenario.tags_config.sharing.mode) if scenario.tags_config and scenario.tags_config.sharing else 'unique',
        #         'numeric_fields_names': ', '.join(scenario.numeric_fields.keys()) if scenario.numeric_fields else '',
        #         'numeric_fields_ranges': str(scenario.numeric_fields) if scenario.numeric_fields else '',
        #         # Add empty fields for failed scenarios to match CSV structure
        #         'search_memory_kb': 0,
        #         'reclaimable_memory_kb': 0,
        #         'insertion_time': 0,
        #         'keys_per_second': 0,
        #         'total_memory_kb': 0,
        #         'tag_memory_accuracy': 0,
        #         'total_memory_accuracy': 0,
        #         'index_state': 'failed',
        #         'phase_time': 0,
        #     }
        #     assert False, f"Scenario {scenario.name} failed: {e}"
            
        # finally:
        #     # Clean up resources
        #     if client_pool is not None:
        #         client_pool.close_all()
            # Only stop monitor if we created it
        if should_stop_monitor:
            monitor.stop()
        return result
    
    def execute_workload_stages(self, client, scenario: BenchmarkScenario, index_name: str, 
                               generator: HashKeyGenerator, monitor: ProgressMonitor, 
                               initial_keys: int) -> Dict[str, Any]:
        """Execute workload stages with multi-threaded operations"""
        if not scenario.workload_stages:
            return {}
        
        monitor.log("")
        monitor.log("🔄 WORKLOAD EXECUTION PHASE")
        monitor.log("─" * 50)
        monitor.log(f"📊 Stages: {len(scenario.workload_stages)}")
        
        # Track all keys for operations
        all_keys = self.get_all_keys(client, prefix="key:")
        current_key_count = len(all_keys)
        deleted_keys = set()
        stage_results = []
        
        for stage_idx, stage in enumerate(scenario.workload_stages):
            monitor.log("")
            monitor.log(f"▶️  STAGE {stage_idx + 1}: {stage.name}")
            monitor.log(f"   Operations: {', '.join(str(op) for op in stage.operations)}")
            
            stage_start_time = time.time()
            ft_info = client.execute_command("FT.INFO", index_name)
            info_dict = parse_ft_info(ft_info)
            
            stage_metrics = {
                'stage_name': stage.name,
                'operations': [str(op) for op in stage.operations],
                'start_key_count': current_key_count,
                'start_memory': get_memory_info_summary(client),
                'start_search_memory': client.execute_command("info", "modules")['search_used_memory_bytes'] // 1024,
                'start_info_dict': info_dict
            }
            
            # Execute operations in parallel
            operation_results = self.execute_parallel_operations(
                client, stage, all_keys, deleted_keys, current_key_count, 
                initial_keys, index_name, generator, monitor, scenario.vector_dim
            )
            
            # Update state after stage
            all_keys = self.get_all_keys(client, prefix="key:")
            current_key_count = len(all_keys)
            ft_info = client.execute_command("FT.INFO", index_name)
            info_dict = parse_ft_info(ft_info)
            stage_end_time = time.time()
            stage_metrics.update({
                'end_key_count': current_key_count,
                'end_memory': get_memory_info_summary(client),
                'end_search_memory': client.execute_command("info", "modules")['search_used_memory_bytes'] // 1024,
                'end_info_dict': info_dict,
                'duration_seconds': stage_end_time - stage_start_time,
                'operation_results': operation_results
            })
            
            # Log stage summary
            monitor.log(f"   ✅ Stage complete:")
            monitor.log(f"      Keys: {stage_metrics['start_key_count']:,} → {stage_metrics['end_key_count']:,}")
            monitor.log(f"      Memory: {stage_metrics['start_memory']['used_memory'] // (1024**2):,} MB → "
                       f"{stage_metrics['end_memory']['used_memory'] // (1024**2):,} MB")
            monitor.log(f"      Duration: {stage_metrics['duration_seconds']:.1f}s")
            monitor.log(f"      Search Memory: {stage_metrics['start_search_memory']:,} KB → "
                       f"{stage_metrics['end_search_memory']:,} KB")
            # log diff in FT.INFO
            for key, value in stage_metrics['start_info_dict'].items():
                if key in stage_metrics['end_info_dict'] and stage_metrics['end_info_dict'][key] != value:
                    # if value is array or dict print only the diff
                    if isinstance(value, dict):
                        for k,v in value.items():
                            if k in stage_metrics['end_info_dict'][key] and stage_metrics['end_info_dict'][key][k] != v:
                                monitor.log(f"      Index Info: {key}.{k}: {v} → {stage_metrics['end_info_dict'][key][k]}")                    
                    else:
                        monitor.log(f"      Index Info: {key}: {value} → {stage_metrics['end_info_dict'][key]}")
                    
                # else:
                #     # If no change, just log the original value
                #     value = stage_metrics['start_info_dict'][key]

                #     monitor.log(f"      Index Info: {key}: {value} → {stage_metrics['end_info_dict'][key]}")
            monitor.log(f"      Operations: {len(stage.operations)} executed")
            stage_results.append(stage_metrics)
        
        return {
            'stages': stage_results,
            'total_stages': len(scenario.workload_stages),
            'final_key_count': current_key_count,
            'deleted_keys_count': len(deleted_keys)
        }
    
    def execute_parallel_operations(self, client, stage: WorkloadStage, all_keys: List[str], 
                                   deleted_keys: Set[str], current_key_count: int, 
                                   initial_keys: int, index_name: str, 
                                   generator: HashKeyGenerator, monitor: ProgressMonitor,
                                   vector_dim: int = None) -> List[Dict]:
        """Execute multiple operations in parallel within a stage"""
        operation_results = []
        threads = []
        
        for operation in stage.operations:
            if operation.type == WorkloadType.DELETE:
                thread = threading.Thread(
                    target=self._execute_delete_operation,
                    args=(client, operation, all_keys, deleted_keys, 
                          current_key_count, monitor, operation_results)
                )
            elif operation.type == WorkloadType.QUERY:
                thread = threading.Thread(
                    target=self._execute_query_operation,
                    args=(client, operation, index_name, stage.duration_seconds,
                          monitor, operation_results, vector_dim)
                )
            elif operation.type == WorkloadType.INSERT:
                thread = threading.Thread(
                    target=self._execute_insert_operation,
                    args=(client, operation, generator, deleted_keys,
                          current_key_count, initial_keys, monitor, operation_results)
                )
            elif operation.type == WorkloadType.OVERWRITE:
                thread = threading.Thread(
                    target=self._execute_overwrite_operation,
                    args=(client, operation, all_keys, generator,
                          stage.duration_seconds, monitor, operation_results)
                )
            else:
                continue
            
            thread.start()
            threads.append(thread)
        
        # Wait for all operations to complete
        for thread in threads:
            thread.join()
        
        return operation_results
    
    def _execute_delete_operation(self, client, operation: WorkloadOperation, 
                                 all_keys: List[str], deleted_keys: Set[str],
                                 current_key_count: int, monitor: ProgressMonitor,
                                 results: List[Dict]):
        """Execute delete operation with multiple threads"""
        target_count = int(current_key_count * (1 - operation.target_value))
        keys_to_delete = current_key_count - target_count
        
        if keys_to_delete <= 0:
            return
        
        # Select random keys to delete (excluding already deleted)
        available_keys = [k for k in all_keys if k not in deleted_keys]
        if len(available_keys) < keys_to_delete:
            keys_to_delete = len(available_keys)
        
        delete_list = random.sample(available_keys, keys_to_delete)
        
        monitor.log(f"   🗑️  Deleting {keys_to_delete:,} keys to reach {operation.target_value*100:.0f}% fill")
        
        start_time = time.time()
        batch_size = 100
        deleted_count = 0
        
        # Delete in batches
        for i in range(0, len(delete_list), batch_size):
            batch = delete_list[i:i + batch_size]
            pipe = client.pipeline(transaction=False)
            for key in batch:
                pipe.delete(key)
            pipe.execute()
            deleted_keys.update(batch)
            deleted_count += len(batch)
            
            if deleted_count % 1000 == 0:
                monitor.update_status({
                    "Delete Progress": f"{deleted_count:,}/{keys_to_delete:,}"
                })
        
        duration = time.time() - start_time
        results.append({
            'operation': 'delete',
            'target_percentage': operation.target_value,
            'keys_deleted': deleted_count,
            'duration_seconds': duration,
            'keys_per_second': deleted_count / duration if duration > 0 else 0
        })
    
    def _execute_query_operation(self, client, operation: WorkloadOperation,
                                index_name: str, stage_duration: int,
                                monitor: ProgressMonitor, results: List[Dict],
                                vector_dim: int = None):
        """Execute query operation for specified duration"""
        duration = operation.duration_seconds or stage_duration or 60  # Default 60s
        
        monitor.log(f"   🔍 Running queries for {duration}s")
        
        start_time = time.time()
        query_count = 0
        total_results = 0
        
        # Use provided vector dimension or default
        if vector_dim is None:
            vector_dim = 8  # Default fallback
            monitor.log(f"   ⚠️  No vector dimension provided, using default: {vector_dim}")
        else:
            monitor.log(f"   📐 Using vector dimension: {vector_dim}")
        
        while (time.time() - start_time) < duration:
            # Generate random vector for KNN query
            query_vector = np.random.rand(vector_dim).astype(np.float32).tobytes()
            
            # Execute KNN query
            try:
                result = client.execute_command(
                    "FT.SEARCH", index_name,
                    "*=>[KNN 10 @vector $vec]",
                    "PARAMS", "2", "vec", query_vector,
                    "LIMIT", "0", "10",
                    "RETURN", "1", "__vector_score"
                )
                query_count += 1
                total_results += (result[0] if isinstance(result, list) else 0)
                
                if query_count % 100 == 0:
                    elapsed = time.time() - start_time
                    qps = query_count / elapsed if elapsed > 0 else 0
                    monitor.update_status({
                        "Query Progress": f"{query_count:,} queries, {qps:.0f} QPS"
                    })
            except Exception as e:
                if "vector blob size" in str(e):
                    monitor.log(f"   ❌ Vector dimension mismatch: {e}")
                    monitor.log(f"      Expected vector size: {vector_dim * 4} bytes ({vector_dim} floats)")
                    # Extract expected size from error message
                    import re
                    match = re.search(r"expected size \((\d+)\)", str(e))
                    if match:
                        expected_bytes = int(match.group(1))
                        expected_dim = expected_bytes // 4
                        monitor.log(f"      Index expects: {expected_bytes} bytes ({expected_dim} floats)")
                        monitor.log(f"      Adjusting vector dimension to {expected_dim}")
                        vector_dim = expected_dim
                        # Regenerate query vector with correct dimension
                        query_vector = np.random.rand(vector_dim).astype(np.float32).tobytes()
                        # Retry query with correct dimension
                        try:
                            result = client.execute_command(
                                "FT.SEARCH", index_name,
                                "*=>[KNN 10 @vector $vec]",
                                "PARAMS", "2", "vec", query_vector,
                                "LIMIT", "0", "10",
                                "RETURN", "1", "__vector_score"
                            )
                            query_count += 1
                            total_results += (result[0] if isinstance(result, list) else 0)
                            monitor.log(f"      ✅ Query succeeded with adjusted dimension")
                        except Exception as retry_e:
                            monitor.log(f"      ❌ Retry failed: {retry_e}")
                else:
                    monitor.log(f"   ⚠️  Query error: {e}")
                assert False, f"Query operation failed: {e}"
        
        final_duration = time.time() - start_time
        results.append({
            'operation': 'query',
            'duration_seconds': final_duration,
            'query_count': query_count,
            'queries_per_second': query_count / final_duration if final_duration > 0 else 0,
            'avg_results_per_query': total_results / query_count if query_count > 0 else 0
        })
    
    def _execute_insert_operation(self, client, operation: WorkloadOperation,
                                 generator: HashKeyGenerator, deleted_keys: Set[str],
                                 current_key_count: int, initial_keys: int,
                                 monitor: ProgressMonitor, results: List[Dict]):
        """Execute insert operation to reach target fill percentage"""
        target_count = int(initial_keys * operation.target_value)
        keys_to_insert = target_count - current_key_count
        
        if keys_to_insert <= 0:
            return
        
        monitor.log(f"   ➕ Inserting {keys_to_insert:,} keys to reach {operation.target_value*100:.0f}% fill")
        
        start_time = time.time()
        inserted_count = 0
        batch_size = 100
        
        # Generate and insert new keys
        key_offset = initial_keys  # Start from where initial insertion ended
        
        for batch_start in range(0, keys_to_insert, batch_size):
            batch_end = min(batch_start + batch_size, keys_to_insert)
            batch_count = batch_end - batch_start
            
            pipe = client.pipeline(transaction=False)
            for i in range(batch_count):
                key_num = key_offset + inserted_count + i
                key = f"key:{key_num:08d}"
                
                # Generate fields similar to original data
                fields = generator._generate_fields(key_num)
                pipe.hset(key, mapping=fields)
            
            pipe.execute()
            inserted_count += batch_count
            
            # Remove from deleted set if we're reusing keys
            for i in range(batch_count):
                key_num = key_offset + inserted_count - batch_count + i
                key = f"key:{key_num:08d}"
                deleted_keys.discard(key)
            
            if inserted_count % 1000 == 0:
                monitor.update_status({
                    "Insert Progress": f"{inserted_count:,}/{keys_to_insert:,}"
                })
        
        duration = time.time() - start_time
        results.append({
            'operation': 'insert',
            'target_percentage': operation.target_value,
            'keys_inserted': inserted_count,
            'duration_seconds': duration,
            'keys_per_second': inserted_count / duration if duration > 0 else 0
        })
    
    def _execute_overwrite_operation(self, client, operation: WorkloadOperation,
                                    all_keys: List[str], generator: HashKeyGenerator,
                                    stage_duration: int, monitor: ProgressMonitor,
                                    results: List[Dict]):
        """Execute overwrite operation on portion of keyspace"""
        duration = stage_duration or 60  # Default 60s
        overwrite_percentage = operation.target_value or 0.1  # Default 10%
        
        keys_to_overwrite = int(len(all_keys) * overwrite_percentage)
        monitor.log(f"   🔄 Overwriting {overwrite_percentage*100:.0f}% of keyspace "
                   f"({keys_to_overwrite:,} keys) for {duration}s")
        
        start_time = time.time()
        overwrites_done = 0
        
        while (time.time() - start_time) < duration:
            # Select random keys to overwrite
            batch_size = min(100, keys_to_overwrite)
            overwrite_batch = random.sample(all_keys, batch_size)
            
            pipe = client.pipeline(transaction=False)
            for key in overwrite_batch:
                # Generate new fields
                # Handle both bytes and string keys
                if isinstance(key, bytes):
                    key_str = key.decode('utf-8')
                else:
                    key_str = key
                key_num = int(key_str.split(':')[1])
                fields = generator._generate_fields(key_num)
                pipe.hset(key, mapping=fields)
            
            pipe.execute()
            overwrites_done += len(overwrite_batch)
            
            if overwrites_done % 1000 == 0:
                elapsed = time.time() - start_time
                ops = overwrites_done / elapsed if elapsed > 0 else 0
                monitor.update_status({
                    "Overwrite Progress": f"{overwrites_done:,} overwrites, {ops:.0f} OPS"
                })
        
        final_duration = time.time() - start_time
        results.append({
            'operation': 'overwrite',
            'target_percentage': overwrite_percentage,
            'keys_overwritten': overwrites_done,
            'duration_seconds': final_duration,
            'overwrites_per_second': overwrites_done / final_duration if final_duration > 0 else 0
        })
    
    def get_all_keys(self, client, prefix: str = "key:") -> List[str]:
        """Get all keys with given prefix"""
        cursor = 0
        all_keys = []
        
        while True:
            cursor, keys = client.scan(cursor, match=f"{prefix}*", count=1000)
            all_keys.extend(keys)
            if cursor == 0:
                break
        
        return all_keys
    
    def test_search_memory_usage(self):
        """Benchmark search memory usage"""
        # Set up file logging
        log_file = self.setup_file_logging("search_memory")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f"test_search_memory_usage_{timestamp}.csv"
        # Create a simple monitor for overall progress logging
        monitor = ProgressMonitor(self.server, csv_filename, "search_memory_main")
        monitor.start()
        monitor.log("=== SEARCH MEMORY BENCHMARK ===")

        # Log system memory information
        if PSUTIL_AVAILABLE:
            memory = psutil.virtual_memory()
            monitor.log(f"System Memory Status:")
            monitor.log(f"  - Total: {memory.total / (1024**3):.1f} GB")
            monitor.log(f"  - Available: {memory.available / (1024**3):.1f} GB")
            monitor.log(f"  - Used: {memory.percent:.1f}%")
            monitor.log(f"  - Memory limit for tests: {memory.available * 0.5 / (1024**3):.1f} GB (50% of available)\n")
        else:
            available_bytes = self.get_available_memory_bytes()
            monitor.log(f"System Memory Status (limited info - psutil not available):")
            monitor.log(f"  - Available: {available_bytes / (1024**3):.1f} GB")
            monitor.log(f"  - Memory limit for tests: {available_bytes * 0.5 / (1024**3):.1f} GB (50% of available)\n")
    
        
        scenarios = [
            BenchmarkScenario(
                name="super_quick_10K",
                total_keys=10000,
                tags_config=TagsConfig(
                    num_keys=10000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),  # Exactly 1 tag per key
                    tag_length=LengthConfig(avg=1000, min=1000, max=1000),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description="Test with delete and query workload",
                vector_dim=1500,
                vector_algorithm=VectorAlgorithm.HNSW,
                vector_metric=VectorMetric.COSINE,
            ),
            BenchmarkScenario(
                name="super_quick_100K",
                total_keys=100000,
                tags_config=TagsConfig(
                    num_keys=100000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),  # Exactly 1 tag per key
                    tag_length=LengthConfig(avg=1000, min=1000, max=1000),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description="Test with delete and query workload",
                vector_dim=1500,
                vector_algorithm=VectorAlgorithm.HNSW,
                vector_metric=VectorMetric.COSINE,
            ),
            # BenchmarkScenario(
            #     name="Delete_Query_Test",
            #     total_keys=1000000,
            #     tags_config=TagsConfig(
            #         num_keys=1000000,
            #         tags_per_key=TagDistribution(avg=1, min=1, max=1),  # Exactly 1 tag per key
            #         tag_length=LengthConfig(avg=1000, min=1000, max=1000),
            #         sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
            #     ),
            #     description="Test with delete and query workload",
            #     vector_dim=1,
            #     vector_algorithm=VectorAlgorithm.HNSW,
            #     vector_metric=VectorMetric.COSINE,
            #     workload_string="del:50%"
            # ),
            # BenchmarkScenario(
            #     name="Delete_Query_Test",
            #     total_keys=10000,
            #     tags_config=TagsConfig(
            #         num_keys=10000,
            #         tags_per_key=TagDistribution(avg=1, min=1, max=1),  # Exactly 1 tag per key
            #         tag_length=LengthConfig(avg=10, min=10, max=10),
            #         sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
            #     ),
            #     description="Test with delete and query workload",
            #     vector_dim=1500,
            #     vector_algorithm=VectorAlgorithm.HNSW,
            #     vector_metric=VectorMetric.COSINE,
            #     workload_string="del:50%,insert:80%,del:20%,insert:100%,overwrite:100%:5min,del:20%,insert:100%,overwrite:100%:1min"
            # ),
            BenchmarkScenario(
                name="Quick_SingleUniqueTag100Keys10K",
                total_keys=10000,
                tags_config=TagsConfig(
                    num_keys=10000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),  # Exactly 1 tag per key
                    tag_length=LengthConfig(avg=100, min=100, max=100),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description="1 unique tag per key, tag length 100 bytes, 10K keys + 8-dim vectors + 2 numeric fields",
                vector_dim=8,
                vector_algorithm=VectorAlgorithm.FLAT,
                vector_metric=VectorMetric.COSINE,
                include_numeric=True
            ),
            BenchmarkScenario(
                name="Quick_SingleUniqueTag100Keys100K",
                total_keys=100000,
                tags_config=TagsConfig(
                    num_keys=100000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),  # Exactly 1 tag per key
                    tag_length=LengthConfig(avg=100, min=100, max=100),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description="1 unique tag per key, tag length 100 bytes, 100K keys + 16-dim vectors + 2 numeric fields",
                vector_dim=16,
                vector_algorithm=VectorAlgorithm.FLAT,
                vector_metric=VectorMetric.COSINE,
                include_numeric=True
            ),
            BenchmarkScenario(
                name="Quick_SingleUniqueTag1000Keys10K",
                total_keys=10000,
                tags_config=TagsConfig(
                    num_keys=10000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),  # Exactly 1 tag per key
                    tag_length=LengthConfig(avg=1000, min=1000, max=1000),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description="1 unique tag per key, tag length 1000 bytes, 10K keys + 32-dim vectors + 2 numeric fields",
                vector_dim=32,
                vector_algorithm=VectorAlgorithm.FLAT,
                vector_metric=VectorMetric.L2,
                include_numeric=True
            ),
            BenchmarkScenario(
                name="Quick_SingleUniqueTag1000Keys100K",
                total_keys=100000,
                tags_config=TagsConfig(
                    num_keys=100000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),  # Exactly 1 tag per key
                    tag_length=LengthConfig(avg=1000, min=1000, max=1000),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description="1 unique tag per key, tag length 1000 bytes, 100K keys + 64-dim vectors + 2 numeric fields",
                vector_dim=64,
                vector_algorithm=VectorAlgorithm.HNSW,
                vector_metric=VectorMetric.COSINE,
                include_numeric=True
            ),
            
            # Tag Sharing Scenarios - Test non-linear effects of keys_per_tag
            BenchmarkScenario(
                name="Quick_TagShare_Low",
                total_keys=50000,
                tags_config=TagsConfig(
                    num_keys=50000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),
                    tag_length=LengthConfig(avg=100, min=100, max=100),
                    sharing=TagSharingConfig(
                        mode=TagSharingMode.SHARED_POOL,
                        pool_size=5000  # 10 keys per tag
                    )
                ),
                description="Tag sharing: 10 keys per tag, 50K keys, 5K unique tags + 128-dim vectors + 2 numeric fields",
                vector_dim=128,
                vector_algorithm=VectorAlgorithm.FLAT,
                vector_metric=VectorMetric.IP,
                include_numeric=True
            ),
            BenchmarkScenario(
                name="Quick_TagShare_Medium", 
                total_keys=50000,
                tags_config=TagsConfig(
                    num_keys=50000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),
                    tag_length=LengthConfig(avg=100, min=100, max=100),
                    sharing=TagSharingConfig(
                        mode=TagSharingMode.SHARED_POOL,
                        pool_size=500  # 100 keys per tag
                    )
                ),
                description="Tag sharing: 100 keys per tag, 50K keys, 500 unique tags + 256-dim vectors + 2 numeric fields",
                vector_dim=256,
                vector_algorithm=VectorAlgorithm.HNSW,
                vector_metric=VectorMetric.COSINE,
                include_numeric=True
            ),
            BenchmarkScenario(
                name="Quick_TagShare_High",
                total_keys=50000,
                tags_config=TagsConfig(
                    num_keys=50000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),
                    tag_length=LengthConfig(avg=100, min=100, max=100),
                    sharing=TagSharingConfig(
                        mode=TagSharingMode.SHARED_POOL,
                        pool_size=50  # 1000 keys per tag
                    )
                ),
                description="Tag sharing: 1000 keys per tag, 50K keys, 50 unique tags + 512-dim vectors + 2 numeric fields",
                vector_dim=512,
                vector_algorithm=VectorAlgorithm.HNSW,
                vector_metric=VectorMetric.L2,
                include_numeric=True,
                numeric_fields={"score": (0.0, 100.0), "timestamp": (1000000000.0, 2000000000.0), "priority": (1.0, 10.0)}
            ),
            
            # Prefix Sharing Scenarios - Test Patricia tree efficiency
            BenchmarkScenario(
                name="Quick_PrefixShare_25pct",
                total_keys=30000,
                tags_config=TagsConfig(
                    num_keys=30000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),
                    tag_length=LengthConfig(avg=64, min=64, max=64),
                    tag_prefix=PrefixConfig(
                        enabled=True,
                        min_shared=16,  # 25% of 64-char tags = 16 chars
                        max_shared=16,
                        share_probability=0.8,  # High chance of sharing prefix
                        prefix_pool_size=4  # 4 different prefixes
                    ),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description="Prefix sharing: 25% common prefix, 30K keys, unique tags + 768-dim vectors + 2 numeric fields",
                vector_dim=768,
                vector_algorithm=VectorAlgorithm.HNSW,
                vector_metric=VectorMetric.COSINE,
                include_numeric=True
            ),
            BenchmarkScenario(
                name="Quick_PrefixShare_75pct",
                total_keys=1000000,
                tags_config=TagsConfig(
                    num_keys=1000000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),
                    tag_length=LengthConfig(avg=64, min=64, max=64),
                    tag_prefix=PrefixConfig(
                        enabled=True,
                        min_shared=48,  # 75% of 64-char tags = 48 chars
                        max_shared=48,
                        share_probability=0.9,  # Very high chance of sharing prefix
                        prefix_pool_size=4  # 4 different prefixes
                    ),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description="Prefix sharing: 75% common prefix, 30K keys, unique tags + 128-dim vectors + 3 numeric fields",
                vector_dim=128,
                vector_algorithm=VectorAlgorithm.HNSW,
                vector_metric=VectorMetric.IP,
                include_numeric=True,
                numeric_fields={"score": (0.0, 100.0), "timestamp": (1000000000.0, 2000000000.0), "rating": (1.0, 5.0)}
            ),
            
            # Mixed Scenario - Combined effects
            BenchmarkScenario(
                name="HNSW_wNumeric_HighShare_HighPrefix",
                total_keys=1000000,
                tags_config=TagsConfig(
                    num_keys=1000000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),
                    tag_length=LengthConfig(avg=64, min=64, max=64),
                    tag_prefix=PrefixConfig(
                        enabled=True,
                        min_shared=48,  # 75% prefix sharing
                        max_shared=48,
                        share_probability=0.95,  # Very high prefix reuse
                        prefix_pool_size=2  # Only 2 different prefixes
                    ),
                    sharing=TagSharingConfig(
                        mode=TagSharingMode.SHARED_POOL,
                        pool_size=100  # High sharing: 400 keys per tag
                    )
                ),
                description="Mixed: High tag sharing + 75% prefix overlap, 40K keys + 1500-dim vectors + 4 numeric fields",
                vector_dim=1500,
                vector_algorithm=VectorAlgorithm.HNSW,
                vector_metric=VectorMetric.COSINE,
                include_numeric=True,
                numeric_fields={
                    "score": (0.0, 100.0),
                    "timestamp": (1000000000.0, 2000000000.0),
                    "priority": (1.0, 10.0),
                    "confidence": (0.0, 1.0)
                }
            ),
            # # Mixed Scenario - Combined effects
            BenchmarkScenario(
                name="HNSW_wNumeric_TagNotShared_noPrefixShare",
                total_keys=1000000,
                tags_config=TagsConfig(
                    num_keys=1000000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),  # Exactly 1 tag per key
                    tag_length=LengthConfig(avg=500, min=2, max=1000),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description="Mixed: No Tag sharing between keys and no prefix sharing, 100K keys + 1500-dim vectors + 4 numeric fields",
                vector_dim=1500,
                vector_algorithm=VectorAlgorithm.HNSW,
                vector_metric=VectorMetric.COSINE,
                include_numeric=True,
                numeric_fields={
                    "score": (0.0, 100.0),
                    "timestamp": (1000000000.0, 2000000000.0),
                    "priority": (1.0, 10.0),
                    "confidence": (0.0, 1.0)
                }
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
            ),
            BenchmarkScenario(
                name="10M_Quick_Mixed_HighShare_HighPrefix",
                total_keys=10000000,
                tags_config=TagsConfig(
                    num_keys=10000000,
                    tags_per_key=TagDistribution(avg=5, min=1, max=1000),
                    tag_length=LengthConfig(avg=64, min=1, max=100),
                    tag_prefix=PrefixConfig(
                        enabled=True,
                        min_shared=1,  # 75% prefix sharing
                        max_shared=900,
                        share_probability=0.95,  # Very high prefix reuse
                        prefix_pool_size=100  # 100 different prefixes
                    ),
                    sharing=TagSharingConfig(
                        mode=TagSharingMode.SHARED_POOL,
                        pool_size=100  # High sharing: 400 keys per tag
                    )
                ),
                description="Mixed: High tag sharing + 75% prefix overlap, 40K keys, 100 unique tags"
            ),
            BenchmarkScenario(
                name="10M_Unique",
                total_keys=1000000,
                tags_config=TagsConfig(
                    num_keys=1000000,
                    tags_per_key=TagDistribution(avg=2, min=1, max=5000),
                    tag_length=LengthConfig(avg=64, min=2, max=3000),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description="Unique tags 10M keys (2 tags/key, 64 bytes/tag) average, no sharing"
            )
        ]
        monitor_csv_filename = f"search_memory_monitoring_{timestamp}.csv"
        results = []
        for i, scenario in enumerate(scenarios):
            monitor.log(f"\n{'='*60}")
            monitor.log(f"Running Scenario {i+1}/{len(scenarios)}: {scenario.name}")
            monitor.log(f"{'='*60}\n")
            try:
                # Pass None for monitor so each scenario creates its own with proper config
                result = self.run_benchmark_scenario(monitor_csv_filename, scenario, monitor=None)
                results.append(result)
                # Append to CSV incrementally
                self.append_to_csv(csv_filename, result, monitor, write_header=(i == 0))
                
            except Exception as e:
                monitor.error(f"  ❌ Scenario failed: {e}")
                failed_result = {
                    'scenario_name': scenario.name,
                    'description': scenario.description,
                    'total_keys': scenario.total_keys,
                    'skipped': True,
                    'reason': f"Failed: {str(e)}"
                }
                results.append(failed_result)
                self.append_to_csv(csv_filename, failed_result, monitor, write_header=(i == 0))
                assert False, f"Scenario {scenario.name} failed: {e}"
        
        # Quick summary
        monitor.log("")
        monitor.log("┏" + "━" * 78 + "┓")
        monitor.log("┃" + " " * 28 + "⚡ SEARCH MEMORY BENCHMARK SUMMARY" + " " * 24 + "┃")
        monitor.log("┗" + "━" * 78 + "┛")
        monitor.log("")
        for r in results:
            if r.get('skipped'):
                monitor.log(f"{r['scenario_name']:<20}: SKIPPED - {r.get('reason', 'Unknown')}")
            else:
                monitor.log(f"{r['scenario_name']:<20}: {r['tag_index_memory_kb']:>8,} KB tag index")
        
        monitor.log(f"Results saved to {csv_filename}")
        monitor.log(f"Log file: {log_file}")
        
        # Print summary table  
        monitor.log("")
        monitor.log("┏" + "━" * 180 + "┓")
        monitor.log("┃" + " " * 76 + "📊 SEARCH MEMORY BENCHMARK RESULTS" + " " * 77 + "┃")
        monitor.log("┗" + "━" * 180 + "┛")
        monitor.log("")
        
        monitor.log(f"{'Scenario':<35} {'Keys':>8} {'WoIndexKB':>10} {'wIndexKB':>10} {'IndexKB':>10} {'TagIdxKB':>10} {'SearchKB':>10} {'SReclaimKB':>10} {'VecKB':>8} {'VecDims':>8} {'NumKB':>8} {'NumFlds':>8} {'InsTime':>8} {'K/s':>8} {'TagAcc':>8} {'TotAcc':>8} {'Mode':<15}")
        monitor.log("─" * 180)
        
        for r in results:
            if r.get('skipped'):
                monitor.log(f"{r['scenario_name']:<35} "
                            f"{r['total_keys']:>8,} "
                            f"{'SKIPPED':>10} "
                            f"{'':>10} "
                            f"{'':>10} "
                            f"{'':>10} "
                            f"{'':>10} "
                            f"{'':>10} "
                            f"{'':>8} "
                            f"{'':>8} "
                            f"{'':>8} "
                            f"{'':>8} "
                            f"{'':>8} "
                            f"{'':>8} "
                            f"{'':>8} "
                            f"{'':>8} "
                            f"Memory limit exceeded")
            else:
                monitor.log(f"{r['scenario_name']:<35} "
                            f"{r['total_keys']:>8,} "
                            f"{r['data_memory_kb']:>10,} "
                            f"{r['total_memory_kb']:>10,} "
                            f"{r['index_overhead_kb']:>10,} "
                            f"{r['tag_index_memory_kb']:>10,} "
                            f"{r['search_used_memory_kb']:>10,} "
                            f"{r['search_index_reclaimable_memory_kb']:>10,} "
                            f"{r.get('vector_memory_kb', 0):>8,} "
                            f"{r.get('vector_dims', 0):>8} "
                            f"{r.get('numeric_memory_kb', 0):>8,} "
                            f"{r.get('numeric_fields_count', 0):>8} "
                            f"{r.get('insertion_time', 0):>8.1f} "
                            f"{r.get('keys_per_second', 0):>8.0f} "
                            f"{r.get('tag_memory_accuracy', 0):>8.2f} "
                            f"{r.get('total_memory_accuracy', 0):>8.2f} "
                            f"{r['tags_config']:<15}")
        
        # # Analyze results by category
        # monitor.log("")
        # monitor.log("┏" + "━" * 78 + "┓")
        # monitor.log("┃" + " " * 32 + "🔍 KEY FINDINGS" + " " * 31 + "┃")
        # monitor.log("┗" + "━" * 78 + "┛")
        # monitor.log("")
        # Stop the main monitor
        monitor.stop()
    
    def test_tag_memory_patterns(self):
        """Test tag memory patterns by systematically varying one dimension at a time"""
        # Set up file logging
        log_file = self.setup_file_logging("tag_memory_patterns")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Two CSV files as expected:
        # 1. Monitor CSV: consolidated time-based data from all scenarios
        monitor_csv_filename = f"tag_memory_patterns_monitor_{timestamp}.csv"
        # 2. Summary CSV: one line per scenario with final results  
        summary_csv_filename = f"tag_memory_patterns_summary_{timestamp}.csv"
        
        # Create main monitor for logging only (no CSV monitoring)
        log_filename = f"tag_memory_patterns_log_{timestamp}.log"
        monitor = ProgressMonitor(self.server, log_filename, "tag_memory_patterns")
        monitor.start()
        monitor.log("=== TAG MEMORY PATTERNS BENCHMARK ===")
        monitor.log("Testing systematic variations in tag patterns to understand memory impact")
        monitor.log("Base config: 8-dim vectors, FLAT algorithm, no numeric fields")
        monitor.log(f"Monitor CSV (time-series data): {monitor_csv_filename}")
        monitor.log(f"Summary CSV (per-scenario results): {summary_csv_filename}\n")

        # Define comprehensive test scenarios
        scenarios = []
        
        # # 1. Unique Tags Tests - scaling keys, tag length, and tags per key
        base_keys = [1000, 50000, 100000, 500000]
        tag_lengths = [10, 50, 100, 500, 1000]
        tags_per_key_values = [1, 2, 50, 100]
        
        for keys in base_keys:
            for tag_len in tag_lengths:
                scenarios.append(BenchmarkScenario(
                    name=f"Unique_Keys{keys}_TagLen{tag_len}",
                    total_keys=keys,
                    tags_config=TagsConfig(
                        num_keys=keys,
                        tags_per_key=TagDistribution(avg=1, min=1, max=1),
                        tag_length=LengthConfig(avg=tag_len, min=tag_len, max=tag_len),
                        sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                    ),
                    description=f"HNSW vectors dim=1500 + numeric + Unique tags: {keys} keys, {tag_len}B tags",
                vector_dim=1500,
                vector_algorithm=VectorAlgorithm.HNSW,
                vector_metric=VectorMetric.COSINE,
                include_numeric=True,
                numeric_fields={
                    "score": (0.0, 100.0),
                    "timestamp": (1000000000.0, 2000000000.0),
                    "priority": (1.0, 10.0),
                    "confidence": (0.0, 1.0)
                }
                ))
        # base_keys = [1000, 50000, 100000, 500000]
        # tag_lengths = [10, 50, 100, 500, 1000]
        # tags_per_key_values = [1, 2, 50, 100]
        
        # for keys in base_keys:
        #     for tag_len in tag_lengths:
        #         scenarios.append(BenchmarkScenario(
        #             name=f"Unique_Keys{keys}_TagLen{tag_len}",
        #             total_keys=keys,
        #             tags_config=TagsConfig(
        #                 num_keys=keys,
        #                 tags_per_key=TagDistribution(avg=1, min=1, max=1),
        #                 tag_length=LengthConfig(avg=tag_len, min=tag_len, max=tag_len),
        #                 sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
        #             ),
        #             description=f"Unique tags: {keys} keys, {tag_len}B tags",
        #             vector_dim=8,
        #             vector_algorithm=VectorAlgorithm.FLAT,
        #             vector_metric=VectorMetric.COSINE,
        #         ))
        
        # for keys in [1000000]:  # Fixed key count for tags per key variation
        #     for tpk in tags_per_key_values:
        #         scenarios.append(BenchmarkScenario(
        #             name=f"Unique_Keys{keys}_TagsPerKey{tpk}",
        #             total_keys=keys,
        #             tags_config=TagsConfig(
        #                 num_keys=keys,
        #                 tags_per_key=TagDistribution(avg=tpk, min=tpk, max=tpk),
        #                 tag_length=LengthConfig(avg=100, min=100, max=100),
        #                 sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
        #             ),
        #             description=f"Unique tags: {keys} keys, {tpk} tags/key",
        #             vector_dim=8,
        #             vector_algorithm=VectorAlgorithm.FLAT,
        #             vector_metric=VectorMetric.COSINE,
        #         ))
        
        # # 2. Tag Sharing Tests - varying sharing ratios and group sizes
        # sharing_ratios = [0.1, 0.25, 0.5, 0.75, 0.9]  # 10% to 90% sharing
        # group_sizes = [10, 500, 1000, 5000]
        
        # for ratio in sharing_ratios:
        #     scenarios.append(BenchmarkScenario(
        #         name=f"Sharing_Ratio{int(ratio*100)}pct",
        #         total_keys=1000000,
        #         tags_config=TagsConfig(
        #             num_keys=1000000,
        #             tags_per_key=TagDistribution(avg=2, min=2, max=2),
        #             tag_length=LengthConfig(avg=100, min=100, max=100),
        #             sharing=TagSharingConfig(
        #                 mode=TagSharingMode.SHARED_POOL,
        #                 pool_size=200,
        #                 reuse_probability=ratio
        #             )
        #         ),
        #         description=f"Tag sharing: {int(ratio*100)}% shared, 2 tags/key",
        #         vector_dim=8,
        #         vector_algorithm=VectorAlgorithm.FLAT,
        #         vector_metric=VectorMetric.COSINE,
        #     ))
        
        # for group_size in group_sizes:
        #     scenarios.append(BenchmarkScenario(
        #         name=f"GroupBased_Size{group_size}",
        #         total_keys=1000000,
        #         tags_config=TagsConfig(
        #             num_keys=1000000,
        #             tags_per_key=TagDistribution(avg=3, min=3, max=3),
        #             tag_length=LengthConfig(avg=100, min=100, max=100),
        #             sharing=TagSharingConfig(
        #                 mode=TagSharingMode.GROUP_BASED,
        #                 keys_per_group=group_size,
        #                 tags_per_group=10
        #             )
        #         ),
        #         description=f"Group-based sharing: groups of {group_size}",
        #         vector_dim=8,
        #         vector_algorithm=VectorAlgorithm.FLAT,
        #         vector_metric=VectorMetric.COSINE,
        #     ))
        
        # 3. Prefix Sharing Tests - varying prefix share ratios and pool sizes
        # prefix_ratios = [0.1, 0.5, 0.9]
        # prefix_pool_sizes = [100, 500]
        
        # for ratio in prefix_ratios:
        #     scenarios.append(BenchmarkScenario(
        #         name=f"PrefixShare_{int(ratio*100)}pct",
        #         total_keys=10000,
        #         tags_config=TagsConfig(
        #             num_keys=10000,
        #             tags_per_key=TagDistribution(avg=2, min=2, max=2),
        #             tag_length=LengthConfig(avg=200, min=200, max=200),
        #             sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE),
        #             tag_prefix=PrefixConfig(
        #                 enabled=True,
        #                 min_shared=50,
        #                 max_shared=50,
        #                 share_probability=ratio,
        #                 prefix_pool_size=20
        #             )
        #         ),
        #         description=f"Prefix sharing: {int(ratio*100)}% shared prefixes",
        #         vector_dim=8,
        #         vector_algorithm=VectorAlgorithm.FLAT,
        #         vector_metric=VectorMetric.COSINE,
        #     ))
        
        # for pool_size in prefix_pool_sizes:
        #     scenarios.append(BenchmarkScenario(
        #         name=f"PrefixPool_{pool_size}",
        #         total_keys=1000000,
        #         tags_config=TagsConfig(
        #             num_keys=10000,
        #             tags_per_key=TagDistribution(avg=2, min=2, max=2),
        #             tag_length=LengthConfig(avg=200, min=200, max=200),
        #             sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE),
        #             tag_prefix=PrefixConfig(
        #                 enabled=True,
        #                 min_shared=50,
        #                 max_shared=50,
        #                 share_probability=0.5,
        #                 prefix_pool_size=pool_size
        #             )
        #         ),
        #         description=f"Prefix pool: {pool_size} unique prefixes",
        #         vector_dim=8,
        #         vector_algorithm=VectorAlgorithm.FLAT,
        #         vector_metric=VectorMetric.COSINE,
        #     ))
        
        # # 4. Combined Pattern Tests - both prefix and tag sharing
        # combined_configs = [
        #     (0.25, 0.3),  # 25% tag sharing, 30% prefix sharing
        #     (0.5, 0.5),   # 50% tag sharing, 50% prefix sharing
        #     (0.75, 0.7),  # 75% tag sharing, 70% prefix sharing
        # ]
        
        # for tag_ratio, prefix_ratio in combined_configs:
        #     scenarios.append(BenchmarkScenario(
        #         name=f"Combined_Tag{int(tag_ratio*100)}_Prefix{int(prefix_ratio*100)}",
        #         total_keys=1000000,
        #         tags_config=TagsConfig(
        #             num_keys=1000000,
        #             tags_per_key=TagDistribution(avg=3, min=3, max=3),
        #             tag_length=LengthConfig(avg=150, min=150, max=150),
        #             sharing=TagSharingConfig(
        #                 mode=TagSharingMode.SHARED_POOL,
        #                 pool_size=150,
        #                 reuse_probability=tag_ratio
        #             ),
        #             tag_prefix=PrefixConfig(
        #                 enabled=True,
        #                 min_shared=40,
        #                 max_shared=140,
        #                 share_probability=prefix_ratio,
        #                 prefix_pool_size=20
        #             )
        #         ),
        #         description=f"Combined: {int(tag_ratio*100)}% tag + {int(prefix_ratio*100)}% prefix sharing",
        #         vector_dim=8,
        #         vector_algorithm=VectorAlgorithm.FLAT,
        #         vector_metric=VectorMetric.COSINE,
        #     ))
        
        # # 5. Extreme Cases - edge cases and stress tests
        extreme_scenarios = [
        #     BenchmarkScenario(
        #         name="Extreme_TinyTags",
        #         total_keys=500000,
        #         tags_config=TagsConfig(
        #             num_keys=500000,
        #             tags_per_key=TagDistribution(avg=1, min=1, max=1),
        #             tag_length=LengthConfig(avg=5, min=5, max=5),
        #             sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
        #         ),
        #         description="Extreme: 50K keys with 5-byte unique tags",
        #         vector_dim=8,
        #         vector_algorithm=VectorAlgorithm.FLAT,
        #         vector_metric=VectorMetric.COSINE,
        #     ),
        #     BenchmarkScenario(
        #         name="Extreme_LargeTags",
        #         total_keys=100000,
        #         tags_config=TagsConfig(
        #             num_keys=100000,
        #             tags_per_key=TagDistribution(avg=1, min=1, max=1),
        #             tag_length=LengthConfig(avg=5000, min=5000, max=5000),
        #             sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
        #         ),
        #         description="Extreme: 1K keys with 5KB unique tags",
        #         vector_dim=8,
        #         vector_algorithm=VectorAlgorithm.FLAT,
        #         vector_metric=VectorMetric.COSINE,
        #     ),
            BenchmarkScenario(
                name="Extreme_ManyTags",
                total_keys=5000000,
                tags_config=TagsConfig(
                    num_keys=5000000,
                    tags_per_key=TagDistribution(avg=20, min=20, max=20),
                    tag_length=LengthConfig(avg=50, min=50, max=50),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description="Extreme: 5K keys with 20 unique tags each",
                vector_dim=1500,
                vector_algorithm=VectorAlgorithm.HNSW,
                vector_metric=VectorMetric.COSINE,
                include_numeric=True,
                numeric_fields={
                    "score": (0.0, 100.0),
                    "timestamp": (1000000000.0, 2000000000.0),
                    "priority": (1.0, 10.0),
                    "confidence": (0.0, 1.0)
                }
            ),
            BenchmarkScenario(
                name="Extreme_PerfectOverlap",
                total_keys=1000000,
                tags_config=TagsConfig(
                    num_keys=1000000,
                    tags_per_key=TagDistribution(avg=5, min=5, max=5),
                    tag_length=LengthConfig(avg=100, min=100, max=100),
                    sharing=TagSharingConfig(mode=TagSharingMode.PERFECT_OVERLAP)
                ),
                description="Extreme: 100K keys all sharing same 5 tags",
                vector_dim=1500,
                vector_algorithm=VectorAlgorithm.HNSW,
                vector_metric=VectorMetric.COSINE,
                include_numeric=True,
                numeric_fields={
                    "score": (0.0, 100.0),
                    "timestamp": (1000000000.0, 2000000000.0),
                    "priority": (1.0, 10.0),
                    "confidence": (0.0, 1.0)
                }
            ),
        ]
        
        scenarios.extend(extreme_scenarios)
        
        monitor.log(f"Total scenarios to test: {len(scenarios)}")
        monitor.log("")

        # Create shared monitor for all scenarios' time-series data
        shared_monitor = ProgressMonitor(self.server, monitor_csv_filename, "tag_patterns_shared")
        shared_monitor.start()
        
        # Run all scenarios
        all_results = []
        for i, scenario in enumerate(scenarios):
            monitor.log(f"[{i+1}/{len(scenarios)}] Running scenario: {scenario.name}")
            monitor.log(f"  Description: {scenario.description}")
            
            # try:
            # Use shared monitor for time-series data collection
            result = self.run_benchmark_scenario(
                monitor_csv_filename=monitor_csv_filename,
                scenario=scenario,
                monitor=shared_monitor  # Use shared monitor, not None
            )
            
            if result:
                all_results.append(result)
                
                # Write to summary CSV (one line per scenario)
                self.append_to_csv(summary_csv_filename, result, monitor, write_header=(i == 0))
                
                monitor.log(f"  ✓ Completed successfully - Memory: {result.get('total_memory_kb', 0):.0f} KB")
            else:
                monitor.log(f"  ✗ Failed or skipped")
                    
            # except Exception as e:
            #     monitor.log(f"  ✗ Error: {str(e)}")
            #     assert False, f"Scenario {scenario.name} failed: {e}"
            #     continue
            
            monitor.log("")
        
        # Stop shared monitor
        shared_monitor.stop()
        
        # Generate summary report
        monitor.log("=" * 80)
        monitor.log("TAG MEMORY PATTERNS SUMMARY")
        monitor.log("=" * 80)
        monitor.log("")
        
        if all_results:
            # Sort results by memory efficiency
            all_results.sort(key=lambda x: x.get('total_memory_kb', float('inf')))
            
            monitor.log("Most Memory Efficient Configurations:")
            monitor.log("-" * 50)
            for i, result in enumerate(all_results[:10]):  # Top 10
                monitor.log(f"{i+1:2}. {result['scenario_name']:<35} "
                           f"Memory: {result.get('total_memory_kb', 0):>8.0f} KB "
                           f"Keys: {result.get('total_keys', 0):>6}")
            
            monitor.log("")
            monitor.log("Least Memory Efficient Configurations:")
            monitor.log("-" * 50)
            for i, result in enumerate(all_results[-10:]):  # Bottom 10
                monitor.log(f"{i+1:2}. {result['scenario_name']:<35} "
                           f"Memory: {result.get('total_memory_kb', 0):>8.0f} KB "
                           f"Keys: {result.get('total_keys', 0):>6}")
        
        monitor.log("")
        monitor.log(f"Total scenarios completed: {len(all_results)}/{len(scenarios)}")
        
        # Generate scenario schema analysis
        self.generate_scenario_schema_analysis(scenarios, timestamp)
        
        monitor.stop()
        
    def generate_scenario_schema_analysis(self, scenarios: List[BenchmarkScenario], timestamp: str):
        """Generate detailed schema analysis for all scenarios and export to CSV"""
        schema_csv_filename = f"tag_memory_patterns_schemas_{timestamp}.csv"
        
        # Define comprehensive schema fields
        schema_fields = [
            # Basic scenario info
            'scenario_name', 'description', 'total_keys',
            
            # Vector configuration
            'vector_dim', 'vector_algorithm', 'vector_metric', 'hnsw_m', 'hnsw_ef_construction',
            
            # Tag configuration basics
            'tags_per_key_avg', 'tags_per_key_min', 'tags_per_key_max', 'tags_per_key_distribution',
            'tag_length_avg', 'tag_length_min', 'tag_length_max', 'tag_length_distribution',
            
            # Tag sharing configuration
            'tag_sharing_mode', 'tag_sharing_pool_size', 'tag_sharing_reuse_probability',
            'tag_sharing_keys_per_group', 'tag_sharing_tags_per_group',
            
            # Prefix configuration
            'prefix_enabled', 'prefix_min_shared', 'prefix_max_shared', 
            'prefix_share_probability', 'prefix_pool_size',
            
            # String generation
            'tag_string_type', 'tag_charset', 'tag_batch_size',
            
            # Numeric fields
            'include_numeric', 'num_numeric_fields', 'numeric_field_names', 'numeric_field_ranges',
            
            # Workload configuration  
            'workload_stages_count', 'workload_string', 'workload_details',
            
            # Calculated estimates
            'estimated_unique_tags', 'estimated_total_tag_memory', 'estimated_keys_per_tag',
            'estimated_prefix_savings', 'estimated_sharing_factor',
            
            # Memory predictions
            'predicted_tag_memory_kb', 'predicted_vector_memory_kb', 
            'predicted_numeric_memory_kb', 'predicted_total_memory_kb'
        ]
        
        schema_data = []
        
        for scenario in scenarios:
            # Extract all configuration details
            schema_row = self._extract_scenario_schema(scenario)
            schema_data.append(schema_row)
        
        # Write to CSV
        with open(schema_csv_filename, 'w', newline='', encoding='utf-8') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=schema_fields, extrasaction='ignore')
            writer.writeheader()
            
            for row in schema_data:
                # Ensure all fields have default values
                complete_row = {field: row.get(field, '') for field in schema_fields}
                writer.writerow(complete_row)
        
        print(f"📊 Scenario schema analysis exported to: {schema_csv_filename}")
        print(f"   Total scenarios analyzed: {len(scenarios)}")
        
        # Optionally generate visual summaries for first few scenarios
        self.print_scenario_summaries(scenarios[:5])  # Show first 5 as examples
    
    def print_scenario_summaries(self, scenarios: List[BenchmarkScenario]):
        """Print detailed visual summaries for selected scenarios"""
        print("\n" + "="*80)
        print("📋 SCENARIO CONFIGURATION SUMMARIES")
        print("="*80)
        
        for i, scenario in enumerate(scenarios):
            self.print_single_scenario_summary(scenario, i+1)
            if i < len(scenarios) - 1:
                print("\n" + "-"*80 + "\n")
    
    def print_single_scenario_summary(self, scenario: BenchmarkScenario, scenario_num: int):
        """Print a detailed summary screen for a single scenario"""
        tags_config = scenario.tags_config
        estimates = self._calculate_scenario_estimates(scenario)
        
        print(f"📊 SCENARIO #{scenario_num}: {scenario.name}")
        print(f"📝 Description: {scenario.description}")
        print()
        
        # Basic Configuration
        print("🔧 BASIC CONFIGURATION")
        print(f"   • Total Keys: {scenario.total_keys:,}")
        print(f"   • Vector Dimensions: {scenario.vector_dim}")
        print(f"   • Vector Algorithm: {scenario.vector_algorithm.name if hasattr(scenario.vector_algorithm, 'name') else scenario.vector_algorithm}")
        print(f"   • Vector Metric: {scenario.vector_metric.name if hasattr(scenario.vector_metric, 'name') else scenario.vector_metric}")
        if scenario.hnsw_m:
            print(f"   • HNSW M Parameter: {scenario.hnsw_m}")
        print()
        
        # Tag Configuration
        if tags_config:
            print("🏷️  TAG CONFIGURATION")
            if tags_config.tags_per_key:
                print(f"   • Tags per Key: {tags_config.tags_per_key.avg} avg (range: {tags_config.tags_per_key.min}-{tags_config.tags_per_key.max})")
            if tags_config.tag_length:
                print(f"   • Tag Length: {tags_config.tag_length.avg} bytes avg (range: {tags_config.tag_length.min}-{tags_config.tag_length.max})")
            
            # Sharing configuration
            if tags_config.sharing:
                sharing = tags_config.sharing
                mode_name = sharing.mode.name if hasattr(sharing.mode, 'name') else str(sharing.mode)
                print(f"   • Sharing Mode: {mode_name}")
                
                if mode_name == 'SHARED_POOL':
                    pool_size = getattr(sharing, 'pool_size', 'N/A')
                    reuse_prob = getattr(sharing, 'reuse_probability', 'N/A')
                    print(f"     - Pool Size: {pool_size}")
                    print(f"     - Reuse Probability: {reuse_prob}")
                elif mode_name == 'GROUP_BASED':
                    keys_per_group = getattr(sharing, 'keys_per_group', 'N/A')
                    tags_per_group = getattr(sharing, 'tags_per_group', 'N/A')
                    print(f"     - Keys per Group: {keys_per_group}")
                    print(f"     - Tags per Group: {tags_per_group}")
            
            # Prefix configuration
            if tags_config.tag_prefix and tags_config.tag_prefix.enabled:
                prefix = tags_config.tag_prefix
                print(f"   • Prefix Sharing: ENABLED")
                print(f"     - Shared Length: {prefix.min_shared}-{prefix.max_shared} chars")
                print(f"     - Share Probability: {prefix.share_probability}")
                print(f"     - Prefix Pool Size: {prefix.prefix_pool_size}")
            else:
                print(f"   • Prefix Sharing: DISABLED")
            print()
        
        # Numeric Fields
        if scenario.include_numeric and scenario.numeric_fields:
            print("🔢 NUMERIC FIELDS")
            print(f"   • Number of Fields: {len(scenario.numeric_fields)}")
            for field_name, (min_val, max_val) in scenario.numeric_fields.items():
                print(f"   • {field_name}: {min_val} to {max_val}")
            print()
        
        # Workload
        if scenario.workload_string or scenario.workload_stages:
            print("⚡ WORKLOAD CONFIGURATION")
            if scenario.workload_string:
                print(f"   • Workload String: {scenario.workload_string}")
            if scenario.workload_stages:
                print(f"   • Number of Stages: {len(scenario.workload_stages)}")
            print()
        
        # Memory Estimates
        print("💾 MEMORY ESTIMATES")
        print(f"   • Estimated Unique Tags: {estimates['estimated_unique_tags']:,}")
        print(f"   • Sharing Factor: {estimates['estimated_sharing_factor']}x")
        print(f"   • Tag Memory: {estimates['predicted_tag_memory_kb']:.1f} KB")
        print(f"   • Vector Memory: {estimates['predicted_vector_memory_kb']:.1f} KB")
        if estimates['predicted_numeric_memory_kb'] > 0:
            print(f"   • Numeric Memory: {estimates['predicted_numeric_memory_kb']:.1f} KB")
        print(f"   • Total Predicted: {estimates['predicted_total_memory_kb']:.1f} KB ({estimates['predicted_total_memory_kb']/1024:.1f} MB)")
        
        if estimates['estimated_prefix_savings'] > 0:
            print(f"   • Prefix Savings: {estimates['estimated_prefix_savings']:,} bytes")
        print()
        
        # Key Statistics
        print("📈 KEY STATISTICS")
        if tags_config and tags_config.tags_per_key:
            total_tag_instances = scenario.total_keys * tags_config.tags_per_key.avg
            print(f"   • Total Tag Instances: {total_tag_instances:,.0f}")
            print(f"   • Average Keys per Tag: {estimates['estimated_keys_per_tag']}")
            
            # Memory efficiency metrics
            if estimates['estimated_unique_tags'] > 0:
                dedup_ratio = total_tag_instances / estimates['estimated_unique_tags']
                print(f"   • Deduplication Ratio: {dedup_ratio:.1f}:1")
                
                memory_efficiency = (1 - estimates['predicted_tag_memory_kb'] / (total_tag_instances * tags_config.tag_length.avg / 1024)) * 100
                if memory_efficiency > 0:
                    print(f"   • Memory Efficiency: {memory_efficiency:.1f}% savings")
        print()
    
    def _extract_scenario_schema(self, scenario: BenchmarkScenario) -> dict:
        """Extract detailed schema information from a BenchmarkScenario"""
        tags_config = scenario.tags_config
        
        # Basic scenario info
        schema = {
            'scenario_name': scenario.name,
            'description': scenario.description,
            'total_keys': scenario.total_keys,
        }
        
        # Vector configuration
        schema.update({
            'vector_dim': scenario.vector_dim,
            'vector_algorithm': scenario.vector_algorithm.name if hasattr(scenario.vector_algorithm, 'name') else str(scenario.vector_algorithm),
            'vector_metric': scenario.vector_metric.name if hasattr(scenario.vector_metric, 'name') else str(scenario.vector_metric),
            'hnsw_m': scenario.hnsw_m or '',
            'hnsw_ef_construction': getattr(scenario, 'hnsw_ef_construction', '') or '',
        })
        
        # Tag configuration basics
        if tags_config and tags_config.tags_per_key:
            schema.update({
                'tags_per_key_avg': tags_config.tags_per_key.avg,
                'tags_per_key_min': tags_config.tags_per_key.min,
                'tags_per_key_max': tags_config.tags_per_key.max,
                'tags_per_key_distribution': tags_config.tags_per_key.distribution.name if hasattr(tags_config.tags_per_key.distribution, 'name') else str(tags_config.tags_per_key.distribution),
            })
        
        if tags_config and tags_config.tag_length:
            schema.update({
                'tag_length_avg': tags_config.tag_length.avg,
                'tag_length_min': tags_config.tag_length.min,
                'tag_length_max': tags_config.tag_length.max,
                'tag_length_distribution': tags_config.tag_length.distribution.name if hasattr(tags_config.tag_length.distribution, 'name') else str(tags_config.tag_length.distribution),
            })
        
        # Tag sharing configuration
        if tags_config and tags_config.sharing:
            sharing = tags_config.sharing
            schema.update({
                'tag_sharing_mode': sharing.mode.name if hasattr(sharing.mode, 'name') else str(sharing.mode),
                'tag_sharing_pool_size': getattr(sharing, 'pool_size', ''),
                'tag_sharing_reuse_probability': getattr(sharing, 'reuse_probability', ''),
                'tag_sharing_keys_per_group': getattr(sharing, 'keys_per_group', ''),
                'tag_sharing_tags_per_group': getattr(sharing, 'tags_per_group', ''),
            })
        
        # Prefix configuration  
        if tags_config and tags_config.tag_prefix:
            prefix = tags_config.tag_prefix
            schema.update({
                'prefix_enabled': prefix.enabled,
                'prefix_min_shared': prefix.min_shared,
                'prefix_max_shared': prefix.max_shared,
                'prefix_share_probability': prefix.share_probability,
                'prefix_pool_size': prefix.prefix_pool_size,
            })
        
        # String generation
        if tags_config:
            schema.update({
                'tag_string_type': tags_config.tag_string_type.name if hasattr(tags_config.tag_string_type, 'name') else str(tags_config.tag_string_type) if tags_config.tag_string_type else '',
                'tag_charset': tags_config.tag_charset or '',
                'tag_batch_size': tags_config.batch_size or '',
            })
        
        # Numeric fields
        schema.update({
            'include_numeric': scenario.include_numeric,
            'num_numeric_fields': len(scenario.numeric_fields) if scenario.numeric_fields else 0,
            'numeric_field_names': ', '.join(scenario.numeric_fields.keys()) if scenario.numeric_fields else '',
            'numeric_field_ranges': str(scenario.numeric_fields) if scenario.numeric_fields else '',
        })
        
        # Workload configuration
        schema.update({
            'workload_stages_count': len(scenario.workload_stages) if scenario.workload_stages else 0,
            'workload_string': scenario.workload_string or '',
            'workload_details': str(scenario.workload_stages) if scenario.workload_stages else '',
        })
        
        # Calculate estimates
        estimates = self._calculate_scenario_estimates(scenario)
        schema.update(estimates)
        
        return schema
    
    def _calculate_scenario_estimates(self, scenario: BenchmarkScenario) -> dict:
        """Calculate detailed estimates for scenario memory usage"""
        tags_config = scenario.tags_config
        estimates = {}
        
        if not tags_config:
            return {
                'estimated_unique_tags': 0,
                'estimated_total_tag_memory': 0,
                'estimated_keys_per_tag': 0,
                'estimated_prefix_savings': 0,
                'estimated_sharing_factor': 1.0,
                'predicted_tag_memory_kb': 0,
                'predicted_vector_memory_kb': 0,
                'predicted_numeric_memory_kb': 0,
                'predicted_total_memory_kb': 0,
            }
        
        # Basic calculations
        total_keys = scenario.total_keys
        avg_tags_per_key = tags_config.tags_per_key.avg if tags_config.tags_per_key else 1
        avg_tag_length = tags_config.tag_length.avg if tags_config.tag_length else 50
        
        # Estimate unique tags based on sharing mode
        sharing = tags_config.sharing
        if not sharing or sharing.mode.name == 'UNIQUE':
            estimated_unique_tags = total_keys * avg_tags_per_key
            sharing_factor = 1.0
        elif sharing.mode.name == 'PERFECT_OVERLAP':
            estimated_unique_tags = avg_tags_per_key
            sharing_factor = total_keys
        elif sharing.mode.name == 'SHARED_POOL':
            pool_size = getattr(sharing, 'pool_size', 1000)
            estimated_unique_tags = min(pool_size, total_keys * avg_tags_per_key)
            sharing_factor = (total_keys * avg_tags_per_key) / estimated_unique_tags if estimated_unique_tags > 0 else 1.0
        elif sharing.mode.name == 'GROUP_BASED':
            keys_per_group = getattr(sharing, 'keys_per_group', 100)
            tags_per_group = getattr(sharing, 'tags_per_group', 10)
            num_groups = (total_keys + keys_per_group - 1) // keys_per_group
            estimated_unique_tags = num_groups * tags_per_group
            sharing_factor = (total_keys * avg_tags_per_key) / estimated_unique_tags if estimated_unique_tags > 0 else 1.0
        else:
            estimated_unique_tags = total_keys * avg_tags_per_key * 0.5  # Assume 50% sharing
            sharing_factor = 2.0
        
        # Estimate prefix savings
        prefix_savings = 0
        if tags_config.tag_prefix and tags_config.tag_prefix.enabled:
            avg_shared_prefix = (tags_config.tag_prefix.min_shared + tags_config.tag_prefix.max_shared) / 2
            prefix_share_prob = tags_config.tag_prefix.share_probability
            prefix_savings = estimated_unique_tags * avg_shared_prefix * prefix_share_prob
        
        # Memory calculations (in bytes)
        estimated_total_tag_memory = estimated_unique_tags * avg_tag_length - prefix_savings
        estimated_keys_per_tag = sharing_factor
        
        # Predict memory usage
        predicted_tag_memory_kb = estimated_total_tag_memory / 1024
        
        # Vector memory (assuming float32 = 4 bytes per dimension)
        predicted_vector_memory_kb = (total_keys * scenario.vector_dim * 4) / 1024
        
        # Numeric memory (assuming 8 bytes per numeric field)
        num_numeric_fields = len(scenario.numeric_fields) if scenario.numeric_fields else 0
        predicted_numeric_memory_kb = (total_keys * num_numeric_fields * 8) / 1024
        
        # Total predicted memory (approximate)
        predicted_total_memory_kb = predicted_tag_memory_kb + predicted_vector_memory_kb + predicted_numeric_memory_kb
        
        estimates.update({
            'estimated_unique_tags': int(estimated_unique_tags),
            'estimated_total_tag_memory': int(estimated_total_tag_memory),
            'estimated_keys_per_tag': round(estimated_keys_per_tag, 2),
            'estimated_prefix_savings': int(prefix_savings),
            'estimated_sharing_factor': round(sharing_factor, 2),
            'predicted_tag_memory_kb': round(predicted_tag_memory_kb, 1),
            'predicted_vector_memory_kb': round(predicted_vector_memory_kb, 1),
            'predicted_numeric_memory_kb': round(predicted_numeric_memory_kb, 1),
            'predicted_total_memory_kb': round(predicted_total_memory_kb, 1),
        })
        
        return estimates
        
    
    