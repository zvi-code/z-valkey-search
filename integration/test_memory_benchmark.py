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
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Iterator, Any, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from collections import Counter, defaultdict
import json

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logging.warning("psutil not available - memory validation will use fallback method")

from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase

# Suppress client logging
import contextlib
import io

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


class SilentValkeyClient(Valkey):
    """
    A Valkey client wrapper that suppresses client creation logs.
    Intercepts and redirects specific log messages to avoid cluttering test logs.
    """
    
    def __init__(self, *args, **kwargs):
        # Save original logging state for multiple loggers
        original_levels = {}
        loggers_to_silence = [
            '',  # root logger
            'valkey',
            'valkey.client',
            'valkey.connection',
            'valkey_search',
            'valkey_search_test_case',
            'ValkeySearchTestCaseBase',
            __name__  # current module logger
        ]
        
        # Disable all potentially noisy loggers
        for logger_name in loggers_to_silence:
            logger = logging.getLogger(logger_name)
            original_levels[logger_name] = logger.level
            logger.setLevel(logging.CRITICAL)
        
        # Also temporarily disable all handlers
        original_root_handlers = logging.root.handlers[:]
        null_handler = logging.NullHandler()
        
        # Save original stdout/stderr in case client prints directly
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        
        try:
            # Replace root handlers with null handler
            logging.root.handlers = [null_handler]
            
            # Redirect stdout/stderr to null
            sys.stdout = io.StringIO()
            sys.stderr = io.StringIO()
            
            # Create the client
            super().__init__(*args, **kwargs)
            
        finally:
            # Restore stdout/stderr
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            
            # Restore original logging configuration
            logging.root.handlers = original_root_handlers
            
            # Restore all logger levels
            for logger_name, level in original_levels.items():
                logger = logging.getLogger(logger_name)
                logger.setLevel(level)


class SilentClientPool:
    """Custom client pool that uses SilentValkeyClient to suppress logging"""
    
    def __init__(self, server, pool_size: int):
        self.server = server
        self.pool_size = pool_size
        self.clients = []
        self.lock = threading.Lock()
        self.thread_local = threading.local()
        
        # Pre-create all clients using our silent wrapper
        for i in range(pool_size):
            client = self._create_silent_client()
            self.clients.append(client)
    
    def _create_silent_client(self) -> SilentValkeyClient:
        """Create a new client with logging suppressed"""
        # Get connection info from server
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
    
    def get_client_for_thread(self, thread_index: int) -> SilentValkeyClient:
        """Get a dedicated client for a specific thread index"""
        if thread_index >= self.pool_size:
            raise ValueError(f"Thread index {thread_index} exceeds pool size {self.pool_size}")
        return self.clients[thread_index]
    
    def get_client(self) -> SilentValkeyClient:
        """Get a client - backward compatibility method that uses thread-local storage"""
        if hasattr(self.thread_local, 'client'):
            return self.thread_local.client
        
        thread_id = threading.get_ident()
        client_index = thread_id % self.pool_size
        self.thread_local.client = self.clients[client_index]
        return self.thread_local.client
    
    def return_client(self, client: SilentValkeyClient):
        """Return a client to the pool - no-op for thread-indexed clients"""
        pass
    
    def close_all(self):
        """Close all clients in the pool"""
        for client in self.clients:
            try:
                client.close()
            except:
                pass


@dataclass
class DistributionStats:
    """Statistics for a distribution"""
    count: int = 0
    min: float = float('inf')
    max: float = float('-inf')
    sum: float = 0
    histogram: Counter = field(default_factory=Counter)
    
    def add(self, value: float):
        """Add a value to the distribution"""
        self.count += 1
        self.min = min(self.min, value)
        self.max = max(value, self.max)
        self.sum += value
        self.histogram[value] += 1
    
    @property
    def mean(self) -> float:
        return self.sum / self.count if self.count > 0 else 0
    
    def get_percentile(self, p: float) -> float:
        """Get percentile value (0-100)"""
        if not self.histogram:
            return 0
        
        sorted_values = sorted(self.histogram.items())
        total = sum(count for _, count in sorted_values)
        target = total * p / 100
        
        cumulative = 0
        for value, count in sorted_values:
            cumulative += count
            if cumulative >= target:
                return value
        
        return sorted_values[-1][0] if sorted_values else 0


class DistributionCollector:
    """Collects distribution statistics during data generation"""
    
    def __init__(self):
        # Tag length distribution
        self.tag_lengths = DistributionStats()
        
        # Tags per key distribution
        self.tags_per_key = DistributionStats()
        
        # Tag usage: how many keys have each tag
        self.tag_usage = Counter()
        
        # For thread safety
        self.lock = threading.Lock()
        
        # Track unique tags
        self.unique_tags = set()
        
    def process_key(self, key: str, tags_field: str):
        """Process a single key's tags"""
        tags = [tag.strip() for tag in tags_field.split(',') if tag.strip()]
        
        with self.lock:
            # Tags per key
            self.tags_per_key.add(len(tags))
            
            # Process each tag
            for tag in tags:
                # Tag length
                self.tag_lengths.add(len(tag))
                
                # Tag usage
                self.tag_usage[tag] += 1
                
                # Track unique tags
                self.unique_tags.add(tag)
    
    def get_tag_usage_distribution(self) -> DistributionStats:
        """Get distribution of how many keys use each tag"""
        usage_dist = DistributionStats()
        for tag, count in self.tag_usage.items():
            usage_dist.add(count)
        return usage_dist
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics"""
        tag_usage_dist = self.get_tag_usage_distribution()
        
        return {
            'tag_lengths': {
                'count': self.tag_lengths.count,
                'min': self.tag_lengths.min,
                'max': self.tag_lengths.max,
                'mean': self.tag_lengths.mean,
                'p50': self.tag_lengths.get_percentile(50),
                'p95': self.tag_lengths.get_percentile(95),
                'p99': self.tag_lengths.get_percentile(99)
            },
            'tags_per_key': {
                'count': self.tags_per_key.count,
                'min': self.tags_per_key.min,
                'max': self.tags_per_key.max,
                'mean': self.tags_per_key.mean,
                'p50': self.tags_per_key.get_percentile(50),
                'p95': self.tags_per_key.get_percentile(95),
                'p99': self.tags_per_key.get_percentile(99)
            },
            'tag_usage': {
                'unique_tags': len(self.unique_tags),
                'total_tag_instances': sum(self.tag_usage.values()),
                'min_keys_per_tag': tag_usage_dist.min,
                'max_keys_per_tag': tag_usage_dist.max,
                'mean_keys_per_tag': tag_usage_dist.mean,
                'p50_keys_per_tag': tag_usage_dist.get_percentile(50),
                'p95_keys_per_tag': tag_usage_dist.get_percentile(95),
                'p99_keys_per_tag': tag_usage_dist.get_percentile(99)
            }
        }
    
    def get_detailed_distributions(self) -> Dict[str, Any]:
        """Get detailed distribution data for visualization"""
        return {
            'tag_lengths_histogram': dict(self.tag_lengths.histogram),
            'tags_per_key_histogram': dict(self.tags_per_key.histogram),
            'tag_usage_histogram': dict(Counter(self.tag_usage.values()))
        }


@dataclass
class BenchmarkScenario:
    """Configuration for a benchmark scenario"""
    name: str
    total_keys: int
    tags_config: TagsConfig
    description: str



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
        # Add thread ID for debugging
        thread_id = threading.get_ident()
        thread_name = threading.current_thread().name
        timestamped_message = f"[Thread-{thread_id}|{thread_name}] {message}"
        
        with self.lock:
            self.messages.append(timestamped_message)
    
    def error(self, message: str):
        """Add an error message to be printed by the monitoring thread"""
        self.log(f"❌ ERROR: {message}")
            
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
    
    def _create_silent_client(self) -> SilentValkeyClient:
        """Create a silent client for monitoring"""
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
            
    def _monitor(self):
        """Background monitoring loop"""
        last_report = time.time()
        # Create a silent client to avoid logging noise
        client = self._create_silent_client()
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
                    state_name = "monitoring:"
                    info_all = client.execute_command("info","memory")
                    self.log(f"{state_name}:used_memory_human={info_all['used_memory_human']}")           
                    search_info = client.execute_command("info","modules")
                    self.log(f"{state_name}:search_used_memory_human={search_info['search_used_memory_human']}")  
                    # Get memory info
                    memory_info = client.info("memory")
                    current_memory_kb = memory_info['used_memory'] // 1024
                    memory_delta = current_memory_kb - self.last_memory
                    
                    # Get key count from db info
                    db_info = client.execute_command("info","keyspace")
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
        with self.lock:
            while self.messages:
                message = self.messages.pop(0)
                logging.info(message)
                sys.stdout.flush()


class TestMemoryBenchmark(ValkeySearchTestCaseBase):
    """Comprehensive Tag Index memory benchmarking using hash_generator"""
    
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
        """Append a result to CSV file incrementally"""
        # Add timestamp to the result
        result['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Check if file exists
        file_exists = os.path.exists(csv_filename)
        
        # Write to CSV
        with open(csv_filename, 'a') as f:
            headers = list(result.keys())
            
            # Write header if needed (new file or explicitly requested)
            if write_header or not file_exists:
                f.write(','.join(headers) + '\n')
            
            # Write the data row
            f.write(','.join(str(result.get(h, '')) for h in headers) + '\n')
        
        # Log through monitor if available, otherwise use direct logging
        if monitor:
            monitor.log(f"Results appended to {csv_filename}")
        else:
            logging.info(f"Results appended to {csv_filename}")
    
    def get_available_memory_bytes(self) -> int:
        """Get available memory on the system in bytes"""
        if PSUTIL_AVAILABLE:
            memory = psutil.virtual_memory()
            return memory.available
        else:
            # Fallback: parse /proc/meminfo on Linux
            try:
                with open('/proc/meminfo', 'r') as f:
                    for line in f:
                        if line.startswith('MemAvailable:'):
                            # MemAvailable is in kB
                            return int(line.split()[1]) * 1024
            except:
                # Conservative fallback: assume 2GB available
                logging.warning("Could not determine available memory, using conservative 2GB estimate")
                return 2 * 1024 * 1024 * 1024
    
    def estimate_memory_usage(self, scenario: BenchmarkScenario) -> Dict[str, int]:
        """Estimate memory usage for a scenario in bytes"""
        # Base estimates
        key_overhead = 64  # Redis key overhead
        hash_overhead = 64  # Hash structure overhead
        field_overhead = 24  # Per field overhead
        
        # Tag field estimate
        avg_tags_per_key = scenario.tags_config.tags_per_key.avg
        avg_tag_length = scenario.tags_config.tag_length.avg
        tag_field_size = avg_tags_per_key * (avg_tag_length + 10)  # +10 for separators/overhead
        
        # Vector field estimate (8 dimensions * 4 bytes per float)
        vector_field_size = 8 * 4
        
        # Per-key memory estimate
        per_key_memory = (
            key_overhead + 
            hash_overhead + 
            2 * field_overhead +  # tags and vector fields
            tag_field_size + 
            vector_field_size
        )
        
        # Total data memory
        data_memory = scenario.total_keys * per_key_memory
        
        # Index memory estimate (conservative)
        # Tag index overhead depends on sharing pattern
        if scenario.tags_config.sharing.mode == TagSharingMode.UNIQUE:
            # Worst case: each tag is unique
            unique_tags = scenario.total_keys * avg_tags_per_key
            tag_index_memory = unique_tags * 100  # ~100 bytes per unique tag entry
        elif scenario.tags_config.sharing.mode == TagSharingMode.PERFECT_OVERLAP:
            # Best case: all keys share same tags
            unique_tags = avg_tags_per_key
            tag_index_memory = unique_tags * 100 + scenario.total_keys * 20
        else:
            # Estimate based on pool size or group configuration
            if scenario.tags_config.sharing.pool_size:
                unique_tags = scenario.tags_config.sharing.pool_size
            else:
                # Group-based or other patterns
                unique_tags = scenario.total_keys * avg_tags_per_key * 0.3  # 30% unique tags estimate
            tag_index_memory = unique_tags * 100 + scenario.total_keys * 20
        
        # Vector index memory (FLAT algorithm)
        vector_index_memory = scenario.total_keys * 8 * 4  # dimensions * bytes per float
        
        # Total index memory
        index_memory = tag_index_memory + vector_index_memory
        
        # Add 30% overhead for Redis internals, fragmentation, etc.
        total_memory = int((data_memory + index_memory) * 1.3)
        
        return {
            'data_memory': data_memory,
            'tag_index_memory': tag_index_memory,
            'vector_index_memory': vector_index_memory,
            'index_memory': index_memory,
            'total_memory': total_memory
        }
    
    def validate_memory_requirements(self, scenario: BenchmarkScenario) -> Tuple[bool, str]:
        """Validate if scenario can run without using swap"""
        available_memory = self.get_available_memory_bytes()
        estimates = self.estimate_memory_usage(scenario)
        
        # Ensure we use less than 50% of available memory to avoid swap
        memory_limit = available_memory * 0.5
        
        if estimates['total_memory'] > memory_limit:
            return False, (
                f"Scenario '{scenario.name}' requires ~{estimates['total_memory'] / (1024**3):.1f}GB "
                f"but only {memory_limit / (1024**3):.1f}GB is safely available "
                f"(50% of {available_memory / (1024**3):.1f}GB free memory)"
            )
        
        return True, f"Memory check passed: ~{estimates['total_memory'] / (1024**3):.1f}GB required, {available_memory / (1024**3):.1f}GB available"
    
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
            info = client.execute_command("FT.INFO", index_name)
            info_dict = {}
            for i in range(0, len(info), 2):
                key = info[i].decode() if isinstance(info[i], bytes) else str(info[i])
                value = info[i+1]
                if isinstance(value, bytes):
                    value = value.decode()
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
            state_name = "Index is still processing:"
            info_all = client.execute_command("info","memory")
            monitor.log(f"{state_name}:used_memory_human={info_all['used_memory_human']}")           
            search_info = client.execute_command("info","modules")
            monitor.log(f"{state_name}:search_used_memory_human={search_info['search_used_memory_human']}")
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
                state_name = "Indexing DONE:"
                info_all = client.execute_command("info","memory")
                monitor.log(f"{state_name}:used_memory_human={info_all['used_memory_human']}")           
                search_info = client.execute_command("info","modules")
                monitor.log(f"{state_name}:search_used_memory_human={search_info['search_used_memory_human']}")                
                sys.stdout.flush()  # Ensure immediate output
                return
                
            time.sleep(1)  # Check every 1 second for more responsive monitoring

        monitor.log(f"    ⚠ Warning: Indexing timeout after {timeout}s, proceeding anyway")
        state_name = "Indexing NOT DONE:"
        info_all = client.execute_command("info","memory")
        monitor.log(f"{state_name}:used_memory_human={info_all['used_memory_human']}")           
        search_info = client.execute_command("info","modules")
        monitor.log(f"{state_name}:search_used_memory_human={search_info['search_used_memory_human']}")  
        sys.stdout.flush()  # Ensure immediate output
    
    def run_benchmark_scenario(self, scenario: BenchmarkScenario, monitor: ProgressMonitor = None) -> Dict:
        """Run a single benchmark scenario with full monitoring"""
        # Use provided monitor or create a new one if none provided
        should_stop_monitor = False
        if monitor is None:
            monitor = ProgressMonitor(self.server, scenario.name)
            monitor.start()
            should_stop_monitor = True
        monitor.log(f"{'*'*80}STARTING SCENARIO {scenario.name} - {scenario.description}")
        # Validate memory requirements before starting
        can_run, message = self.validate_memory_requirements(scenario)
        monitor.log(f"Memory validation: {message}")
        
        if not can_run:
            monitor.error(f"❌ Skipping scenario due to memory constraints")
            return {
                'scenario_name': scenario.name,
                'description': scenario.description,
                'total_keys': scenario.total_keys,
                'skipped': True,
                'reason': message
            }
        

        
        # Get main client
        client = self.get_silent_client()
        
        try:
            # Verify server connection
            if not self.verify_server_connection(client, monitor):
                if should_stop_monitor:
                    monitor.stop()
                raise RuntimeError(f"Failed to connect to valkey server for scenario {scenario.name}")
            
            # Clean up any existing data
            client.flushall()
            time.sleep(1)
            
            # Measure baseline
            baseline_memory = client.info("memory")['used_memory']
            monitor.log(f"  Baseline memory: {baseline_memory // 1024:,} KB")
            state_name = "Baseline memory:"
            info_all = client.execute_command("info","memory")
            monitor.log(f"{state_name}:used_memory_human={info_all['used_memory_human']}")           
            search_info = client.execute_command("info","modules")
            monitor.log(f"{state_name}:search_used_memory_human={search_info['search_used_memory_human']}")  
            # Log memory estimates
            estimates = self.estimate_memory_usage(scenario)
            monitor.log(f"  Estimated memory usage:")
            monitor.log(f"    - Data: {estimates['data_memory'] // (1024**2):,} MB")
            monitor.log(f"    - Tag index: {estimates['tag_index_memory'] // (1024**2):,} MB")
            monitor.log(f"    - Vector index: {estimates['vector_index_memory'] // (1024**2):,} MB")
            monitor.log(f"    - Total (with overhead): {estimates['total_memory'] // (1024**2):,} MB")
            
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
            num_threads = min(64, max(2, scenario.total_keys // config.batch_size))
            client_pool = SilentClientPool(self.server, num_threads)
            
            # Create distribution collector
            dist_collector = DistributionCollector()
            
            # Insert data using generator with parallel processing
            insertion_start_time = time.time()
            keys_processed = ThreadSafeCounter(0)
            # info_all = client.execute_command("info","memory")
            # for key, value in info_all.items():
            #     if "_human" in key:
            #         monitor.log(f"ZZZZZZZZZZZZZZZZZZZZZZZ{key}: {value}")
            # search_info = client.execute_command("info","smodules")
            # for key, value in search_info.items():
            #     if "search_" in key and value != 0:
            #         monitor.log(f"ZZZZZZZZZZZZZZZZZZZZZZZ{key}: {value}")
            # Process batches with thread pool
            def process_batch(batch_data: List[Tuple[str, Dict[str, Any]]], thread_id: int) -> Tuple[int, float]:
                """Process a batch of keys in a worker thread"""
                thread_client = client_pool.get_client_for_thread(thread_id)
                
                batch_start = time.time()
                pipe = thread_client.pipeline(transaction=False)
                
                for key, fields in batch_data:
                    pipe.hset(key, mapping=fields)
                    
                    # Collect distribution statistics
                    if 'tags' in fields:
                        dist_collector.process_key(key, fields['tags'])
                
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
            state_name = "Starting data ingestion:"
            info_all = client.execute_command("info","memory")
            monitor.log(f"{state_name}:used_memory_human={info_all['used_memory_human']}")           
            search_info = client.execute_command("info","modules")
            monitor.log(f"{state_name}:search_used_memory_human={search_info['search_used_memory_human']}")  
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
            state_name = "Data insertion complete:"
            info_all = client.execute_command("info","memory")
            monitor.log(f"{state_name}:used_memory_human={info_all['used_memory_human']}")           
            search_info = client.execute_command("info","modules")
            monitor.log(f"{state_name}:search_used_memory_human={search_info['search_used_memory_human']}")  
            # Log distribution statistics
            dist_summary = dist_collector.get_summary()
            monitor.log(f"  Distribution Statistics:")
            monitor.log(f"    Tag Lengths: min={dist_summary['tag_lengths']['min']}, "
                       f"max={dist_summary['tag_lengths']['max']}, "
                       f"mean={dist_summary['tag_lengths']['mean']:.1f}, "
                       f"p95={dist_summary['tag_lengths']['p95']}")
            monitor.log(f"    Tags per Key: min={dist_summary['tags_per_key']['min']}, "
                       f"max={dist_summary['tags_per_key']['max']}, "
                       f"mean={dist_summary['tags_per_key']['mean']:.1f}, "
                       f"p95={dist_summary['tags_per_key']['p95']}")
            monitor.log(f"    Tag Usage: {dist_summary['tag_usage']['unique_tags']} unique tags, "
                       f"mean keys/tag={dist_summary['tag_usage']['mean_keys_per_tag']:.1f}, "
                       f"max={dist_summary['tag_usage']['max_keys_per_tag']}")
            
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
                search_info = client.execute_command("info","modules")
                search_memory_kb = search_info.get('search_used_memory_bytes', 0) // 1024
            except:
                search_memory_kb = index_overhead_kb
            
            # Calculate metrics
            vector_memory_kb = (scenario.total_keys * 8 * 4) // 1024  # 8 dim * 4 bytes
            tag_index_memory_kb = max(0, search_memory_kb - vector_memory_kb)
            
            # Include distribution statistics in results
            dist_stats = dist_collector.get_summary()
            
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
                'tags_config': str(scenario.tags_config.sharing.mode.value),
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
                'keys_per_tag_p95': dist_stats['tag_usage']['p95_keys_per_tag']
            }
            
            monitor.log(f"  Results: Data={data_memory_kb:,}KB, TagIndex={tag_index_memory_kb:,}KB, "
                       f"Vector={vector_memory_kb:,}KB, Total={total_memory_kb:,}KB")
            
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
            # Only stop monitor if we created it
            if should_stop_monitor:
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
        for pool_size in [100, 10000]:
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
        for group_size in [100, 10000]:
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
        for prefix_ratio in [0.3, 0.8]:
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
        for avg_tags in [10, 1000]:
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
        for tag_len in [64, 1200]:
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
        # Set up file logging
        log_file = self.setup_file_logging("comprehensive")
        monitor = ProgressMonitor(self.server, "comprehensive")
        monitor.start()
        monitor.log("=== COMPREHENSIVE MEMORY BENCHMARK ===")
        monitor.log("Testing various tag sharing patterns and configurations\n")
        
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
        
        # Create scenarios
        scenarios = self.create_comprehensive_scenarios(base_keys=1000000)
        results = []
        
        # CSV filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f"comprehensive_benchmark_results_{timestamp}.csv"
        
        # Run each scenario
        for i, scenario in enumerate(scenarios):
            monitor.log(f"--- Scenario {i+1}/{len(scenarios)}: {scenario.name} ---")
            try:
                monitor.log(f"  Description: {scenario.description}")
                # monitor.stop()
                result = self.run_benchmark_scenario(scenario, monitor)
                results.append(result)
                # monitor.start()
                # Append to CSV incrementally (write header only for first result)
                self.append_to_csv(csv_filename, result, monitor, write_header=(i == 0))
                
            except Exception as e:
                monitor.error(f"  ❌ Scenario failed: {e}")
                # Log failed scenario to CSV as well
                failed_result = {
                    'scenario_name': scenario.name,
                    'description': scenario.description,
                    'total_keys': scenario.total_keys,
                    'skipped': True,
                    'reason': f"Failed: {str(e)}"
                }
                results.append(failed_result)
                self.append_to_csv(csv_filename, failed_result, monitor, write_header=(i == 0))
                continue
        
        # Print summary table
        monitor.log("="*120)
        monitor.log("COMPREHENSIVE BENCHMARK RESULTS SUMMARY")
        monitor.log("="*120)
        
        monitor.log(f"{'Scenario':<30} {'Keys':>8} {'DataKB':>10} {'TagIdxKB':>10} {'TotalKB':>10} {'Time(s)':>8} {'Mode':<15}")
        monitor.log("-" * 120)
        
        for r in results:
            if r.get('skipped'):
                monitor.log(f"{r['scenario_name']:<30} "
                            f"{r['total_keys']:>8,} "
                            f"{'SKIPPED':>10} "
                            f"{'':>10} "
                            f"{'':>10} "
                            f"{'':>8} "
                            f"Memory limit exceeded")
            else:
                monitor.log(f"{r['scenario_name']:<30} "
                            f"{r['total_keys']:>8,} "
                            f"{r['data_memory_kb']:>10,} "
                            f"{r['tag_index_memory_kb']:>10,} "
                            f"{r['total_memory_kb']:>10,} "
                            f"{r['insertion_time']:>8.1f} "
                            f"{r['tags_config']:<15}")
        
        # Analyze results by category
        monitor.log("="*80)
        monitor.log("KEY FINDINGS")
        monitor.log("="*80)
        
        # Find extremes (excluding skipped scenarios)
        baseline = next((r for r in results if 'Baseline' in r['scenario_name'] and not r.get('skipped')), None)
        perfect = next((r for r in results if 'Perfect' in r['scenario_name'] and not r.get('skipped')), None)
        
        if baseline and perfect:
            savings = (baseline['tag_index_memory_kb'] - perfect['tag_index_memory_kb']) / baseline['tag_index_memory_kb'] * 100
            monitor.log(f"Tag Sharing Impact:")
            monitor.log(f"  - Unique tags (baseline): {baseline['tag_index_memory_kb']:,} KB")
            monitor.log(f"  - Perfect overlap: {perfect['tag_index_memory_kb']:,} KB")
            monitor.log(f"  - Memory savings: {savings:.1f}%")
        
        # Group-based analysis
        group_results = [r for r in results if 'GroupBased' in r['scenario_name'] and not r.get('skipped')]
        if group_results:
            monitor.log(f"Group-based Sharing:")
            for r in sorted(group_results, key=lambda x: x['scenario_name']):
                monitor.log(f"  - {r['description']}: {r['tag_index_memory_kb']:,} KB")

        monitor.log(f"Detailed results saved to {csv_filename}")
        monitor.log(f"Log file: {log_file}")
        
        # Stop the main monitor
        monitor.stop()
    
    def test_quick_memory_benchmark(self):
        """Quick benchmark with smaller dataset"""
        # Set up file logging
        log_file = self.setup_file_logging("quick")
        monitor = ProgressMonitor(self.server, "quick")
        monitor.start()
        monitor.log("=== QUICK MEMORY BENCHMARK (10K keys) ===")
        
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
        
        # CSV filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f"quick_benchmark_results_{timestamp}.csv"
        
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
        for i, scenario in enumerate(scenarios):
            monitor.log(f"Running: {scenario.name}")
            try:
                #monitor.stop()
                result = self.run_benchmark_scenario(scenario, monitor)
                results.append(result)
                #monitor.start()
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
                continue
        
        # Quick summary
        monitor.log("" + "="*80)
        monitor.log("QUICK BENCHMARK SUMMARY")
        monitor.log("="*80)
        for r in results:
            if r.get('skipped'):
                monitor.log(f"{r['scenario_name']:<20}: SKIPPED - {r.get('reason', 'Unknown')}")
            else:
                monitor.log(f"{r['scenario_name']:<20}: {r['tag_index_memory_kb']:>8,} KB tag index")
        
        monitor.log(f"Results saved to {csv_filename}")
        monitor.log(f"Log file: {log_file}")
        
        # Stop the main monitor
        monitor.stop()