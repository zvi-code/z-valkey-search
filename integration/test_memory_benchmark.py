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

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logging.warning("psutil not available - memory validation will use fallback method")

from valkey import ResponseError
from valkey.client import Valkey
from valkey.asyncio import Valkey as AsyncValkey
from valkey_search_test_case import ValkeySearchTestCaseBase, ValkeySearchClusterTestCase

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


class AsyncSilentValkeyClient(AsyncValkey):
    """
    An async Valkey client wrapper that suppresses client creation logs.
    Provides high-performance async I/O operations.
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
            
            # Create the async client
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


class AsyncSilentClientPool:
    """Async client pool that uses AsyncSilentValkeyClient for high-performance operations"""
    
    def __init__(self, server, pool_size: int, max_concurrent_per_client: int = 10):
        self.server = server
        self.pool_size = pool_size
        self.clients = []
        self.semaphores = []  # One semaphore per client to limit concurrent ops
        self.max_concurrent_per_client = max_concurrent_per_client
        self.lock = asyncio.Lock()
        
    async def initialize(self):
        """Initialize all async clients - must be called in async context"""
        for i in range(self.pool_size):
            client = await self._create_silent_async_client()
            semaphore = asyncio.Semaphore(self.max_concurrent_per_client)
            self.clients.append(client)
            self.semaphores.append(semaphore)
    
    async def _create_silent_async_client(self) -> AsyncSilentValkeyClient:
        """Create a new async client with logging suppressed"""
        if hasattr(self.server, 'get_new_client'):
            # Temporarily suppress logging while getting connection params
            original_level = logging.getLogger().level
            logging.getLogger().setLevel(logging.CRITICAL)
            
            try:
                # Get connection params from sync client
                temp_client = self.server.get_new_client()
                connection_kwargs = temp_client.connection_pool.connection_kwargs.copy()
                temp_client.close()
            finally:
                # Restore logging level
                logging.getLogger().setLevel(original_level)
            
            # Remove sync-specific params and add async-specific ones
            connection_kwargs.pop('connection_pool', None)
            connection_kwargs.pop('connection_class', None)
            
            # Create async client with extracted params
            return AsyncSilentValkeyClient(**connection_kwargs)
        else:
            # Fallback: create with default params
            return AsyncSilentValkeyClient(host='localhost', port=6379, decode_responses=True)
    
    def get_client_for_task(self, task_id: int) -> tuple[AsyncSilentValkeyClient, asyncio.Semaphore]:
        """Get a client and its semaphore for a specific task ID"""
        client_id = task_id % self.pool_size
        return self.clients[client_id], self.semaphores[client_id]
    
    async def close_all(self):
        """Close all async clients in the pool"""
        for client in self.clients:
            try:
                await client.aclose()
            except:
                continue


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
    
    
    def close_all(self):
        """Close all clients in the pool"""
        for client in self.clients:
            while True:
                try:
                    client.close()
                    break
                except:
                    continue


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
        
        # Stall detection
        self.stall_detection_enabled = True
        self.stall_threshold_seconds = 30  # Alert if no change for 30 seconds
        self.last_change_time = time.time()
        self.last_search_memory = 0
        self.stall_callback = None  # Optional callback when stall detected
        
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
    
    def set_stall_detection(self, enabled: bool = True, threshold_seconds: int = 30, callback=None):
        """Configure stall detection
        
        Args:
            enabled: Whether to enable stall detection
            threshold_seconds: How many seconds without change before considering it stalled
            callback: Optional function to call when stall detected (receives monitor instance)
        """
        with self.lock:
            self.stall_detection_enabled = enabled
            self.stall_threshold_seconds = threshold_seconds
            self.stall_callback = callback
            self.last_change_time = time.time()  # Reset on configuration change
    
    def collect_diagnostics(self, reason: str = "Manual request"):
        """Manually trigger diagnostic collection
        
        Args:
            reason: Reason for collecting diagnostics
        """
        self.log(f"📊 DIAGNOSTIC COLLECTION: {reason}")
        # Set a flag that the monitor thread will check
        with self.lock:
            self.messages.append("__COLLECT_DIAGNOSTICS__")
    
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
    
    def _collect_and_log_diagnostics(self, client, reason: str = ""):
        """Collect and log full diagnostic information
        
        Args:
            client: Valkey client to use for commands
            reason: Optional reason for collection
        """
        try:
            if reason:
                self.log(f"📊 FULL DIAGNOSTIC INFO ({reason}):")
            else:
                self.log("📊 FULL DIAGNOSTIC INFO:")
            self.log("─" * 60)
            
            # Get all sections of INFO command
            info_sections = ["server", "clients", "memory", "persistence", 
                           "stats", "replication", "cpu", "commandstats", 
                           "keyspace", "modules"]
            
            for section in info_sections:
                try:
                    info_data = client.execute_command("info", section)
                    self.log(f"\n[INFO {section.upper()}]")
                    
                    # Format the info output
                    if isinstance(info_data, dict):
                        for key, value in info_data.items():
                            self.log(f"  {key}: {value}")
                    else:
                        # Raw string format
                        for line in str(info_data).split('\n'):
                            if line.strip():
                                self.log(f"  {line}")
                except Exception as e:
                    self.log(f"  ❌ Failed to get {section} info: {e}")
            
            # Get index-specific information if indexes are being monitored
            with self.lock:
                current_indexes = self.active_index_names.copy()
            
            if current_indexes:
                self.log("\n[INDEX INFORMATION]")
                for idx_name in current_indexes:
                    try:
                        ft_info = client.execute_command("FT.INFO", idx_name)
                        self.log(f"\nIndex: {idx_name}")
                        for i in range(0, len(ft_info), 2):
                            if i + 1 < len(ft_info):
                                key = ft_info[i].decode() if isinstance(ft_info[i], bytes) else str(ft_info[i])
                                value = ft_info[i + 1]
                                if isinstance(value, bytes):
                                    try:
                                        value = value.decode()
                                    except:
                                        pass
                                self.log(f"  {key}: {value}")
                    except Exception as e:
                        self.log(f"  ❌ Failed to get info for index {idx_name}: {e}")
            
            self.log("─" * 60)
            
        except Exception as e:
            self.log(f"❌ Failed to collect diagnostic info: {e}")
    
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
                        if message == "__COLLECT_DIAGNOSTICS__":
                            # Handle diagnostic collection request
                            self._collect_and_log_diagnostics(client, "Manual request")
                        else:
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
                    
                    # Get search module memory
                    search_memory_kb = 0
                    if 'search_used_memory' in search_info:
                        search_memory_kb = search_info['search_used_memory'] // 1024
                    
                    # Get key count from db info
                    db_info = client.execute_command("info","keyspace")
                    if 'db0' not in db_info:
                        current_keys = 0
                    elif 'db0' in db_info and isinstance(db_info['db0'], dict) and 'keys' in db_info['db0']:
                        current_keys = db_info['db0']['keys']

                    elif 'db0' in db_info and isinstance(db_info['db0'], str) and 'keys=' in db_info['db0']:
                        # Parse "keys=123,expires=0,avg_ttl=0" format
                        try:
                            current_keys = int(db_info['db0'].split('keys=')[1].split(',')[0])
                        except:
                            assert False, "Expected 'db0' in keyspace info"
                    else:
                        assert False, "Expected 'db0' in keyspace info"
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
                    search_memory_delta = search_memory_kb - self.last_search_memory
                    
                    # Stall detection
                    with self.lock:
                        if self.stall_detection_enabled:
                            # Check if there's been any change
                            has_change = (keys_delta != 0 or memory_delta != 0 or search_memory_delta != 0)
                            
                            if has_change:
                                self.last_change_time = current_time
                            else:
                                # Check if we've been stalled too long
                                stall_duration = current_time - self.last_change_time
                                if stall_duration >= self.stall_threshold_seconds:
                                    # Log stall warning
                                    self.log(f"⚠️  STALL DETECTED: No changes in keys/memory for {stall_duration:.0f}s")
                                    
                                    # Collect and log full diagnostic info
                                    self._collect_and_log_diagnostics(client, f"Stall detected after {stall_duration:.0f}s")
                                    
                                    # Call callback if configured
                                    if self.stall_callback:
                                        try:
                                            self.stall_callback(self)
                                        except Exception as e:
                                            self.log(f"❌ Stall callback error: {e}")
                                    
                                    # Reset timer to avoid repeated alerts
                                    self.last_change_time = current_time - (self.stall_threshold_seconds // 2)
                    
                    # Build status report
                    status_parts = [
                        f"Time: {elapsed:.0f}s",
                        f"Memory: {current_memory_kb:,} KB (+{memory_delta:,})",
                        f"Keys: {current_keys:,} (+{keys_delta:,})"
                    ]
                    
                    # Add search memory if available
                    if search_memory_kb > 0:
                        status_parts.append(f"Search: {search_memory_kb:,} KB (+{search_memory_delta:,})")
                    
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
                    self.last_search_memory = search_memory_kb
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
    
    async def run_async_insertion(self, scenario: BenchmarkScenario, generator, monitor: ProgressMonitor, 
                                  insertion_start_time: float, keys_processed: ThreadSafeCounter, 
                                  dist_collector: DistributionCollector, config) -> float:
        """Run async insertion for maximum performance"""
        
        # Create async client pool with optimal client-to-task ratio
        num_tasks = min(200, max(20, scenario.total_keys // config.batch_size))  # Many concurrent tasks
        num_clients = min(20, max(5, num_tasks // 10))  # Fewer clients, shared among tasks
        async_client_pool = AsyncSilentClientPool(self.server, num_clients)
        await async_client_pool.initialize()
        
        monitor.log(f"🚀 Using ASYNC I/O: {num_tasks} concurrent tasks sharing {num_clients} clients")
        
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
                    "Phase": "Async Insertion",
                    "Progress": f"{processed_count:,}/{scenario.total_keys:,} ({progress_pct:.1f}%)",
                    "Speed": f"{keys_per_sec:.0f} keys/sec",
                    "Tasks": f"{num_tasks} async",
                    "ETA": eta_str
                })
            
            return len(batch_data), batch_time
        
        # Create all async tasks
        tasks = []
        task_id = 0
        
        for batch in generator:
            task = asyncio.create_task(process_batch_async(batch, task_id % num_tasks))
            tasks.append(task)
            task_id += 1
        
        # Wait for all tasks to complete
        try:
            await asyncio.gather(*tasks)
            monitor.log(f"✅ All async tasks completed, performing final sync...")
            
            # CRITICAL FIX: Ensure all async operations are committed and visible
            # Create a sync client to verify data is committed
            sync_client = self.get_silent_client()
            
            # Force a small delay to ensure all async writes are committed
            await asyncio.sleep(0.1)
            
            # Verify key count is correct using sync client  
            db_info = sync_client.execute_command("info", "keyspace")
            current_keys = 0
            if 'db0' in db_info and isinstance(db_info['db0'], dict) and 'keys' in db_info['db0']:
                current_keys = db_info['db0']['keys']
            elif 'db0' in db_info and isinstance(db_info['db0'], str) and 'keys=' in db_info['db0']:
                # Parse "keys=123,expires=0,avg_ttl=0" format
                try:
                    keys_part = db_info['db0'].split(',')[0]
                    current_keys = int(keys_part.split('=')[1])
                except:
                    current_keys = 0
            
            expected_keys = keys_processed.get()
            monitor.log(f"🔍 Post-async verification: Expected {expected_keys:,} keys, Found {current_keys:,} keys")
            
            if current_keys < expected_keys:
                monitor.log(f"⚠️  Warning: Key count mismatch detected. Waiting for sync completion...")
                # Wait up to 5 seconds for all keys to be visible
                for i in range(50):  # 50 * 0.1s = 5s max
                    await asyncio.sleep(0.1)
                    db_info = sync_client.execute_command("info", "keyspace")
                    if 'db0' in db_info and isinstance(db_info['db0'], dict) and 'keys' in db_info['db0']:
                        current_keys = db_info['db0']['keys']
                    elif 'db0' in db_info and isinstance(db_info['db0'], str) and 'keys=' in db_info['db0']:
                        try:
                            keys_part = db_info['db0'].split(',')[0]
                            current_keys = int(keys_part.split('=')[1])
                        except:
                            current_keys = 0
                    
                    if current_keys >= expected_keys:
                        monitor.log(f"✅ Sync complete: {current_keys:,} keys now visible after {(i+1)*0.1:.1f}s")
                        break
                else:
                    monitor.log(f"⚠️  Still missing keys after 5s wait: {current_keys:,}/{expected_keys:,}")
            else:
                monitor.log(f"✅ All keys immediately visible: {current_keys:,}/{expected_keys:,}")
            
            sync_client.close()
            
        except Exception as e:
            monitor.log(f"❌ Async batch failed: {e}")
        finally:
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
    
    def calculate_comprehensive_memory(self, total_keys, unique_tags, avg_tag_length, avg_tags_per_key, avg_keys_per_tag, vector_dims=8, hnsw_m=16):
        """
        Calculate comprehensive search module memory including all major components.
        
        Based on analysis of:
        - src/indexes/tag.h, src/utils/patricia_tree.h, src/utils/string_interning.h  
        - src/indexes/vector_base.h, vector_flat.h, third_party/hnswlib/
        - Valkey core memory patterns
        
        Args:
            total_keys: Total number of keys
            unique_tags: Number of unique tag strings
            avg_tag_length: Average length of tag strings
            avg_tags_per_key: Average tags per key
            avg_keys_per_tag: Average keys per tag (sharing factor)
            vector_dims: Vector dimensions (default 8)
        
        Returns:
            Dictionary with detailed memory breakdown in KB
        """
        
        # === 1. VALKEY CORE OVERHEAD ===
        # Valkey stores HASH keys with internal overhead
        # Each key: hash table entry + key string + metadata
        avg_key_length = 32  # estimate based on typical patterns
        valkey_key_overhead = total_keys * (
            32 +  # hash table entry overhead
            avg_key_length + 8 +  # key string + length
            16  # misc metadata per key
        )
        
        # Hash field storage (tags + vector fields per key)
        valkey_fields_overhead = total_keys * (
            (avg_tags_per_key * avg_tag_length * 1.2) +  # tag data with overhead
            (vector_dims * 4 * 1.1) +  # vector data with overhead
            32  # field metadata
        )
        
        valkey_memory = valkey_key_overhead + valkey_fields_overhead
        
        # === 2. TAG INDEX MEMORY ===
        # String Interning (with better estimates)  
        string_intern_overhead_per_string = 32  # InternedString + weak_ptr in global map
        tag_interning_memory = unique_tags * (avg_tag_length + string_intern_overhead_per_string)
        tag_interning_memory += unique_tags * 48  # global hash map overhead
        
        # Key interning (all keys are interned)
        key_interning_memory = total_keys * (avg_key_length + string_intern_overhead_per_string)
        key_interning_memory += total_keys * 48  # key interning map overhead
        
        # tracked_tags_by_keys_ (InternedStringMap<TagInfo>)
        taginfo_per_key = (
            16 +  # InternedStringPtr to raw_tag_string  
            32 +  # flat_hash_set overhead for parsed tags
            (avg_tags_per_key * 16)  # string_view per tag (ptr + size)
        )
        tracked_keys_memory = total_keys * (8 + taginfo_per_key + 16)  # map entry overhead
        
        # Patricia Tree (more accurate estimation)
        # Tree nodes based on prefix patterns - heavily dependent on tag diversity
        prefix_diversity_factor = min(2.0, avg_tag_length / 4.0)  # more characters = more nodes
        estimated_tree_nodes = int(unique_tags * prefix_diversity_factor)
        
        patricia_node_memory = estimated_tree_nodes * (
            48 +  # children map overhead (absl::flat_hash_map)
            8 +   # subtree_values_count
            16    # optional overhead
        )
        
        # Leaf node values (flat_hash_set<InternedStringPtr> per unique tag)
        patricia_values_memory = unique_tags * (
            32 +  # flat_hash_set overhead
            (avg_keys_per_tag * 8)  # InternedStringPtr per key
        )
        
        patricia_tree_memory = patricia_node_memory + patricia_values_memory
        
        # untracked_keys_ set
        untracked_keys_memory = total_keys * 0.1 * 24  # assume 10% untracked
        
        tag_index_memory = (
            tag_interning_memory + 
            tracked_keys_memory + 
            patricia_tree_memory + 
            untracked_keys_memory
        )
        
        # === 3. VECTOR INDEX MEMORY ===
        # The benchmark uses 8-dimensional vectors, but we need to determine if FLAT or HNSW is used
        # For most benchmarks with small datasets, FLAT is typically used, but let's account for both
        
        # Common components for both algorithms:
        # Vector data storage (ChunkedArray)
        vector_data_size = total_keys * vector_dims * 4  # float32
        
        # tracked_metadata_by_key_ (key -> internal_id + magnitude mapping)
        key_metadata_memory = total_keys * (
            8 +   # key pointer in map
            8 +   # internal_id (uint64_t)
            4 +   # magnitude (float)
            16    # map overhead per entry
        )
        
        # key_by_internal_id_ reverse mapping
        reverse_mapping_memory = total_keys * 24  # similar hash map overhead
        
        # FixedSizeAllocator for vector storage optimization  
        allocator_overhead = max(1024, total_keys * 0.1)  # allocator bookkeeping
        
        # Algorithm-specific overhead (assume HNSW for more comprehensive estimation)
        # Based on third_party/hnswlib/hnswalg.h analysis:
        
        # HNSW Graph Structure Memory:
        # Each node has connections stored in link lists
        M = hnsw_m  # M parameter (typically 16)
        maxM0 = M * 2  # connections in layer 0 (typically 32)
        
        # data_level0_memory_ - ChunkedArray storing layer 0 data
        # Each element: size_links_level0_ + sizeof(char*) + sizeof(labeltype)
        size_links_level0 = maxM0 * 4 + 4  # maxM0 * sizeof(tableint) + sizeof(linklistsizeint)
        size_data_per_element = size_links_level0 + 8 + 4  # + char* + labeltype
        
        data_level0_memory = total_keys * size_data_per_element
        
        # linkLists_ - ChunkedArray for higher level connections  
        # Most nodes are only in level 0, but some have higher levels
        # Estimate ~10% of nodes have level 1, ~1% have level 2, etc.
        higher_level_nodes = int(total_keys * 0.1)  # rough estimate
        size_links_per_element = M * 4 + 4  # M * sizeof(tableint) + sizeof(linklistsizeint)
        higher_level_memory = higher_level_nodes * size_links_per_element
        
        # label_lookup_ mapping (unordered_map<labeltype, tableint>)
        label_lookup_memory = total_keys * 24  # hash map overhead + key/value
        
        # element_levels_ vector (keeps level of each element)
        element_levels_memory = total_keys * 4  # vector<int>
        
        # visited_list_pool_ for search operations
        visited_list_pool_memory = max(1024, total_keys * 0.01)  # search working memory
        
        # Additional HNSW overhead: mutexes, atomics, deleted_elements tracking
        hnsw_coordination_overhead = total_keys * 2  # various coordination structures
        
        vector_index_memory = (
            vector_data_size +
            key_metadata_memory +
            reverse_mapping_memory +
            allocator_overhead +
            data_level0_memory +
            higher_level_memory +
            label_lookup_memory +
            element_levels_memory +
            visited_list_pool_memory +
            hnsw_coordination_overhead
        )
        
        # === 4. SEARCH MODULE OVERHEAD ===
        # Index management, schema storage, coordination overhead
        module_base_overhead = 8192  # base module structures
        index_schema_overhead = 2048  # FT.CREATE schema storage
        coordination_overhead = 1024  # various coordination structures
        
        search_module_overhead = module_base_overhead + index_schema_overhead + coordination_overhead
        
        # === 5. MEMORY FRAGMENTATION & ALIGNMENT ===
        # Real allocators have significant overhead
        total_allocated = valkey_memory + tag_index_memory + vector_index_memory + search_module_overhead
        fragmentation_factor = 1.25  # 25% overhead typical for mixed allocation patterns
        fragmentation_overhead = total_allocated * (fragmentation_factor - 1.0)
        
        # === SUMMARY ===
        breakdown = {
            'valkey_core_kb': int(valkey_memory // 1024),
            'key_interning_kb': int(key_interning_memory // 1024),
            'tag_index_kb': int(tag_index_memory // 1024),
            'vector_index_kb': int(vector_index_memory // 1024), 
            'search_module_overhead_kb': int(search_module_overhead // 1024),
            'fragmentation_overhead_kb': int(fragmentation_overhead // 1024),
            'total_estimated_kb': int((total_allocated + fragmentation_overhead) // 1024),
            # Detailed vector breakdown for analysis
            'vector_data_kb': int(vector_data_size // 1024),
            'hnsw_level0_kb': int(data_level0_memory // 1024),
            'hnsw_higher_levels_kb': int(higher_level_memory // 1024),
            'vector_mappings_kb': int((key_metadata_memory + reverse_mapping_memory + label_lookup_memory) // 1024),
            # HNSW configuration used
            'hnsw_m': M,
            'hnsw_maxM0': maxM0,
            'connections_per_node_l0': maxM0
        }
        
        return breakdown
    
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
                assert False, "Could not determine available memory, using conservative 2GB estimate"
    
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
    
    def verify_memory(self, client: Valkey, prefix: str = "key:", monitor: ProgressMonitor = None) -> Dict[str, int]:
        """
        Read all generated keys and calculate the total length of data.
        
        Returns a dictionary with:
        - total_keys: number of keys found
        - sum_key_lengths: sum of all key name lengths
        - sum_tag_lengths: sum of all tag field data lengths
        - sum_vector_lengths: sum of all vector field data lengths (in bytes)
        - total_data_size: total size of all data
        """
        if monitor:
            monitor.log("🔍 Starting memory verification...")
            monitor.log(f"   Scanning keys with prefix: {prefix}")
        
        # Initialize counters
        total_keys = 0
        sum_key_lengths = 0
        sum_tag_lengths = 0
        sum_vector_lengths = 0
        
        # Use SCAN to iterate through all keys with the prefix
        cursor = 0
        batch_count = 0
        
        while True:
            cursor, keys = client.scan(cursor, match=f"{prefix}*", count=1000)
            
            if keys:
                batch_count += 1
                # Use pipeline for efficient batch reading
                pipe = client.pipeline(transaction=False)
                
                # Queue all HGETALL commands
                for key in keys:
                    pipe.hgetall(key)
                
                # Execute and process results
                results = pipe.execute()
                
                for i, (key, fields) in enumerate(zip(keys, results)):
                    if fields:  # Make sure we got data
                        total_keys += 1
                        
                        # Add key name length (handle bytes)
                        if isinstance(key, bytes):
                            sum_key_lengths += len(key)
                        else:
                            sum_key_lengths += len(str(key))
                        
                        # Debug first few keys
                        if total_keys <= 3 and monitor:
                            field_keys = [k.decode('utf-8') if isinstance(k, bytes) else k for k in fields.keys()]
                            monitor.log(f"   Debug key {key}: fields={field_keys}")
                        
                        # Process fields - handle both string and bytes keys
                        tags_key = b'tags' if isinstance(list(fields.keys())[0], bytes) else 'tags'
                        vector_key = b'vector' if isinstance(list(fields.keys())[0], bytes) else 'vector'
                        
                        if tags_key in fields:
                            # Tags are stored as string
                            tag_value = fields[tags_key]
                            if isinstance(tag_value, bytes):
                                sum_tag_lengths += len(tag_value)
                            else:
                                sum_tag_lengths += len(str(tag_value))
                        
                        if vector_key in fields:
                            # Vector is stored as binary data
                            vector_value = fields[vector_key]
                            if isinstance(vector_value, bytes):
                                sum_vector_lengths += len(vector_value)
                            else:
                                sum_vector_lengths += len(str(vector_value))
                
                # Update progress periodically
                if monitor and batch_count % 10 == 0:
                    monitor.log(f"   Processed {total_keys:,} keys so far...")
            
            # Check if we're done
            if cursor == 0:
                break
        
        # Calculate totals
        total_data_size = sum_key_lengths + sum_tag_lengths + sum_vector_lengths
        
        # Log results
        if monitor:
            monitor.log("✅ Memory verification complete!")
            monitor.log("─" * 50)
            monitor.log(f"📊 Total keys found: {total_keys:,}")
            monitor.log(f"🔑 Sum of key lengths: {sum_key_lengths:,} bytes ({sum_key_lengths / (1024**2):.2f} MB)")
            monitor.log(f"🏷️  Sum of tag lengths: {sum_tag_lengths:,} bytes ({sum_tag_lengths / (1024**2):.2f} MB)")
            monitor.log(f"📐 Sum of vector lengths: {sum_vector_lengths:,} bytes ({sum_vector_lengths / (1024**2):.2f} MB)")
            monitor.log(f"💾 Total data size: {total_data_size:,} bytes ({total_data_size / (1024**2):.2f} MB)")
            monitor.log("")
            
            # Calculate averages
            if total_keys > 0:
                monitor.log("📈 Average sizes per key:")
                monitor.log(f"   Key name: {sum_key_lengths / total_keys:.1f} bytes")
                monitor.log(f"   Tags field: {sum_tag_lengths / total_keys:.1f} bytes")
                monitor.log(f"   Vector field: {sum_vector_lengths / total_keys:.1f} bytes")
                monitor.log(f"   Total per key: {total_data_size / total_keys:.1f} bytes")
            monitor.log("")
        
        return {
            'total_keys': total_keys,
            'sum_key_lengths': sum_key_lengths,
            'sum_tag_lengths': sum_tag_lengths,
            'sum_vector_lengths': sum_vector_lengths,
            'total_data_size': total_data_size
        }
    
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
    
    def run_benchmark_scenario(self, scenario: BenchmarkScenario, monitor: ProgressMonitor = None, use_async: bool = True) -> Dict:
        """Run a single benchmark scenario with full monitoring"""
        # Use provided monitor or create a new one if none provided
        should_stop_monitor = False
        if monitor is None:
            monitor = ProgressMonitor(self.server, scenario.name)
            monitor.start()
            should_stop_monitor = True
        monitor.log("")
        monitor.log("┏" + "━" * 78 + "┓")
        monitor.log(f"┃ 🚀 STARTING SCENARIO: {scenario.name:<50} ┃")
        monitor.log(f"┃    {scenario.description:<65} ┃")
        monitor.log("┗" + "━" * 78 + "┛")
        monitor.log("")
        
        # Validate memory requirements before starting
        can_run, message = self.validate_memory_requirements(scenario)
        monitor.log(f"📋 Memory Validation: {message}")
        
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
        client_pool = None  # Initialize to None to avoid undefined variable error
        
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
            info_all = client.execute_command("info","memory")
            search_info = client.execute_command("info","modules")
            
            monitor.log("📊 BASELINE MEASUREMENTS")
            monitor.log("─" * 50)
            monitor.log(f"💾 System Memory: {info_all['used_memory_human']}")
            monitor.log(f"🔍 Search Memory: {search_info['search_used_memory_human']}")
            monitor.log(f"📈 Baseline: {baseline_memory // 1024:,} KB")
            monitor.log("")
            
            # Log memory estimates
            estimates = self.estimate_memory_usage(scenario)
            monitor.log("🎯 ESTIMATED USAGE")
            monitor.log("─" * 50)
            monitor.log(f"📁 Data Memory:      {estimates['data_memory'] // (1024**2):,} MB")
            monitor.log(f"🏷️  Tag Index:       {estimates['tag_index_memory'] // (1024**2):,} MB")  
            monitor.log(f"🎯 Vector Index:     {estimates['vector_index_memory'] // (1024**2):,} MB")
            monitor.log(f"💰 Total (overhead): {estimates['total_memory'] // (1024**2):,} MB")
            monitor.log("")
            
            # Create schema and generator
            index_name = f"idx_{scenario.name.lower().replace(' ', '_')}"
            schema = self.create_schema(index_name)
            
            config = HashGeneratorConfig(
                num_keys=scenario.total_keys,
                schema=schema,
                tags_config=scenario.tags_config,
                key_length=LengthConfig(avg=8, min=8, max=8),  # Fixed length keys
                batch_size=100,  # Optimize batch size
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
            if use_async:
                num_tasks = min(100, max(10, scenario.total_keys // config.batch_size))
                monitor.log(f"🚀 Mode: ASYNC I/O with {num_tasks} concurrent tasks")
            else:
                monitor.log(f"🧵 Mode: SYNC with {num_threads} threads")
            monitor.log(f"📦 Batch size: {config.batch_size:,}")
            monitor.log("")
            
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
            
            # Choose insertion method based on use_async parameter
            if use_async:
                # Use async I/O for maximum performance
                monitor.log("🚀 Starting ASYNC data ingestion")
                try:
                    insertion_time = asyncio.run(
                        self.run_async_insertion(scenario, generator, monitor, insertion_start_time, 
                                               keys_processed, dist_collector, config)
                    )
                except Exception as e:
                    monitor.log(f"❌ Async insertion failed, falling back to sync: {e}")
                    use_async = False
            
            if not use_async:
                # Use traditional threaded approach
                monitor.log(f"🧵 Starting SYNC data ingestion: {scenario.total_keys:,} keys using {num_threads} threads")
                
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
                            monitor.log(f"❌ Batch failed: {e}")
                
                insertion_time = time.time() - insertion_start_time
            
            total_keys_inserted = keys_processed.get()
            info_all = client.execute_command("info","memory")
            search_info = client.execute_command("info","modules")
            
            # Clear any ongoing status updates since insertion is complete
            monitor.update_status({})
            
            monitor.log("✅ INSERTION COMPLETE")
            monitor.log("─" * 50)
            monitor.log(f"📊 Keys Inserted: {total_keys_inserted:,}")
            monitor.log(f"⏱️  Time Taken: {insertion_time:.1f}s")
            monitor.log(f"🚀 Speed: {total_keys_inserted/insertion_time:.0f} keys/sec")
            monitor.log(f"💾 System Memory: {info_all['used_memory_human']}")
            monitor.log(f"🔍 Search Memory: {search_info['search_used_memory_human']}")
            monitor.log("")
            
            # Log distribution statistics
            dist_summary = dist_collector.get_summary()
            monitor.log("📈 DATA DISTRIBUTION STATS")
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
                monitor.log("🔍 MEMORY VERIFICATION")
                monitor.log("─" * 50)
                verification_start = time.time()
                memory_verification = self.verify_memory(client, monitor=monitor)
                verification_time = time.time() - verification_start
                monitor.log(f"⏱️  Verification time: {verification_time:.1f}s")
                monitor.log("")
            
            # Export dataset to CSV files for analysis
            hashes_csv = ""
            tags_csv = ""
            if scenario.total_keys <= 100000:  # Only export for smaller datasets to avoid huge files
                monitor.log("📁 DATASET EXPORT")
                monitor.log("─" * 50)
                export_start = time.time()
                
                # Get timestamp from current scenario
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                
                # Create a client without decode_responses for proper binary handling
                export_client = SilentValkeyClient(
                    host=client.connection_pool.connection_kwargs['host'],
                    port=client.connection_pool.connection_kwargs['port'],
                    decode_responses=False  # Important for binary data
                )
                
                try:
                    hashes_csv, tags_csv = self.export_dataset_to_csv(
                        export_client, 
                        prefix="key:",  # Assuming standard prefix
                        scenario_name=scenario.name,
                        timestamp=timestamp,
                        monitor=monitor
                    )
                    export_time = time.time() - export_start
                    monitor.log(f"⏱️  Export time: {export_time:.1f}s")
                finally:
                    assert export_client is not None
                    export_client.close()

                monitor.log("")
            
            # Measure memory after data insertion
            data_memory = client.info("memory")['used_memory']
            data_memory_kb = (data_memory - baseline_memory) // 1024
            
            monitor.log("🏗️  INDEX CREATION PHASE")
            monitor.log("─" * 50)
            monitor.log(f"📊 Data Memory (no index): {data_memory_kb:,} KB")
            monitor.log(f"🏷️  Index Name: {index_name}")
            monitor.log("")
            
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
            search_info = client.execute_command("info","modules")
            search_memory_kb = search_info.get('search_used_memory_bytes', 0) // 1024

            
            # Get distribution statistics first
            dist_stats = dist_collector.get_summary()
            
            # Calculate metrics with improved tag index memory estimation
            vector_memory_kb = (scenario.total_keys * 8 * 4) // 1024  # 8 dim * 4 bytes
            
            # Use comprehensive memory estimation based on all C++ structures
            memory_breakdown = self.calculate_comprehensive_memory(
                scenario.total_keys, 
                dist_stats['tag_usage']['unique_tags'],
                dist_stats['tag_lengths']['mean'],
                dist_stats['tags_per_key']['mean'],
                dist_stats['tag_usage']['mean_keys_per_tag'],
                vector_dims=8
            )
            
            # Extract components for comparison
            estimated_total_kb = memory_breakdown['total_estimated_kb']
            estimated_tag_memory_kb = memory_breakdown['tag_index_kb']
            estimated_vector_memory_kb = memory_breakdown['vector_index_kb']
            
            # Compare with actual search memory usage
            actual_tag_memory_kb = max(0, search_memory_kb - vector_memory_kb)
            tag_index_memory_kb = actual_tag_memory_kb
            
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
                'search_used_memory_kb': search_memory_kb,  # Add missing field for fuzzy test
                'tag_memory_accuracy': (estimated_tag_memory_kb / max(1, actual_tag_memory_kb)) if actual_tag_memory_kb > 0 else 0,
                'total_memory_accuracy': (estimated_total_kb / max(1, search_memory_kb)) if search_memory_kb > 0 else 0,
                'vector_memory_kb': vector_memory_kb,
                'insertion_time': insertion_time,
                'insertion_time_sec': insertion_time,  # Add for fuzzy test compatibility
                'keys_per_second': total_keys_inserted/insertion_time if insertion_time > 0 else 0,  # Add for fuzzy test
                'tags_config': str(scenario.tags_config.sharing.mode.value),
                # Memory breakdown components
                'valkey_core_kb': memory_breakdown['valkey_core_kb'],
                'key_interning_kb': memory_breakdown['key_interning_kb'],
                'search_module_overhead_kb': memory_breakdown['search_module_overhead_kb'],
                'fragmentation_overhead_kb': memory_breakdown['fragmentation_overhead_kb'],
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
            
            monitor.log("🎯 FINAL RESULTS")
            monitor.log("─" * 60)
            monitor.log(f"📁 Data Memory:          {data_memory_kb:,} KB")
            monitor.log(f"🔍 Search Module Total:  {search_memory_kb:,} KB (actual)")
            monitor.log(f"🧮 Search Module Est.:   {estimated_total_kb:,} KB ({(estimated_total_kb/max(1,search_memory_kb)*100):.1f}% accuracy)")
            monitor.log("")
            monitor.log("📊 COMPREHENSIVE MEMORY BREAKDOWN:")
            monitor.log(f"   🗄️  Valkey Core:       {memory_breakdown['valkey_core_kb']:,} KB")
            monitor.log(f"   🔗 Key Interning:     {memory_breakdown['key_interning_kb']:,} KB") 
            monitor.log(f"   🏷️  Tag Index:         {estimated_tag_memory_kb:,} KB (est) vs {actual_tag_memory_kb:,} KB (actual)")
            monitor.log(f"   🎯 Vector Index:      {estimated_vector_memory_kb:,} KB (est) vs {vector_memory_kb:,} KB (calc)")
            monitor.log(f"      • Vector data:     {memory_breakdown['vector_data_kb']:,} KB")
            monitor.log(f"      • HNSW L0 graph:   {memory_breakdown['hnsw_level0_kb']:,} KB ({memory_breakdown['connections_per_node_l0']} conn/node)")
            monitor.log(f"      • HNSW higher:     {memory_breakdown['hnsw_higher_levels_kb']:,} KB")
            monitor.log(f"      • Mappings:        {memory_breakdown['vector_mappings_kb']:,} KB")
            monitor.log(f"   ⚙️  Module Overhead:   {memory_breakdown['search_module_overhead_kb']:,} KB")
            monitor.log(f"   🔀 Fragmentation:     {memory_breakdown['fragmentation_overhead_kb']:,} KB")
            monitor.log("─" * 60)
            monitor.log(f"📈 Index Overhead:       {index_overhead_kb:,} KB")
            monitor.log(f"💰 Total Memory:         {total_memory_kb:,} KB")
            monitor.log("")
            
            # Accuracy analysis
            total_accuracy = estimated_total_kb / max(1, search_memory_kb)
            tag_accuracy = estimated_tag_memory_kb / max(1, actual_tag_memory_kb) if actual_tag_memory_kb > 0 else 0
            
            monitor.log("🎯 ESTIMATION ACCURACY:")
            if total_accuracy > 0:
                if total_accuracy < 0.7:
                    monitor.log(f"   ⚠️  Total: {total_accuracy:.2f}x (underestimating) - missing components?")
                elif total_accuracy > 1.5:
                    monitor.log(f"   ⚠️  Total: {total_accuracy:.2f}x (overestimating) - double counting?")
                else:
                    monitor.log(f"   ✅ Total: {total_accuracy:.2f}x (good overall accuracy)")
            
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
            
            # Cleanup
            client.execute_command("FT.DROPINDEX", index_name)
            
            # Clear index monitoring and status
            monitor.clear_index_names()
            monitor.update_status({})  # Clear any lingering status updates
        
        except Exception as e:
            # Handle any errors during the benchmark
            monitor.error(f"  ❌ Scenario failed: {e}")
            result = {
                'scenario_name': scenario.name,
                'description': scenario.description,
                'total_keys': scenario.total_keys,
                'skipped': True,
                'reason': f"Failed: {str(e)}"
            }
            
        finally:
            # Clean up resources
            if client_pool is not None:
                client_pool.close_all()
            # Only stop monitor if we created it
            if should_stop_monitor:
                monitor.stop()
        
        return result
    
    def create_comprehensive_scenarios(self, base_keys: int = 5000000) -> List[BenchmarkScenario]:
        """Create comprehensive test scenarios"""
        scenarios = []
        
        # 1. Baseline: Unique tags (no sharing)
        scenarios.append(BenchmarkScenario(
            name="Baseline_Unique",
            total_keys=base_keys,
            tags_config=TagsConfig(
                num_keys=base_keys,
                tags_per_key=TagDistribution(avg=5, min=3, max=8),
                tag_length=LengthConfig(avg=200, min=10, max=300),
                sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
            ),
            description="Unique tags per key (no sharing)"
        ))
        
        # 2. Single unique tag per key
        scenarios.append(BenchmarkScenario(
            name="Single_Unique_Tag",
            total_keys=base_keys,
            tags_config=TagsConfig(
                num_keys=base_keys,
                tags_per_key=TagDistribution(avg=1, min=1, max=1),  # Exactly 1 tag per key
                tag_length=LengthConfig(avg=200, min=10, max=300),
                sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
            ),
            description="1 unique tag per key"
        ))
        
        # 3. Perfect overlap (all keys share same tags)
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
        
        # 4. Shared pool with varying pool sizes
        for pool_size in [100, 100000]:
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
        
        # 5. Group-based sharing (realistic scenarios)
        for group_size in [100, 100000]:
            scenarios.append(BenchmarkScenario(
                name=f"GroupBased_{group_size}",
                total_keys=base_keys,
                tags_config=TagsConfig(
                    num_keys=base_keys,
                    tags_per_key=TagDistribution(avg=5, min=3, max=8),
                    tag_length=LengthConfig(avg=200, min=10, max=3000),
                    sharing=TagSharingConfig(
                        mode=TagSharingMode.GROUP_BASED,
                        keys_per_group=group_size,
                        tags_per_group=2000
                    )
                ),
                description=f"Groups of {group_size} keys sharing 20 tags"
            ))
        
        # 6. Prefix sharing variations
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
        
        # 7. Tag count variations
        for avg_tags in [10, 50]:
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
        
        # 8. Tag length variations
        for tag_len in [64, 200]:
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
        
        # 9. Distribution variations
        for dist in [Distribution.UNIFORM, Distribution.NORMAL, Distribution.ZIPF]:
            scenarios.append(BenchmarkScenario(
                name=f"Distribution_{dist.value}",
                total_keys=base_keys,
                tags_config=TagsConfig(
                    num_keys=base_keys,
                    tags_per_key=TagDistribution(avg=25, min=1, max=100, distribution=dist),
                    tag_length=LengthConfig(avg=40, min=10, max=300),
                    sharing=TagSharingConfig(mode=TagSharingMode.SHARED_POOL, pool_size=5000)
                ),
                description=f"{dist.value} distribution of tags per key"
            ))
        
        return scenarios
    
    def test_comprehensive_memory_benchmark(self, use_async: bool = True):
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
        scenarios = self.create_comprehensive_scenarios(base_keys=10000000)
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
                result = self.run_benchmark_scenario(scenario, monitor, use_async)
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
        monitor.log("")
        monitor.log("┏" + "━" * 118 + "┓")
        monitor.log("┃" + " " * 35 + "📊 COMPREHENSIVE BENCHMARK RESULTS" + " " * 48 + "┃")
        monitor.log("┗" + "━" * 118 + "┛")
        monitor.log("")
        
        monitor.log(f"{'Scenario':<30} {'Keys':>8} {'DataKB':>10} {'TagIdxKB':>10} {'TotalKB':>10} {'Time(s)':>8} {'Mode':<15}")
        monitor.log("─" * 120)
        
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
        monitor.log("")
        monitor.log("┏" + "━" * 78 + "┓")
        monitor.log("┃" + " " * 32 + "🔍 KEY FINDINGS" + " " * 31 + "┃")
        monitor.log("┗" + "━" * 78 + "┛")
        monitor.log("")
        
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
    
    def test_quick_memory_benchmark(self, use_async: bool = True):
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
                name="Quick_SingleUnique",
                total_keys=10000,
                tags_config=TagsConfig(
                    num_keys=10000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),  # Exactly 1 tag per key
                    tag_length=LengthConfig(avg=20, min=10, max=30),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description="1 unique tag per key"
            ),
            BenchmarkScenario(
                name="Quick_Unique",
                total_keys=10000,
                tags_config=TagsConfig(
                    num_keys=10000,
                    tags_per_key=TagDistribution(avg=5, min=3, max=8),
                    tag_length=LengthConfig(avg=20, min=10, max=30),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description="Unique tags baseline (5 tags/key)"
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
                result = self.run_benchmark_scenario(scenario, monitor, use_async)
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
        monitor.log("")
        monitor.log("┏" + "━" * 78 + "┓")
        monitor.log("┃" + " " * 28 + "⚡ QUICK BENCHMARK SUMMARY" + " " * 24 + "┃")
        monitor.log("┗" + "━" * 78 + "┛")
        monitor.log("")
        for r in results:
            if r.get('skipped'):
                monitor.log(f"{r['scenario_name']:<20}: SKIPPED - {r.get('reason', 'Unknown')}")
            else:
                monitor.log(f"{r['scenario_name']:<20}: {r['tag_index_memory_kb']:>8,} KB tag index")
        
        monitor.log(f"Results saved to {csv_filename}")
        monitor.log(f"Log file: {log_file}")
        
        # Stop the main monitor
        monitor.stop()
    
    def test_async_performance_comparison(self):
        """Compare async vs sync performance on the same scenario"""
        # Set up file logging
        log_file = self.setup_file_logging("performance_comparison")
        monitor = ProgressMonitor(self.server, "performance_comparison")
        monitor.start()
        
        monitor.log("")
        monitor.log("┏" + "━" * 78 + "┓")
        monitor.log("┃" + " " * 25 + "🏁 ASYNC vs SYNC PERFORMANCE TEST" + " " * 18 + "┃")
        monitor.log("┗" + "━" * 78 + "┛")
        monitor.log("")
        
        # Create a test scenario
        test_scenario = BenchmarkScenario(
            name="Performance_Test",
            total_keys=50000,  # Medium-sized test
            tags_config=TagsConfig(
                num_keys=50000,
                tags_per_key=TagDistribution(avg=5, min=3, max=8),
                tag_length=LengthConfig(avg=20, min=10, max=30),
                sharing=TagSharingConfig(mode=TagSharingMode.SHARED_POOL, pool_size=1000)
            ),
            description="Async vs Sync performance comparison"
        )
        
        results = []
        
        # Test ASYNC mode
        monitor.log("🚀 TESTING ASYNC MODE")
        monitor.log("─" * 50)
        try:
            async_result = self.run_benchmark_scenario(test_scenario, monitor, use_async=True)
            async_result['mode'] = 'ASYNC'
            results.append(async_result)
        except Exception as e:
            monitor.log(f"❌ Async test failed: {e}")
        
        monitor.log("")
        
        # Test SYNC mode  
        monitor.log("🧵 TESTING SYNC MODE")
        monitor.log("─" * 50)
        try:
            sync_result = self.run_benchmark_scenario(test_scenario, monitor, use_async=False)
            sync_result['mode'] = 'SYNC'
            results.append(sync_result)
        except Exception as e:
            monitor.log(f"❌ Sync test failed: {e}")
        
        # Compare results
        if len(results) == 2:
            async_time = results[0]['insertion_time']
            sync_time = results[1]['insertion_time']
            
            improvement = ((sync_time - async_time) / sync_time) * 100
            speed_ratio = sync_time / async_time
            
            monitor.log("")
            monitor.log("┏" + "━" * 78 + "┓")
            monitor.log("┃" + " " * 30 + "📊 PERFORMANCE COMPARISON" + " " * 22 + "┃")
            monitor.log("┗" + "━" * 78 + "┛")
            monitor.log("")
            monitor.log(f"🚀 Async Time:  {async_time:.2f}s")
            monitor.log(f"🧵 Sync Time:   {sync_time:.2f}s")
            monitor.log(f"📈 Improvement: {improvement:+.1f}% ({'faster' if improvement > 0 else 'slower'})")
            monitor.log(f"⚡ Speed Ratio: {speed_ratio:.2f}x")
            
            if improvement > 0:
                monitor.log(f"🏆 ASYNC IS FASTER by {improvement:.1f}%!")
            else:
                monitor.log(f"🐌 ASYNC IS SLOWER by {abs(improvement):.1f}%")
        
        monitor.log(f"\nLog file: {log_file}")
        monitor.stop()
    
    def test_stall_detection_example(self):
        """Example test showing how to use stall detection"""
        client = self.server.get_new_client()
        monitor = ProgressMonitor(self.server, "stall_detection_test")
        
        # Define what to do when stall detected
        def on_stall_detected(monitor_instance):
            monitor_instance.log("🚨 STALL HANDLER: Taking recovery action!")
            # You could:
            # - Stop the test gracefully
            # - Try to recover from the stall
            # - Send alerts
            # - Kill stuck processes
            # - etc.
        
        # Configure stall detection: alert after 10 seconds of no activity
        monitor.set_stall_detection(
            enabled=True, 
            threshold_seconds=10,
            callback=on_stall_detected
        )
        
        monitor.start()
        
        try:
            monitor.log("Starting stall detection test...")
            
            # Insert some data
            for i in range(100):
                client.hset(f"key:{i}", mapping={"data": f"value{i}"})
                time.sleep(0.1)
            
            monitor.log("Data insertion complete, now waiting...")
            
            # Example: Manually trigger diagnostics after 5 seconds
            time.sleep(5)
            monitor.collect_diagnostics("Pre-stall check")
            
            # Wait for stall detection to trigger (will show full diagnostics)
            time.sleep(15)
            
        finally:
            monitor.stop()
            client.flushdb()
    
    def test_csv_export_functionality(self):
        """Test the CSV export functionality with a small dataset"""
        client = self.server.get_new_client()
        monitor = ProgressMonitor(self.server, "csv_export_test")
        
        monitor.start()
        
        try:
            monitor.log("=== TESTING CSV EXPORT FUNCTIONALITY ===")
            
            # Clear any existing data
            client.flushdb()
            
            # Create test data with known structure
            test_data = [
                ("key:test1", {"tags": "tag_a,tag_b,tag_c", "vector": b"\x00\x01\x02\x03\x04\x05\x06\x07"}),
                ("key:test2", {"tags": "tag_b,tag_c,tag_d", "vector": b"\x10\x11\x12\x13\x14\x15\x16\x17"}),
                ("key:test3", {"tags": "tag_a,tag_d", "vector": b"\x20\x21\x22\x23\x24\x25\x26\x27"}),
                ("key:test4", {"tags": "tag_e,tag_f", "vector": b"\x30\x31\x32\x33\x34\x35\x36\x37"}),
            ]
            
            # Insert test data
            monitor.log("Inserting test data...")
            for key, fields in test_data:
                client.hset(key, mapping=fields)
            
            # Test CSV export
            monitor.log("Testing CSV export...")
            
            # Create a client without decode_responses for proper binary handling
            export_client = SilentValkeyClient(
                host=client.connection_pool.connection_kwargs['host'],
                port=client.connection_pool.connection_kwargs['port'],
                decode_responses=False
            )
            
            try:
                hashes_csv, tags_csv = self.export_dataset_to_csv(
                    export_client,
                    prefix="key:",
                    scenario_name="test_export",
                    timestamp="test_123",
                    monitor=monitor
                )
                
                # Verify files were created
                import os
                assert os.path.exists(hashes_csv), f"Hashes CSV not created: {hashes_csv}"
                assert os.path.exists(tags_csv), f"Tags CSV not created: {tags_csv}"
                
                # Check hashes CSV content
                import csv
                with open(hashes_csv, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    hash_rows = list(reader)
                    
                assert len(hash_rows) == 4, f"Expected 4 hash rows, got {len(hash_rows)}"
                
                # Check tags CSV content  
                with open(tags_csv, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    tag_rows = list(reader)
                    
                # Should have unique tags: tag_a, tag_b, tag_c, tag_d, tag_e, tag_f
                assert len(tag_rows) == 6, f"Expected 6 unique tags, got {len(tag_rows)}"
                
                monitor.log("✅ CSV export test passed!")
                
                # Log some sample data
                monitor.log(f"Sample hash row: {hash_rows[0]}")
                monitor.log(f"Sample tag row: {tag_rows[0]}")
                
                # Cleanup test files
                os.remove(hashes_csv)
                os.remove(tags_csv)
                monitor.log("🧹 Test files cleaned up")
                
            finally:
                export_client.close()
            
        finally:
            client.flushdb()
            monitor.stop()
    
    def test_verify_memory_function(self):
        """Test the verify_memory function with a small dataset"""
        # Use the regular client which has decode_responses=True
        client = self.server.get_new_client()
        monitor = ProgressMonitor(self.server, "verify_memory_test")
        log_file = self.setup_file_logging("verify_memory_test")
        
        monitor.start()
        
        monitor.log("=== TESTING VERIFY MEMORY FUNCTION ===")
        monitor.log("Creating a small test dataset to verify memory calculation")
        monitor.log("")
        
        # Clear any existing data
        client.flushdb()
        
        # Create test data with known sizes
        test_keys = [
            ("key:test1", {"tags": "tag1,tag2,tag3", "vector": b"\x00\x01\x02\x03\x04\x05\x06\x07"}),
            ("key:test2", {"tags": "tag2,tag3,tag4,tag5", "vector": b"\x10\x11\x12\x13\x14\x15\x16\x17"}),
            ("key:test3", {"tags": "tag1,tag4", "vector": b"\x20\x21\x22\x23\x24\x25\x26\x27"}),
        ]
        
        # Insert test data
        monitor.log("Inserting test data...")
        for key, fields in test_keys:
            client.hset(key, mapping=fields)
        
        # Debug: Check what's actually stored
        monitor.log("Debug: Checking stored data...")
        for key, _ in test_keys:
            stored_data = client.hgetall(key)
            monitor.log(f"  {key}: {stored_data}")
        
        # Run verification - need to create a new client without decode_responses for binary data
        monitor.log("")
        # Create a client specifically for verify_memory that doesn't decode responses
        verify_client = SilentValkeyClient(
            host=client.connection_pool.connection_kwargs['host'],
            port=client.connection_pool.connection_kwargs['port'],
            decode_responses=False  # Important: Don't decode for binary data
        )
        results = self.verify_memory(verify_client, monitor=monitor)
        verify_client.close()
        
        # Validate results
        monitor.log("📊 VALIDATION")
        monitor.log("─" * 50)
        
        # Expected values
        expected_keys = len(test_keys)
        expected_key_lengths = sum(len(k) for k, _ in test_keys)
        expected_tag_lengths = sum(len(f["tags"]) for _, f in test_keys)
        expected_vector_lengths = sum(len(f["vector"]) for _, f in test_keys)
        
        monitor.log(f"Expected keys: {expected_keys}, Found: {results['total_keys']}")
        monitor.log(f"Expected key lengths: {expected_key_lengths}, Found: {results['sum_key_lengths']}")
        monitor.log(f"Expected tag lengths: {expected_tag_lengths}, Found: {results['sum_tag_lengths']}")
        monitor.log(f"Expected vector lengths: {expected_vector_lengths}, Found: {results['sum_vector_lengths']}")
        
        # Assert correctness
        assert results['total_keys'] == expected_keys, f"Key count mismatch"
        assert results['sum_key_lengths'] == expected_key_lengths, f"Key length sum mismatch"
        assert results['sum_tag_lengths'] == expected_tag_lengths, f"Tag length sum mismatch"
        assert results['sum_vector_lengths'] == expected_vector_lengths, f"Vector length sum mismatch"
        
        monitor.log("")
        monitor.log("✅ All validations passed!")
        monitor.log(f"Log file: {log_file}")
        
        # Cleanup
        client.flushdb()
        monitor.stop()
    
    def test_fuzzy_memory_estimation(self, use_async: bool = True):
        """Comprehensive fuzzy testing of memory estimation with 1-10M keys"""
        import random
        import statistics
        from enum import Enum
        
        # Set up file logging
        log_file = self.setup_file_logging("fuzzy_estimation")
        monitor = ProgressMonitor(self.server, "fuzzy_estimation")
        monitor.start()
        
        monitor.log("🎲 FUZZY MEMORY ESTIMATION TESTING")
        monitor.log("=" * 80)
        monitor.log("Testing memory estimation accuracy with realistic large datasets")
        monitor.log("Comparing actual memory usage vs our estimation functions")
        monitor.log("")
        
        # CSV setup
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f"fuzzy_estimation_results_{timestamp}.csv"
        
        # Define fuzzy test patterns with more reasonable sizes
        # Start smaller and scale up based on what works
        fuzzy_patterns = [
            {
                'name': 'Medium_NoSharing_500K',
                'total_keys': 500000,
                'tags_config': TagsConfig(
                    num_keys=500000,
                    tags_per_key=TagDistribution(avg=3, min=2, max=4),
                    tag_length=LengthConfig(avg=25, min=20, max=30),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                'description': '500K keys, unique tags (no sharing), 3 tags/key'
            },
            {
                'name': 'Large_SharedPool_10M',
                'total_keys': 10000000,
                'tags_config': TagsConfig(
                    num_keys=10000000,
                    tags_per_key=TagDistribution(avg=5, min=3, max=700),
                    tag_length=LengthConfig(avg=300, min=25, max=905),
                    sharing=TagSharingConfig(
                        mode=TagSharingMode.SHARED_POOL,
                        pool_size=10000,
                        reuse_probability=0.3
                    )
                ),
                'description': '1M keys, shared pool (10K unique tags), 5 tags/key'
            },
            {
                'name': 'XLarge_GroupBased_2M',
                'total_keys': 2000000,
                'tags_config': TagsConfig(
                    num_keys=2000000,
                    tags_per_key=TagDistribution(avg=4, min=2, max=6),
                    tag_length=LengthConfig(avg=35, min=30, max=40),
                    sharing=TagSharingConfig(
                        mode=TagSharingMode.GROUP_BASED,
                        keys_per_group=50000,
                        tags_per_group=25
                    )
                ),
                'description': '2M keys, group-based sharing (50K keys/group), 4 tags/key'
            },
            {
                'name': 'HighSharing_1M',
                'total_keys': 1000000,
                'tags_config': TagsConfig(
                    num_keys=1000000,
                    tags_per_key=TagDistribution(avg=6, min=4, max=8),
                    tag_length=LengthConfig(avg=20, min=15, max=25),
                    sharing=TagSharingConfig(
                        mode=TagSharingMode.SHARED_POOL,
                        pool_size=1000,
                        reuse_probability=0.9
                    )
                ),
                'description': '1M keys, high sharing (1K unique tags), 6 tags/key'
            },
            {
                'name': 'Zipf_Distribution_1M',
                'total_keys': 1000000,
                'tags_config': TagsConfig(
                    num_keys=1000000,
                    tags_per_key=TagDistribution(avg=5, min=1, max=20, distribution=Distribution.ZIPF),
                    tag_length=LengthConfig(avg=40, min=20, max=80),
                    sharing=TagSharingConfig(
                        mode=TagSharingMode.SHARED_POOL,
                        pool_size=5000,
                        reuse_probability=0.8
                    )
                ),
                'description': '1M keys, zipf distribution, variable tags (1-20), mixed lengths'
            }
        ]
        
        fuzzy_results = []
        
        for i, pattern in enumerate(fuzzy_patterns):
            monitor.log(f"--- Fuzzy Pattern {i+1}/{len(fuzzy_patterns)}: {pattern['name']} ---")
            monitor.log(f"  📝 {pattern['description']}")
            
            # Create benchmark scenario
            scenario = BenchmarkScenario(
                name=pattern['name'],
                total_keys=pattern['total_keys'],
                tags_config=pattern['tags_config'],
                description=pattern['description']
            )
            
            try:
                # Validate memory requirements
                can_run, reason = self.validate_memory_requirements(scenario)
                if not can_run:
                    monitor.log(f"  ⚠️  Skipping: {reason}")
                    fuzzy_results.append({
                        'scenario_name': pattern['name'],
                        'description': pattern['description'],
                        'total_keys': pattern['total_keys'],
                        'skipped': True,
                        'reason': reason,
                        'estimated_memory_kb': 0,
                        'actual_memory_kb': 0,
                        'accuracy_ratio': 0
                    })
                    continue
                
                monitor.log(f"  ✅ Memory validation passed")
                
                # Get memory estimation BEFORE running the test
                estimated_stats = self._calculate_estimated_stats(scenario)
                estimated_memory = self.calculate_comprehensive_memory(
                    total_keys=scenario.total_keys,
                    unique_tags=estimated_stats['unique_tags'],
                    avg_tag_length=estimated_stats['avg_tag_length'],
                    avg_tags_per_key=estimated_stats['avg_tags_per_key'],
                    avg_keys_per_tag=estimated_stats['avg_keys_per_tag'],
                    vector_dims=8,
                    hnsw_m=16
                )
                
                monitor.log(f"  📊 Estimated total memory: {estimated_memory['total_estimated_kb']:,} KB")
                monitor.log(f"     • Tag index: {estimated_memory['tag_index_kb']:,} KB")
                monitor.log(f"     • Vector index: {estimated_memory['vector_index_kb']:,} KB")
                
                # Run the actual benchmark
                monitor.log(f"  🚀 Running actual benchmark...")
                benchmark_result = self.run_benchmark_scenario(scenario, monitor, use_async)
                
                if benchmark_result.get('skipped'):
                    fuzzy_results.append({
                        'scenario_name': pattern['name'],
                        'description': pattern['description'],
                        'total_keys': pattern['total_keys'],
                        'skipped': True,
                        'reason': benchmark_result.get('reason', 'Unknown'),
                        'estimated_memory_kb': estimated_memory['total_estimated_kb'],
                        'actual_memory_kb': 0,
                        'accuracy_ratio': 0
                    })
                    continue
                
                # Extract actual memory usage
                actual_memory_kb = benchmark_result.get('search_used_memory_kb', 0)
                
                # Calculate accuracy
                if actual_memory_kb > 0:
                    accuracy_ratio = estimated_memory['total_estimated_kb'] / actual_memory_kb
                else:
                    accuracy_ratio = 0
                
                monitor.log(f"  📈 Actual memory usage: {actual_memory_kb:,} KB")
                monitor.log(f"  🎯 Accuracy ratio: {accuracy_ratio:.2f}x")
                
                if 0.5 <= accuracy_ratio <= 2.0:
                    monitor.log(f"  ✅ Good accuracy (within 2x)")
                elif accuracy_ratio < 0.5:
                    monitor.log(f"  ⚠️  Underestimation (>{accuracy_ratio:.1f}x too low)")
                else:
                    monitor.log(f"  ⚠️  Overestimation ({accuracy_ratio:.1f}x too high)")
                
                # Store detailed results
                fuzzy_result = {
                    'scenario_name': pattern['name'],
                    'description': pattern['description'],
                    'total_keys': pattern['total_keys'],
                    'skipped': False,
                    'estimated_memory_kb': estimated_memory['total_estimated_kb'],
                    'estimated_tag_index_kb': estimated_memory['tag_index_kb'],
                    'estimated_vector_index_kb': estimated_memory['vector_index_kb'],
                    'actual_memory_kb': actual_memory_kb,
                    'accuracy_ratio': accuracy_ratio,
                    'insertion_time_sec': benchmark_result.get('insertion_time_sec', 0),
                    'keys_per_second': benchmark_result.get('keys_per_second', 0),
                    'unique_tags': estimated_stats['unique_tags'],
                    'avg_tags_per_key': estimated_stats['avg_tags_per_key'],
                    'avg_tag_length': estimated_stats['avg_tag_length']
                }
                fuzzy_results.append(fuzzy_result)
                
                # Write to CSV incrementally
                self.append_fuzzy_to_csv(csv_filename, fuzzy_result, monitor, write_header=(i == 0))
                
            except Exception as e:
                monitor.log(f"  ❌ Pattern failed: {e}")
                fuzzy_results.append({
                    'scenario_name': pattern['name'],
                    'description': pattern['description'],
                    'total_keys': pattern['total_keys'],
                    'skipped': True,
                    'reason': f"Error: {str(e)}",
                    'estimated_memory_kb': 0,
                    'actual_memory_kb': 0,
                    'accuracy_ratio': 0
                })
        
        # Analyze fuzzy results
        self._analyze_fuzzy_results(fuzzy_results, monitor)
        
        monitor.log(f"\n📁 Fuzzy results CSV: {csv_filename}")
        monitor.log(f"📝 Log file: {log_file}")
        monitor.stop()
    
    def _calculate_estimated_stats(self, scenario: BenchmarkScenario) -> dict:
        """Calculate estimated statistics for a scenario"""
        tags_config = scenario.tags_config
        
        # Calculate average tags per key
        avg_tags_per_key = tags_config.tags_per_key.avg
        
        # Calculate unique tags based on sharing mode
        if tags_config.sharing.mode == TagSharingMode.UNIQUE:
            unique_tags = scenario.total_keys * avg_tags_per_key
        elif tags_config.sharing.mode == TagSharingMode.PERFECT_OVERLAP:
            unique_tags = avg_tags_per_key  # All keys share same tags
        elif tags_config.sharing.mode == TagSharingMode.SHARED_POOL:
            unique_tags = tags_config.sharing.pool_size
        elif tags_config.sharing.mode == TagSharingMode.GROUP_BASED:
            num_groups = scenario.total_keys // tags_config.sharing.keys_per_group
            unique_tags = num_groups * tags_config.sharing.tags_per_group
        else:
            unique_tags = scenario.total_keys * avg_tags_per_key * 0.3  # Default estimate
        
        # Calculate keys per tag
        total_tag_instances = scenario.total_keys * avg_tags_per_key
        avg_keys_per_tag = total_tag_instances / max(1, unique_tags)
        
        return {
            'unique_tags': int(unique_tags),
            'avg_tags_per_key': avg_tags_per_key,
            'avg_keys_per_tag': avg_keys_per_tag,
            'avg_tag_length': tags_config.tag_length.avg
        }
    
    def append_fuzzy_to_csv(self, filename: str, result: dict, monitor, write_header=False):
        """Append fuzzy test result to CSV file"""
        import csv
        
        fieldnames = [
            'scenario_name', 'description', 'total_keys', 'skipped', 'reason',
            'estimated_memory_kb', 'estimated_tag_index_kb', 'estimated_vector_index_kb',
            'actual_memory_kb', 'accuracy_ratio', 'insertion_time_sec', 'keys_per_second',
            'unique_tags', 'avg_tags_per_key', 'avg_tag_length'
        ]
        
        try:
            mode = 'w' if write_header else 'a'
            with open(filename, mode, newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                if write_header:
                    writer.writeheader()
                
                # Fill missing fields with defaults
                csv_result = {field: result.get(field, '') for field in fieldnames}
                writer.writerow(csv_result)
                
        except Exception as e:
            monitor.log(f"⚠️ CSV write failed: {e}")
    
    def _analyze_fuzzy_results(self, results: list, monitor):
        """Analyze and summarize fuzzy test results"""
        monitor.log("")
        monitor.log("🎯 FUZZY ESTIMATION ANALYSIS")
        monitor.log("=" * 80)
        
        # Filter non-skipped results
        valid_results = [r for r in results if not r.get('skipped', True) and r.get('accuracy_ratio', 0) > 0]
        
        if not valid_results:
            monitor.log("❌ No valid results to analyze")
            return
        
        # Calculate statistics
        accuracies = [r['accuracy_ratio'] for r in valid_results]
        mean_accuracy = statistics.mean(accuracies)
        median_accuracy = statistics.median(accuracies)
        min_accuracy = min(accuracies)
        max_accuracy = max(accuracies)
        
        # Count accuracy ranges
        excellent = sum(1 for a in accuracies if 0.8 <= a <= 1.2)  # Within ±20%
        good = sum(1 for a in accuracies if 0.5 <= a <= 2.0)       # Within 2x
        poor = len(accuracies) - good
        
        monitor.log(f"📊 Patterns Analyzed: {len(valid_results)}")
        monitor.log(f"📈 Mean Accuracy: {mean_accuracy:.2f}x")
        monitor.log(f"📊 Median Accuracy: {median_accuracy:.2f}x")
        monitor.log(f"📉 Range: {min_accuracy:.2f}x - {max_accuracy:.2f}x")
        monitor.log("")
        monitor.log(f"✅ Excellent (±20%): {excellent}/{len(valid_results)} ({excellent/len(valid_results):.1%})")
        monitor.log(f"🟡 Good (within 2x): {good}/{len(valid_results)} ({good/len(valid_results):.1%})")
        monitor.log(f"🔴 Poor (>2x off): {poor}/{len(valid_results)} ({poor/len(valid_results):.1%})")
        monitor.log("")
        
        # Best and worst cases
        best = max(valid_results, key=lambda r: 1/abs(r['accuracy_ratio'] - 1.0))
        worst = min(valid_results, key=lambda r: 1/abs(r['accuracy_ratio'] - 1.0))
        
        monitor.log(f"🏆 Best Case: {best['scenario_name']} ({best['accuracy_ratio']:.2f}x)")
        monitor.log(f"   {best['description']}")
        monitor.log(f"   Estimated: {best['estimated_memory_kb']:,} KB, Actual: {best['actual_memory_kb']:,} KB")
        monitor.log("")
        monitor.log(f"⚠️  Worst Case: {worst['scenario_name']} ({worst['accuracy_ratio']:.2f}x)")
        monitor.log(f"   {worst['description']}")
        monitor.log(f"   Estimated: {worst['estimated_memory_kb']:,} KB, Actual: {worst['actual_memory_kb']:,} KB")
        monitor.log("")
        
        # Overall assessment
        if mean_accuracy >= 0.8 and mean_accuracy <= 1.2 and excellent/len(valid_results) >= 0.6:
            monitor.log("🎉 EXCELLENT: Memory estimation is highly accurate!")
        elif mean_accuracy >= 0.5 and mean_accuracy <= 2.0 and good/len(valid_results) >= 0.7:
            monitor.log("✅ GOOD: Memory estimation is reasonably accurate")
        else:
            monitor.log("⚠️ NEEDS IMPROVEMENT: Memory estimation accuracy should be enhanced")
        
        monitor.log("")