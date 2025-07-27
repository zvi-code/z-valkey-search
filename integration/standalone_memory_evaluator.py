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

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logging.warning("psutil not available - memory validation will use fallback method")

from valkey import ResponseError
from valkey.client import Valkey
from valkey.asyncio import Valkey as AsyncValkey
import subprocess
import tempfile
import shutil

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


# Helper function to safely get values from info dictionaries
def safe_get(info_dict: Dict[str, Any], key: str, default: Any = 0) -> Any:
    """Safely get a value from an info dictionary with a default if key is missing."""
    if info_dict is None:
        return default
    return info_dict.get(key, default)


def get_key_count_from_db_info(db_info: Dict[str, Any]) -> int:
    """Extract key count from db info, handling different formats."""
    if not db_info or 'db0' not in db_info:
        return 0
    
    db0 = db_info['db0']
    
    # Handle dict format
    if isinstance(db0, dict) and 'keys' in db0:
        return db0['keys']
    
    # Handle string format like "keys=123,expires=0,avg_ttl=0"
    if isinstance(db0, str) and 'keys=' in db0:
        try:
            keys_part = db0.split(',')[0]
            return int(keys_part.split('=')[1])
        except (ValueError, IndexError):
            return 0
    
    return 0


def get_memory_info_summary(client, state_name: str = "") -> Dict[str, Any]:
    """Get comprehensive memory information from client."""
    info_all = client.execute_command("info", "memory")
    search_info = client.execute_command("info", "modules")
    memory_info = client.info("memory")
    
    return {
        'info_all': info_all,
        'search_info': search_info,
        'memory_info': memory_info,
        'used_memory': safe_get(memory_info, 'used_memory', 0),
        'used_memory_kb': safe_get(memory_info, 'used_memory', 0) // 1024,
        'used_memory_human': safe_get(info_all, 'used_memory_human', 'N/A'),
        'search_used_memory_human': safe_get(search_info, 'search_used_memory_human', 'N/A'),
        'search_used_memory': safe_get(search_info, 'search_used_memory', 0),
        'search_used_memory_kb': safe_get(search_info, 'search_used_memory', 0) // 1024,
        'used_memory_dataset': safe_get(info_all, 'used_memory_dataset', 0)
    }


def log_memory_info(monitor, memory_info: Dict[str, Any], state_name: str = ""):
    """Log memory information using a monitor."""
    if state_name:
        monitor.log(f"{state_name}:used_memory_human={memory_info['used_memory_human']}")
        monitor.log(f"{state_name}:search_used_memory_human={memory_info['search_used_memory_human']}")
    else:
        monitor.log(f"💾 System Memory: {memory_info['used_memory_human']}")
        monitor.log(f"🔍 Search Memory: {memory_info['search_used_memory_human']}")


def parse_ft_info(ft_info_response) -> Dict[str, Any]:
    """Parse FT.INFO response into a dictionary."""
    index_data = {}
    for i in range(0, len(ft_info_response), 2):
        if i + 1 < len(ft_info_response):
            key = ft_info_response[i].decode() if isinstance(ft_info_response[i], bytes) else str(ft_info_response[i])
            value = ft_info_response[i + 1]
            if isinstance(value, bytes):
                try:
                    value = value.decode()
                except:
                    pass
            index_data[key] = value
    return index_data


def calculate_progress_stats(processed_count: int, total_items: int, elapsed_time: float) -> Dict[str, Any]:
    """Calculate progress statistics for monitoring."""
    progress_pct = (processed_count / total_items) * 100 if total_items > 0 else 0
    items_per_sec = processed_count / elapsed_time if elapsed_time > 0 else 0
    eta_seconds = (total_items - processed_count) / items_per_sec if items_per_sec > 0 else 0
    eta_str = f"{eta_seconds/60:.1f}m" if eta_seconds > 60 else f"{eta_seconds:.0f}s"
    
    return {
        'progress_pct': progress_pct,
        'items_per_sec': items_per_sec,
        'eta_str': eta_str,
        'progress_text': f"{processed_count:,}/{total_items:,} ({progress_pct:.1f}%)",
        'speed_text': f"{items_per_sec:.0f} items/sec"
    }


def get_index_state_info(ft_info_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Extract index state information from FT.INFO response."""
    return {
        'num_docs': int(ft_info_dict.get('num_docs', 0)),
        'mutation_queue_size': int(ft_info_dict.get('mutation_queue_size', 0)),
        'backfill_in_progress': int(ft_info_dict.get('backfill_in_progress', 0)),
        'state': ft_info_dict.get('state', 'unknown'),
        'is_indexing': int(ft_info_dict.get('mutation_queue_size', 0)) > 0 or int(ft_info_dict.get('backfill_in_progress', 0)) > 0
    }


def create_silent_client_from_server(server) -> 'SilentValkeyClient':
    """Create a silent Valkey client from a server instance."""
    if hasattr(server, 'get_new_client'):
        # Temporarily suppress logging while getting connection params
        original_level = logging.getLogger().level
        logging.getLogger().setLevel(logging.CRITICAL)
        
        try:
            # Get a regular client first to extract connection params
            temp_client = server.get_new_client()
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


async def create_silent_async_client_from_server(server) -> 'AsyncSilentValkeyClient':
    """Create a silent async Valkey client from a server instance."""
    if hasattr(server, 'get_new_client'):
        # Temporarily suppress logging while getting connection params
        original_level = logging.getLogger().level
        logging.getLogger().setLevel(logging.CRITICAL)
        
        try:
            # Get connection params from sync client
            temp_client = server.get_new_client()
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


def write_csv_data(filename: str, data: List[Dict[str, Any]], fieldnames: Optional[List[str]] = None):
    """Write data to CSV file with proper error handling."""
    import csv
    
    if not data:
        return
    
    # Auto-detect fieldnames if not provided
    if fieldnames is None:
        fieldnames = list(data[0].keys()) if data else []
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
    except Exception as e:
        logging.error(f"Failed to write CSV file {filename}: {e}")
        raise


def silence_valkey_loggers():
    """Temporarily silence valkey-related loggers and return restoration function."""
    import sys
    import io
    
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
    
    # Replace root handlers with null handler
    logging.root.handlers = [null_handler]
    
    # Redirect stdout/stderr to null
    sys.stdout = io.StringIO()
    sys.stderr = io.StringIO()
    
    def restore_loggers():
        """Restore original logging configuration."""
        # Restore stdout/stderr
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        
        # Restore root handlers
        logging.root.handlers = original_root_handlers
        
        # Restore logger levels
        for logger_name, level in original_levels.items():
            logging.getLogger(logger_name).setLevel(level)
    
    return restore_loggers


class SilentValkeyClient(Valkey):
    """
    A Valkey client wrapper that suppresses client creation logs.
    Intercepts and redirects specific log messages to avoid cluttering test logs.
    """
    
    def __init__(self, *args, **kwargs):
        """Initialize client with suppressed logging"""
        restore_loggers = silence_valkey_loggers()
        
        try:
            # Create the client
            super().__init__(*args, **kwargs)
        finally:
            restore_loggers()


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
        return await create_silent_async_client_from_server(self.server)
    
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
        return create_silent_client_from_server(self.server)
    
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
    # Vector configuration
    vector_dim: int = 8
    vector_algorithm: VectorAlgorithm = VectorAlgorithm.HNSW
    vector_metric: VectorMetric = VectorMetric.COSINE
    hnsw_m: int = 16  # HNSW M parameter (number of connections)
    # Numeric configuration  
    include_numeric: bool = True
    numeric_fields: Dict[str, Tuple[float, float]] = field(default_factory=lambda: {
        "score": (0.0, 100.0),
        "timestamp": (1000000000.0, 2000000000.0)
    })



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
        return create_silent_client_from_server(self.server)
            
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
                    memory_summary = get_memory_info_summary(client)
                    log_memory_info(self, memory_summary, "monitoring:")
                    
                    current_memory_kb = memory_summary['used_memory_kb']
                    memory_delta = current_memory_kb - self.last_memory
                    search_memory_kb = memory_summary['search_used_memory_kb']
                    
                    # Get key count from db info
                    db_info = client.execute_command("info","keyspace")
                    current_keys = get_key_count_from_db_info(db_info)
                    # Try to get index information if available (during indexing phase)
                    index_info = {}
                    current_index_names = set()
                    with self.lock:
                        current_index_names = self.active_index_names.copy()
                    
                    if current_index_names:
                        for index_name in current_index_names:
                            try:
                                ft_info = client.execute_command("FT.INFO", index_name)
                                index_data = parse_ft_info(ft_info)
                                
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


class StandaloneMemoryBenchmark:
    """Comprehensive Tag Index memory benchmarking using hash_generator - Standalone version"""
    
    def __init__(self, valkey_port: int = 6379, redis_host: str = "localhost"):
        self.valkey_port = valkey_port
        self.redis_host = redis_host
        self.server = None
        self.testdir = None
        self.client = None
        
        # Paths from environment
        self.valkey_server_path = os.getenv("VALKEY_SERVER_PATH", "/home/ubuntu/valkey/build/bin/valkey-server")
        self.module_path = os.getenv("MODULE_PATH", "/home/ubuntu/valkey-search/.build-release/libsearch.so")
        
    def start_valkey(self):
        """Start Valkey server with search module"""
        # Create temporary directory
        self.testdir = tempfile.mkdtemp(prefix="valkey-mem-bench-")
        
        # Create config file
        conf_file = os.path.join(self.testdir, "valkey.conf")
        with open(conf_file, "w") as f:
            f.write(f"port {self.valkey_port}\n")
            f.write(f"dir {self.testdir}\n")
            f.write("save \"\"\n")
            f.write(f"loadmodule {self.module_path}\n")
            f.write("enable-debug-command yes\n")
        
        # Start Valkey
        self.server = subprocess.Popen(
            [self.valkey_server_path, conf_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Wait for startup
        for i in range(50):
            try:
                self.client = Valkey(host=self.redis_host, port=self.valkey_port, decode_responses=True)
                self.client.ping()
                logging.info("Valkey started successfully")
                break
            except Exception:
                time.sleep(0.2)
        else:
            raise RuntimeError("Failed to start Valkey")
    
    def stop_valkey(self):
        """Stop Valkey server"""
        if self.client:
            try:
                self.client.close()
            except:
                pass
            self.client = None
        
        if self.server:
            self.server.terminate()
            self.server.wait()
            self.server = None
        
        if self.testdir and os.path.exists(self.testdir):
            shutil.rmtree(self.testdir)
            self.testdir = None
    
    def cleanup(self):
        """Cleanup resources"""
        self.stop_valkey()
    
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
                temp_client = Valkey(host=self.redis_host, port=self.valkey_port, decode_responses=True)
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
    
    def calculate_comprehensive_memory(self, total_keys, unique_tags, avg_tag_length, avg_tags_per_key, avg_keys_per_tag, vector_dims=8, hnsw_m=16, include_numeric=True, monitor=None):
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
            monitor: Optional progress monitor for detailed logging
        
        Returns:
            Dictionary with detailed memory breakdown in KB
        """
        
        if monitor:
            monitor.log("🔍 COMPREHENSIVE MEMORY BREAKDOWN CALCULATION")
            monitor.log("─" * 60)
            monitor.log("📊 Input Parameters:")
            monitor.log(f"   • Total keys: {total_keys:,}")
            monitor.log(f"   • Unique tags: {unique_tags:,}")
            monitor.log(f"   • Avg tag length: {avg_tag_length:.1f} bytes")
            monitor.log(f"   • Avg tags per key: {avg_tags_per_key:.1f}")
            monitor.log(f"   • Avg keys per tag: {avg_keys_per_tag:.1f}")
            monitor.log(f"   • Vector dimensions: {vector_dims}")
            monitor.log("")
        
        # === 1. VALKEY CORE OVERHEAD ===
        # Valkey stores HASH keys with internal overhead
        # Each key: hash table entry + key string + metadata
        avg_key_length = 8  # estimate based on typical patterns
        valkey_key_overhead = total_keys * (
            32 +  # hash table entry overhead
            avg_key_length + 8 +  # key string + length
            16  # misc metadata per key
        )
        
        # Hash field storage (tags + vector + numeric fields per key)
        numeric_fields_size = 0
        if include_numeric:
            # Assume 2 numeric fields by default (score and timestamp), 8 bytes each as doubles
            # In real usage, this should be passed as a parameter
            numeric_fields_count = 2
            numeric_fields_size = numeric_fields_count * 8 * 1.1  # with overhead
        
        valkey_fields_overhead = total_keys * (
            (avg_tags_per_key * avg_tag_length * 1.2) +  # tag data with overhead
            (vector_dims * 4 * 1.1) +  # vector data with overhead
            numeric_fields_size +  # numeric fields with overhead
            32  # field metadata
        )
        
        valkey_memory = valkey_key_overhead + valkey_fields_overhead
        
        if monitor:
            monitor.log("🗄️  VALKEY CORE CALCULATION:")
            monitor.log(f"   Key overhead: {total_keys:,} × (32 + {avg_key_length} + 8 + 16) = {valkey_key_overhead:,.0f} bytes")
            numeric_info = f" + {numeric_fields_size:.0f}" if include_numeric else ""
            monitor.log(f"   Field storage: {total_keys:,} × ({avg_tags_per_key:.1f} × {avg_tag_length:.1f} × 1.2 + {vector_dims} × 4 × 1.1{numeric_info} + 32) = {valkey_fields_overhead:,.0f} bytes")
            monitor.log(f"   Valkey total: {valkey_memory:,.0f} bytes = {valkey_memory / 1024:.0f} KB")
            monitor.log("")
        
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
            patricia_tree_memory
        )
        
        if monitor:
            monitor.log("🏷️  TAG INDEX CALCULATION:")
            monitor.log(f"   Tag interning: {unique_tags:,} × ({avg_tag_length:.1f} + 32) + {unique_tags:,} × 48 = {tag_interning_memory:,.0f} bytes")
            monitor.log(f"   Key interning: {total_keys:,} × ({avg_key_length} + 32) + {total_keys:,} × 48 = {key_interning_memory:,.0f} bytes")
            monitor.log(f"   Tracked keys: {total_keys:,} × (8 + {taginfo_per_key:.0f} + 16) = {tracked_keys_memory:,.0f} bytes")
            monitor.log(f"   Patricia tree: {estimated_tree_nodes:,} nodes × 72 + {unique_tags:,} tags × (32 + {avg_keys_per_tag:.1f} × 8) = {patricia_tree_memory:,.0f} bytes")
            # monitor.log(f"   Untracked keys: {total_keys:,} × 0.1 × 24 = {untracked_keys_memory:,.0f} bytes")
            monitor.log(f"   Tag index total: {tag_index_memory:,.0f} bytes = {tag_index_memory / 1024:.0f} KB")
            monitor.log("")
        
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
        
        # === 5. NUMERIC INDEX MEMORY (if enabled) ===
        numeric_index_memory = 0
        if include_numeric:
            # Numeric range tree structures for fields
            # Each numeric index uses a sorted tree structure
            numeric_index_memory = total_keys * numeric_fields_count * (
                24 +  # tree node overhead
                8 +   # numeric value storage
                8     # pointer to document
            )
        
        if monitor and include_numeric:
            monitor.log("🔢 NUMERIC INDEX CALCULATION:")
            monitor.log(f"   Numeric index nodes: {total_keys * numeric_fields_count:,} × 40 bytes = {numeric_index_memory:,.0f} bytes")
            monitor.log(f"   Numeric index total: {numeric_index_memory:,.0f} bytes = {numeric_index_memory // 1024:.0f} KB")
            monitor.log("")
        
        # === 6. MEMORY FRAGMENTATION & ALIGNMENT ===
        # Real allocators have significant overhead
        total_allocated = valkey_memory + tag_index_memory + vector_index_memory + numeric_index_memory + search_module_overhead
        fragmentation_factor = 1.25  # 25% overhead typical for mixed allocation patterns
        fragmentation_overhead = total_allocated * (fragmentation_factor - 1.0)
        
        # === SUMMARY ===
        breakdown = {
            'valkey_core_kb': int(valkey_memory // 1024),
            'key_interning_kb': int(key_interning_memory // 1024),
            'tag_index_kb': int(tag_index_memory // 1024),
            'vector_index_kb': int(vector_index_memory // 1024),
            'numeric_index_kb': int(numeric_index_memory // 1024),
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
        
        if monitor:
            monitor.log("📊 FINAL COMPREHENSIVE BREAKDOWN:")
            monitor.log(f"   • Valkey Core:           {breakdown['valkey_core_kb']:,} KB")
            monitor.log(f"   • Key Interning:         {breakdown['key_interning_kb']:,} KB")
            monitor.log(f"   • Tag Index:             {breakdown['tag_index_kb']:,} KB")
            monitor.log(f"   • Vector Index:          {breakdown['vector_index_kb']:,} KB")
            monitor.log(f"     - Vector Data:         {breakdown['vector_data_kb']:,} KB")
            monitor.log(f"     - HNSW Level 0:        {breakdown['hnsw_level0_kb']:,} KB")
            monitor.log(f"     - HNSW Higher:         {breakdown['hnsw_higher_levels_kb']:,} KB")
            monitor.log(f"     - Mappings:            {breakdown['vector_mappings_kb']:,} KB")
            if include_numeric:
                monitor.log(f"   • Numeric Index:         {breakdown['numeric_index_kb']:,} KB")
            monitor.log(f"   • Module Overhead:       {breakdown['search_module_overhead_kb']:,} KB")
            monitor.log(f"   • Fragmentation:         {breakdown['fragmentation_overhead_kb']:,} KB")
            monitor.log(f"   ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            monitor.log(f"   🎯 TOTAL ESTIMATED:      {breakdown['total_estimated_kb']:,} KB")
            monitor.log("")
        
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
    
    def estimate_memory_usage(self, scenario: BenchmarkScenario, monitor=None) -> Dict[str, int]:
        """Estimate memory usage using enhanced non-linear model with detailed logging"""
        # Get scenario statistics
        stats = self._calculate_estimated_stats(scenario)
        
        # Enhanced model coefficients (from analysis)
        coef = {
            'constant': -4091.88,
            'per_key': 0.987045,
            'per_unique_tag': 0.117864,
            'per_tag_length': 4.973969,
            'tag_content': 0.002756,  # tags × length
            'sharing_efficiency': -1213.788388,  # log(keys_per_tag)
            'prefix_compression': 0.117864,  # for prefix sharing
            'content_per_key': 0.052673  # keys × tag_len / tags
        }
        
        # Extract parameters
        num_keys = scenario.total_keys
        unique_tags = stats['unique_tags']
        avg_tag_len = stats['avg_tag_length']
        keys_per_tag = stats['avg_keys_per_tag']
        
        # Calculate sharing effects
        import math
        tag_sharing_factor = math.log(max(keys_per_tag, 1.1))  # log sharing effect
        
        # Detect prefix sharing (simplified detection)
        prefix_sharing_factor = 1.0  # Default no prefix sharing
        if hasattr(scenario.tags_config, 'tag_prefix') and scenario.tags_config.tag_prefix:
            if scenario.tags_config.tag_prefix.enabled:
                # Estimate compression based on prefix config
                prefix_ratio = scenario.tags_config.tag_prefix.min_shared / max(avg_tag_len, 1)
                prefix_sharing_factor = prefix_ratio
        # Enhanced model calculation (in KB)
        term1 = coef['constant']
        term2 = coef['per_key'] * num_keys
        term3 = coef['per_unique_tag'] * unique_tags
        term4 = coef['per_tag_length'] * avg_tag_len
        term5 = coef['tag_content'] * (unique_tags * avg_tag_len)
        term6 = coef['sharing_efficiency'] * tag_sharing_factor
        term7 = coef['prefix_compression'] * (prefix_sharing_factor * unique_tags)
        term8 = coef['content_per_key'] * (num_keys * avg_tag_len / max(unique_tags, 1))
        
        base_memory_kb = term1 + term2 + term3 + term4 + term5 + term6 + term7 + term8
        
        # Calculate vector index memory based on actual dimensions and algorithm
        vector_dims = scenario.vector_dim
        vector_memory_kb = 0
        
        if scenario.vector_algorithm == VectorAlgorithm.FLAT:
            # FLAT algorithm: Simple storage of vectors
            vector_data_kb = (num_keys * vector_dims * 4) // 1024  # float32
            vector_metadata_kb = (num_keys * 32) // 1024  # metadata per vector
            vector_memory_kb = vector_data_kb + vector_metadata_kb
        else:  # HNSW
            # HNSW algorithm: More complex with graph structure
            M = scenario.hnsw_m  # Use actual HNSW M parameter
            vector_data_kb = (num_keys * vector_dims * 4) // 1024  # float32
            # Level 0 connections (bidirectional)
            level0_kb = (num_keys * M * 2 * 8) // 1024  # M*2 neighbors * 8 bytes
            # Higher levels (probabilistic, ~5% of vectors per level)
            higher_levels_kb = (num_keys * 0.05 * M * 8 * 3) // 1024  # Estimate 3 levels avg
            vector_metadata_kb = (num_keys * 48) // 1024  # More metadata for HNSW
            vector_memory_kb = vector_data_kb + level0_kb + higher_levels_kb + vector_metadata_kb
        
        # Calculate numeric index memory if enabled
        numeric_memory_kb = 0
        if scenario.include_numeric and scenario.numeric_fields:
            # Each numeric field needs a sorted index structure
            num_numeric_fields = len(scenario.numeric_fields)
            # Tree nodes + value storage + document pointers
            numeric_memory_kb = (num_keys * num_numeric_fields * 40) // 1024
        
        # Add vector and numeric memory to the base estimate
        total_memory_kb = base_memory_kb + vector_memory_kb + numeric_memory_kb
        
        # Ensure minimum reasonable value
        original_total = total_memory_kb
        total_memory_kb = max(total_memory_kb, num_keys * 0.5)  # At least 0.5KB per key
        
        if monitor and total_memory_kb != original_total:
            monitor.log(f"   Applied minimum: {total_memory_kb:.1f} KB (was {original_total:.1f} KB)")
        
        # Break down into components
        total_memory_bytes = int(total_memory_kb * 1024)
        
        # Calculate component percentages based on actual values
        tag_memory_kb = base_memory_kb * 0.8  # Most of base memory is for tags
        data_memory_kb = base_memory_kb * 0.2  # Remaining base memory
        
        data_memory = int(data_memory_kb * 1024)
        tag_index_memory = int(tag_memory_kb * 1024)
        vector_index_memory = int(vector_memory_kb * 1024)
        index_memory = tag_index_memory + vector_index_memory + int(numeric_memory_kb * 1024)  
        # Log the detailed formula and inputs
        if monitor:
            monitor.log("🧮 ENHANCED MEMORY ESTIMATION MODEL")
            monitor.log("─" * 50)
            monitor.log("📊 Input Parameters:")
            monitor.log(f"   • Keys: {num_keys:,}")
            monitor.log(f"   • Unique tags: {unique_tags:,}")
            monitor.log(f"   • Avg tag length: {avg_tag_len:.1f} bytes")
            monitor.log(f"   • Keys per tag: {keys_per_tag:.1f}")
            monitor.log(f"   • Tag sharing factor: log({keys_per_tag:.1f}) = {tag_sharing_factor:.3f}")
            monitor.log(f"   • Prefix sharing factor: {prefix_sharing_factor:.3f}")
            monitor.log("")
            monitor.log("🔢 Formula Components:")
            monitor.log(f"   Constant:               {coef['constant']:>8.2f} KB")
            monitor.log(f"   Keys term:              {coef['per_key']:.6f} × {num_keys:,} = {coef['per_key'] * num_keys:>8.1f} KB")
            monitor.log(f"   Unique tags term:       {coef['per_unique_tag']:.6f} × {unique_tags:,} = {coef['per_unique_tag'] * unique_tags:>8.1f} KB")
            monitor.log(f"   Tag length term:        {coef['per_tag_length']:.6f} × {avg_tag_len:.1f} = {coef['per_tag_length'] * avg_tag_len:>8.1f} KB")
            monitor.log(f"   Tag content term:       {coef['tag_content']:.6f} × ({unique_tags:,} × {avg_tag_len:.1f}) = {coef['tag_content'] * (unique_tags * avg_tag_len):>8.1f} KB")
            monitor.log(f"   Sharing efficiency:     {coef['sharing_efficiency']:.6f} × {tag_sharing_factor:.3f} = {coef['sharing_efficiency'] * tag_sharing_factor:>8.1f} KB")
            monitor.log(f"   Prefix compression:     {coef['prefix_compression']:.6f} × ({prefix_sharing_factor:.3f} × {unique_tags:,}) = {coef['prefix_compression'] * (prefix_sharing_factor * unique_tags):>8.1f} KB")
            monitor.log(f"   Content per key:        {coef['content_per_key']:.6f} × ({num_keys:,} × {avg_tag_len:.1f} / {unique_tags:,}) = {coef['content_per_key'] * (num_keys * avg_tag_len / max(unique_tags, 1)):>8.1f} KB")
            monitor.log("")
            monitor.log("📈 Model Calculation:")
            monitor.log(f"   Base (tags) = {term1:.1f} + {term2:.1f} + {term3:.1f} + {term4:.1f} + {term5:.1f} + {term6:.1f} + {term7:.1f} + {term8:.1f}")
            monitor.log(f"   Base memory = {base_memory_kb:.1f} KB")
            monitor.log("")
            monitor.log("🎯 Vector Index Calculation:")
            monitor.log(f"   • Vector dimensions: {vector_dims}")
            monitor.log(f"   • Algorithm: {scenario.vector_algorithm.value}")
            if scenario.vector_algorithm == VectorAlgorithm.FLAT:
                monitor.log(f"   • Vector data: {num_keys:,} × {vector_dims} × 4 bytes = {(num_keys * vector_dims * 4) // 1024:,} KB")
                monitor.log(f"   • Vector metadata: {num_keys:,} × 32 bytes = {(num_keys * 32) // 1024:,} KB")
            else:
                monitor.log(f"   • Vector data: {num_keys:,} × {vector_dims} × 4 bytes = {(num_keys * vector_dims * 4) // 1024:,} KB")
                monitor.log(f"   • HNSW Level 0: {num_keys:,} × {M*2} × 8 bytes = {(num_keys * M * 2 * 8) // 1024:,} KB")
                monitor.log(f"   • HNSW Higher levels: ~{(num_keys * 0.05 * M * 8 * 3) // 1024:,} KB")
                monitor.log(f"   • Vector metadata: {num_keys:,} × 48 bytes = {(num_keys * 48) // 1024:,} KB")
            monitor.log(f"   • Total vector memory: {vector_memory_kb:,} KB")
            monitor.log("")
            if scenario.include_numeric and scenario.numeric_fields:
                monitor.log("🔢 Numeric Index Calculation:")
                monitor.log(f"   • Numeric fields: {len(scenario.numeric_fields)} ({', '.join(scenario.numeric_fields.keys())})")
                monitor.log(f"   • Per field: {num_keys:,} × 40 bytes = {(num_keys * 40) // 1024:,} KB")
                monitor.log(f"   • Total numeric memory: {numeric_memory_kb:,} KB")
                monitor.log("")
            monitor.log("🏗️  Component Breakdown:")
            monitor.log(f"   • Data memory:       {data_memory // 1024:,} KB")
            monitor.log(f"   • Tag index:         {tag_index_memory // 1024:,} KB")
            monitor.log(f"   • Vector index:      {vector_index_memory // 1024:,} KB")
            if numeric_memory_kb > 0:
                monitor.log(f"   • Numeric index:     {numeric_memory_kb:,} KB")
            monitor.log(f"   • Total estimated:   {total_memory_bytes // 1024:,} KB")
            monitor.log("")
        
        return {
            'data_memory': data_memory,
            'tag_index_memory': tag_index_memory,
            'vector_index_memory': vector_index_memory,
            'numeric_index_memory': int(numeric_memory_kb * 1024),
            'index_memory': index_memory,
            'total_memory': total_memory_bytes
        }
    
    def estimate_memory_usage_v2(self, scenario: BenchmarkScenario, monitor=None) -> Dict[str, int]:
        """
        Improved pre-ingestion memory estimation based on index definition and data properties.
        Provides the best possible estimate without access to actual server state.
        """
        # Get scenario statistics
        stats = self._calculate_estimated_stats(scenario)
        num_keys = scenario.total_keys
        unique_tags = stats['unique_tags']
        avg_tag_len = stats['avg_tag_length']
        keys_per_tag = stats['avg_keys_per_tag']
        avg_tags_per_key = stats['avg_tags_per_key']
        
        # === 1. VALKEY CORE MEMORY ===
        # Hash key overhead (based on Valkey's hash implementation)
        avg_key_length = 20  # Typical "key:XXXXX" pattern
        valkey_key_overhead = num_keys * (
            32 +  # Hash entry overhead
            avg_key_length +  # Key name
            8 +   # Expires field
            16    # Additional metadata
        )
        
        # Hash field storage
        valkey_fields_overhead = num_keys * (
            (avg_tags_per_key * avg_tag_len * 1.2) +  # tag data with overhead
            (scenario.vector_dim * 4 * 1.1) +  # vector data with overhead
            32  # field metadata
        )
        
        # Add numeric fields if enabled
        if scenario.include_numeric and scenario.numeric_fields:
            numeric_fields_size = len(scenario.numeric_fields) * 8 * 1.1  # doubles with overhead
            valkey_fields_overhead += num_keys * numeric_fields_size
        
        valkey_memory = valkey_key_overhead + valkey_fields_overhead
        
        # === 2. SEARCH MODULE MEMORY ===
        
        # Key Interning (all keys are interned in search module)
        key_interning_memory = num_keys * (
            avg_key_length + 32 +  # Key string + hash table entry
            48  # InternalId mapping
        )
        
        # Tag Index Memory
        import math
        
        # 1. STRING INTERNING: This is the biggest component - actual tag string storage
        # Each unique tag string needs to be stored once, regardless of how many keys use it
        total_tag_string_bytes = unique_tags * avg_tag_len  # Raw string data
        string_interning_overhead = unique_tags * 32  # Hash table entries, metadata per string
        string_interning_memory = total_tag_string_bytes + string_interning_overhead
        
        # 2. PATRICIA TREE: Just for indexing the interned strings, not storing them
        # The tree nodes are much smaller since they don't store the full strings
        prefix_diversity_factor = 1.0
        if hasattr(scenario.tags_config, 'tag_prefix') and scenario.tags_config.tag_prefix:
            if scenario.tags_config.tag_prefix.enabled:
                # With prefix sharing, fewer tree nodes needed
                prefix_diversity_factor = 0.3
        else:
            # For unique tags, we need fewer nodes since there's no common prefixes
            # Each node might cover multiple unique strings
            prefix_diversity_factor = min(1.0, avg_tag_len / 10.0)  # More realistic for unique tags
        
        estimated_tree_nodes = max(unique_tags // 2, int(unique_tags * prefix_diversity_factor))
        patricia_node_size = 48  # Smaller since just pointers, not full strings
        patricia_tree_memory = estimated_tree_nodes * patricia_node_size
        
        # 3. POSTING LISTS: Inverted index mapping tag -> list of document IDs
        posting_list_overhead = unique_tags * 32  # Base structure per unique tag
        posting_list_entries = num_keys * avg_tags_per_key * 8  # Document IDs
        
        # If tags are shared, posting lists are more efficient
        if keys_per_tag > 1:
            posting_efficiency = 1.0 - (0.2 * math.log10(keys_per_tag))  # Compression factor
            posting_list_entries = int(posting_list_entries * posting_efficiency)
        
        tag_index_memory = (
            string_interning_memory +
            patricia_tree_memory + 
            posting_list_overhead + 
            posting_list_entries
        )
        
        # === 3. VECTOR INDEX MEMORY ===
        vector_index_memory = 0
        if scenario.vector_dim > 0:
            # Vector data storage
            vector_data_size = num_keys * scenario.vector_dim * 4  # float32
            
            # Metadata for vectors
            vector_metadata = num_keys * 24  # key -> internal_id + magnitude
            
            if scenario.vector_algorithm == VectorAlgorithm.FLAT:
                # FLAT: Simple array storage
                vector_index_memory = (
                    vector_data_size +
                    vector_metadata +
                    num_keys * 8  # Additional bookkeeping
                )
            else:  # HNSW
                M = scenario.hnsw_m  # Use actual HNSW M parameter
                # Level 0 graph (all nodes connected)
                level0_memory = num_keys * M * 2 * 8  # Bidirectional edges
                
                # Higher levels (logarithmic decrease)
                prob_higher_level = 1.0 / (2 * M)
                expected_levels = -math.log(1e-6) / math.log(2 * M)  # ~3-4 levels typically
                nodes_at_higher_levels = num_keys * prob_higher_level
                higher_levels_memory = int(nodes_at_higher_levels * M * 8 * expected_levels)
                
                # HNSW specific metadata
                hnsw_metadata = num_keys * 32  # Level info, visited lists, etc.
                
                vector_index_memory = (
                    vector_data_size +
                    vector_metadata +
                    level0_memory +
                    higher_levels_memory +
                    hnsw_metadata
                )
        
        # === 4. NUMERIC INDEX MEMORY ===
        numeric_index_memory = 0
        if scenario.include_numeric and scenario.numeric_fields:
            # Range tree implementation for numeric fields
            for field_name in scenario.numeric_fields:
                # B-tree nodes for range queries
                tree_height = max(3, int(math.log(num_keys, 32)))  # B-tree with fanout ~32
                internal_nodes = num_keys // 16  # Approximate
                leaf_nodes = num_keys
                
                # Each node stores values and pointers
                node_size = 64  # Average node size
                numeric_index_memory += (internal_nodes + leaf_nodes) * node_size
        
        # === 5. MODULE OVERHEAD ===
        # Fixed overhead for module structures
        module_base_overhead = 64 * 1024  # 64KB base
        # Index schema and coordination structures
        index_overhead = 16 * 1024 + (num_keys * 0.1)  # Scales slightly with data
        
        search_module_overhead = module_base_overhead + index_overhead
        
        # === TOTAL CALCULATION ===
        total_memory = (
            valkey_memory +
            key_interning_memory +
            tag_index_memory +
            vector_index_memory +
            numeric_index_memory +
            search_module_overhead
        )
        
        # Convert to KB for consistency
        total_memory_kb = total_memory // 1024
        
        # Log detailed breakdown if monitor provided
        if monitor:
            monitor.log("🧮 IMPROVED MEMORY ESTIMATION (V2)")
            monitor.log("─" * 50)
            monitor.log("📊 Input Parameters:")
            monitor.log(f"   • Keys: {num_keys:,}")
            monitor.log(f"   • Unique tags: {unique_tags:,}")
            monitor.log(f"   • Avg tag length: {avg_tag_len:.1f} bytes")
            monitor.log(f"   • Avg tags per key: {avg_tags_per_key:.1f}")
            monitor.log(f"   • Keys per tag: {keys_per_tag:.1f}")
            monitor.log(f"   • Vector dimensions: {scenario.vector_dim}")
            monitor.log(f"   • Vector algorithm: {scenario.vector_algorithm.value}")
            if scenario.include_numeric:
                monitor.log(f"   • Numeric fields: {len(scenario.numeric_fields)}")
            monitor.log("")
            
            monitor.log("💾 VALKEY CORE:")
            monitor.log(f"   • Key overhead: {valkey_key_overhead:,} bytes ({valkey_key_overhead // 1024:,} KB)")
            monitor.log(f"   • Fields overhead: {valkey_fields_overhead:,} bytes ({valkey_fields_overhead // 1024:,} KB)")
            monitor.log(f"   • Total: {valkey_memory // 1024:,} KB")
            monitor.log("")
            
            monitor.log("🔍 SEARCH MODULE:")
            monitor.log(f"   • Key interning: {key_interning_memory:,} bytes ({key_interning_memory // 1024:,} KB)")
            monitor.log(f"   • Tag index: {tag_index_memory:,} bytes ({tag_index_memory // 1024:,} KB)")
            monitor.log(f"     - String interning: {string_interning_memory:,} bytes ({unique_tags:,} × {avg_tag_len:.0f} bytes)")
            monitor.log(f"     - Patricia tree: {patricia_tree_memory:,} bytes ({estimated_tree_nodes:,} nodes)")
            monitor.log(f"     - Posting lists: {posting_list_overhead + posting_list_entries:,} bytes")
            if scenario.vector_dim > 0:
                monitor.log(f"   • Vector index: {vector_index_memory:,} bytes ({vector_index_memory // 1024:,} KB)")
                monitor.log(f"     - Vector data: {vector_data_size:,} bytes")
                if scenario.vector_algorithm == VectorAlgorithm.HNSW:
                    monitor.log(f"     - HNSW graph: {level0_memory + higher_levels_memory:,} bytes")
            if numeric_index_memory > 0:
                monitor.log(f"   • Numeric index: {numeric_index_memory:,} bytes ({numeric_index_memory // 1024:,} KB)")
            monitor.log(f"   • Module overhead: {search_module_overhead:,} bytes ({search_module_overhead // 1024:,} KB)")
            monitor.log("")
            
            monitor.log("📈 TOTAL ESTIMATED:")
            monitor.log(f"   • Total memory: {total_memory:,} bytes")
            monitor.log(f"   • Total memory: {total_memory_kb:,} KB ({total_memory_kb / 1024:.1f} MB)")
            monitor.log("")
        
        # Return breakdown in bytes for consistency with existing code
        return {
            'valkey_memory': valkey_memory,
            'key_interning_memory': key_interning_memory,
            'tag_index_memory': tag_index_memory,
            'vector_index_memory': vector_index_memory,
            'numeric_index_memory': numeric_index_memory,
            'search_module_overhead': search_module_overhead,
            'data_memory': valkey_memory,  # For compatibility
            'index_memory': tag_index_memory + vector_index_memory + numeric_index_memory,
            'total_memory': total_memory
        }
    
    def validate_memory_requirements(self, scenario: BenchmarkScenario) -> Tuple[bool, str]:
        """Validate if scenario can run without using swap"""
        available_memory = self.get_available_memory_bytes()
        estimates = self.estimate_memory_usage_v2(scenario)
        
        # Ensure we use less than 50% of available memory to avoid swap
        memory_limit = available_memory * 0.5
        
        if estimates['total_memory'] > memory_limit:
            return False, (
                f"Scenario '{scenario.name}' requires ~{estimates['total_memory'] / (1024**3):.1f}GB "
                f"but only {memory_limit / (1024**3):.1f}GB is safely available "
                f"(50% of {available_memory / (1024**3):.1f}GB free memory)"
            )
        
        return True, f"Memory check passed: ~{estimates['total_memory'] / (1024**3):.1f}GB required, {available_memory / (1024**3):.1f}GB available"
    
    def calculate_comprehensive_memory_v2(self, client: Valkey, scenario: BenchmarkScenario, 
                                        memory_info: Dict[str, Any], search_info: Dict[str, Any],
                                        stats: Dict[str, Any], baseline_memory: int, data_memory: int, 
                                        monitor=None) -> Dict[str, int]:
        """
        Post-ingestion memory breakdown based on actual server state.
        Uses info memory data and known implementation details to deduce component usage.
        
        Args:
            client: Valkey client connection
            scenario: Benchmark scenario with index configuration
            memory_info: Output from INFO memory command
            search_info: Output from INFO modules (search module info)
            stats: Data statistics (unique_tags, avg_tag_length, etc.)
            baseline_memory: Initial server memory before any data ingestion
            data_memory: Server memory after data ingestion but before index creation
            monitor: Optional progress monitor for logging
        """
        # Extract key metrics from memory info
        used_memory = memory_info.get('used_memory', 0)
        used_memory_dataset = memory_info.get('used_memory_dataset', 0)
        used_memory_overhead = memory_info.get('used_memory_overhead', 0)
        allocator_allocated = memory_info.get('allocator_allocated', 0)
        allocator_fragmentation = allocator_allocated - used_memory
        mem_clients_normal = memory_info.get('mem_clients_normal', 0)
        
        # Search module memory
        search_used_memory = search_info.get('search_used_memory_bytes', 0)
        
        # Extract stats
        num_keys = scenario.total_keys
        unique_tags = stats.get('unique_tags', 0)
        avg_tag_length = stats.get('avg_tag_length', 0)
        avg_tags_per_key = stats.get('avg_tags_per_key', 1)
        avg_keys_per_tag = stats.get('avg_keys_per_tag', 1)
        
        # === 1. VALKEY CORE MEMORY BREAKDOWN ===
        # used_memory_dataset includes all user data (keys + values)
        # This is the actual Valkey hash storage
        valkey_data_memory = data_memory
        
        # Overhead includes:
        # - Server structs, buffers, client connections
        # - Hash table overhead for key storage
        # - Memory allocator metadata
        valkey_overhead = used_memory_overhead
        
        # Client connections memory (separate from dataset)
        client_memory = mem_clients_normal
        
        # === 2. SEARCH MODULE MEMORY BREAKDOWN ===
        # The search module reports its ACTUAL total memory usage
        # We deduce component breakdown by subtracting known components from actual usage
        
        # Key interning memory - this is fairly predictable based on patterns
        avg_key_length = 20  # Typical "key:XXXXX" pattern
        key_interning_memory = num_keys * (avg_key_length + 80)  # String + hash entry + internal ID
        
        # Module overhead - fixed structures that can be estimated reliably
        module_overhead = 64 * 1024 + (num_keys * 0.1)  # Base overhead + scaling factor
        
        # Start with total search memory and subtract reliably known components
        remaining_memory = search_used_memory - key_interning_memory - module_overhead
        
        # Calculate expected minimum memory for indexes to guide deduction
        expected_vector_memory = 0
        if scenario.vector_dim > 0:
            # Minimum vector memory = vector data + basic metadata
            vector_data_size = num_keys * scenario.vector_dim * 4  # float32
            expected_vector_memory = vector_data_size + (num_keys * 24)  # basic metadata
            
            if scenario.vector_algorithm == VectorAlgorithm.HNSW:
                # Add HNSW graph overhead estimate
                M = scenario.hnsw_m  # Use actual HNSW M parameter
                level0_edges = num_keys * M * 2 * 8  # Level 0 bidirectional edges
                expected_vector_memory += level0_edges
        
        expected_numeric_memory = 0
        if scenario.include_numeric and scenario.numeric_fields:
            # Minimum numeric memory = value storage + tree overhead
            expected_numeric_memory = num_keys * len(scenario.numeric_fields) * 40
        
        # Deduce vector index actual memory usage
        if scenario.vector_dim > 0:
            # Vector memory usage can vary, but should be reasonably close to expected
            # Use actual remaining memory to guide the deduction
            total_expected_indexes = expected_vector_memory + expected_numeric_memory
            
            if total_expected_indexes > 0:
                # Allocate remaining memory proportionally, but cap at reasonable bounds
                vector_proportion = expected_vector_memory / total_expected_indexes
                vector_index_memory = int(remaining_memory * vector_proportion)
                
                # Apply reasonable bounds (50% to 300% of expected)
                min_vector = int(expected_vector_memory * 0.5)
                max_vector = int(expected_vector_memory * 3.0)
                vector_index_memory = max(min_vector, min(max_vector, vector_index_memory))
            else:
                vector_index_memory = expected_vector_memory
            
            remaining_memory -= vector_index_memory
        else:
            vector_index_memory = 0
        
        # Deduce numeric index actual memory usage
        if scenario.include_numeric and scenario.numeric_fields:
            # Similar approach for numeric index
            if expected_numeric_memory > 0:
                # Use what's left, but cap at reasonable bounds
                numeric_index_memory = min(remaining_memory // 2, int(expected_numeric_memory * 2.0))
                numeric_index_memory = max(int(expected_numeric_memory * 0.5), numeric_index_memory)
            else:
                numeric_index_memory = 0
            
            remaining_memory -= numeric_index_memory
        else:
            numeric_index_memory = 0
        
        # Whatever memory is left goes to tag index (the most variable component)
        # This represents the ACTUAL tag index memory usage deduced from server data
        tag_index_memory = max(0, remaining_memory)
        
        # === 3. FRAGMENTATION ANALYSIS ===
        # Calculate actual fragmentation from allocator data
        actual_fragmentation = allocator_fragmentation
        fragmentation_ratio = allocator_allocated / used_memory if used_memory > 0 else 1.0
        
        # === COMPREHENSIVE BREAKDOWN ===
        breakdown = {
            # Valkey core
            'valkey_data_memory': valkey_data_memory,
            'valkey_overhead': valkey_overhead,
            'client_memory': client_memory,
            'valkey_total': valkey_data_memory + valkey_overhead,
            
            # Search module components
            'search_total_memory': search_used_memory,
            'key_interning_memory': key_interning_memory,
            'tag_index_memory': tag_index_memory,
            'vector_index_memory': int(vector_index_memory),
            'numeric_index_memory': numeric_index_memory,
            'search_module_overhead': module_overhead,
            
            # Memory allocator
            'allocator_allocated': allocator_allocated,
            'allocator_fragmentation': actual_fragmentation,
            'fragmentation_ratio': fragmentation_ratio,
            
            # Totals
            'used_memory_total': used_memory,
            'total_memory_bytes': allocator_allocated
        }
        
        # Convert to KB for consistency
        breakdown_kb = {
            key + '_kb' if not key.endswith('_ratio') else key: 
            value // 1024 if not key.endswith('_ratio') else value
            for key, value in breakdown.items()
        }
        
        # Log detailed analysis if monitor provided
        if monitor:
            monitor.log("🔬 COMPREHENSIVE MEMORY ANALYSIS (V2)")
            monitor.log("═" * 60)
            
            monitor.log("📊 SERVER MEMORY STATE:")
            monitor.log(f"   • Used memory: {used_memory:,} bytes ({used_memory / (1024**2):.1f} MB)")
            monitor.log(f"   • Allocator allocated: {allocator_allocated:,} bytes ({allocator_allocated / (1024**2):.1f} MB)")
            monitor.log(f"   • Fragmentation: {fragmentation_ratio:.2f}x ({actual_fragmentation / (1024**2):.1f} MB)")
            monitor.log("")
            
            monitor.log("💾 VALKEY CORE BREAKDOWN:")
            monitor.log(f"   • Dataset (keys+values): {valkey_data_memory:,} bytes ({valkey_data_memory / (1024**2):.1f} MB)")
            monitor.log(f"   • Server overhead: {valkey_overhead:,} bytes ({valkey_overhead / (1024**2):.1f} MB)")
            monitor.log(f"   • Client connections: {client_memory:,} bytes ({client_memory / (1024**2):.1f} MB)")
            monitor.log("")
            
            monitor.log("🔍 SEARCH MODULE BREAKDOWN:")
            monitor.log(f"   • Total search memory: {search_used_memory:,} bytes ({search_used_memory / (1024**2):.1f} MB)")
            monitor.log(f"   • Key interning: {key_interning_memory:,} bytes ({key_interning_memory / (1024**2):.1f} MB)")
            monitor.log(f"   • Tag index: {tag_index_memory:,} bytes ({tag_index_memory / (1024**2):.1f} MB)")
            if vector_index_memory > 0:
                monitor.log(f"   • Vector index: {int(vector_index_memory):,} bytes ({vector_index_memory / (1024**2):.1f} MB)")
            if numeric_index_memory > 0:
                monitor.log(f"   • Numeric index: {numeric_index_memory:,} bytes ({numeric_index_memory / (1024**2):.1f} MB)")
            monitor.log(f"   • Module overhead: {module_overhead:,} bytes ({module_overhead / (1024**2):.1f} MB)")
            monitor.log("")
            
            monitor.log("📈 MEMORY ATTRIBUTION (Based on Actual Measurements):")
            
            # Use the three measurement points for accurate attribution:
            # 1. baseline_memory = server overhead only
            # 2. data_memory = server overhead + dataset  
            # 3. used_memory = server overhead + dataset + search module
            
            total_allocated = allocator_allocated
            
            # Server overhead: from initial baseline measurement
            server_overhead = baseline_memory
            server_pct = server_overhead / total_allocated * 100
            
            # Dataset: use used_memory_dataset (actual user data)
            dataset = used_memory_dataset
            dataset_pct = dataset / total_allocated * 100
            
            # Search module: what was added after index creation
            # This is the difference between final memory and data-only memory
            search_module_actual = used_memory - data_memory
            search_pct = search_module_actual / total_allocated * 100
            
            # Fragmentation 
            frag_pct = actual_fragmentation / total_allocated * 100
            
            # Verify our accounting
            total_accounted_pct = server_pct + dataset_pct + search_pct + frag_pct
            
            monitor.log(f"   • Server overhead: {server_pct:.1f}% ({server_overhead:,} bytes)")
            monitor.log(f"   • Dataset (keys+values): {dataset_pct:.1f}% ({dataset:,} bytes)")
            monitor.log(f"   • Search module: {search_pct:.1f}% ({search_module_actual:,} bytes)")
            monitor.log(f"   • Fragmentation: {frag_pct:.1f}% ({actual_fragmentation:,} bytes)")
            monitor.log(f"   • Total accounted: {total_accounted_pct:.1f}%")
            
            # Warn if attribution doesn't make sense
            if abs(total_accounted_pct - 100) > 2:
                monitor.log(f"   ⚠️  WARNING: Total doesn't sum to 100% - accounting error")
            
            # Cross-check with search module's own reporting
            search_reported_pct = search_used_memory / total_allocated * 100
            if abs(search_pct - search_reported_pct) > 5:
                monitor.log(f"   ⚠️  NOTE: Search module reports {search_reported_pct:.1f}% vs measured {search_pct:.1f}%")
            monitor.log("═" * 60)
        
        return breakdown_kb
    
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
        vector_dims = None  # Will detect from first vector
        numeric_fields_count = 0
        numeric_field_names = set()
        
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
                                # Detect vector dimensions from first vector (float32 = 4 bytes per dim)
                                if vector_dims is None:
                                    vector_dims = len(vector_value) // 4
                            else:
                                sum_vector_lengths += len(str(vector_value))
                        
                        # Check for numeric fields (score, timestamp, etc)
                        for field_name, field_value in fields.items():
                            field_name_str = field_name.decode('utf-8') if isinstance(field_name, bytes) else field_name
                            if field_name_str not in ['tags', 'vector']:
                                # Try to convert to float to check if it's numeric
                                try:
                                    float(field_value)
                                    numeric_field_names.add(field_name_str)
                                except (ValueError, TypeError):
                                    pass
                
                # Update progress periodically
                if monitor and batch_count % 10 == 0:
                    monitor.log(f"   Processed {total_keys:,} keys so far...")
            
            # Check if we're done
            if cursor == 0:
                break
        
        # Calculate totals
        total_data_size = sum_key_lengths + sum_tag_lengths + sum_vector_lengths
        numeric_fields_count = len(numeric_field_names)
        
        # Log results
        if monitor:
            monitor.log("✅ Memory verification complete!")
            monitor.log("─" * 50)
            monitor.log(f"📊 Total keys found: {total_keys:,}")
            monitor.log(f"🔑 Sum of key lengths: {sum_key_lengths:,} bytes ({sum_key_lengths / (1024**2):.2f} MB)")
            monitor.log(f"🏷️  Sum of tag lengths: {sum_tag_lengths:,} bytes ({sum_tag_lengths / (1024**2):.2f} MB)")
            monitor.log(f"📐 Sum of vector lengths: {sum_vector_lengths:,} bytes ({sum_vector_lengths / (1024**2):.2f} MB)")
            if vector_dims:
                monitor.log(f"   Vector dimensions detected: {vector_dims}")
            if numeric_fields_count > 0:
                monitor.log(f"🔢 Numeric fields detected: {numeric_fields_count} ({', '.join(sorted(numeric_field_names))})")
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
            'total_data_size': total_data_size,
            'vector_dims': vector_dims if vector_dims else 8,  # Default to 8 if not detected
            'numeric_fields_count': numeric_fields_count
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
    
    def create_schema(self, index_name: str, vector_dim: int = 8, include_numeric: bool = True, 
                     vector_algorithm: VectorAlgorithm = VectorAlgorithm.FLAT,
                     vector_metric: VectorMetric = VectorMetric.COSINE,
                     numeric_fields: Dict[str, Tuple[float, float]] = None,
                     hnsw_m: int = 16) -> IndexSchema:
        """Create a schema with tags, vector, and optionally numeric fields"""
        from hash_generator import create_numeric_field
        
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
            #get dataset size before insertion
            memory_info = client.execute_command("info", "memory")
            dataset_size = safe_get(memory_info, 'used_memory_dataset', 0)
            
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
            estimates = self.estimate_memory_usage_v2(scenario, monitor)
            monitor.log("─" * 50)
            monitor.log("─" * 50)
            monitor.log("🎯 ESTIMATED USAGE")
            monitor.log("─" * 50)
            monitor.log(f"📁 Data Memory:      {estimates['data_memory'] // (1024**2):,} MB")
            monitor.log(f"🏷️  Tag Index:       {estimates['tag_index_memory'] // (1024**2):,} MB")  
            monitor.log(f"🎯 Vector Index:     {estimates['vector_index_memory'] // (1024**2):,} MB")
            monitor.log(f"💰 Total (overhead): {estimates['total_memory'] // (1024**2):,} MB")
            monitor.log("")
            monitor.log("─" * 50)
            monitor.log("─" * 50)
            
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
            memory_summary = get_memory_info_summary(client)
            
            # Clear any ongoing status updates since insertion is complete
            monitor.update_status({})
            
            monitor.log("✅ INSERTION COMPLETE")
            monitor.log("─" * 50)
            monitor.log(f"📊 Keys Inserted: {total_keys_inserted:,}")
            monitor.log(f"⏱️  Time Taken: {insertion_time:.1f}s")
            monitor.log(f"🚀 Speed: {total_keys_inserted/insertion_time:.0f} keys/sec")
            log_memory_info(monitor, memory_summary)
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
                hashes_csv = ""
                tags_csv = ""
                # try:
                #     hashes_csv, tags_csv = self.export_dataset_to_csv(
                #         export_client, 
                #         prefix="key:",  # Assuming standard prefix
                #         scenario_name=scenario.name,
                #         timestamp=timestamp,
                #         monitor=monitor
                #     )
                #     export_time = time.time() - export_start
                #     monitor.log(f"⏱️  Export time: {export_time:.1f}s")
                # finally:
                #     assert export_client is not None
                #     export_client.close()

                monitor.log("")
            
            # Measure memory after data insertion
            data_memory_summary = get_memory_info_summary(client)
            data_memory = data_memory_summary['used_memory_dataset']
            data_memory_kb = data_memory // 1024

            monitor.log("🏗️  INDEX CREATION PHASE")
            monitor.log("─" * 50)
            monitor.log(f"📊 Data Memory (no index): {data_memory_kb:,} KB")
            monitor.log(f"🏷️  Index Name: {index_name}")
            monitor.log("")
            
            monitor.set_index_name(index_name)
            
            cmd = generator.generate_ft_create_command()
            monitor.log(f"📜 Creating index with command: {cmd}")
            client.execute_command(*cmd.split())
            # check index exists and print ft.info
            index_info = client.execute_command("FT.INFO", index_name)
            monitor.log(f"📑 Index Info: {index_info}")
            # Wait for indexing
            self.wait_for_indexing(client, index_name, scenario.total_keys, monitor)            
            # Measure final memory
            final_memory_summary = get_memory_info_summary(client)
            final_memory = final_memory_summary['used_memory_dataset']
            total_memory_kb = final_memory // 1024
            index_overhead_kb = (final_memory - data_memory) // 1024
            
            # Get search module info
            search_info = client.execute_command("info","modules")
            search_memory_kb = search_info.get('search_used_memory_bytes', 0) // 1024

            
            # Get distribution statistics first
            dist_stats = dist_collector.get_summary()
            
            # Get actual vector dimensions from data verification
            prefix = schema.prefix[0] if schema.prefix else "key:"  # Get prefix from schema
            verify_result = self.verify_memory(client, prefix, monitor)
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
            memory_breakdown = self.calculate_comprehensive_memory_v2(
                client, scenario, memory_info, search_info, stats_dict, baseline_memory, data_memory, monitor
            )
            # info_all = client.execute_command("info", "memory")
            # monitor.log(f"📊 Memory Info: {info_all}")
            # Extract components for comparison - use PRE-INGESTION estimates vs POST-INGESTION actuals
            pre_ingestion_estimates = estimates  # From earlier estimate_memory_usage_v2 call
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
            # The estimate comes from estimate_memory_usage_v2, the actual comes from calculate_comprehensive_memory_v2
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
    
    def test_search_memory_usage(self, use_async: bool = True):
        """Benchmark search memory usage"""
        # Set up file logging
        log_file = self.setup_file_logging("search_memory")
        monitor = ProgressMonitor(self.server, "search_memory")
        monitor.start()
        monitor.log("=== SEARCH MEMORY BENCHMARK (10K keys) ===")

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
        csv_filename = f"search_memory_benchmark_results_{timestamp}.csv"
        
        scenarios = [
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
                total_keys=100000,
                tags_config=TagsConfig(
                    num_keys=100000,
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
                total_keys=100000,
                tags_config=TagsConfig(
                    num_keys=100000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),  # Exactly 1 tag per key
                    tag_length=LengthConfig(avg=1000, min=1000, max=1000),
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
            # BenchmarkScenario(
            #     name="10M_Unique",
            #     total_keys=1000000,
            #     tags_config=TagsConfig(
            #         num_keys=1000000,
            #         tags_per_key=TagDistribution(avg=2, min=1, max=5000),
            #         tag_length=LengthConfig(avg=64, min=2, max=3000),
            #         sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
            #     ),
            #     description="Unique tags 10M keys (2 tags/key, 64 bytes/tag) average, no sharing"
            # ),
            # BenchmarkScenario(
            #     name="10M_Quick_Mixed_HighShare_HighPrefix",
            #     total_keys=10000000,
            #     tags_config=TagsConfig(
            #         num_keys=10000000,
            #         tags_per_key=TagDistribution(avg=5, min=1, max=1000),
            #         tag_length=LengthConfig(avg=64, min=1, max=10000),
            #         tag_prefix=PrefixConfig(
            #             enabled=True,
            #             min_shared=1,  # 75% prefix sharing
            #             max_shared=900,
            #             share_probability=0.95,  # Very high prefix reuse
            #             prefix_pool_size=100  # 100 different prefixes
            #         ),
            #         sharing=TagSharingConfig(
            #             mode=TagSharingMode.SHARED_POOL,
            #             pool_size=100  # High sharing: 400 keys per tag
            #         )
            #     ),
            #     description="Mixed: High tag sharing + 75% prefix overlap, 40K keys, 100 unique tags"
            # ),
            # BenchmarkScenario(
            #     name="Quick_SharedPool",
            #     total_keys=10000,
            #     tags_config=TagsConfig(
            #         num_keys=10000,
            #         tags_per_key=TagDistribution(avg=5, min=3, max=8),
            #         tag_length=LengthConfig(avg=20, min=10, max=30),
            #         sharing=TagSharingConfig(mode=TagSharingMode.SHARED_POOL, pool_size=500)
            #     ),
            #     description="Shared pool of 500 tags"
            # ),
            # BenchmarkScenario(
            #     name="Quick_Groups",
            #     total_keys=10000,
            #     tags_config=TagsConfig(
            #         num_keys=10000,
            #         tags_per_key=TagDistribution(avg=5, min=3, max=8),
            #         tag_length=LengthConfig(avg=20, min=10, max=30),
            #         sharing=TagSharingConfig(
            #             mode=TagSharingMode.GROUP_BASED,
            #             keys_per_group=100,
            #             tags_per_group=20
            #         )
            #     ),
            #     description="Groups of 100 keys"
            # )
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
        
        monitor.log(f"{'Scenario':<35} {'Keys':>8} {'WoIndexKB':>10} {'wIndexKB':>10} {'IndexKB':>10} {'TagIdxKB':>10} {'SearchKB':>10} {'VecKB':>8} {'VecDims':>8} {'NumKB':>8} {'NumFlds':>8} {'InsTime':>8} {'K/s':>8} {'TagAcc':>8} {'TotAcc':>8} {'Mode':<15}")
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


def main():
    """Main entry point for standalone memory benchmark"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Standalone Memory Benchmark for Valkey Search")
    parser.add_argument("--port", type=int, default=6379, help="Valkey port (default: 6379)")
    parser.add_argument("--host", default="localhost", help="Valkey host (default: localhost)")
    parser.add_argument("--keys", type=int, default=10000, help="Number of keys to test (default: 10000)")
    parser.add_argument("--vector-dim", type=int, default=128, help="Vector dimensions (default: 128)")
    parser.add_argument("--tags", type=int, default=1000, help="Number of unique tags (default: 1000)")
    parser.add_argument("--tag-length", type=int, default=10, help="Average tag length (default: 10)")
    parser.add_argument("--algorithm", choices=["FLAT", "HNSW"], default="FLAT", help="Vector algorithm")
    parser.add_argument("--start-server", action="store_true", help="Start Valkey server automatically")
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Create benchmark instance
    benchmark = StandaloneMemoryBenchmark(valkey_port=args.port, redis_host=args.host)
    
    
    if args.start_server:
        logging.info("Starting Valkey server...")
        benchmark.start_valkey()
        benchmark.client = Valkey(host=args.host, port=args.port, decode_responses=True)
    else:
        # Just connect to existing server
        benchmark.client = Valkey(host=args.host, port=args.port, decode_responses=True)
        benchmark.client.ping()  # Test connection
    
    # Create a simple benchmark scenario
    from tags_builder import TagsConfig, TagDistribution, TagSharingConfig, TagSharingMode
    from string_generator import LengthConfig
    # try:
    tags_config = TagsConfig(
        num_keys=args.keys,
        tags_per_key=TagDistribution(avg=3, min=1, max=5),
        tag_length=LengthConfig(avg=args.tag_length, min=5, max=20),
        sharing=TagSharingConfig(
            mode=TagSharingMode.SHARED_POOL,
            pool_size=args.tags,
            reuse_probability=0.8
        )
    )
    
    # Create scenario
    scenario = BenchmarkScenario(
        name="standalone_test",
        total_keys=args.keys,
        tags_config=tags_config,
        description=f"Standalone test with {args.keys} keys",
        vector_dim=args.vector_dim,
        vector_algorithm=VectorAlgorithm.FLAT if args.algorithm == "FLAT" else VectorAlgorithm.HNSW,
        include_numeric=True
    )
    
    print(f"\nRunning memory benchmark:")
    print(f"  Keys: {args.keys:,}")
    print(f"  Vector dimensions: {args.vector_dim}")
    print(f"  Unique tags: {args.tags}")
    print(f"  Tag length: {args.tag_length}")
    print(f"  Algorithm: {args.algorithm}")
    
    # Run the benchmark
    result = benchmark.run_benchmark_scenario(scenario)
    print(f"\nBenchmark results: {len(result)} keys")
    for key, value in result.items():
        if isinstance(value, float):
            value = f"{value:.2f}"
        elif isinstance(value, int):
            value = f"{value:,}"
        elif isinstance(value, str):
            value = f'"{value}"'
        else:
            print(f"Unknown type for {key}: {type(value)}")
            continue
        print(f"  {key}: {value}")
    # print(f"\nResults:")
    # print(f"  Memory used: {result['total_memory_kb']:,} bytes ({result['memory_used']/1024/1024:.1f} MB)")
    # print(f"  Keys ingested: {result['keys_ingested']:,}")
    # print(f"  Ingestion time: {result['ingestion_time']:.2f} seconds")
    # print(f"  Keys/second: {result['keys_ingested']/result['ingestion_time']:,.0f}")
    
    # if 'memory_breakdown' in result:
    #     print(f"\nMemory breakdown:")
    #     for component, memory in result['memory_breakdown'].items():
    #         print(f"  {component}: {memory:,} bytes ({memory/1024/1024:.1f} MB)")
    
    # except Exception as e:
    #     logging.error(f"Benchmark failed: {e}")
    #     return 1
    
    # finally:
    #     benchmark.cleanup()
    
    return 0


if __name__ == "__main__":    
    sys.exit(main())