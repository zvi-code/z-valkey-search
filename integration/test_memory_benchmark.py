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
from enum import Enum
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
    assert  'search_index_reclaimable_memory' in search_info, \
        f"Expected 'search_index_reclaimable_memory' in search info: {search_info}"
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
        'search_index_reclaimable_memory_kb': safe_get(search_info, 'search_index_reclaimable_memory', 0) // 1024,
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


class WorkloadType(Enum):
    """Types of workload operations"""
    DELETE = "del"
    QUERY = "query"
    INSERT = "insert"
    OVERWRITE = "overwrite"

@dataclass
class WorkloadOperation:
    """Single workload operation within a stage"""
    type: WorkloadType
    target_value: float = None  # For percentage-based operations (0.0-1.0)
    duration_seconds: int = None  # For time-based operations
    
    def __str__(self):
        if self.target_value is not None:
            if self.type in [WorkloadType.DELETE, WorkloadType.INSERT]:
                return f"{self.type.value}:{self.target_value*100:.0f}%"
            else:
                return f"{self.type.value}:{self.target_value:.0f}%"
        elif self.duration_seconds is not None:
            return f"{self.type.value}:{self.duration_seconds}s"
        else:
            return self.type.value

@dataclass
class WorkloadStage:
    """A stage containing one or more parallel operations"""
    name: str
    operations: List[WorkloadOperation]
    duration_seconds: int = None  # Optional stage duration override
    
    def __str__(self):
        ops_str = "+".join(str(op) for op in self.operations)
        if self.duration_seconds:
            return f"{self.name}[{ops_str}]:{self.duration_seconds}s"
        return f"{self.name}[{ops_str}]"

class WorkloadParser:
    """Parser for workload stage descriptions"""
    
    @staticmethod
    def parse_workload_string(workload_str: str) -> List[WorkloadStage]:
        """
        Parse workload string like:
        "del:50%,query+insert:80%,query:5min,query+del:50%,insert:100%,overwrite:10%:1min"
        
        Returns list of WorkloadStage objects
        """
        stages = []
        stage_strings = workload_str.split(',')
        
        for i, stage_str in enumerate(stage_strings):
            stage_str = stage_str.strip()
            stage_name = f"stage_{i+1}"
            operations = []
            stage_duration = None
            
            # Check if stage has explicit duration at the end
            if ':' in stage_str and (stage_str.endswith('min') or stage_str.endswith('s')):
                parts = stage_str.rsplit(':', 1)
                stage_str = parts[0]
                duration_str = parts[1]
                stage_duration = WorkloadParser._parse_duration(duration_str)
            
            # Parse parallel operations (separated by +)
            op_strings = stage_str.split('+')
            
            for op_str in op_strings:
                op_str = op_str.strip()
                operation = WorkloadParser._parse_operation(op_str)
                if operation:
                    operations.append(operation)
            
            if operations:
                stages.append(WorkloadStage(
                    name=stage_name,
                    operations=operations,
                    duration_seconds=stage_duration
                ))
        
        return stages
    
    @staticmethod
    def _parse_operation(op_str: str) -> Optional[WorkloadOperation]:
        """Parse a single operation string like 'del:50%' or 'query:5min'"""
        if ':' not in op_str:
            # Simple operation without parameters
            op_type = WorkloadParser._get_workload_type(op_str)
            if op_type:
                return WorkloadOperation(type=op_type)
            return None
        
        parts = op_str.split(':', 1)
        op_name = parts[0].strip()
        op_value = parts[1].strip()
        
        op_type = WorkloadParser._get_workload_type(op_name)
        if not op_type:
            return None
        
        # Parse value
        if op_value.endswith('%'):
            # Percentage value
            try:
                percentage = float(op_value[:-1]) / 100.0
                return WorkloadOperation(type=op_type, target_value=percentage)
            except ValueError:
                return None
        elif op_value.endswith('min') or op_value.endswith('s'):
            # Duration value
            duration = WorkloadParser._parse_duration(op_value)
            if duration:
                return WorkloadOperation(type=op_type, duration_seconds=duration)
        else:
            # Try to parse as raw number (assume percentage)
            try:
                value = float(op_value)
                # If > 1, assume it's a percentage, otherwise a fraction
                if value > 1:
                    value = value / 100.0
                return WorkloadOperation(type=op_type, target_value=value)
            except ValueError:
                return None
        
        return None
    
    @staticmethod
    def _get_workload_type(name: str) -> Optional[WorkloadType]:
        """Convert string to WorkloadType enum"""
        name = name.lower().strip()
        for wt in WorkloadType:
            if wt.value == name:
                return wt
        # Also support full names
        if name == "delete":
            return WorkloadType.DELETE
        elif name == "query":
            return WorkloadType.QUERY
        elif name == "insert":
            return WorkloadType.INSERT
        elif name == "overwrite":
            return WorkloadType.OVERWRITE
        return None
    
    @staticmethod
    def _parse_duration(duration_str: str) -> Optional[int]:
        """Parse duration string to seconds"""
        duration_str = duration_str.strip()
        try:
            if duration_str.endswith('min'):
                return int(float(duration_str[:-3]) * 60)
            elif duration_str.endswith('s'):
                return int(float(duration_str[:-1]))
            else:
                return int(float(duration_str))
        except ValueError:
            return None

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
    # Workload stages configuration
    workload_stages: List[WorkloadStage] = field(default_factory=list)
    workload_string: str = None  # Raw workload string to parse
    
    def __post_init__(self):
        """Parse workload string if provided"""
        if self.workload_string and not self.workload_stages:
            self.workload_stages = WorkloadParser.parse_workload_string(self.workload_string)



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

    def __init__(self, server, monitor_csv_filename: str, operation_name: str, index_config: Dict[str, Any] = None):
        self.server = server
        self.monitor_csv_filename = monitor_csv_filename
        self.operation_name = operation_name
        self.running = False
        self.thread = None
        self.start_time = time.time()
        self.last_memory = 0
        # Store index configuration passed from test case
        self.index_config = index_config or {}
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
        self.last_reclaimable_memory = 0
        self.stall_callback = None  # Optional callback when stall detected
        
    def _safe_extract_speed(self, speed_str: str) -> str:
        """Safely extract numeric speed value from string like '2567 keys/sec'"""
        try:
            if isinstance(speed_str, str) and 'keys/sec' in speed_str:
                return speed_str.replace(' keys/sec', '').strip()
            return str(speed_str) if speed_str else ''
        except:
            return ''
    
    def _safe_extract_tasks(self, tasks_str: str) -> str:
        """Safely extract tasks value from string like '200 async'"""
        try:
            if isinstance(tasks_str, str) and ' async' in tasks_str:
                return tasks_str.replace(' async', '').strip()
            return str(tasks_str) if tasks_str else ''
        except:
            return ''
    
    def _safe_to_int(self, value) -> int:
        """Safely convert value to int with fallback"""
        try:
            if isinstance(value, (int, float)):
                return int(value)
            elif isinstance(value, str):
                # Remove any non-numeric characters except minus
                import re
                numeric_str = re.sub(r'[^\d-]', '', value)
                return int(numeric_str) if numeric_str else 0
            return 0
        except:
            return 0
        
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
        # open a .csv file to log memory and key stats
        # 2025-07-28 11:42:16 - INFO - [Thread-276715714507136|Thread-1 (_monitor)] monitoring::used_memory_human=888.16M
        # 2025-07-28 11:42:16 - INFO - [Thread-276715714507136|Thread-1 (_monitor)] monitoring::search_used_memory_human=18.02KiB
        # 2025-07-28 11:42:20 - INFO - 🔄 [search_memory] Time: 281s | Memory: 905,880 KB (+-3,600) | Keys: 700,000 (+0) 
        # | Phase: Insertion | Progress: 700,000/1,000,000 (70.0%) | Speed: 2567 keys/sec | Tasks: 200 async | ETA: 1.9m
        # log operation name as a column as well as timestamp (of the sample)
        # csv_filename = f"monitoring_{self.operation_name.replace(' ', '_')}.csv"
        csv_fieldnames = ['timestamp', 'operation_name', # Index metadata fields
                          'phase', 'index_type', 'vector_dim', 'vector_algorithm', 'hnsw_m', 
                          'num_tag_fields', 'num_numeric_fields', 'num_vector_fields',
                          'index_state', 'index_num_docs', 'index_mutation_queue',
                          # Tag configuration fields
                          'tag_avg_length', 'tag_prefix_sharing', 'tag_avg_per_key', 'tag_avg_keys_per_tag',
                          'tag_unique_ratio', 'tag_reuse_factor',
                          # Numeric fields info
                          'numeric_fields_names', 'numeric_fields_ranges', 'used_memory_kb', 'search_used_memory_kb', 
                          'search_index_reclaimable_memory_kb', 'keys_count', 'speed', 'eta', 'tasks', 
                          'progress', 'phase_progress', 'phase_speed', 'phase_eta', 
                          'phase_tasks', 'keys_delta', 'memory_delta', 'search_memory_delta',
                          'reclaimable_memory_delta', 
                          ]
        
        # Open CSV file and write header if needed
        csv_file = None
        csv_writer = None
        try:
            if not os.path.exists(self.monitor_csv_filename):
                csv_file = open(self.monitor_csv_filename, 'w', newline='', encoding='utf-8')
                csv_writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)
                csv_writer.writeheader()
            else:
                csv_file = open(self.monitor_csv_filename, 'a', newline='', encoding='utf-8')
                csv_writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)
        except Exception as e:
            self.log(f"⚠️  Failed to open CSV file {self.monitor_csv_filename}: {e}")
            csv_file = None
            csv_writer = None
        
        self.log("📊 Starting monitoring thread...")
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
                    reclaimable_memory_kb = memory_summary['search_index_reclaimable_memory_kb']

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
                    reclaimable_memory_delta = reclaimable_memory_kb - self.last_reclaimable_memory
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
                    if reclaimable_memory_kb > 0:
                        status_parts.append(f"Reclaimable: {reclaimable_memory_kb:,} KB (+{reclaimable_memory_delta:,})")
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
                    
                    # Write data to CSV if file is open
                    if csv_writer and csv_file:
                        try:
                            # Prepare CSV row data
                            csv_row = {
                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],  # Include milliseconds
                                'operation_name': self.operation_name,
                                'phase': self.current_status.get('Phase', 'Unknown') if self.current_status else 'Unknown',                                
                                # Initialize index metadata fields with defaults
                                'index_type': '',
                                'vector_dim': -1,
                                'vector_algorithm': '',
                                'hnsw_m': -1,
                                'num_tag_fields': 0,
                                'num_numeric_fields': 0,
                                'num_vector_fields': 0,
                                'index_state': '',
                                'index_num_docs': 0,
                                'index_mutation_queue': 0,
                                # Initialize tag configuration fields
                                'tag_avg_length': 0,
                                'tag_prefix_sharing': 0,
                                'tag_avg_per_key': 0,
                                'tag_avg_keys_per_tag': 0,
                                'tag_unique_ratio': 0,
                                'tag_reuse_factor': 0,
                                # Initialize numeric fields info
                                'numeric_fields_names': '',
                                'numeric_fields_ranges': '',                                
                                'used_memory_kb': current_memory_kb,
                                'search_used_memory_kb': search_memory_kb,
                                'search_index_reclaimable_memory_kb': reclaimable_memory_kb,
                                'keys_count': current_keys,
                                'speed': self._safe_extract_speed(self.current_status.get('Speed', '')) if self.current_status else '',
                                'eta': self.current_status.get('ETA', '') if self.current_status else '',
                                'tasks': self._safe_extract_tasks(self.current_status.get('Tasks', '')) if self.current_status else '',
                                'progress': self.current_status.get('Progress', '') if self.current_status else '',
                                'phase_progress': '',  # Can be populated from phase-specific status
                                'phase_speed': '',     # Can be populated from phase-specific status
                                'phase_eta': '',       # Can be populated from phase-specific status
                                'phase_tasks': '',     # Can be populated from phase-specific status
                                'keys_delta': keys_delta,
                                'memory_delta': memory_delta,
                                'search_memory_delta': search_memory_delta,
                                'reclaimable_memory_delta': reclaimable_memory_delta                                
                            }
                            
                            # Use index configuration passed from test case
                            if self.index_config:
                                # Set index type
                                csv_row['index_type'] = self.index_config.get('type', 'vector')
                                
                                # Vector field configuration
                                csv_row['vector_dim'] = self.index_config.get('vector_dim', -1)
                                csv_row['vector_algorithm'] = self.index_config.get('vector_algorithm', '')
                                csv_row['hnsw_m'] = self.index_config.get('hnsw_m', -1)
                                
                                # Field counts
                                csv_row['num_tag_fields'] = self.index_config.get('num_tag_fields', 0)
                                csv_row['num_numeric_fields'] = self.index_config.get('num_numeric_fields', 0)
                                csv_row['num_vector_fields'] = self.index_config.get('num_vector_fields', 0)
                                
                                # Tag configuration
                                csv_row['tag_avg_length'] = self.index_config.get('tag_avg_length', 0)
                                csv_row['tag_prefix_sharing'] = self.index_config.get('tag_prefix_sharing', 0)
                                csv_row['tag_avg_per_key'] = self.index_config.get('tag_avg_per_key', 0)
                                csv_row['tag_avg_keys_per_tag'] = self.index_config.get('tag_avg_keys_per_tag', 0)
                                csv_row['tag_unique_ratio'] = self.index_config.get('tag_unique_ratio', 0)
                                csv_row['tag_reuse_factor'] = self.index_config.get('tag_reuse_factor', 0)
                                
                                # Numeric fields info
                                csv_row['numeric_fields_names'] = self.index_config.get('numeric_fields_names', '')
                                csv_row['numeric_fields_ranges'] = self.index_config.get('numeric_fields_ranges', '')
                            
                            # Extract runtime index state from FT.INFO if available
                            if index_info:
                                # Process each index (usually just one)
                                for idx_name, info in index_info.items():
                                    # Get runtime index state
                                    csv_row['index_state'] = info.get('state', '')
                                    csv_row['index_num_docs'] = self._safe_to_int(info.get('num_docs', 0))
                                    csv_row['index_mutation_queue'] = self._safe_to_int(info.get('mutation_queue_size', 0))
                                    break  # Only process first index
                            
                            # Write the row
                            csv_writer.writerow(csv_row)
                            csv_file.flush()  # Ensure data is written immediately
                            
                        except Exception as e:
                            self.log(f"⚠️  Failed to write to CSV: {e}")
                    
                    self.last_memory = current_memory_kb
                    self.last_keys = current_keys
                    self.last_search_memory = search_memory_kb
                    self.last_reclaimable_memory = reclaimable_memory_kb
                    last_report = current_time
                    
            except Exception as e:
                logging.info(f"⚠️ Monitor error: {e}")
                sys.stdout.flush()
                
            time.sleep(1)  # Check every second for messages, report every 5 seconds
        
        # Clean up: close CSV file if it was opened
        if csv_file:
            try:
                csv_file.close()
                self.log(f"📊 Monitoring data saved to {self.monitor_csv_filename}")
            except Exception as e:
                self.log(f"⚠️  Error closing CSV file: {e}")
        
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
                    "Phase": "Insertion",
                    "Progress": f"{processed_count:,}/{scenario.total_keys:,} ({progress_pct:.1f}%)",
                    "Speed": f"{keys_per_sec:.0f} keys/sec",
                    "Tasks": f"{num_tasks} async",
                    "ETA": eta_str
                })
            
            return len(batch_data), batch_time
        
        # Create async tasks with streaming approach
        active_tasks = set()
        task_id = 0
        generator_iter = iter(generator)
        generator_exhausted = False
        
        # Process batches in a streaming fashion
        while not generator_exhausted or active_tasks:
            # Submit new tasks if we have capacity
            while len(active_tasks) < num_tasks and not generator_exhausted:
                try:
                    batch = next(generator_iter)
                    task = asyncio.create_task(process_batch_async(batch, task_id % num_tasks))
                    active_tasks.add(task)
                    task_id += 1
                except StopIteration:
                    generator_exhausted = True
                    break
            
            # Wait for at least one task to complete
            if active_tasks:
                done, active_tasks = await asyncio.wait(
                    active_tasks, 
                    return_when=asyncio.FIRST_COMPLETED
                )
                # Process completed tasks
                for task in done:
                    try:
                        await task  # This will raise any exceptions
                    except Exception as e:
                        monitor.log(f"❌ Async task failed: {e}")
        
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
        fixed_headers = [
            'timestamp', 'scenario_name', 'description', 'total_keys', 'total_memory_kb', 
            'search_memory_kb', 'reclaimable_memory_kb', 'insertion_time', 'keys_per_second',
            # Index metadata
            'vector_dim', 'vector_algorithm', 'hnsw_m', 'num_tag_fields', 'num_numeric_fields',
            # Tag configuration 
            'tag_avg_length', 'tag_prefix_sharing', 'tag_avg_per_key', 'tag_avg_keys_per_tag',
            'tag_unique_ratio', 'tag_reuse_factor', 'tag_sharing_mode',
            # Numeric fields
            'numeric_fields_names', 'numeric_fields_ranges',
            # Memory accuracy
            'tag_memory_accuracy', 'total_memory_accuracy',
            # Additional fields that might be present
            'tags_config', 'index_state', 'phase_time'
        ]
        
        # Check if file exists
        file_exists = os.path.exists(csv_filename)
        
        # Write to CSV with consistent structure
        with open(csv_filename, 'a', newline='', encoding='utf-8') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=fixed_headers, extrasaction='ignore')
            
            # Write header if needed (new file or explicitly requested)
            if write_header or not file_exists:
                writer.writeheader()
            
            # Ensure all required fields have default values
            complete_row = {header: result.get(header, '') for header in fixed_headers}
            
            # Write the data row  
            writer.writerow(complete_row)
        
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
    
    def calculate_comprehensive_memory(self, client: Valkey, scenario: BenchmarkScenario, 
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

    def run_benchmark_scenario(self, monitor_csv_filename: str, scenario: BenchmarkScenario, monitor: ProgressMonitor = None) -> Dict:
        """Run a single benchmark scenario with full monitoring"""
        # Prepare index configuration for monitor
        index_config = {
            'type': 'vector',
            'vector_dim': scenario.vector_dim,
            'vector_algorithm': scenario.vector_algorithm.name if hasattr(scenario.vector_algorithm, 'name') else str(scenario.vector_algorithm),
            'hnsw_m': scenario.hnsw_m,
            'num_tag_fields': 1,  # We always have tags field
            'num_numeric_fields': len(scenario.numeric_fields) if scenario.numeric_fields else 0,
            'num_vector_fields': 1,  # We always have vector field
            
            # Tag configuration from scenario
            'tag_avg_length': scenario.tags_config.tag_length.avg if scenario.tags_config.tag_length else 0,
            'tag_prefix_sharing': scenario.tags_config.tag_prefix.share_probability if scenario.tags_config.tag_prefix else 0,
            'tag_avg_per_key': scenario.tags_config.tags_per_key.avg if scenario.tags_config.tags_per_key else 0,
            
            # Calculate tag sharing metrics based on sharing mode
            'tag_avg_keys_per_tag': 0,  # Will be calculated based on sharing mode
            'tag_unique_ratio': 0,  # Will be calculated based on sharing mode
            'tag_reuse_factor': 0,  # Will be calculated based on sharing mode
            
            # Numeric fields info
            'numeric_fields_names': ','.join(scenario.numeric_fields.keys()) if scenario.numeric_fields else '',
            'numeric_fields_ranges': ';'.join([f"{k}:{v[0]}-{v[1]}" for k, v in scenario.numeric_fields.items()]) if scenario.numeric_fields else ''
        }
        
        # Calculate tag sharing metrics based on sharing mode
        if scenario.tags_config.sharing:
            if scenario.tags_config.sharing.mode == TagSharingMode.UNIQUE:
                index_config['tag_unique_ratio'] = 1.0
                index_config['tag_avg_keys_per_tag'] = 1.0
            elif scenario.tags_config.sharing.mode == TagSharingMode.SHARED_POOL:
                pool_size = scenario.tags_config.sharing.pool_size or 1000
                index_config['tag_unique_ratio'] = pool_size / scenario.total_keys if scenario.total_keys > 0 else 0
                index_config['tag_avg_keys_per_tag'] = scenario.total_keys / pool_size if pool_size > 0 else 0
                index_config['tag_reuse_factor'] = scenario.tags_config.sharing.reuse_probability
            elif scenario.tags_config.sharing.mode == TagSharingMode.GROUP_BASED:
                keys_per_group = scenario.tags_config.sharing.keys_per_group or 100
                index_config['tag_avg_keys_per_tag'] = keys_per_group
        
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
        can_run, message = self.validate_memory_requirements(scenario)
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
        

        try:
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
            estimates = self.estimate_memory_usage(scenario, monitor)
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
            num_tasks = min(100, max(10, scenario.total_keys // config.batch_size))
            monitor.log(f"🚀 Mode: ASYNC I/O with {num_tasks} concurrent tasks")
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
                        "Phase": "HSET (Batch)",
                        "Progress": f"{processed_count:,}/{scenario.total_keys:,} ({progress_pct:.1f}%)",
                        "Speed": f"{keys_per_sec:.0f} keys/sec",
                        "Threads": f"{num_threads} active",
                        "ETA": eta_str
                    })
                
                return len(batch_data), batch_time
            
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
            search_info = client.execute_command("info", "modules")
            search_memory_kb = search_info['search_used_memory_bytes'] // 1024
            search_reclaimed_kb = search_info['search_index_reclaimable_memory'] // 1024
            monitor.log(f"search info: {search_info}")
            monitor.log(f"📊 Search Memory: {search_memory_kb:,} KB"
                        f" (reclaimable: {search_reclaimed_kb:,} KB)")
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
            memory_breakdown = self.calculate_comprehensive_memory(
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
    
        except Exception as e:
            # Handle any errors during the benchmark
            monitor.error(f"  ❌ Scenario failed: {e}")
            result = {
                'scenario_name': scenario.name,
                'description': scenario.description,
                'total_keys': scenario.total_keys,
                'skipped': True,
                'reason': f"Failed: {str(e)}",
                
                # Add basic configuration for failed scenarios too
                'vector_dim': scenario.vector_dim,
                'vector_algorithm': scenario.vector_algorithm.name if hasattr(scenario.vector_algorithm, 'name') else str(scenario.vector_algorithm),
                'hnsw_m': scenario.hnsw_m if (hasattr(scenario.vector_algorithm, 'name') and scenario.vector_algorithm.name == 'HNSW') or (str(scenario.vector_algorithm) == 'HNSW') else '',
                'num_tag_fields': 1,
                'num_numeric_fields': len(scenario.numeric_fields) if scenario.numeric_fields else 0,
                'tag_avg_length': scenario.tags_config.tag_length.avg if scenario.tags_config and scenario.tags_config.tag_length else 0,
                'tag_prefix_sharing': scenario.tags_config.tag_prefix.share_probability if scenario.tags_config and scenario.tags_config.tag_prefix else 0,
                'tag_avg_per_key': scenario.tags_config.tags_per_key.avg if scenario.tags_config and scenario.tags_config.tags_per_key else 0,
                'tag_sharing_mode': scenario.tags_config.sharing.mode.name if scenario.tags_config and scenario.tags_config.sharing and hasattr(scenario.tags_config.sharing.mode, 'name') else str(scenario.tags_config.sharing.mode) if scenario.tags_config and scenario.tags_config.sharing else 'unique',
                'numeric_fields_names': ', '.join(scenario.numeric_fields.keys()) if scenario.numeric_fields else '',
                'numeric_fields_ranges': str(scenario.numeric_fields) if scenario.numeric_fields else '',
                # Add empty fields for failed scenarios to match CSV structure
                'search_memory_kb': 0,
                'reclaimable_memory_kb': 0,
                'insertion_time': 0,
                'keys_per_second': 0,
                'total_memory_kb': 0,
                'tag_memory_accuracy': 0,
                'total_memory_accuracy': 0,
                'index_state': 'failed',
                'phase_time': 0,
            }
            assert False, f"Scenario {scenario.name} failed: {e}"
            
        finally:
            # Clean up resources
            if client_pool is not None:
                client_pool.close_all()
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
        
        # 1. Unique Tags Tests - scaling keys, tag length, and tags per key
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
                    description=f"Unique tags: {keys} keys, {tag_len}B tags",
                    vector_dim=8,
                    vector_algorithm=VectorAlgorithm.FLAT,
                    vector_metric=VectorMetric.COSINE,
                ))
        
        for keys in [1000000]:  # Fixed key count for tags per key variation
            for tpk in tags_per_key_values:
                scenarios.append(BenchmarkScenario(
                    name=f"Unique_Keys{keys}_TagsPerKey{tpk}",
                    total_keys=keys,
                    tags_config=TagsConfig(
                        num_keys=keys,
                        tags_per_key=TagDistribution(avg=tpk, min=tpk, max=tpk),
                        tag_length=LengthConfig(avg=100, min=100, max=100),
                        sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                    ),
                    description=f"Unique tags: {keys} keys, {tpk} tags/key",
                    vector_dim=8,
                    vector_algorithm=VectorAlgorithm.FLAT,
                    vector_metric=VectorMetric.COSINE,
                ))
        
        # 2. Tag Sharing Tests - varying sharing ratios and group sizes
        sharing_ratios = [0.1, 0.25, 0.5, 0.75, 0.9]  # 10% to 90% sharing
        group_sizes = [10, 500, 1000, 5000]
        
        for ratio in sharing_ratios:
            scenarios.append(BenchmarkScenario(
                name=f"Sharing_Ratio{int(ratio*100)}pct",
                total_keys=1000000,
                tags_config=TagsConfig(
                    num_keys=1000000,
                    tags_per_key=TagDistribution(avg=2, min=2, max=2),
                    tag_length=LengthConfig(avg=100, min=100, max=100),
                    sharing=TagSharingConfig(
                        mode=TagSharingMode.SHARED_POOL,
                        pool_size=200,
                        reuse_probability=ratio
                    )
                ),
                description=f"Tag sharing: {int(ratio*100)}% shared, 2 tags/key",
                vector_dim=8,
                vector_algorithm=VectorAlgorithm.FLAT,
                vector_metric=VectorMetric.COSINE,
            ))
        
        for group_size in group_sizes:
            scenarios.append(BenchmarkScenario(
                name=f"GroupBased_Size{group_size}",
                total_keys=1000000,
                tags_config=TagsConfig(
                    num_keys=1000000,
                    tags_per_key=TagDistribution(avg=3, min=3, max=3),
                    tag_length=LengthConfig(avg=100, min=100, max=100),
                    sharing=TagSharingConfig(
                        mode=TagSharingMode.GROUP_BASED,
                        keys_per_group=group_size,
                        tags_per_group=10
                    )
                ),
                description=f"Group-based sharing: groups of {group_size}",
                vector_dim=8,
                vector_algorithm=VectorAlgorithm.FLAT,
                vector_metric=VectorMetric.COSINE,
            ))
        
        # 3. Prefix Sharing Tests - varying prefix share ratios and pool sizes
        prefix_ratios = [0.1, 0.25, 0.5, 0.75]
        prefix_pool_sizes = [100, 500, 1000, 5000]
        
        for ratio in prefix_ratios:
            scenarios.append(BenchmarkScenario(
                name=f"PrefixShare_{int(ratio*100)}pct",
                total_keys=10000,
                tags_config=TagsConfig(
                    num_keys=10000,
                    tags_per_key=TagDistribution(avg=2, min=2, max=2),
                    tag_length=LengthConfig(avg=200, min=200, max=200),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE),
                    tag_prefix=PrefixConfig(
                        enabled=True,
                        min_shared=50,
                        max_shared=50,
                        share_probability=ratio,
                        prefix_pool_size=20
                    )
                ),
                description=f"Prefix sharing: {int(ratio*100)}% shared prefixes",
                vector_dim=8,
                vector_algorithm=VectorAlgorithm.FLAT,
                vector_metric=VectorMetric.COSINE,
            ))
        
        for pool_size in prefix_pool_sizes:
            scenarios.append(BenchmarkScenario(
                name=f"PrefixPool_{pool_size}",
                total_keys=1000000,
                tags_config=TagsConfig(
                    num_keys=10000,
                    tags_per_key=TagDistribution(avg=2, min=2, max=2),
                    tag_length=LengthConfig(avg=200, min=200, max=200),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE),
                    tag_prefix=PrefixConfig(
                        enabled=True,
                        min_shared=50,
                        max_shared=50,
                        share_probability=0.5,
                        prefix_pool_size=pool_size
                    )
                ),
                description=f"Prefix pool: {pool_size} unique prefixes",
                vector_dim=8,
                vector_algorithm=VectorAlgorithm.FLAT,
                vector_metric=VectorMetric.COSINE,
            ))
        
        # 4. Combined Pattern Tests - both prefix and tag sharing
        combined_configs = [
            (0.25, 0.3),  # 25% tag sharing, 30% prefix sharing
            (0.5, 0.5),   # 50% tag sharing, 50% prefix sharing
            (0.75, 0.7),  # 75% tag sharing, 70% prefix sharing
        ]
        
        for tag_ratio, prefix_ratio in combined_configs:
            scenarios.append(BenchmarkScenario(
                name=f"Combined_Tag{int(tag_ratio*100)}_Prefix{int(prefix_ratio*100)}",
                total_keys=1000000,
                tags_config=TagsConfig(
                    num_keys=1000000,
                    tags_per_key=TagDistribution(avg=3, min=3, max=3),
                    tag_length=LengthConfig(avg=150, min=150, max=150),
                    sharing=TagSharingConfig(
                        mode=TagSharingMode.SHARED_POOL,
                        pool_size=150,
                        reuse_probability=tag_ratio
                    ),
                    tag_prefix=PrefixConfig(
                        enabled=True,
                        min_shared=40,
                        max_shared=140,
                        share_probability=prefix_ratio,
                        prefix_pool_size=20
                    )
                ),
                description=f"Combined: {int(tag_ratio*100)}% tag + {int(prefix_ratio*100)}% prefix sharing",
                vector_dim=8,
                vector_algorithm=VectorAlgorithm.FLAT,
                vector_metric=VectorMetric.COSINE,
            ))
        
        # 5. Extreme Cases - edge cases and stress tests
        extreme_scenarios = [
            BenchmarkScenario(
                name="Extreme_TinyTags",
                total_keys=500000,
                tags_config=TagsConfig(
                    num_keys=500000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),
                    tag_length=LengthConfig(avg=5, min=5, max=5),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description="Extreme: 50K keys with 5-byte unique tags",
                vector_dim=8,
                vector_algorithm=VectorAlgorithm.FLAT,
                vector_metric=VectorMetric.COSINE,
            ),
            BenchmarkScenario(
                name="Extreme_LargeTags",
                total_keys=100000,
                tags_config=TagsConfig(
                    num_keys=100000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),
                    tag_length=LengthConfig(avg=5000, min=5000, max=5000),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description="Extreme: 1K keys with 5KB unique tags",
                vector_dim=8,
                vector_algorithm=VectorAlgorithm.FLAT,
                vector_metric=VectorMetric.COSINE,
            ),
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
                vector_dim=8,
                vector_algorithm=VectorAlgorithm.FLAT,
                vector_metric=VectorMetric.COSINE,
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
                vector_dim=8,
                vector_algorithm=VectorAlgorithm.FLAT,
                vector_metric=VectorMetric.COSINE,
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
            
            try:
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
                    
            except Exception as e:
                monitor.log(f"  ✗ Error: {str(e)}")
                continue
            
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