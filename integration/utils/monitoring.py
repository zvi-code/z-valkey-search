
#!/usr/bin/env python3
"""
Progress monitoring utilities for long-running operations.
Provides background monitoring of memory, keys, and indexing progress.
"""

import os
import sys
import time
import threading
import logging
import csv
from datetime import datetime
from typing import Dict, List, Any

from .valkey_clients import SilentValkeyClient, create_silent_client_from_server


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