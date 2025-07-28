

import logging
import time
from typing import Any, Dict, Tuple
from integration.utils.hash_generator import VectorAlgorithm
from integration.utils.monitoring import ProgressMonitor
from integration.utils.tags_builder import TagSharingMode
from integration.utils.workload_scenario import BenchmarkScenario
from valkey import ResponseError
from valkey.client import Valkey

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logging.warning("psutil not available - memory validation will use fallback method")



def _calculate_estimated_stats(scenario: BenchmarkScenario) -> dict:
    """Calculate estimated statistics for a scenario"""
    tags_config = scenario.tags_config
    
    # Calculate average tags per key
    avg_tags_per_key = tags_config.tags_per_key.avg if tags_config.tags_per_key else 5  # Default to 5 if not specified
    
    # Calculate unique tags based on sharing mode
    if not tags_config.sharing:
        # Default to unique if no sharing config
        unique_tags = scenario.total_keys * avg_tags_per_key
    elif tags_config.sharing.mode == TagSharingMode.UNIQUE:
        unique_tags = scenario.total_keys * avg_tags_per_key
    elif tags_config.sharing.mode == TagSharingMode.PERFECT_OVERLAP:
        unique_tags = avg_tags_per_key  # All keys share same tags
    elif tags_config.sharing.mode == TagSharingMode.SHARED_POOL:
        unique_tags = tags_config.sharing.pool_size
    elif tags_config.sharing.mode == TagSharingMode.GROUP_BASED:
        keys_per_group = tags_config.sharing.keys_per_group or 100  # Default to 100 if not specified
        tags_per_group = tags_config.sharing.tags_per_group or 10   # Default to 10 if not specified
        num_groups = scenario.total_keys // keys_per_group
        unique_tags = num_groups * tags_per_group
    else:
        unique_tags = scenario.total_keys * avg_tags_per_key * 0.3  # Default estimate
    
    # Calculate keys per tag
    total_tag_instances = scenario.total_keys * avg_tags_per_key
    avg_keys_per_tag = total_tag_instances / max(1, unique_tags)
    
    return {
        'unique_tags': int(unique_tags),
        'avg_tags_per_key': avg_tags_per_key,
        'avg_keys_per_tag': avg_keys_per_tag,
        'avg_tag_length': tags_config.tag_length.avg if tags_config.tag_length else 10  # Default to 10 if not specified
    }

def estimate_memory_usage(scenario: BenchmarkScenario, monitor :ProgressMonitor=None) -> Dict[str, int]:
    """
    Improved pre-ingestion memory estimation based on index definition and data properties.
    Provides the best possible estimate without access to actual server state.
    """
    # Get scenario statistics
    stats = _calculate_estimated_stats(scenario)
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
        monitor.log("─" * 50)
        monitor.log("─" * 50)
        monitor.log("🎯 ESTIMATED USAGE")
        monitor.log("─" * 50)
        monitor.log(f"📁 Data Memory:      {valkey_memory // (1024**2):,} MB")
        monitor.log(f"🏷️  Tag Index:       {tag_index_memory // (1024**2):,} MB")  
        monitor.log(f"🎯 Vector Index:     {vector_index_memory // (1024**2):,} MB")
        monitor.log(f"💰 Total (overhead): {total_memory // (1024**2):,} MB")
        monitor.log("")
        monitor.log("─" * 50)
        monitor.log("─" * 50)
    
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

def get_available_memory_bytes() -> int:
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


def validate_memory_requirements(scenario: BenchmarkScenario) -> Tuple[bool, str]:
    """Validate if scenario can run without using swap"""
    available_memory = get_available_memory_bytes()
    estimates = estimate_memory_usage(scenario)
    
    # Ensure we use less than 50% of available memory to avoid swap
    memory_limit = available_memory * 0.5
    
    if estimates['total_memory'] > memory_limit:
        return False, (
            f"Scenario '{scenario.name}' requires ~{estimates['total_memory'] / (1024**3):.1f}GB "
            f"but only {memory_limit / (1024**3):.1f}GB is safely available "
            f"(50% of {available_memory / (1024**3):.1f}GB free memory)"
        )
    
    return True, f"Memory check passed: ~{estimates['total_memory'] / (1024**3):.1f}GB required, {available_memory / (1024**3):.1f}GB available"

def calculate_comprehensive_memory(client: Valkey, scenario: BenchmarkScenario, 
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

def verify_memory(client: Valkey, prefix: str = "key:", monitor: ProgressMonitor = None) -> Dict[str, int]:
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
    monitor.log("🔍 MEMORY VERIFICATION")
    monitor.log("─" * 50)
    verification_start = time.time()

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
    verification_time = time.time() - verification_start
    monitor.log(f"⏱️  Verification time: {verification_time:.1f}s")
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