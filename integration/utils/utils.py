

from dataclasses import dataclass, field
import logging
import threading
from typing import Any, Counter, Dict, List, Optional


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

@dataclass
class DistributionStats:
    """Statistics for a distribution with bucketed histogram for memory efficiency"""
    count: int = 0
    min: float = float('inf')
    max: float = float('-inf')
    sum: float = 0
    histogram: Dict[str, int] = field(default_factory=dict)  # bucket_range -> count
    bucket_size: int = 1  # Default bucket size for grouping values
    
    def __post_init__(self):
        # Initialize common buckets for length distributions
        if not self.histogram:
            self._init_length_buckets()
    
    def _init_length_buckets(self):
        """Initialize buckets optimized for length distributions"""
        # Common length ranges for tags, keys, etc.
        buckets = ["1-5", "6-10", "11-20", "21-50", "51-100", "101-200", "201-500", "500+"]
        for bucket in buckets:
            self.histogram[bucket] = 0
    
    def _get_bucket_key(self, value: float) -> str:
        """Get bucket key for a value"""
        val = int(value)
        if val <= 5: return "1-5"
        elif val <= 10: return "6-10"
        elif val <= 20: return "11-20"
        elif val <= 50: return "21-50"
        elif val <= 100: return "51-100"
        elif val <= 200: return "101-200"
        elif val <= 500: return "201-500"
        else: return "500+"
    
    def add(self, value: float):
        """Add a value to the distribution"""
        self.count += 1
        self.min = min(self.min, value)
        self.max = max(self.max, value)
        self.sum += value
        
        # Add to appropriate bucket
        bucket_key = self._get_bucket_key(value)
        self.histogram[bucket_key] += 1
    
    @property
    def mean(self) -> float:
        return self.sum / self.count if self.count > 0 else 0
    
    def get_percentile(self, p: float) -> float:
        """Get approximate percentile value using bucket midpoints"""
        if not self.histogram or self.count == 0:
            return 0
        
        # Convert buckets to sorted list with midpoint values
        bucket_data = []
        for bucket_range, count in self.histogram.items():
            if count == 0:
                continue
            
            # Calculate bucket midpoint
            if bucket_range == "500+":
                midpoint = 750  # Estimate for 500+ range
            else:
                parts = bucket_range.split('-')
                if len(parts) == 2:
                    start, end = int(parts[0]), int(parts[1])
                    midpoint = (start + end) / 2
                else:
                    midpoint = int(parts[0])
            
            bucket_data.append((midpoint, count))
        
        # Sort by midpoint value
        bucket_data.sort(key=lambda x: x[0])
        
        target = self.count * p / 100
        cumulative = 0
        
        for midpoint, count in bucket_data:
            cumulative += count
            if cumulative >= target:
                return midpoint
        
        return bucket_data[-1][0] if bucket_data else 0


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