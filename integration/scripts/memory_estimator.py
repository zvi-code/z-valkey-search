#!/usr/bin/env python3
"""
Memory estimator using isolated coefficients.
Provides accurate memory estimates based on component-specific measurements.
"""

import json
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class MemoryCoefficients:
    """Memory coefficients from isolated testing covering all dimensions"""
    
    # Base infrastructure costs
    base_overhead: float = 0.0              # Base index overhead
    per_key_cost: float = 0.0               # Per key in index (independent of fields)
    
    # Tag storage costs
    bytes_per_tag_character: float = 0.0    # Per character in tag values
    per_unique_tag: float = 0.0             # Per unique tag in index
    tag_prefix_sharing_factor: float = 1.0  # Reduction factor for prefix sharing
    
    # Vector storage costs  
    bytes_per_vector_byte: float = 0.0      # Overhead factor for vector storage
    vector_flat_overhead: float = 0.0       # FLAT algorithm overhead per vector
    vector_hnsw_overhead: float = 0.0       # HNSW algorithm base overhead
    hnsw_m_factor: float = 0.0              # Additional cost per M value
    
    # Numeric field costs
    bytes_per_numeric_field: float = 0.0    # Per numeric field per key
    numeric_field_overhead: float = 0.0     # Per numeric field in schema
    
    # Schema definition overhead
    schema_base_cost: float = 0.0           # Base schema overhead
    cost_per_field_definition: float = 0.0  # Per field in schema
    
    @classmethod
    def from_file(cls, filepath: str) -> 'MemoryCoefficients':
        """Load coefficients from JSON file"""
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        # Create instance with available coefficients, using defaults for missing ones
        kwargs = {}
        for field in cls.__dataclass_fields__:
            kwargs[field] = data.get(field, getattr(cls(), field))
        
        return cls(**kwargs)
    
    @classmethod
    def from_isolated_results(cls, coefficients_dict: Dict[str, float]) -> 'MemoryCoefficients':
        """Create from coefficient_finder_isolated.py results"""
        return cls(
            base_overhead=coefficients_dict.get('base_overhead', 0.0),
            per_key_cost=coefficients_dict.get('per_key_cost', 0.0),
            bytes_per_tag_character=coefficients_dict.get('bytes_per_tag_character', 0.0),
            per_unique_tag=coefficients_dict.get('per_unique_tag', 0.0),
            bytes_per_vector_byte=coefficients_dict.get('bytes_per_vector_byte', 0.0),
            vector_flat_overhead=coefficients_dict.get('vector_overhead', 0.0),
            # Use defaults for coefficients not yet measured
            bytes_per_numeric_field=8.0,  # Reasonable default
            cost_per_field_definition=100.0  # Reasonable default
        )


class MemoryEstimator:
    """Estimates memory usage for Valkey Search indexes"""
    
    def __init__(self, coefficients: Optional[MemoryCoefficients] = None):
        self.coefficients = coefficients or MemoryCoefficients()
    
    def estimate_memory(self, 
                       # Core dimensions
                       num_keys: int,
                       per_key_length: int = 0,  # Average key name length
                       
                       # Vector dimensions
                       vector_dim: int = 0,
                       num_vectors: int = None,  # If different from num_keys
                       vector_algorithm: str = "FLAT",  # "FLAT" or "HNSW"
                       hnsw_m: int = 16,
                       
                       # Tag dimensions
                       avg_tag_field_length: int = 0,  # Total tag field length per key
                       avg_tags_per_key: float = 0,
                       avg_tag_length: float = 0,
                       unique_tags: int = 0,
                       avg_keys_per_tag: float = 0,
                       tag_prefix_sharing: float = 0.0,  # 0.0 = no sharing, 1.0 = full sharing
                       
                       # Numeric dimensions
                       num_numeric_fields: int = 0,
                       avg_numeric_field_length: int = 8,  # bytes per numeric field
                       
                       # Schema dimensions
                       num_field_definitions: int = 0) -> Dict[str, int]:
        """
        Estimate memory usage for a search index covering all dimensions.
        
        Args:
            num_keys: Number of keys to index
            per_key_length: Average length of key names
            vector_dim: Vector dimensions (0 if no vectors)
            num_vectors: Number of vectors (defaults to num_keys)
            vector_algorithm: "FLAT" or "HNSW"
            hnsw_m: M parameter for HNSW
            avg_tag_field_length: Average total length of tag field per key
            avg_tags_per_key: Average number of tags per key
            avg_tag_length: Average length of individual tags
            unique_tags: Number of unique tag values
            avg_keys_per_tag: Average keys sharing each tag
            tag_prefix_sharing: Fraction of tags sharing prefixes (0.0-1.0)
            num_numeric_fields: Number of numeric fields
            avg_numeric_field_length: Bytes per numeric field value
            num_field_definitions: Number of fields in schema
            
        Returns:
            Dict with memory breakdown by component
        """
        
        if num_vectors is None:
            num_vectors = num_keys if vector_dim > 0 else 0
        
        # 1. Base infrastructure cost
        base_cost = self.coefficients.base_overhead
        
        # 2. Schema definition overhead
        schema_cost = (self.coefficients.schema_base_cost + 
                      num_field_definitions * self.coefficients.cost_per_field_definition)
        
        # 3. Per-key overhead (independent of field properties)
        key_cost = num_keys * self.coefficients.per_key_cost
        
        # 4. Key name storage (if per_key_length provided)
        key_name_cost = num_keys * per_key_length * 1.1  # Small overhead for key storage
        
        # 5. Tag storage cost
        if avg_tag_field_length > 0:
            # Direct tag field storage
            total_tag_field_bytes = num_keys * avg_tag_field_length
            tag_content_cost = total_tag_field_bytes * self.coefficients.bytes_per_tag_character
        else:
            # Calculate from component parts
            total_tag_characters = num_keys * avg_tags_per_key * avg_tag_length
            tag_content_cost = total_tag_characters * self.coefficients.bytes_per_tag_character
        
        # Tag index cost (per unique tag)
        tag_index_cost = unique_tags * self.coefficients.per_unique_tag
        
        # Apply prefix sharing reduction
        if tag_prefix_sharing > 0:
            prefix_reduction = tag_prefix_sharing * self.coefficients.tag_prefix_sharing_factor
            tag_content_cost *= (1.0 - prefix_reduction)
            tag_index_cost *= (1.0 - prefix_reduction * 0.5)  # Index benefits less from prefix sharing
        
        # 6. Vector storage cost
        if vector_dim > 0:
            total_vector_bytes = num_vectors * vector_dim * 4  # 4 bytes per float32
            vector_storage_cost = total_vector_bytes * self.coefficients.bytes_per_vector_byte
            
            # Algorithm-specific overhead
            if vector_algorithm.upper() == "HNSW":
                vector_algorithm_cost = (num_vectors * self.coefficients.vector_hnsw_overhead +
                                       num_vectors * hnsw_m * self.coefficients.hnsw_m_factor)
            else:  # FLAT
                vector_algorithm_cost = num_vectors * self.coefficients.vector_flat_overhead
                
            vector_cost = vector_storage_cost + vector_algorithm_cost
        else:
            vector_cost = 0
        
        # 7. Numeric field cost
        numeric_storage_cost = (num_keys * num_numeric_fields * avg_numeric_field_length * 
                               self.coefficients.bytes_per_numeric_field)
        numeric_index_cost = num_numeric_fields * self.coefficients.numeric_field_overhead
        numeric_cost = numeric_storage_cost + numeric_index_cost
        
        breakdown = {
            "base_overhead": int(base_cost),
            "schema_definition": int(schema_cost),
            "key_overhead": int(key_cost),
            "key_names": int(key_name_cost),
            "tag_content": int(tag_content_cost),
            "tag_index": int(tag_index_cost),
            "vector_storage": int(vector_storage_cost if vector_dim > 0 else 0),
            "vector_algorithm": int(vector_algorithm_cost if vector_dim > 0 else 0),
            "numeric_storage": int(numeric_storage_cost),
            "numeric_index": int(numeric_index_cost),
            "total": int(base_cost + schema_cost + key_cost + key_name_cost + 
                        tag_content_cost + tag_index_cost + vector_cost + numeric_cost)
        }
        
        return breakdown
    
    def estimate_memory_simple(self,
                              num_keys: int,
                              unique_tags: int,
                              avg_tag_length: float,
                              vector_dim: int = 0) -> int:
        """Simplified estimation with common defaults"""
        result = self.estimate_memory(
            num_keys=num_keys,
            unique_tags=unique_tags,
            avg_tag_length=avg_tag_length,
            avg_tags_per_key=3.0,  # Common default
            vector_dim=vector_dim
        )
        return result["total"]
    
    def print_breakdown(self, breakdown: Dict[str, int]):
        """Print formatted memory breakdown"""
        print(f"Memory Breakdown:")
        print(f"  Base overhead:      {breakdown['base_overhead']:>10,} bytes ({breakdown['base_overhead']/1024/1024:6.1f} MB)")
        print(f"  Schema definition:  {breakdown['schema_definition']:>10,} bytes ({breakdown['schema_definition']/1024/1024:6.1f} MB)")
        print(f"  Key overhead:       {breakdown['key_overhead']:>10,} bytes ({breakdown['key_overhead']/1024/1024:6.1f} MB)")
        print(f"  Key names:          {breakdown['key_names']:>10,} bytes ({breakdown['key_names']/1024/1024:6.1f} MB)")
        print(f"  Tag content:        {breakdown['tag_content']:>10,} bytes ({breakdown['tag_content']/1024/1024:6.1f} MB)")
        print(f"  Tag index:          {breakdown['tag_index']:>10,} bytes ({breakdown['tag_index']/1024/1024:6.1f} MB)")
        print(f"  Vector storage:     {breakdown['vector_storage']:>10,} bytes ({breakdown['vector_storage']/1024/1024:6.1f} MB)")
        print(f"  Vector algorithm:   {breakdown['vector_algorithm']:>10,} bytes ({breakdown['vector_algorithm']/1024/1024:6.1f} MB)")
        print(f"  Numeric storage:    {breakdown['numeric_storage']:>10,} bytes ({breakdown['numeric_storage']/1024/1024:6.1f} MB)")
        print(f"  Numeric index:      {breakdown['numeric_index']:>10,} bytes ({breakdown['numeric_index']/1024/1024:6.1f} MB)")
        print(f"  {'='*60}")
        print(f"  Total:              {breakdown['total']:>10,} bytes ({breakdown['total']/1024/1024:6.1f} MB)")


def main():
    """Example usage with coefficient loading"""
    import sys
    
    # Try to load coefficients from isolated results if provided
    if len(sys.argv) > 1:
        coefficients_file = sys.argv[1]
        print(f"Loading coefficients from: {coefficients_file}")
        coefficients = MemoryCoefficients.from_file(coefficients_file)
        estimator = MemoryEstimator(coefficients)
    else:
        print("Using default coefficients (run with coefficients file to use measured values)")
        # Create with reasonable defaults for demonstration
        coefficients = MemoryCoefficients(
            base_overhead=1198045.17,
            per_key_cost=515.39,
            bytes_per_tag_character=2.61,
            per_unique_tag=289.21,
            bytes_per_vector_byte=1.60,
            vector_flat_overhead=0,
            bytes_per_numeric_field=1.2,
            cost_per_field_definition=100.0
        )
        estimator = MemoryEstimator(coefficients)
    
    print("\nMemory Estimation Examples with Full Dimensions\n")
    
    # Example 1: Small dataset with comprehensive parameters
    print("Example 1: Small dataset")
    print("  10K keys, 1K unique tags, 128d FLAT vectors, 2 numeric fields")
    breakdown = estimator.estimate_memory(
        num_keys=10000,
        per_key_length=20,  # "key:12345" format
        vector_dim=128,
        vector_algorithm="FLAT",
        avg_tags_per_key=3,
        avg_tag_length=10,
        unique_tags=1000,
        num_numeric_fields=2,
        num_field_definitions=4  # tags, vec, score, timestamp
    )
    estimator.print_breakdown(breakdown)
    print()
    
    # Example 2: Medium dataset with HNSW
    print("Example 2: Medium dataset with HNSW")
    print("  100K keys, 10K unique tags, 256d HNSW vectors (M=32), 3 numeric fields")
    breakdown = estimator.estimate_memory(
        num_keys=100000,
        per_key_length=25,
        vector_dim=256,
        vector_algorithm="HNSW",
        hnsw_m=32,
        avg_tags_per_key=4,
        avg_tag_length=15,
        unique_tags=10000,
        tag_prefix_sharing=0.3,  # 30% prefix sharing
        num_numeric_fields=3,
        num_field_definitions=5
    )
    estimator.print_breakdown(breakdown)
    print()
    
    # Example 3: Large dataset with tag field length
    print("Example 3: Large dataset with direct tag field specification")
    print("  1M keys, 512d vectors, avg 100-char tag fields")
    breakdown = estimator.estimate_memory(
        num_keys=1000000,
        per_key_length=30,
        vector_dim=512,
        vector_algorithm="FLAT",
        avg_tag_field_length=100,  # Direct specification
        unique_tags=50000,
        num_numeric_fields=5,
        num_field_definitions=7
    )
    estimator.print_breakdown(breakdown)
    print()
    
    # Show coefficients used
    print("Coefficients used:")
    for field, value in coefficients.__dict__.items():
        print(f"  {field}: {value}")


def load_and_estimate(coefficients_file: str, **kwargs):
    """Helper function to load coefficients and estimate memory"""
    coefficients = MemoryCoefficients.from_file(coefficients_file)
    estimator = MemoryEstimator(coefficients)
    return estimator.estimate_memory(**kwargs)


if __name__ == "__main__":
    main()