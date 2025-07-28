#!/usr/bin/env python3
"""
Complete memory analysis pipeline that runs coefficient finding, estimation, and validation.
"""

import sys
import os
import json
import logging
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from integration.scripts.coefficient_finder_isolated import IsolatedCoefficientFinder
from integration.scripts.memory_estimator import MemoryEstimator, MemoryCoefficients
from integration.scripts.validate_estimator import EstimatorValidator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_complete_analysis():
    """Run the complete memory analysis pipeline"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print("=" * 80)
    print("COMPLETE MEMORY ANALYSIS PIPELINE")
    print("=" * 80)
    
    # Step 1: Find coefficients
    print("\n1. FINDING COEFFICIENTS...")
    print("-" * 40)
    
    finder = IsolatedCoefficientFinder()
    coefficients_dict = finder.run_full_analysis()
    
    coefficients_file = f"complete_coefficients_{timestamp}.json"
    with open(coefficients_file, 'w') as f:
        json.dump(coefficients_dict, f, indent=2)
    
    print(f"\nCoefficients saved to: {coefficients_file}")
    
    # Step 2: Create memory estimator
    print("\n2. TESTING MEMORY ESTIMATOR...")
    print("-" * 40)
    
    coefficients = MemoryCoefficients.from_isolated_results(coefficients_dict)
    estimator = MemoryEstimator(coefficients)
    
    # Test scenarios covering all dimensions
    scenarios = [
        {
            "name": "Small FLAT",
            "params": {
                "num_keys": 10000,
                "per_key_length": 20,
                "vector_dim": 128,
                "vector_algorithm": "FLAT",
                "avg_tags_per_key": 3,
                "avg_tag_length": 10,
                "unique_tags": 1000,
                "num_numeric_fields": 2,
                "num_field_definitions": 4
            }
        },
        {
            "name": "Medium HNSW with sharing", 
            "params": {
                "num_keys": 100000,
                "per_key_length": 25,
                "vector_dim": 256,
                "vector_algorithm": "HNSW",
                "hnsw_m": 32,
                "avg_tags_per_key": 4,
                "avg_tag_length": 15,
                "unique_tags": 10000,
                "tag_prefix_sharing": 0.3,
                "num_numeric_fields": 3,
                "num_field_definitions": 5
            }
        },
        {
            "name": "Large with direct tag field",
            "params": {
                "num_keys": 500000,
                "per_key_length": 30,
                "vector_dim": 512,
                "avg_tag_field_length": 80,
                "unique_tags": 25000,
                "num_numeric_fields": 5,
                "num_field_definitions": 7
            }
        }
    ]
    
    for scenario in scenarios:
        print(f"\nScenario: {scenario['name']}")
        breakdown = estimator.estimate_memory(**scenario['params'])
        estimator.print_breakdown(breakdown)
        print()
    
    # Step 3: Validate against real measurements
    print("\n3. VALIDATING ESTIMATOR...")
    print("-" * 40)
    
    validator = EstimatorValidator(coefficients_file)
    validation_results = validator.run_validation()
    
    # Step 4: Summary
    print("\n4. SUMMARY")
    print("-" * 40)
    
    print(f"\nCoefficients found:")
    for name, value in coefficients_dict.items():
        print(f"  {name}: {value:.4f}")
    
    if validation_results:
        avg_accuracy = sum(r["accuracy"] for r in validation_results) / len(validation_results)
        print(f"\nValidation accuracy: {avg_accuracy:.1f}%")
        for result in validation_results:
            print(f"  {result['scenario']}: {result['accuracy']:.1f}%")
    
    print(f"\nFiles generated:")
    print(f"  Coefficients: {coefficients_file}")
    print(f"  Detailed results: isolated_analysis_results_{timestamp}.csv")
    
    return coefficients_dict, validation_results


def demonstrate_all_dimensions():
    """Demonstrate estimation with all available dimensions"""
    print("\n" + "=" * 80)
    print("COMPREHENSIVE DIMENSION DEMONSTRATION")
    print("=" * 80)
    
    # Use the latest coefficients if available
    import glob
    coef_files = glob.glob("complete_coefficients_*.json")
    if coef_files:
        latest_coef = sorted(coef_files)[-1]
        coefficients = MemoryCoefficients.from_file(latest_coef)
        print(f"Using coefficients from: {latest_coef}")
    else:
        print("No coefficient files found, using defaults")
        coefficients = MemoryCoefficients()
    
    estimator = MemoryEstimator(coefficients)
    
    print("\nDemonstrating all dimensions:")
    print("- #keys, per-key-len, length of vector field(4*dim), #of vectors")
    print("- length of tags field for key, number of tags per key, number of keys per-tag")  
    print("- tag prefix sharing, number of numeric fields, numeric field len")
    print("- HNSW/FLAT, index schema definition, M value")
    
    breakdown = estimator.estimate_memory(
        # Core dimensions
        num_keys=1000000,                    # 1M keys
        per_key_length=25,                   # Average key name length
        
        # Vector dimensions  
        vector_dim=384,                      # 384d vectors (1536 bytes per vector)
        num_vectors=1000000,                 # Same as keys
        vector_algorithm="HNSW",             # HNSW algorithm
        hnsw_m=48,                          # M=48 for HNSW
        
        # Tag dimensions
        avg_tag_field_length=120,            # 120 chars total tag field per key
        avg_tags_per_key=5,                  # 5 tags per key on average
        avg_tag_length=20,                   # 20 chars per tag average
        unique_tags=100000,                  # 100K unique tags
        avg_keys_per_tag=50,                 # 50 keys share each tag on average
        tag_prefix_sharing=0.4,              # 40% of tags share prefixes
        
        # Numeric dimensions
        num_numeric_fields=8,                # 8 numeric fields
        avg_numeric_field_length=8,          # 8 bytes per numeric value
        
        # Schema dimensions
        num_field_definitions=10             # 10 fields in schema definition
    )
    
    print(f"\nMemory estimation for comprehensive scenario:")
    estimator.print_breakdown(breakdown)
    
    # Show which dimensions have the biggest impact
    print(f"\nDimension impact analysis:")
    components = [
        ("Base infrastructure", breakdown["base_overhead"]),
        ("Key overhead", breakdown["key_overhead"]), 
        ("Tag content", breakdown["tag_content"]),
        ("Tag index", breakdown["tag_index"]),
        ("Vector storage", breakdown["vector_storage"]),
        ("Vector algorithm", breakdown["vector_algorithm"]),
        ("Numeric storage", breakdown["numeric_storage"]),
    ]
    
    components.sort(key=lambda x: x[1], reverse=True)
    total = breakdown["total"]
    
    for name, value in components:
        if value > 0:
            percentage = (value / total) * 100
            print(f"  {name:18}: {percentage:6.1f}% ({value/1024/1024:8.1f} MB)")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Complete memory analysis pipeline")
    parser.add_argument("--demo-only", action="store_true", 
                       help="Only run dimension demonstration")
    parser.add_argument("--coefficients", 
                       help="Use existing coefficients file")
    
    args = parser.parse_args()
    
    if args.demo_only:
        demonstrate_all_dimensions()
    elif args.coefficients:
        # Use existing coefficients for estimation only
        print(f"Using existing coefficients: {args.coefficients}")
        coefficients = MemoryCoefficients.from_file(args.coefficients)
        estimator = MemoryEstimator(coefficients)
        
        # Run examples
        breakdown = estimator.estimate_memory(
            num_keys=100000,
            vector_dim=256,
            avg_tags_per_key=3,
            avg_tag_length=15,
            unique_tags=10000,
            num_field_definitions=3
        )
        estimator.print_breakdown(breakdown)
    else:
        # Run complete analysis
        run_complete_analysis()
        demonstrate_all_dimensions()


if __name__ == "__main__":
    main()