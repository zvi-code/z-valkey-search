#!/usr/bin/env python3
"""
Validation tool to compare memory estimates with actual measurements.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from integration.scripts.memory_estimator import MemoryEstimator, MemoryCoefficients
from integration.scripts.coefficient_finder_isolated import IsolatedCoefficientFinder
import tempfile
import shutil
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EstimatorValidator:
    """Validates memory estimator against real measurements"""
    
    def __init__(self, coefficients_file: str = None):
        if coefficients_file:
            coefficients = MemoryCoefficients.from_file(coefficients_file)
            self.estimator = MemoryEstimator(coefficients)
        else:
            self.estimator = MemoryEstimator()
        self.finder = IsolatedCoefficientFinder(valkey_port=16380)  # Different port
    
    def validate_scenario(self, num_keys: int, unique_tags: int, avg_tag_length: int, vector_dim: int = 128):
        """Validate estimator for a specific scenario"""
        logger.info(f"\n=== Validating: {num_keys} keys, {unique_tags} tags, {vector_dim}d vectors ===")
        
        # Get estimate using the comprehensive parameter set
        estimated = self.estimator.estimate_memory(
            num_keys=num_keys,
            per_key_length=20,  # Typical key name length
            vector_dim=vector_dim,
            vector_algorithm="FLAT",
            avg_tags_per_key=3.0,
            avg_tag_length=avg_tag_length,
            unique_tags=unique_tags,
            num_field_definitions=2  # tags + vectors
        )
        
        # Measure actual
        try:
            actual = self._measure_actual(num_keys, unique_tags, avg_tag_length, vector_dim)
            
            # Compare
            accuracy = (actual / estimated["total"]) * 100
            difference = abs(actual - estimated["total"])
            
            logger.info(f"Estimated: {estimated['total']:,} bytes ({estimated['total']/1024/1024:.1f} MB)")
            logger.info(f"Actual:    {actual:,} bytes ({actual/1024/1024:.1f} MB)")
            logger.info(f"Accuracy:  {accuracy:.1f}% (difference: {difference:,} bytes)")
            
            return {
                "scenario": f"{num_keys}k_{unique_tags}t_{vector_dim}d",
                "estimated": estimated["total"],
                "actual": actual,
                "accuracy": accuracy,
                "difference": difference
            }
            
        except Exception as e:
            logger.error(f"Failed to measure actual memory: {e}")
            return None
    
    def _measure_actual(self, num_keys: int, unique_tags: int, avg_tag_length: int, vector_dim: int) -> int:
        """Measure actual memory usage"""
        import numpy as np
        from integration.utils.hash_generator import IndexSchema, FieldSchema, FieldType, VectorFieldSchema, VectorAlgorithm, VectorMetric
        
        self.finder.restart_valkey()
        initial_memory = self.finder._get_valkey_memory()
        
        # Create index
        vector_config = VectorFieldSchema(
            algorithm=VectorAlgorithm.FLAT,
            dim=vector_dim,
            distance_metric=VectorMetric.L2,
            datatype="FLOAT32"
        )
        schema = IndexSchema(
            index_name="validate_idx",
            prefix=["v:"],
            fields=[
                FieldSchema(name="tags", type=FieldType.TAG),
                FieldSchema(name="vec", type=FieldType.VECTOR, vector_config=vector_config)
            ]
        )
        self.finder._create_index(schema)
        
        # Generate data
        tags = [f"tag{i:0{avg_tag_length}d}"[:avg_tag_length] for i in range(unique_tags)]
        
        # Insert data
        pipeline = self.finder.client.pipeline()
        for i in range(num_keys):
            # Generate tags for this key (typically 3 tags)
            key_tags = [tags[j % unique_tags] for j in range(i, i+3)]
            tag_string = ",".join(key_tags)
            
            # Generate vector
            vector = np.random.random(vector_dim).astype(np.float32)
            vector_bytes = vector.tobytes()
            
            pipeline.hset(f"v:{i}", mapping={"tags": tag_string, "vec": vector_bytes})
            
            if i % 1000 == 999:
                pipeline.execute()
                pipeline = self.finder.client.pipeline()
        pipeline.execute()
        
        # Measure final memory
        self.finder.client.execute_command("MEMORY", "PURGE")
        time.sleep(1)
        final_memory = self.finder._get_valkey_memory()
        
        return final_memory - initial_memory
    
    def run_validation(self):
        """Run validation on multiple scenarios"""
        scenarios = [
            (5000, 500, 10, 64),
            (10000, 1000, 15, 128), 
            (20000, 2000, 20, 256),
        ]
        
        results = []
        try:
            for num_keys, unique_tags, tag_len, vector_dim in scenarios:
                result = self.validate_scenario(num_keys, unique_tags, tag_len, vector_dim)
                if result:
                    results.append(result)
        
        finally:
            self.finder.stop_valkey()
        
        # Summary
        if results:
            logger.info(f"\n=== VALIDATION SUMMARY ===")
            total_accuracy = sum(r["accuracy"] for r in results) / len(results)
            logger.info(f"Average accuracy: {total_accuracy:.1f}%")
            
            for result in results:
                logger.info(f"  {result['scenario']}: {result['accuracy']:.1f}% accurate")
        
        return results


def main():
    import sys
    
    coefficients_file = None
    if len(sys.argv) > 1:
        coefficients_file = sys.argv[1]
        print(f"Using coefficients from: {coefficients_file}")
    
    validator = EstimatorValidator(coefficients_file)
    results = validator.run_validation()
    
    print(f"\nValidation completed. Results:")
    for result in results:
        print(f"  {result['scenario']}: {result['accuracy']:.1f}% accuracy")


if __name__ == "__main__":
    main()