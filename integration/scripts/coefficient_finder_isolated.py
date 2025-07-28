#!/usr/bin/env python3
"""
Isolated coefficient finder for memory estimation formula.
Tests each component separately to get accurate coefficients.
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import List, Dict
from enum import Enum
import logging
import json
import time
from datetime import datetime
import subprocess
import os
import tempfile
import shutil
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from valkey import Valkey
from integration.utils.tags_builder import TagsConfig, TagDistribution, TagSharingConfig, TagSharingMode
from integration.utils.string_generator import LengthConfig
from integration.utils.hash_generator import (
    HashKeyGenerator, HashGeneratorConfig, IndexSchema, FieldSchema,
    FieldType, VectorFieldSchema, VectorAlgorithm, VectorMetric
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class TestResult:
    """Result from a single test"""
    test_name: str
    description: str
    memory_used: int
    parameters: Dict[str, any]
    timestamp: datetime = field(default_factory=datetime.now)


class IsolatedCoefficientFinder:
    """Find coefficients by testing each component in isolation"""
    
    def __init__(self, valkey_port: int = 16379, production_mode: bool = True):
        self.valkey_port = valkey_port
        self.production_mode = production_mode  # True = 1GB+ datasets, False = smaller for testing
        self.results: List[TestResult] = []
        self.valkey_process = None
        self.testdir = None
        self.client = None
        
        # Paths from environment
        self.valkey_server_path = os.getenv("VALKEY_SERVER_PATH", "/home/ubuntu/valkey/build/bin/valkey-server")
        self.module_path = os.getenv("MODULE_PATH", "/home/ubuntu/valkey-search/.build-release/libsearch.so")
        
        if not production_mode:
            logger.warning("Running in test mode with smaller datasets - coefficients may be less accurate")
    
    def test_base_key_cost(self) -> Dict[str, float]:
        """Test base cost per key with minimal fields - using large datasets to reach 1GB+"""
        logger.info("=== Testing Base Key Cost ===")
        results = []
        
        # Test with different numbers of keys, single 1-byte tag
        if self.production_mode:
            # Scale up to ensure we reach 1GB+ memory usage for accurate coefficients
            key_counts = [100000, 250000, 500000, 750000, 1000000]
            logger.info("Production mode: Using large datasets (100K-1M keys) for 1GB+ memory")
        else:
            # Smaller datasets for testing
            key_counts = [10000, 25000, 50000, 75000, 100000]
            logger.info("Test mode: Using smaller datasets (10K-100K keys)")
            
        for num_keys in key_counts:
            logger.info(f"\nTesting with {num_keys} keys...")
            
            self.restart_valkey()
            initial_memory = self._get_valkey_memory()
            
            # Create minimal index - single tag field + minimal vector
            vector_config = VectorFieldSchema(
                algorithm=VectorAlgorithm.FLAT,
                dim=1,
                distance_metric=VectorMetric.L2,
                datatype="FLOAT32"
            )
            schema = IndexSchema(
                index_name="test_idx",
                prefix=["k:"],
                fields=[
                    FieldSchema(name="t", type=FieldType.TAG),
                    FieldSchema(name="v", type=FieldType.VECTOR, vector_config=vector_config)
                ]
            )
            self._create_index(schema)
            
            # Insert keys with single character tag and minimal vector
            import numpy as np
            minimal_vector = np.array([0.0], dtype=np.float32).tobytes()
            pipeline = self.client.pipeline()
            for i in range(num_keys):
                pipeline.hset(f"k:{i}", mapping={"t": "a", "v": minimal_vector})
                if i % 1000 == 999:
                    pipeline.execute()
                    pipeline = self.client.pipeline()
            pipeline.execute()
            
            # Measure memory
            self.client.execute_command("MEMORY", "PURGE")
            time.sleep(1)
            final_memory = self._get_valkey_memory()
            memory_used = final_memory - initial_memory
            
            result = TestResult(
                test_name=f"base_keys_{num_keys}",
                description=f"Base cost for {num_keys} keys with minimal tag",
                memory_used=memory_used,
                parameters={"num_keys": num_keys, "tag_value": "a", "tag_length": 1, "vector_dim": 1}
            )
            results.append(result)
            self.results.append(result)
            
            # Validate memory threshold (1GB+ for accurate coefficients)
            memory_gb = memory_used / (1024**3)
            if memory_gb < 1.0:
                logger.warning(f"  Memory {memory_gb:.2f}GB < 1GB threshold - may have allocation artifacts")
            else:
                logger.info(f"  Memory {memory_gb:.2f}GB >= 1GB threshold - good for coefficient calculation")
            
            logger.info(f"Memory used: {memory_used:,} bytes ({memory_used/num_keys:.2f} bytes/key)")
        
        # Filter out results below 1GB threshold for more accurate coefficients
        large_results = [r for r in results if r.memory_used >= 1024**3]
        if len(large_results) < 2:
            logger.warning("Insufficient large memory results for accurate coefficients, using all results")
            large_results = results
        else:
            logger.info(f"Using {len(large_results)}/{len(results)} results above 1GB threshold")
        
        # Linear regression to find per-key cost using large results
        X = np.array([r.parameters["num_keys"] for r in large_results])
        y = np.array([r.memory_used for r in large_results])
        
        # Fit line: memory = base_overhead + per_key_cost * num_keys
        A = np.vstack([X, np.ones(len(X))]).T
        per_key_cost, base_overhead = np.linalg.lstsq(A, y, rcond=None)[0]
        
        logger.info(f"\nBase Key Cost Analysis:")
        logger.info(f"  Base overhead: {base_overhead:,.0f} bytes")
        logger.info(f"  Per key cost: {per_key_cost:.2f} bytes")
        
        return {
            "base_overhead": base_overhead,
            "per_key_cost": per_key_cost
        }
    
    def test_tag_storage_cost(self, base_per_key: float) -> Dict[str, float]:
        """Test tag storage cost separate from base key cost - using large datasets"""
        logger.info("\n=== Testing Tag Storage Cost ===")
        results = []
        
        # Use larger dataset to reach 1GB+ memory usage
        if self.production_mode:
            num_keys = 500000
            logger.info("Production mode: Using 500K keys for tag tests")
        else:
            num_keys = 50000
            logger.info("Test mode: Using 50K keys for tag tests")
        
        # Test 1: Varying tag length (all unique tags)
        logger.info("\nTest 1: Varying tag length")
        for tag_len in [1, 5, 10, 20, 50, 100]:
            self.restart_valkey()
            initial_memory = self._get_valkey_memory()
            
            # Create index with minimal vector requirement
            vector_config = VectorFieldSchema(
                algorithm=VectorAlgorithm.FLAT,
                dim=1,
                distance_metric=VectorMetric.L2,
                datatype="FLOAT32"
            )
            schema = IndexSchema(
                index_name="test_idx",
                prefix=["k:"],
                fields=[
                    FieldSchema(name="t", type=FieldType.TAG),
                    FieldSchema(name="v", type=FieldType.VECTOR, vector_config=vector_config)
                ]
            )
            self._create_index(schema)
            
            # Generate unique tags of specified length
            import numpy as np
            minimal_vector = np.array([0.0], dtype=np.float32).tobytes()
            pipeline = self.client.pipeline()
            for i in range(num_keys):
                tag = f"t{i:0{tag_len}d}"[:tag_len]  # Ensure exact length
                pipeline.hset(f"k:{i}", mapping={"t": tag, "v": minimal_vector})
                if i % 1000 == 999:
                    pipeline.execute()
                    pipeline = self.client.pipeline()
            pipeline.execute()
            
            self.client.execute_command("MEMORY", "PURGE")
            time.sleep(1)
            final_memory = self._get_valkey_memory()
            memory_used = final_memory - initial_memory
            
            # Subtract base cost to get tag-specific memory
            tag_memory = memory_used - (base_per_key * num_keys)
            
            result = TestResult(
                test_name=f"tag_length_{tag_len}",
                description=f"Tag storage with length {tag_len}",
                memory_used=tag_memory,
                parameters={
                    "num_keys": num_keys,
                    "tag_length": tag_len,
                    "unique_tags": num_keys,
                    "total_tag_bytes": num_keys * tag_len
                }
            )
            results.append(result)
            self.results.append(result)
            
            # Validate memory threshold
            total_memory = memory_used
            memory_gb = total_memory / (1024**3)
            if memory_gb < 1.0:
                logger.warning(f"  Total memory {memory_gb:.2f}GB < 1GB threshold")
            else:
                logger.info(f"  Total memory {memory_gb:.2f}GB >= 1GB threshold")
            
            logger.info(f"Tag length {tag_len}: {tag_memory:,} bytes ({tag_memory/(num_keys*tag_len):.2f} bytes/tag-byte)")
        
        # Test 2: Varying number of unique tags (fixed length)
        logger.info("\nTest 2: Varying number of unique tags")
        tag_len = 10
        for unique_tags in [1000, 5000, 10000, 25000, 50000, 100000]:
            self.restart_valkey()
            initial_memory = self._get_valkey_memory()
            
            # Create index with minimal vector requirement
            vector_config = VectorFieldSchema(
                algorithm=VectorAlgorithm.FLAT,
                dim=1,
                distance_metric=VectorMetric.L2,
                datatype="FLOAT32"
            )
            schema = IndexSchema(
                index_name="test_idx",
                prefix=["k:"],
                fields=[
                    FieldSchema(name="t", type=FieldType.TAG),
                    FieldSchema(name="v", type=FieldType.VECTOR, vector_config=vector_config)
                ]
            )
            self._create_index(schema)
            
            # Generate tags with controlled uniqueness
            import numpy as np
            minimal_vector = np.array([0.0], dtype=np.float32).tobytes()
            tags = [f"tag{i:06d}" for i in range(unique_tags)]
            pipeline = self.client.pipeline()
            for i in range(num_keys):
                tag = tags[i % unique_tags]
                pipeline.hset(f"k:{i}", mapping={"t": tag, "v": minimal_vector})
                if i % 1000 == 999:
                    pipeline.execute()
                    pipeline = self.client.pipeline()
            pipeline.execute()
            
            self.client.execute_command("MEMORY", "PURGE")
            time.sleep(1)
            final_memory = self._get_valkey_memory()
            memory_used = final_memory - initial_memory
            
            # Subtract base cost
            tag_memory = memory_used - (base_per_key * num_keys)
            
            result = TestResult(
                test_name=f"unique_tags_{unique_tags}",
                description=f"Tag storage with {unique_tags} unique tags",
                memory_used=tag_memory,
                parameters={
                    "num_keys": num_keys,
                    "tag_length": tag_len,
                    "unique_tags": unique_tags,
                    "keys_per_tag": num_keys / unique_tags
                }
            )
            results.append(result)
            self.results.append(result)
            
            # Validate memory threshold
            total_memory = memory_used
            memory_gb = total_memory / (1024**3)
            if memory_gb < 1.0:
                logger.warning(f"  Total memory {memory_gb:.2f}GB < 1GB threshold")
            else:
                logger.info(f"  Total memory {memory_gb:.2f}GB >= 1GB threshold")
            
            logger.info(f"Unique tags {unique_tags}: {tag_memory:,} bytes ({tag_memory/unique_tags:.2f} bytes/unique-tag)")
        
        # Analyze results
        coefficients = self._analyze_tag_results(results)
        return coefficients
    
    def test_vector_storage_cost(self, base_per_key: float) -> Dict[str, float]:
        """Test vector storage cost - using large datasets to reach 1GB+"""
        logger.info("\n=== Testing Vector Storage Cost ===")
        results = []
        
        # Use larger dataset for better coefficient accuracy
        if self.production_mode:
            num_keys = 500000
            logger.info("Production mode: Using 500K keys for vector tests")
        else:
            num_keys = 50000
            logger.info("Test mode: Using 50K keys for vector tests")
        
        # Test with different vector dimensions, including 0 for baseline
        for dim in [0, 1, 2, 4, 8, 16, 32, 64, 128]:
            self.restart_valkey()
            initial_memory = self._get_valkey_memory()
            
            # Create index with vector field (or minimal tag if dim=0)
            if dim > 0:
                vector_config = VectorFieldSchema(
                    algorithm=VectorAlgorithm.FLAT,
                    dim=dim,
                    distance_metric=VectorMetric.L2,
                    datatype="FLOAT32"
                )
                schema = IndexSchema(
                    index_name="test_idx",
                    prefix=["k:"],
                    fields=[
                        FieldSchema(name="v", type=FieldType.VECTOR, vector_config=vector_config)
                    ]
                )
            else:
                # Minimal index with just tag for dim=0 baseline
                minimal_vector_config = VectorFieldSchema(
                    algorithm=VectorAlgorithm.FLAT,
                    dim=1,
                    distance_metric=VectorMetric.L2,
                    datatype="FLOAT32"
                )
                schema = IndexSchema(
                    index_name="test_idx",
                    prefix=["k:"],
                    fields=[
                        FieldSchema(name="t", type=FieldType.TAG),
                        FieldSchema(name="v", type=FieldType.VECTOR, vector_config=minimal_vector_config)
                    ]
                )
            self._create_index(schema)
            
            # Insert vectors (or minimal data for baseline)
            pipeline = self.client.pipeline()
            for i in range(num_keys):
                if dim > 0:
                    # Create simple vector (all zeros for consistency)
                    vector = np.zeros(dim, dtype=np.float32)
                    vector_bytes = vector.tobytes()
                    pipeline.hset(f"k:{i}", mapping={"v": vector_bytes})
                else:
                    # Baseline: minimal tag + 1D vector
                    minimal_vector = np.zeros(1, dtype=np.float32)
                    minimal_vector_bytes = minimal_vector.tobytes()
                    pipeline.hset(f"k:{i}", mapping={"t": "a", "v": minimal_vector_bytes})
                
                if i % 1000 == 999:
                    pipeline.execute()
                    pipeline = self.client.pipeline()
            pipeline.execute()
            
            self.client.execute_command("MEMORY", "PURGE")
            time.sleep(1)
            final_memory = self._get_valkey_memory()
            memory_used = final_memory - initial_memory
            
            # Store raw memory usage (we'll calculate relative to baseline later)
            result = TestResult(
                test_name=f"vector_dim_{dim}",
                description=f"Vector storage with dimension {dim}",
                memory_used=memory_used,  # Raw memory, not adjusted
                parameters={
                    "num_keys": num_keys,
                    "vector_dim": dim,
                    "total_vector_bytes": num_keys * dim * 4 if dim > 0 else 0
                }
            )
            results.append(result)
            self.results.append(result)
            
            # Validate memory threshold
            memory_gb = memory_used / (1024**3)
            if memory_gb < 1.0:
                logger.warning(f"  Total memory {memory_gb:.2f}GB < 1GB threshold")
            else:
                logger.info(f"  Total memory {memory_gb:.2f}GB >= 1GB threshold")
            
            if dim > 0:
                per_key_per_dim = memory_used / (num_keys * dim)
                logger.info(f"Vector dim {dim}: {memory_used:,} bytes ({per_key_per_dim:.4f} bytes/key/dim)")
            else:
                logger.info(f"Baseline (dim 0): {memory_used:,} bytes")
        
        # Analyze results using baseline (dim=0) as reference
        baseline_memory = next(r.memory_used for r in results if r.parameters["vector_dim"] == 0)
        
        # Calculate vector-specific memory (subtract baseline)
        vector_results = []
        for result in results:
            if result.parameters["vector_dim"] > 0:
                vector_memory = result.memory_used - baseline_memory
                vector_results.append({
                    "vector_bytes": result.parameters["total_vector_bytes"],
                    "vector_memory": vector_memory,
                    "dim": result.parameters["vector_dim"]
                })
                
                per_key_per_dim = vector_memory / (num_keys * result.parameters["vector_dim"])
                logger.info(f"Vector dim {result.parameters['vector_dim']}: {vector_memory:,} bytes ({per_key_per_dim:.4f} bytes/key/dim)")
        
        if vector_results:
            # Filter for large memory results (baseline + vector should be 1GB+)
            large_vector_results = [r for r in vector_results 
                                  if (baseline_memory + r["vector_memory"]) >= 1024**3]
            
            if len(large_vector_results) < 2:
                logger.warning("Insufficient large vector results, using all results")
                large_vector_results = vector_results
            else:
                logger.info(f"Using {len(large_vector_results)}/{len(vector_results)} vector results above 1GB threshold")
            
            # Linear fit on vector-specific memory
            X = np.array([r["vector_bytes"] for r in large_vector_results])
            y = np.array([r["vector_memory"] for r in large_vector_results])
            
            A = np.vstack([X, np.ones(len(X))]).T
            bytes_per_vector_byte, vector_overhead = np.linalg.lstsq(A, y, rcond=None)[0]
            
            logger.info(f"\nVector Storage Analysis:")
            logger.info(f"  Baseline memory (no vectors): {baseline_memory:,.0f} bytes")
            logger.info(f"  Vector overhead: {vector_overhead:,.0f} bytes")
            logger.info(f"  Per vector byte: {bytes_per_vector_byte:.4f}")
            logger.info(f"  Expected (1.0 for direct storage): 1.0")
            logger.info(f"  Overhead factor: {bytes_per_vector_byte:.2f}x")
            
            return {
                "vector_baseline": baseline_memory,
                "vector_overhead": vector_overhead,
                "bytes_per_vector_byte": bytes_per_vector_byte
            }
        else:
            return {
                "vector_baseline": baseline_memory,
                "vector_overhead": 0,
                "bytes_per_vector_byte": 1.0
            }
    
    def _analyze_tag_results(self, results: List[TestResult]) -> Dict[str, float]:
        """Analyze tag test results to extract coefficients"""
        # Separate results by test type
        length_results = [r for r in results if "tag_length_" in r.test_name]
        unique_results = [r for r in results if "unique_tags_" in r.test_name]
        
        coefficients = {}
        
        # Analyze tag length impact
        if length_results:
            X = np.array([r.parameters["total_tag_bytes"] for r in length_results])
            y = np.array([r.memory_used for r in length_results])
            
            A = np.vstack([X, np.ones(len(X))]).T
            bytes_per_tag_char, tag_length_overhead = np.linalg.lstsq(A, y, rcond=None)[0]
            
            coefficients["bytes_per_tag_character"] = bytes_per_tag_char
            coefficients["tag_length_overhead"] = tag_length_overhead
            
            logger.info(f"\nTag Length Analysis:")
            logger.info(f"  Per tag character: {bytes_per_tag_char:.4f} bytes")
            logger.info(f"  Tag overhead: {tag_length_overhead:,.0f} bytes")
        
        # Analyze unique tags impact
        if unique_results:
            X = np.array([r.parameters["unique_tags"] for r in unique_results])
            y = np.array([r.memory_used for r in unique_results])
            
            A = np.vstack([X, np.ones(len(X))]).T
            per_unique_tag, unique_tag_overhead = np.linalg.lstsq(A, y, rcond=None)[0]
            
            coefficients["per_unique_tag"] = per_unique_tag
            coefficients["unique_tag_overhead"] = unique_tag_overhead
            
            logger.info(f"\nUnique Tags Analysis:")
            logger.info(f"  Per unique tag: {per_unique_tag:.2f} bytes")
            logger.info(f"  Unique tag overhead: {unique_tag_overhead:,.0f} bytes")
        
        return coefficients
    
    def restart_valkey(self):
        """Restart Valkey for clean measurement"""
        self.stop_valkey()
        self.start_valkey()
    
    def start_valkey(self):
        """Start Valkey server with search module"""
        self.testdir = tempfile.mkdtemp(prefix="valkey-coef-")
        
        conf_file = os.path.join(self.testdir, "valkey.conf")
        with open(conf_file, "w") as f:
            f.write(f"port {self.valkey_port}\n")
            f.write(f"dir {self.testdir}\n")
            f.write("save \"\"\n")
            f.write(f"loadmodule {self.module_path}\n")
            f.write("enable-debug-command yes\n")
        
        self.valkey_process = subprocess.Popen(
            [self.valkey_server_path, conf_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Wait for startup
        for i in range(50):
            try:
                self.client = Valkey(host="localhost", port=self.valkey_port, decode_responses=False)
                self.client.ping()
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
        
        if self.valkey_process:
            self.valkey_process.terminate()
            self.valkey_process.wait()
            self.valkey_process = None
        
        if self.testdir and os.path.exists(self.testdir):
            shutil.rmtree(self.testdir)
            self.testdir = None
    
    def _get_valkey_memory(self) -> int:
        """Get current Valkey memory usage"""
        try:
            info = self.client.info('memory')
            return info['used_memory']
        except Exception as e:
            logger.error(f"Failed to get Valkey memory: {e}")
            return 0
    
    def _create_index(self, schema: IndexSchema):
        """Create search index"""
        cmd = ["FT.CREATE", schema.index_name, "ON", "HASH", "PREFIX", "1", schema.prefix[0]]
        cmd.extend(["SCHEMA"])
        
        for field in schema.fields:
            if field.type == FieldType.VECTOR and field.vector_config:
                vc = field.vector_config
                cmd.extend([field.name, "VECTOR", vc.algorithm.value])
                
                params = [
                    "TYPE", vc.datatype,
                    "DIM", str(vc.dim),
                    "DISTANCE_METRIC", vc.distance_metric.value
                ]
                
                cmd.extend([str(len(params))] + params)
            elif field.type == FieldType.TAG:
                cmd.extend([field.name, "TAG"])
            else:
                cmd.extend([field.name, field.type.value])
        
        self.client.execute_command(*cmd)
    
    def run_full_analysis(self):
        """Run complete analysis"""
        all_coefficients = {}
        
        try:
            # Step 1: Find base key cost
            base_coeffs = self.test_base_key_cost()
            all_coefficients.update(base_coeffs)
            base_per_key = base_coeffs["per_key_cost"]
            
            # Step 2: Test tag storage
            tag_coeffs = self.test_tag_storage_cost(base_per_key)
            all_coefficients.update(tag_coeffs)
            
            # Step 3: Test vector storage
            vector_coeffs = self.test_vector_storage_cost(base_per_key)
            all_coefficients.update(vector_coeffs)
            
            # Save results
            self.save_results(all_coefficients)
            
            return all_coefficients
            
        finally:
            self.stop_valkey()
    
    def save_results(self, coefficients: Dict[str, float]):
        """Save results to files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save coefficients
        with open(f"isolated_coefficients_{timestamp}.json", "w") as f:
            json.dump(coefficients, f, indent=2)
        
        # Save detailed results
        results_data = []
        for result in self.results:
            data = {
                "test_name": result.test_name,
                "description": result.description,
                "memory_used": result.memory_used,
                **result.parameters
            }
            results_data.append(data)
        
        if results_data:
            df = pd.DataFrame(results_data)
            df.to_csv(f"isolated_analysis_results_{timestamp}.csv", index=False)
        
        logger.info(f"\nResults saved:")
        logger.info(f"  Coefficients: isolated_coefficients_{timestamp}.json")
        logger.info(f"  Detailed results: isolated_analysis_results_{timestamp}.csv")
        
        # Print summary
        logger.info(f"\n=== COEFFICIENT SUMMARY ===")
        for name, value in coefficients.items():
            logger.info(f"  {name}: {value:.4f}")
        
        # Example memory calculation
        logger.info(f"\nExample memory calculation for 100K keys, 10K unique tags (10 chars), 128d vectors:")
        
        mem = coefficients["base_overhead"]
        mem += 100000 * coefficients["per_key_cost"]
        mem += 10000 * coefficients.get("per_unique_tag", 0)
        mem += 100000 * 128 * 4 * coefficients.get("bytes_per_vector_byte", 0)
        
        logger.info(f"  Estimated memory: {mem/1024/1024:.1f} MB")


def main():
    import sys
    
    # Check for test mode
    production_mode = True
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        production_mode = False
        print("Running in test mode with smaller datasets")
    elif len(sys.argv) > 1 and sys.argv[1] == "--production":
        production_mode = True
        print("Running in production mode with 1GB+ datasets")
    else:
        print("Usage: python coefficient_finder_isolated.py [--test|--production]")
        print("Default: production mode")
    
    finder = IsolatedCoefficientFinder(production_mode=production_mode)
    coefficients = finder.run_full_analysis()
    
    print("\n\nFinal coefficients for memory estimation:")
    for name, value in coefficients.items():
        print(f"  {name}: {value:.4f}")
    
    if not production_mode:
        print("\nWARNING: Coefficients calculated with small datasets may be less accurate")
        print("Run with --production for coefficients suitable for production use")


if __name__ == "__main__":
    main()