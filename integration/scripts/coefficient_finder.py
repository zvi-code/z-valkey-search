#!/usr/bin/env python3
"""
Coefficient finder for memory estimation formula.
Systematically tests different dimensions to determine linear approximation coefficients.
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
from valkey import Valkey

from test_memory_benchmark import (
    BenchmarkScenario, MemoryBenchmark
)
from integration.utils.tags_builder import TagsConfig, TagDistribution, TagSharingConfig, TagSharingMode
from integration.utils.string_generator import LengthConfig, Distribution, PrefixConfig
from integration.utils.hash_generator import VectorAlgorithm, VectorMetric

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DimensionGroup(Enum):
    """Groups of related parameters that form dimensions"""
    TAGS = "tags"
    VECTORS = "vectors"
    NUMERIC = "numeric"


@dataclass
class DimensionScenario:
    """Scenario for testing a specific dimension"""
    name: str
    group: DimensionGroup
    description: str
    scenarios: List[BenchmarkScenario]
    variable_params: List[str]  # Parameters being varied
    fixed_params: Dict[str, any]  # Parameters held constant


@dataclass
class MeasurementResult:
    """Result from a single measurement"""
    scenario_name: str
    total_keys: int
    memory_used: int
    parameters: Dict[str, any]
    timestamp: datetime = field(default_factory=datetime.now)


class CoefficientFinder:
    """Find coefficients for memory estimation formula through systematic testing"""
    
    def __init__(self, valkey_port: int = 16379):
        self.valkey_port = valkey_port
        self.results: List[MeasurementResult] = []
        self.valkey_process = None
        self.testdir = None
        
        # Paths from environment
        self.valkey_server_path = os.getenv("VALKEY_SERVER_PATH", "/home/ubuntu/valkey/build/bin/valkey-server")
        self.module_path = os.getenv("MODULE_PATH", "/home/ubuntu/valkey-search/.build-release/libsearch.so")
        
    def create_tag_dimension_scenarios(self) -> List[DimensionScenario]:
        """Create scenarios for testing tag-related dimensions"""
        scenarios = []
        
        # 1. Test impact of number of unique tags (no sharing)
        tag_count_scenarios = []
        for num_tags in [10, 50, 100, 500, 1000, 5000]:
            scenario = BenchmarkScenario(
                name=f"tags_unique_{num_tags}",
                total_keys=10000,
                tags_config=TagsConfig(
                    num_keys=10000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),
                    tag_length=LengthConfig(avg=10, min=10, max=10),
                    sharing=TagSharingConfig(
                        mode=TagSharingMode.SHARED_POOL,
                        pool_size=num_tags,
                        reuse_probability=1.0
                    )
                ),
                description=f"Test with {num_tags} unique tags",
                vector_dim=0,  # No vectors to isolate tag impact
                include_numeric=False
            )
            tag_count_scenarios.append(scenario)
            
        scenarios.append(DimensionScenario(
            name="tag_count_impact",
            group=DimensionGroup.TAGS,
            description="Impact of number of unique tags",
            scenarios=tag_count_scenarios,
            variable_params=["unique_tags"],
            fixed_params={"tag_length": 10, "keys": 10000, "tags_per_key": 1}
        ))
        
        # 2. Test impact of tag length
        tag_length_scenarios = []
        for tag_len in [5, 10, 20, 50, 100, 200]:
            scenario = BenchmarkScenario(
                name=f"tags_length_{tag_len}",
                total_keys=10000,
                tags_config=TagsConfig(
                    num_keys=10000,
                    tags_per_key=TagDistribution(avg=3, min=3, max=3),
                    tag_length=LengthConfig(avg=tag_len, min=tag_len, max=tag_len),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description=f"Test with tag length {tag_len}",
                vector_dim=0,
                include_numeric=False
            )
            tag_length_scenarios.append(scenario)
            
        scenarios.append(DimensionScenario(
            name="tag_length_impact",
            group=DimensionGroup.TAGS,
            description="Impact of tag length",
            scenarios=tag_length_scenarios,
            variable_params=["tag_length"],
            fixed_params={"keys": 10000, "tags_per_key": 3, "sharing": "unique"}
        ))
        
        # 3. Test impact of tag sharing
        tag_sharing_scenarios = []
        for keys_per_tag in [1, 2, 5, 10, 50, 100, 1000]:
            pool_size = 10000 // keys_per_tag
            scenario = BenchmarkScenario(
                name=f"tags_sharing_{keys_per_tag}",
                total_keys=10000,
                tags_config=TagsConfig(
                    num_keys=10000,
                    tags_per_key=TagDistribution(avg=3, min=3, max=3),
                    tag_length=LengthConfig(avg=10, min=10, max=10),
                    sharing=TagSharingConfig(
                        mode=TagSharingMode.SHARED_POOL,
                        pool_size=pool_size,
                        reuse_probability=1.0
                    )
                ),
                description=f"Test with {keys_per_tag} keys per tag",
                vector_dim=0,
                include_numeric=False
            )
            tag_sharing_scenarios.append(scenario)
            
        scenarios.append(DimensionScenario(
            name="tag_sharing_impact",
            group=DimensionGroup.TAGS,
            description="Impact of tag sharing",
            scenarios=tag_sharing_scenarios,
            variable_params=["keys_per_tag"],
            fixed_params={"keys": 10000, "tags_per_key": 3, "tag_length": 10}
        ))
        
        # 4. Test prefix sharing impact
        prefix_scenarios = []
        for prefix_ratio in [0.0, 0.25, 0.5, 0.75, 1.0]:
            scenario = BenchmarkScenario(
                name=f"tags_prefix_{int(prefix_ratio*100)}",
                total_keys=10000,
                tags_config=TagsConfig(
                    num_keys=10000,
                    tags_per_key=TagDistribution(avg=3, min=3, max=3),
                    tag_length=LengthConfig(avg=20, min=20, max=20),
                    tag_prefix=PrefixConfig(
                        enabled=True,
                        prefix_length=LengthConfig(avg=10, min=10, max=10),
                        shared_ratio=prefix_ratio
                    ) if prefix_ratio > 0 else None,
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description=f"Test with {int(prefix_ratio*100)}% prefix sharing",
                vector_dim=0,
                include_numeric=False
            )
            prefix_scenarios.append(scenario)
            
        scenarios.append(DimensionScenario(
            name="prefix_sharing_impact",
            group=DimensionGroup.TAGS,
            description="Impact of prefix sharing",
            scenarios=prefix_scenarios,
            variable_params=["prefix_ratio"],
            fixed_params={"keys": 10000, "tags_per_key": 3, "tag_length": 20}
        ))
        
        return scenarios
    
    def create_vector_dimension_scenarios(self) -> List[DimensionScenario]:
        """Create scenarios for testing vector-related dimensions"""
        scenarios = []
        
        # 1. Test impact of vector dimensions (FLAT algorithm)
        vector_dim_scenarios = []
        for dim in [0, 8, 16, 32, 64, 128, 256, 512]:
            scenario = BenchmarkScenario(
                name=f"vector_dim_{dim}",
                total_keys=10000,
                tags_config=TagsConfig(
                    num_keys=10000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),
                    tag_length=LengthConfig(avg=5, min=5, max=5),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description=f"Test with vector dimension {dim}",
                vector_dim=dim,
                vector_algorithm=VectorAlgorithm.FLAT,
                include_numeric=False
            )
            vector_dim_scenarios.append(scenario)
            
        scenarios.append(DimensionScenario(
            name="vector_dimension_impact",
            group=DimensionGroup.VECTORS,
            description="Impact of vector dimensions",
            scenarios=vector_dim_scenarios,
            variable_params=["vector_dim"],
            fixed_params={"keys": 10000, "algorithm": "FLAT"}
        ))
        
        # 2. Test HNSW algorithm with different M values
        hnsw_m_scenarios = []
        for m in [4, 8, 16, 32, 64]:
            scenario = BenchmarkScenario(
                name=f"vector_hnsw_m_{m}",
                total_keys=10000,
                tags_config=TagsConfig(
                    num_keys=10000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),
                    tag_length=LengthConfig(avg=5, min=5, max=5),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description=f"Test HNSW with M={m}",
                vector_dim=128,
                vector_algorithm=VectorAlgorithm.HNSW,
                hnsw_m=m,
                include_numeric=False
            )
            hnsw_m_scenarios.append(scenario)
            
        scenarios.append(DimensionScenario(
            name="hnsw_m_impact",
            group=DimensionGroup.VECTORS,
            description="Impact of HNSW M parameter",
            scenarios=hnsw_m_scenarios,
            variable_params=["hnsw_m"],
            fixed_params={"keys": 10000, "vector_dim": 128, "algorithm": "HNSW"}
        ))
        
        return scenarios
    
    def create_numeric_dimension_scenarios(self) -> List[DimensionScenario]:
        """Create scenarios for testing numeric field dimensions"""
        scenarios = []
        
        # Test impact of number of numeric fields
        numeric_field_scenarios = []
        for num_fields in [0, 1, 2, 5, 10, 20]:
            numeric_fields = {
                f"field_{i}": (0.0, 1000.0) for i in range(num_fields)
            } if num_fields > 0 else {}
            
            scenario = BenchmarkScenario(
                name=f"numeric_fields_{num_fields}",
                total_keys=10000,
                tags_config=TagsConfig(
                    num_keys=10000,
                    tags_per_key=TagDistribution(avg=1, min=1, max=1),
                    tag_length=LengthConfig(avg=5, min=5, max=5),
                    sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
                ),
                description=f"Test with {num_fields} numeric fields",
                vector_dim=0,
                include_numeric=num_fields > 0,
                numeric_fields=numeric_fields
            )
            numeric_field_scenarios.append(scenario)
            
        scenarios.append(DimensionScenario(
            name="numeric_fields_impact",
            group=DimensionGroup.NUMERIC,
            description="Impact of numeric fields",
            scenarios=numeric_field_scenarios,
            variable_params=["num_numeric_fields"],
            fixed_params={"keys": 10000}
        ))
        
        return scenarios
    
    def measure_scenario(self, scenario: BenchmarkScenario) -> MeasurementResult:
        """Run a single scenario and measure memory usage"""
        logger.info(f"Measuring scenario: {scenario.name}")
        
        # Start fresh Valkey instance
        self.stop_valkey()
        self.start_valkey()
        time.sleep(3)  # Wait for Valkey to start
        
        # Run benchmark
        benchmark = MemoryBenchmark(
            redis_host="localhost",
            redis_port=self.valkey_port
        )
        
        try:
            # Get initial memory
            initial_memory = self._get_valkey_memory()
            
            # Run ingestion
            benchmark.ingest_and_measure(scenario, batch_size=1000)
            
            # Get final memory
            final_memory = self._get_valkey_memory()
            memory_used = final_memory - initial_memory
            
            # Extract key parameters
            stats = benchmark._calculate_estimated_stats(scenario)
            params = {
                "total_keys": scenario.total_keys,
                "unique_tags": stats.get("unique_tags", 0),
                "avg_tag_length": stats.get("avg_tag_length", 0),
                "avg_tags_per_key": stats.get("avg_tags_per_key", 0),
                "keys_per_tag": stats.get("avg_keys_per_tag", 0),
                "vector_dim": scenario.vector_dim,
                "vector_algorithm": scenario.vector_algorithm.value,
                "hnsw_m": scenario.hnsw_m if scenario.vector_algorithm == VectorAlgorithm.HNSW else 0,
                "num_numeric_fields": len(scenario.numeric_fields) if scenario.include_numeric else 0
            }
            
            result = MeasurementResult(
                scenario_name=scenario.name,
                total_keys=scenario.total_keys,
                memory_used=memory_used,
                parameters=params
            )
            
            logger.info(f"Memory used: {memory_used:,} bytes")
            return result
            
        finally:
            benchmark.cleanup()
    
    def start_valkey(self):
        """Start Valkey server with search module"""
        # Create temporary directory for this instance
        self.testdir = tempfile.mkdtemp(prefix="valkey-coef-")
        
        # Create config file
        conf_file = os.path.join(self.testdir, "valkey.conf")
        with open(conf_file, "w") as f:
            f.write(f"port {self.valkey_port}\n")
            f.write(f"dir {self.testdir}\n")
            f.write("save \"\"\n")
            f.write(f"loadmodule {self.module_path}\n")
            f.write("enable-debug-command yes\n")
            
        # Start Valkey
        self.valkey_process = subprocess.Popen(
            [self.valkey_server_path, conf_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Wait for startup
        for _ in range(30):
            try:
                client = Valkey(host="localhost", port=self.valkey_port)
                client.ping()
                logger.info("Valkey started successfully")
                break
            except Exception:
                time.sleep(0.1)
        
    def stop_valkey(self):
        """Stop Valkey server"""
        if self.valkey_process:
            self.valkey_process.terminate()
            self.valkey_process.wait()
            self.valkey_process = None
            
        # Clean up test directory
        if self.testdir and os.path.exists(self.testdir):
            shutil.rmtree(self.testdir)
            self.testdir = None
            
    def _get_valkey_memory(self) -> int:
        """Get current Valkey memory usage"""
        try:
            client = Valkey(host="localhost", port=self.valkey_port)
            info = client.info('memory')
            return info['used_memory']
        except Exception as e:
            logger.error(f"Failed to get Valkey memory: {e}")
            return 0
    
    def calculate_coefficients(self, dimension_results: Dict[str, List[MeasurementResult]]) -> Dict[str, float]:
        """Calculate linear coefficients from measurement results"""
        coefficients = {}
        
        for dimension_name, results in dimension_results.items():
            if not results:
                continue
                
            # Convert to numpy arrays for linear regression
            df = pd.DataFrame([
                {**r.parameters, 'memory': r.memory_used} for r in results
            ])
            
            # Simple linear regression for each variable
            # For more complex scenarios, we'd use multiple regression
            if dimension_name == "tag_count_impact":
                X = np.array(df['unique_tags']).reshape(-1, 1)
                y = np.array(df['memory'])
                coef = np.linalg.lstsq(X, y, rcond=None)[0][0]
                coefficients['per_unique_tag'] = coef
                
            elif dimension_name == "tag_length_impact":
                X = np.array(df['avg_tag_length']).reshape(-1, 1)
                y = np.array(df['memory'])
                coef = np.linalg.lstsq(X, y, rcond=None)[0][0]
                coefficients['per_tag_length'] = coef
                
            elif dimension_name == "vector_dimension_impact":
                X = np.array(df['vector_dim']).reshape(-1, 1)
                y = np.array(df['memory'])
                coef = np.linalg.lstsq(X, y, rcond=None)[0][0]
                coefficients['per_vector_dim'] = coef
                
            elif dimension_name == "numeric_fields_impact":
                X = np.array(df['num_numeric_fields']).reshape(-1, 1)
                y = np.array(df['memory'])
                coef = np.linalg.lstsq(X, y, rcond=None)[0][0]
                coefficients['per_numeric_field'] = coef
                
        return coefficients
    
    def run_full_analysis(self):
        """Run all dimension scenarios and calculate coefficients"""
        all_results = {}
        
        try:
            # Run tag dimension tests
            logger.info("Testing tag dimensions...")
            tag_scenarios = self.create_tag_dimension_scenarios()
            for dim_scenario in tag_scenarios:
                results = []
                for scenario in dim_scenario.scenarios:
                    result = self.measure_scenario(scenario)
                    results.append(result)
                    self.results.append(result)
                all_results[dim_scenario.name] = results
            
            # Run vector dimension tests
            logger.info("Testing vector dimensions...")
            vector_scenarios = self.create_vector_dimension_scenarios()
            for dim_scenario in vector_scenarios:
                results = []
                for scenario in dim_scenario.scenarios:
                    result = self.measure_scenario(scenario)
                    results.append(result)
                    self.results.append(result)
                all_results[dim_scenario.name] = results
            
            # Run numeric dimension tests
            logger.info("Testing numeric dimensions...")
            numeric_scenarios = self.create_numeric_dimension_scenarios()
            for dim_scenario in numeric_scenarios:
                results = []
                for scenario in dim_scenario.scenarios:
                    result = self.measure_scenario(scenario)
                    results.append(result)
                    self.results.append(result)
                all_results[dim_scenario.name] = results
            
            # Calculate coefficients
            coefficients = self.calculate_coefficients(all_results)
            
            # Save results
            self.save_results(coefficients, all_results)
            
            return coefficients
            
        finally:
            self.stop_valkey()
    
    def save_results(self, coefficients: Dict[str, float], dimension_results: Dict[str, List[MeasurementResult]]):
        """Save results to files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save coefficients
        with open(f"coefficients_{timestamp}.json", "w") as f:
            json.dump(coefficients, f, indent=2)
        
        # Save detailed results
        all_results_data = []
        for dim_name, results in dimension_results.items():
            for result in results:
                data = {
                    "dimension": dim_name,
                    "scenario": result.scenario_name,
                    "memory_used": result.memory_used,
                    **result.parameters
                }
                all_results_data.append(data)
        
        df = pd.DataFrame(all_results_data)
        df.to_csv(f"coefficient_analysis_results_{timestamp}.csv", index=False)
        
        logger.info(f"Results saved to coefficients_{timestamp}.json and coefficient_analysis_results_{timestamp}.csv")
        logger.info(f"Calculated coefficients: {coefficients}")


def run_quick_test():
    """Run a quick test with fewer scenarios"""
    finder = CoefficientFinder()
    
    # Test just a few scenarios
    logger.info("Running quick test...")
    results = []
    
    # Test tag count
    for num_tags in [10, 100, 1000]:
        scenario = BenchmarkScenario(
            name=f"quick_tags_{num_tags}",
            total_keys=1000,  # Smaller dataset
            tags_config=TagsConfig(
                num_keys=1000,
                tags_per_key=TagDistribution(avg=1, min=1, max=1),
                tag_length=LengthConfig(avg=10, min=10, max=10),
                sharing=TagSharingConfig(
                    mode=TagSharingMode.SHARED_POOL,
                    pool_size=num_tags,
                    reuse_probability=1.0
                )
            ),
            description=f"Quick test with {num_tags} tags",
            vector_dim=0,
            include_numeric=False
        )
        result = finder.measure_scenario(scenario)
        results.append(result)
    
    # Simple coefficient calculation
    memories = [r.memory_used for r in results]
    tag_counts = [10, 100, 1000]
    
    # Linear fit
    X = np.array(tag_counts).reshape(-1, 1)
    y = np.array(memories)
    coef = np.linalg.lstsq(X, y, rcond=None)[0][0]
    
    print(f"\nQuick test results:")
    print(f"Per unique tag coefficient: {coef:.4f} bytes")
    
    finder.stop_valkey()


def main():
    """Main entry point"""
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--quick":
        run_quick_test()
    else:
        finder = CoefficientFinder()
        coefficients = finder.run_full_analysis()
        
        print("\nCalculated coefficients:")
        for name, value in coefficients.items():
            print(f"  {name}: {value:.4f}")


if __name__ == "__main__":
    main()