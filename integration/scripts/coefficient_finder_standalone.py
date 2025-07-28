#!/usr/bin/env python3
"""
Coefficient finder for memory estimation formula - Standalone version.
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
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from valkey import Valkey
from integration.utils.tags_builder import TagsConfig, TagDistribution, TagSharingConfig, TagSharingMode
from integration.utils.string_generator import LengthConfig, Distribution, PrefixConfig
from integration.utils.hash_generator import (
    HashKeyGenerator, HashGeneratorConfig, IndexSchema, FieldSchema,
    FieldType, VectorFieldSchema, VectorAlgorithm, VectorMetric
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DimensionGroup(Enum):
    """Groups of related parameters that form dimensions"""
    TAGS = "tags"
    VECTORS = "vectors"
    NUMERIC = "numeric"


@dataclass
class BenchmarkScenario:
    """Configuration for a benchmark scenario"""
    name: str
    total_keys: int
    tags_config: TagsConfig
    description: str
    # Vector configuration
    vector_dim: int = 8
    vector_algorithm: VectorAlgorithm = VectorAlgorithm.FLAT
    vector_metric: VectorMetric = VectorMetric.COSINE
    hnsw_m: int = 16
    # Numeric configuration  
    include_numeric: bool = True
    numeric_fields: Dict[str, tuple] = field(default_factory=lambda: {
        "score": (0.0, 100.0),
        "timestamp": (1000000000.0, 2000000000.0)
    })


@dataclass
class DimensionScenario:
    """Scenario for testing a specific dimension"""
    name: str
    group: DimensionGroup
    description: str
    scenarios: List[BenchmarkScenario]
    variable_params: List[str]
    fixed_params: Dict[str, any]


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
        self.client = None
        
        # Paths from environment
        self.valkey_server_path = os.getenv("VALKEY_SERVER_PATH", "/home/ubuntu/valkey/build/bin/valkey-server")
        self.module_path = os.getenv("MODULE_PATH", "/home/ubuntu/valkey-search/.build-release/libsearch.so")
        
    def create_tag_dimension_scenarios(self) -> List[DimensionScenario]:
        """Create scenarios for testing tag-related dimensions"""
        scenarios = []
        
        # 1. Test impact of number of unique tags (no sharing)
        tag_count_scenarios = []
        for num_tags in [100, 500, 1000, 2000, 5000, 10000]:
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
                vector_dim=2,  # Minimal vector to satisfy index requirements
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
        for tag_len in [5, 10, 20, 50, 100]:
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
                vector_dim=2,  # Minimal vector to satisfy index requirements
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
        
        return scenarios
    
    def create_vector_dimension_scenarios(self) -> List[DimensionScenario]:
        """Create scenarios for testing vector-related dimensions"""
        scenarios = []
        
        # Test impact of vector dimensions (FLAT algorithm)
        vector_dim_scenarios = []
        for dim in [2, 8, 16, 32, 64, 128]:
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
        
        return scenarios
    
    def create_index_schema(self, scenario: BenchmarkScenario) -> IndexSchema:
        """Create index schema from scenario"""
        fields = []
        
        # Add tag field
        fields.append(FieldSchema(
            name="tags",
            type=FieldType.TAG,
            separator=","
        ))
        
        # Add vector field if needed
        if scenario.vector_dim > 0:
            vector_config = VectorFieldSchema(
                algorithm=scenario.vector_algorithm,
                dim=scenario.vector_dim,
                distance_metric=scenario.vector_metric,
                m=scenario.hnsw_m if scenario.vector_algorithm == VectorAlgorithm.HNSW else None
            )
            fields.append(FieldSchema(
                name="vec",
                type=FieldType.VECTOR,
                vector_config=vector_config
            ))
        
        # Add numeric fields if needed
        if scenario.include_numeric:
            for field_name in scenario.numeric_fields:
                fields.append(FieldSchema(
                    name=field_name,
                    type=FieldType.NUMERIC
                ))
        
        return IndexSchema(
            index_name="test_idx",
            prefix=["key:"],
            fields=fields
        )
    
    def ingest_data(self, scenario: BenchmarkScenario):
        """Ingest data for a scenario"""
        # Create index
        schema = self.create_index_schema(scenario)
        
        # Create index command
        cmd = ["FT.CREATE", schema.index_name, "ON", "HASH", "PREFIX", "1", schema.prefix[0]]
        cmd.extend(["SCHEMA"])
        
        for field in schema.fields:
            if field.type == FieldType.VECTOR and field.vector_config:
                vc = field.vector_config
                cmd.extend([field.name, "VECTOR", vc.algorithm.value])
                
                # Build parameters list
                params = [
                    "TYPE", vc.datatype,
                    "DIM", str(vc.dim),
                    "DISTANCE_METRIC", vc.distance_metric.value
                ]
                
                # Add algorithm-specific parameters
                if vc.algorithm == VectorAlgorithm.HNSW and vc.m:
                    params.extend(["M", str(vc.m)])
                    if vc.ef_construction:
                        params.extend(["EF_CONSTRUCTION", str(vc.ef_construction)])
                
                cmd.extend([str(len(params))] + params)
                
            elif field.type == FieldType.TAG:
                cmd.extend([field.name, "TAG", "SEPARATOR", field.separator])
            else:
                cmd.extend([field.name, field.type.value])
        
        self.client.execute_command(*cmd)
        
        # Generate and ingest data
        config = HashGeneratorConfig(
            key_prefix="key:",
            num_keys=scenario.total_keys,
            tags_config=scenario.tags_config,
            schema=schema,
            batch_size=1000
        )
        
        generator = HashKeyGenerator(config)
        
        # Ingest in batches
        batch_count = 0
        for batch in generator:
            pipeline = self.client.pipeline()
            for key, fields in batch:
                pipeline.hset(key, mapping=fields)
            pipeline.execute()
            batch_count += 1
            
            if batch_count % 10 == 0:
                logger.info(f"Ingested {batch_count * 1000} keys")
    
    def measure_scenario(self, scenario: BenchmarkScenario) -> MeasurementResult:
        """Run a single scenario and measure memory usage"""
        logger.info(f"Measuring scenario: {scenario.name}")
        
        # Start fresh Valkey instance
        self.stop_valkey()
        self.start_valkey()
        
        try:
            # Get initial memory
            initial_memory = self._get_valkey_memory()
            logger.info(f"Initial memory: {initial_memory:,} bytes")
            
            # Run ingestion
            self.ingest_data(scenario)
            
            # Force memory calculation
            self.client.execute_command("MEMORY", "PURGE")
            time.sleep(1)
            
            # Get final memory
            final_memory = self._get_valkey_memory()
            memory_used = final_memory - initial_memory
            
            logger.info(f"Final memory: {final_memory:,} bytes")
            logger.info(f"Memory used: {memory_used:,} bytes")
            
            # Calculate stats
            params = {
                "total_keys": scenario.total_keys,
                "vector_dim": scenario.vector_dim,
                "vector_algorithm": scenario.vector_algorithm.value,
                "num_numeric_fields": len(scenario.numeric_fields) if scenario.include_numeric else 0
            }
            
            # Add tag stats if available
            if hasattr(scenario.tags_config, 'tags_per_key'):
                params["avg_tags_per_key"] = scenario.tags_config.tags_per_key.avg
            if hasattr(scenario.tags_config, 'tag_length'):
                params["avg_tag_length"] = scenario.tags_config.tag_length.avg
            if hasattr(scenario.tags_config.sharing, 'pool_size'):
                params["unique_tags"] = scenario.tags_config.sharing.pool_size
            
            result = MeasurementResult(
                scenario_name=scenario.name,
                total_keys=scenario.total_keys,
                memory_used=memory_used,
                parameters=params
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error in scenario {scenario.name}: {e}")
            raise
    
    def start_valkey(self):
        """Start Valkey server with search module"""
        # Create temporary directory
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
        for i in range(50):
            try:
                self.client = Valkey(host="localhost", port=self.valkey_port, decode_responses=True)
                self.client.ping()
                logger.info("Valkey started successfully")
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
            
        # Clean up test directory
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
    
    def calculate_coefficients(self, dimension_results: Dict[str, List[MeasurementResult]]) -> Dict[str, float]:
        """Calculate linear coefficients from measurement results"""
        coefficients = {}
        
        for dimension_name, results in dimension_results.items():
            if not results:
                continue
                
            # Convert to dataframe
            df = pd.DataFrame([
                {**r.parameters, 'memory': r.memory_used} for r in results
            ])
            
            # Simple linear regression
            if dimension_name == "tag_count_impact" and 'unique_tags' in df.columns:
                X = np.array(df['unique_tags']).reshape(-1, 1)
                y = np.array(df['memory'])
                coef = np.linalg.lstsq(X, y, rcond=None)[0][0]
                coefficients['per_unique_tag'] = coef
                
            elif dimension_name == "tag_length_impact" and 'avg_tag_length' in df.columns:
                X = np.array(df['avg_tag_length']).reshape(-1, 1)
                y = np.array(df['memory'])
                coef = np.linalg.lstsq(X, y, rcond=None)[0][0]
                coefficients['per_tag_length'] = coef
                
            elif dimension_name == "vector_dimension_impact" and 'vector_dim' in df.columns:
                X = np.array(df['vector_dim']).reshape(-1, 1)
                y = np.array(df['memory'])
                coef = np.linalg.lstsq(X, y, rcond=None)[0][0]
                coefficients['per_vector_dim'] = coef
                
        return coefficients
    
    def run_analysis(self, test_type="quick"):
        """Run analysis based on test type"""
        all_results = {}
        
        try:
            if test_type == "quick":
                # Quick test with fewer scenarios
                logger.info("Running quick test...")
                
                # Test tag count impact
                scenarios = self.create_tag_dimension_scenarios()
                tag_scenario = scenarios[0]  # tag_count_impact
                
                results = []
                for scenario in tag_scenario.scenarios[:4]:  # First 4 for better regression
                    result = self.measure_scenario(scenario)
                    results.append(result)
                    self.results.append(result)
                    
                all_results[tag_scenario.name] = results
                
            else:
                # Full test
                logger.info("Running full analysis...")
                
                # Test all dimensions
                for scenarios in [self.create_tag_dimension_scenarios(), 
                                  self.create_vector_dimension_scenarios()]:
                    for dim_scenario in scenarios:
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
        
        if all_results_data:
            df = pd.DataFrame(all_results_data)
            df.to_csv(f"coefficient_analysis_results_{timestamp}.csv", index=False)
        
        logger.info(f"Results saved to coefficients_{timestamp}.json")
        if all_results_data:
            logger.info(f"Detailed results saved to coefficient_analysis_results_{timestamp}.csv")
        logger.info(f"Calculated coefficients: {coefficients}")


def main():
    """Main entry point"""
    import sys
    
    test_type = "quick" if len(sys.argv) > 1 and sys.argv[1] == "--quick" else "full"
    
    finder = CoefficientFinder()
    coefficients = finder.run_analysis(test_type)
    
    print("\nCalculated coefficients:")
    for name, value in coefficients.items():
        print(f"  {name}: {value:.4f}")


if __name__ == "__main__":
    main()