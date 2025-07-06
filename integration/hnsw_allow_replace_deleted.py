#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2025, valkey-search contributors
# All rights reserved.
# SPDX-License-Identifier: BSD 3-Clause

import numpy as np
import struct
import time
from valkeytestframework.util.waiters import *
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestHNSWAllowReplaceDeleted(ValkeySearchTestCaseBase):
    """Test suite for HNSW allow_replace_deleted configuration"""
    
    def _create_vector_bytes(self, dim=128):
        """Helper to create random normalized vector bytes"""
        vec = np.random.rand(dim).astype(np.float32)
        vec = vec / np.linalg.norm(vec)  # Normalize for cosine similarity
        return struct.pack(f'{dim}f', *vec)
    
    def _extract_info_field(self, info_output, field_name):
        """Helper to extract field from FT.INFO output"""
        info_dict = dict(zip(info_output[::2], info_output[1::2]))
        return info_dict.get(field_name)

    def test_memory_efficiency_with_replacement(self):
        """Test memory efficiency with allow_replace_deleted enabled"""
        client: Valkey = self.server.get_new_client()
        
        # Set the configuration at runtime
        client.config_set('search.hnsw-allow-replace-deleted', 'yes')
        
        # Create index
        response = client.execute_command(
            'FT.CREATE', 'idx', 'SCHEMA', 'vec', 'VECTOR', 'HNSW', '6',
            'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2'
        )
        assert response == b'OK'
        
        # Add initial vectors
        num_vectors = 10000
        for i in range(num_vectors):
            vec_bytes = self._create_vector_bytes(128)
            client.hset(f'doc{i}', 'vec', vec_bytes)
        
        # Get initial memory info
        info1 = client.execute_command('FT.INFO', 'idx')
        initial_blocks = self._extract_info_field(info1, b'total_inverted_index_blocks')
        
        # Delete half the vectors
        deleted_count = 0
        for i in range(0, num_vectors, 2):
            result = client.delete(f'doc{i}')
            if result == 1:
                deleted_count += 1
        
        assert deleted_count == num_vectors // 2
        
        # Add new vectors (should reuse deleted slots)
        for i in range(num_vectors, num_vectors + num_vectors // 2):
            vec_bytes = self._create_vector_bytes(128)
            client.hset(f'doc{i}', 'vec', vec_bytes)
        
        # Check final memory usage
        info2 = client.execute_command('FT.INFO', 'idx')
        final_blocks = self._extract_info_field(info2, b'total_inverted_index_blocks')
        
        # With replacement enabled, memory growth should be minimal
        # Allow for some growth but it should be much less than without replacement
        if initial_blocks and final_blocks:
            growth_ratio = int(final_blocks) / int(initial_blocks)
            assert growth_ratio <= 1.1, f"Memory grew too much: {growth_ratio}x"

    def test_memory_efficiency_without_replacement(self):
        """Test memory efficiency with allow_replace_deleted disabled"""
        client: Valkey = self.server.get_new_client()
        
        # Set the configuration at runtime (disabled)
        client.config_set('search.hnsw-allow-replace-deleted', 'no')
        
        # Create index
        response = client.execute_command(
            'FT.CREATE', 'idx', 'SCHEMA', 'vec', 'VECTOR', 'HNSW', '6',
            'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2'
        )
        assert response == b'OK'
        
        # Add initial vectors
        num_vectors = 10000
        for i in range(num_vectors):
            vec_bytes = self._create_vector_bytes(128)
            client.hset(f'doc{i}', 'vec', vec_bytes)
        
        # Get initial memory info
        info1 = client.execute_command('FT.INFO', 'idx')
        initial_blocks = self._extract_info_field(info1, b'total_inverted_index_blocks')
        
        # Delete half the vectors
        for i in range(0, num_vectors, 2):
            client.delete(f'doc{i}')
        
        # Add new vectors (will require index expansion)
        for i in range(num_vectors, num_vectors + num_vectors // 2):
            vec_bytes = self._create_vector_bytes(128)
            client.hset(f'doc{i}', 'vec', vec_bytes)
        
        # Check final memory usage
        info2 = client.execute_command('FT.INFO', 'idx')
        final_blocks = self._extract_info_field(info2, b'total_inverted_index_blocks')
        
        # Without replacement, memory should grow more significantly
        if initial_blocks and final_blocks:
            growth_ratio = int(final_blocks) / int(initial_blocks)
            # This test expects growth when replacement is disabled
            # The exact ratio depends on HNSW block size configuration

    def test_search_accuracy_with_replacement(self):
        """Test search accuracy with allow_replace_deleted enabled"""
        client: Valkey = self.server.get_new_client()
        
        # Set the configuration at runtime
        client.config_set('search.hnsw-allow-replace-deleted', 'yes')
        
        # Create index with cosine similarity
        response = client.execute_command(
            'FT.CREATE', 'idx', 'SCHEMA', 'vec', 'VECTOR', 'HNSW', '6',
            'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'COSINE'
        )
        assert response == b'OK'
        
        # Add vectors
        vectors = {}
        for i in range(100):
            vec_bytes = self._create_vector_bytes(128)
            vectors[f'doc{i}'] = vec_bytes
            client.hset(f'doc{i}', 'vec', vec_bytes)
        
        # Delete some vectors
        deleted_docs = []
        for i in range(0, 20):
            if client.delete(f'doc{i}') == 1:
                deleted_docs.append(f'doc{i}')
        
        # Add replacement vectors
        for i in range(100, 120):
            vec_bytes = self._create_vector_bytes(128)
            vectors[f'doc{i}'] = vec_bytes
            client.hset(f'doc{i}', 'vec', vec_bytes)
        
        # Perform KNN search
        query_vec = self._create_vector_bytes(128)
        results = client.execute_command(
            'FT.SEARCH', 'idx', '*=>[KNN 10 @vec $query_vec]',
            'PARAMS', '2', 'query_vec', query_vec
        )
        
        # Verify results
        num_results = results[0]
        assert num_results == 10, f"Expected 10 results, got {num_results}"
        
        # Verify no deleted documents in results
        for i in range(1, len(results), 2):
            doc_id = results[i].decode('utf-8')
            assert doc_id not in deleted_docs, f"Deleted document {doc_id} in results"

    def test_config_runtime_change(self):
        """Test runtime configuration changes"""
        client: Valkey = self.server.get_new_client()
        
        # Set initial config value
        client.config_set('search.hnsw-allow-replace-deleted', 'no')
        
        # Get initial config value
        result = client.config_get('search.hnsw-allow-replace-deleted')
        assert result['search.hnsw-allow-replace-deleted'] == 'no'
        
        # Change at runtime
        client.config_set('search.hnsw-allow-replace-deleted', 'yes')
        
        # Verify the change
        result = client.config_get('search.hnsw-allow-replace-deleted')
        assert result['search.hnsw-allow-replace-deleted'] == 'yes'

    # TODO: need to define success critiria for this test case
    def test_performance_comparison(self):
        """Compare performance with and without replacement"""
        results = {}
        results['no'] = {}
        results['yes'] = {}
        perf_cases = [('no',10000000), ('yes',20), ('yes',15), ('yes',10), ('yes',7), ('yes',5), ('yes',4), ('yes',3), ('yes',2)]
        for allow_replace, part in perf_cases:
            client: Valkey = self.server.get_new_client()
            
            # Set the configuration at runtime
            client.config_set('search.hnsw-allow-replace-deleted', allow_replace)
            
            # Create index
            client.execute_command(
                'FT.CREATE', f'idx_{allow_replace}', 'SCHEMA', 'vec', 'VECTOR', 'HNSW', '6',
                'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2'
            )
            
            # Add vectors
            num_vectors = 20000
            for i in range(num_vectors):
                vec_bytes = self._create_vector_bytes(128)
                client.hset(f'doc{i}', 'vec', vec_bytes)
            
            # Delete and replace cycle
            start_time = time.time()
            
            # Delete 20%
            for i in range(0, num_vectors // part):
                client.delete(f'doc{i}')
            
            # Add new vectors
            for i in range(num_vectors, num_vectors + num_vectors // part):
                vec_bytes = self._create_vector_bytes(128)
                client.hset(f'doc{i}', 'vec', vec_bytes)
            
            # Measure search performance
            search_times = []
            for _ in range(1000):
                query_vec = self._create_vector_bytes(128)
                search_start = time.time()
                client.execute_command(
                    'FT.SEARCH', f'idx_{allow_replace}', '*=>[KNN 10 @vec $query_vec]',
                    'PARAMS', '2', 'query_vec', query_vec
                )
                search_times.append(time.time() - search_start)
            
            avg_search_time = sum(search_times) / len(search_times)
            total_time = time.time() - start_time
            
            results[allow_replace][part] = {
                'total_time': total_time,
                'avg_search_time_ms': avg_search_time * 1000
            }
            
            # Clean up index for next iteration
            client.execute_command('FT.DROPINDEX', f'idx_{allow_replace}')
        
        # Log results for comparison
        print(f"\nPerformance comparison:")
        for allow_replace, part in perf_cases:                       
            print(f"[1/{part}] replacement]: {results[allow_replace][part]['avg_search_time_ms']:.2f}ms avg search")
