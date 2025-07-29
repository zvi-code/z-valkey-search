#!/usr/bin/env python3
"""
Silent Valkey client implementations and connection pools.
Provides quiet client wrappers and thread-safe/async-safe pools for testing.
"""

import os
import sys
import threading
import logging
import asyncio
import io
from typing import Dict, List, Optional, Any

from valkey.client import Valkey
from valkey.asyncio import Valkey as AsyncValkey


def silence_valkey_loggers():
    """Temporarily silence valkey-related loggers and return restoration function."""
    import sys
    import io
    
    original_levels = {}
    loggers_to_silence = [
        '',  # root logger
        'valkey',
        'valkey.client', 
        'valkey.connection',
        'valkey_search',
        'valkey_search_test_case',
        'ValkeySearchTestCaseBase',
        __name__  # current module logger
    ]
    
    # Disable all potentially noisy loggers
    for logger_name in loggers_to_silence:
        logger = logging.getLogger(logger_name)
        original_levels[logger_name] = logger.level
        logger.setLevel(logging.CRITICAL)
    
    # Also temporarily disable all handlers
    original_root_handlers = logging.root.handlers[:]
    null_handler = logging.NullHandler()
    
    # Replace root handlers with null handler
    logging.root.handlers = [null_handler]
    
    # Redirect stdout/stderr to null using context managers
    stdout_redirect = io.StringIO()
    stderr_redirect = io.StringIO()
    
    def restore_loggers():
        """Restore original logging configuration."""
        # Restore root handlers
        logging.root.handlers = original_root_handlers
        
        # Restore logger levels
        for logger_name, level in original_levels.items():
            logging.getLogger(logger_name).setLevel(level)
    
    return restore_loggers


def create_silent_client_from_server(server) -> 'SilentValkeyClient':
    """Create a silent Valkey client from a server instance."""
    if hasattr(server, 'get_new_client'):
        # Temporarily suppress logging while getting connection params
        original_level = logging.getLogger().level
        logging.getLogger().setLevel(logging.CRITICAL)
        
        try:
            # Get a regular client first to extract connection params
            temp_client = server.get_new_client()
            connection_kwargs = temp_client.connection_pool.connection_kwargs
            temp_client.close()
        finally:
            # Restore logging level
            logging.getLogger().setLevel(original_level)
        
        # Create our silent client with the same params
        return SilentValkeyClient(**connection_kwargs)
    else:        
        # Fallback: create with default params
        return SilentValkeyClient(host='localhost', port=6379, decode_responses=True)


async def create_silent_async_client_from_server(server) -> 'AsyncSilentValkeyClient':
    """Create a silent async Valkey client from a server instance."""
    if hasattr(server, 'get_new_client'):
        # Temporarily suppress logging while getting connection params
        original_level = logging.getLogger().level
        logging.getLogger().setLevel(logging.CRITICAL)
        
        try:
            # Get connection params from sync client
            temp_client = server.get_new_client()
            connection_kwargs = temp_client.connection_pool.connection_kwargs.copy()
            temp_client.close()
        finally:
            # Restore logging level
            logging.getLogger().setLevel(original_level)
        
        # Remove sync-specific params and add async-specific ones
        connection_kwargs.pop('connection_pool', None)
        connection_kwargs.pop('connection_class', None)
        
        # Create async client with extracted params
        return AsyncSilentValkeyClient(**connection_kwargs)
    else:
        # Fallback: create with default params
        return AsyncSilentValkeyClient(host='localhost', port=6379, decode_responses=True)


class SilentValkeyClient(Valkey):
    """
    A Valkey client wrapper that suppresses client creation logs.
    Intercepts and redirects specific log messages to avoid cluttering test logs.
    """
    
    def __init__(self, *args, **kwargs):
        """Initialize client with suppressed logging"""
        restore_loggers = silence_valkey_loggers()
        
        try:
            # Create the client
            super().__init__(*args, **kwargs)
        finally:
            restore_loggers()


class AsyncSilentValkeyClient(AsyncValkey):
    """
    An async Valkey client wrapper that suppresses client creation logs.
    Provides high-performance async I/O operations.
    """
    
    def __init__(self, *args, **kwargs):
        # Save original logging state for multiple loggers
        original_levels = {}
        loggers_to_silence = [
            '',  # root logger
            'valkey',
            'valkey.client',
            'valkey.connection',
            'valkey_search',
            'valkey_search_test_case',
            'ValkeySearchTestCaseBase',
            __name__  # current module logger
        ]
        
        # Disable all potentially noisy loggers
        for logger_name in loggers_to_silence:
            logger = logging.getLogger(logger_name)
            original_levels[logger_name] = logger.level
            logger.setLevel(logging.CRITICAL)
        
        # Also temporarily disable all handlers
        original_root_handlers = logging.root.handlers[:]
        null_handler = logging.NullHandler()
        
        # Save original stdout/stderr in case client prints directly
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        
        try:
            # Replace root handlers with null handler
            logging.root.handlers = [null_handler]
            
            # Redirect stdout/stderr to null
            sys.stdout = io.StringIO()
            sys.stderr = io.StringIO()
            
            # Create the async client
            super().__init__(*args, **kwargs)
            
        finally:
            # Restore stdout/stderr
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            
            # Restore original logging configuration
            logging.root.handlers = original_root_handlers
            
            # Restore all logger levels
            for logger_name, level in original_levels.items():
                logger = logging.getLogger(logger_name)
                logger.setLevel(level)


class AsyncSilentClientPool:
    """Async client pool that uses AsyncSilentValkeyClient for high-performance operations"""
    
    def __init__(self, server, pool_size: int, max_concurrent_per_client: int = 10):
        self.server = server
        self.pool_size = pool_size
        self.clients = []
        self.semaphores = []  # One semaphore per client to limit concurrent ops
        self.max_concurrent_per_client = max_concurrent_per_client
        self.lock = asyncio.Lock()
        
    async def initialize(self):
        """Initialize all async clients - must be called in async context"""
        for i in range(self.pool_size):
            client = await self._create_silent_async_client()
            semaphore = asyncio.Semaphore(self.max_concurrent_per_client)
            self.clients.append(client)
            self.semaphores.append(semaphore)
    
    async def _create_silent_async_client(self) -> AsyncSilentValkeyClient:
        """Create a new async client with logging suppressed"""
        return await create_silent_async_client_from_server(self.server)
    
    def get_client_for_task(self, task_id: int) -> tuple[AsyncSilentValkeyClient, asyncio.Semaphore]:
        """Get a client and its semaphore for a specific task ID"""
        client_id = task_id % self.pool_size
        return self.clients[client_id], self.semaphores[client_id]
    
    async def close_all(self):
        """Close all async clients in the pool"""
        for client in self.clients:
            try:
                await client.aclose()
            except:
                continue


class SilentClientPool:
    """Custom client pool that uses SilentValkeyClient to suppress logging"""
    
    def __init__(self, server, pool_size: int):
        self.server = server
        self.pool_size = pool_size
        self.clients = []
        self.lock = threading.Lock()
        self.thread_local = threading.local()
        
        # Pre-create all clients using our silent wrapper
        for i in range(pool_size):
            client = self._create_silent_client()
            self.clients.append(client)
    
    def _create_silent_client(self) -> SilentValkeyClient:
        """Create a new client with logging suppressed"""
        return create_silent_client_from_server(self.server)
    
    def get_client_for_thread(self, thread_index: int) -> SilentValkeyClient:
        """Get a dedicated client for a specific thread index"""
        if thread_index >= self.pool_size:
            raise ValueError(f"Thread index {thread_index} exceeds pool size {self.pool_size}")
        return self.clients[thread_index]
    
    def get_client(self) -> SilentValkeyClient:
        """Get a client - backward compatibility method that uses thread-local storage"""
        if hasattr(self.thread_local, 'client'):
            return self.thread_local.client
        
        thread_id = threading.get_ident()
        client_index = thread_id % self.pool_size
        self.thread_local.client = self.clients[client_index]
        return self.thread_local.client
    
    def close_all(self):
        """Close all clients in the pool"""
        max_retries = 5  # Maximum number of retries for closing a client
        for client in self.clients:
            retries = 0
            while retries < max_retries:
                try:
                    client.close()
                    break
                except Exception as e:
                    retries += 1
                    logging.error(f"Error closing client: {e}. Retry {retries}/{max_retries}.")
            else:
                logging.error(f"Failed to close client after {max_retries} retries.")