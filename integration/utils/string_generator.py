#!/usr/bin/env python3
"""
High-performance string generator for Valkey workload testing.
Supports generation of 1B+ strings with configurable properties.
"""

import numpy as np
from typing import Iterator, Optional, Union, Literal, Dict, Any
from dataclasses import dataclass
from enum import Enum
import struct
import random
import string as string_module


class Distribution(Enum):
    """Supported probability distributions"""
    UNIFORM = "uniform"
    NORMAL = "normal"
    EXPONENTIAL = "exponential"
    ZIPF = "zipf"
    PARETO = "pareto"


class StringType(Enum):
    """Types of strings that can be generated"""
    RANDOM_BYTES = "random_bytes"
    ASCII = "ascii"
    ALPHANUMERIC = "alphanumeric"
    NUMERIC = "numeric"
    FLOAT32_VECTOR = "float32_vector"
    CUSTOM_CHARSET = "custom_charset"


@dataclass
class LengthConfig:
    """Configuration for string length generation"""
    avg: int
    min: int = 1
    max: int = 1024
    distribution: Distribution = Distribution.UNIFORM
    # Distribution-specific parameters
    std_dev: Optional[float] = None  # For normal distribution
    alpha: Optional[float] = None    # For Zipf/Pareto


@dataclass
class PrefixConfig:
    """Configuration for prefix sharing between strings"""
    enabled: bool = False
    min_shared: int = 0
    max_shared: int = 32
    distribution: Distribution = Distribution.UNIFORM
    # Probability of sharing prefix with previous string
    share_probability: float = 0.5
    # Number of unique prefixes to maintain
    prefix_pool_size: int = 100


@dataclass
class GeneratorConfig:
    """Main configuration for string generation"""
    count: int
    string_type: StringType = StringType.RANDOM_BYTES
    length: Optional[LengthConfig] = None
    prefix: Optional[PrefixConfig] = None
    # For FLOAT32_VECTOR type
    vector_dim: Optional[int] = None
    # For CUSTOM_CHARSET type
    charset: Optional[str] = None
    # Performance tuning
    batch_size: int = 10000
    seed: Optional[int] = None
    # Memory efficiency: use numpy arrays vs Python lists
    use_numpy: bool = True


class StringGenerator:
    """
    High-performance string generator with iterator interface.
    
    Example usage:
        config = GeneratorConfig(
            count=1_000_000,
            string_type=StringType.ASCII,
            length=LengthConfig(avg=100, min=50, max=200),
            prefix=PrefixConfig(enabled=True, max_shared=20)
        )
        
        gen = StringGenerator(config)
        for batch in gen:
            # Process batch of strings
            pass
    """
    
    def __init__(self, config: GeneratorConfig):
        self.config = config
        self._validate_config()
        
        # Initialize random state
        if config.seed is not None:
            np.random.seed(config.seed)
            random.seed(config.seed)
        
        # Pre-generate length samples for better performance
        self._length_samples = None
        if config.length:
            self._prepare_length_samples()
        
        # Initialize prefix pool
        self._prefix_pool = []
        self._last_string = None
        if config.prefix and config.prefix.enabled:
            self._initialize_prefix_pool()
    
    def _validate_config(self):
        """Validate configuration parameters"""
        if self.config.count <= 0:
            raise ValueError("count must be positive")
        
        if self.config.string_type == StringType.FLOAT32_VECTOR:
            if self.config.vector_dim is None or self.config.vector_dim <= 0:
                raise ValueError("vector_dim required for FLOAT32_VECTOR type")
        
        if self.config.string_type == StringType.CUSTOM_CHARSET:
            if not self.config.charset:
                raise ValueError("charset required for CUSTOM_CHARSET type")
        
        if self.config.length:
            if self.config.length.min > self.config.length.max:
                raise ValueError("min length cannot exceed max length")
            if self.config.length.avg < self.config.length.min or \
               self.config.length.avg > self.config.length.max:
                raise ValueError("avg length must be between min and max")
    
    def _prepare_length_samples(self):
        """Pre-generate length samples based on distribution"""
        cfg = self.config.length
        total_samples = min(self.config.count, 1_000_000)  # Cap for memory
        
        if cfg.distribution == Distribution.UNIFORM:
            self._length_samples = np.random.randint(
                cfg.min, cfg.max + 1, size=total_samples
            )
        elif cfg.distribution == Distribution.NORMAL:
            std_dev = cfg.std_dev or (cfg.max - cfg.min) / 6
            samples = np.random.normal(cfg.avg, std_dev, size=total_samples)
            self._length_samples = np.clip(samples, cfg.min, cfg.max).astype(int)
        elif cfg.distribution == Distribution.EXPONENTIAL:
            scale = cfg.avg - cfg.min
            samples = np.random.exponential(scale, size=total_samples) + cfg.min
            self._length_samples = np.clip(samples, cfg.min, cfg.max).astype(int)
        elif cfg.distribution == Distribution.ZIPF:
            alpha = cfg.alpha or 1.5
            samples = np.random.zipf(alpha, size=total_samples)
            # Scale to fit within bounds
            samples = (samples - 1) * (cfg.max - cfg.min) / (samples.max() - 1) + cfg.min
            self._length_samples = samples.astype(int)
        elif cfg.distribution == Distribution.PARETO:
            alpha = cfg.alpha or 1.0
            samples = (np.random.pareto(alpha, size=total_samples) + 1) * cfg.min
            self._length_samples = np.clip(samples, cfg.min, cfg.max).astype(int)
        
        self._length_idx = 0
    
    def _get_next_length(self) -> int:
        """Get next string length from pre-generated samples"""
        if self._length_samples is None:
            return self.config.vector_dim * 4 if self.config.string_type == StringType.FLOAT32_VECTOR else 16
        
        length = self._length_samples[self._length_idx % len(self._length_samples)]
        self._length_idx += 1
        return int(length)
    
    def _initialize_prefix_pool(self):
        """Initialize pool of prefixes for sharing"""
        cfg = self.config.prefix
        for _ in range(cfg.prefix_pool_size):
            prefix_len = random.randint(cfg.min_shared, cfg.max_shared)
            prefix = self._generate_random_bytes(prefix_len)
            self._prefix_pool.append(prefix)
    
    def _generate_random_bytes(self, length: int) -> bytes:
        """Generate random bytes efficiently"""
        return np.random.bytes(length)
    
    def _generate_string(self) -> bytes:
        """Generate a single string based on configuration"""
        if self.config.string_type == StringType.FLOAT32_VECTOR:
            # Generate random float32 vector
            vec = np.random.randn(self.config.vector_dim).astype(np.float32)
            return vec.tobytes()
        
        length = self._get_next_length()
        
        # Handle prefix sharing
        prefix = b''
        if self.config.prefix and self.config.prefix.enabled and \
           random.random() < self.config.prefix.share_probability:
            if self._last_string and len(self._last_string) >= self.config.prefix.min_shared:
                # Share prefix with last string
                prefix_len = min(
                    random.randint(self.config.prefix.min_shared, self.config.prefix.max_shared),
                    len(self._last_string),
                    length
                )
                prefix = self._last_string[:prefix_len]
            else:
                # Use prefix from pool
                prefix = random.choice(self._prefix_pool)
                prefix = prefix[:min(len(prefix), length)]
        
        remaining_length = length - len(prefix)
        
        if self.config.string_type == StringType.RANDOM_BYTES:
            suffix = self._generate_random_bytes(remaining_length)
        elif self.config.string_type == StringType.ASCII:
            # Printable ASCII (32-126)
            suffix = np.random.randint(32, 127, size=remaining_length, dtype=np.uint8).tobytes()
        elif self.config.string_type == StringType.ALPHANUMERIC:
            chars = string_module.ascii_letters + string_module.digits
            suffix = ''.join(random.choices(chars, k=remaining_length)).encode('ascii')
        elif self.config.string_type == StringType.NUMERIC:
            suffix = ''.join(random.choices(string_module.digits, k=remaining_length)).encode('ascii')
        elif self.config.string_type == StringType.CUSTOM_CHARSET:
            suffix = ''.join(random.choices(self.config.charset, k=remaining_length)).encode('utf-8')
        else:
            suffix = self._generate_random_bytes(remaining_length)
        
        result = prefix + suffix
        self._last_string = result
        return result
    
    def __iter__(self) -> Iterator[Union[np.ndarray, list]]:
        """Iterator interface returning batches of strings"""
        generated = 0
        
        while generated < self.config.count:
            batch_size = min(self.config.batch_size, self.config.count - generated)
            
            if self.config.use_numpy:
                # Use numpy object array for better memory layout
                batch = np.empty(batch_size, dtype=object)
                for i in range(batch_size):
                    batch[i] = self._generate_string()
            else:
                batch = [self._generate_string() for _ in range(batch_size)]
            
            generated += batch_size
            yield batch
    
    def generate_all(self) -> Union[np.ndarray, list]:
        """Generate all strings at once (use with caution for large counts)"""
        if self.config.count > 10_000_000:
            raise ValueError("Use iterator interface for counts > 10M")
        
        all_strings = []
        for batch in self:
            if self.config.use_numpy:
                all_strings.extend(batch)
            else:
                all_strings.extend(batch)
        
        if self.config.use_numpy:
            return np.array(all_strings, dtype=object)
        return all_strings


# Convenience functions
def generate_strings(
    count: int,
    string_type: StringType = StringType.RANDOM_BYTES,
    avg_length: int = 100,
    min_length: int = 10,
    max_length: int = 1000,
    enable_prefix_sharing: bool = False,
    batch_size: int = 10000,
    **kwargs
) -> Iterator[Union[np.ndarray, list]]:
    """
    Convenience function for common use cases.
    
    Example:
        for batch in generate_strings(1_000_000, string_type=StringType.ASCII):
            process_batch(batch)
    """
    config = GeneratorConfig(
        count=count,
        string_type=string_type,
        length=LengthConfig(avg=avg_length, min=min_length, max=max_length),
        prefix=PrefixConfig(enabled=enable_prefix_sharing) if enable_prefix_sharing else None,
        batch_size=batch_size,
        **kwargs
    )
    
    gen = StringGenerator(config)
    return gen


def generate_float_vectors(
    count: int,
    dimension: int,
    batch_size: int = 10000
) -> Iterator[np.ndarray]:
    """
    Generate float32 vectors for vector search testing.
    
    Example:
        for batch in generate_float_vectors(1_000_000, dimension=768):
            # Each item in batch is a 768-dimensional float32 vector as bytes
            process_vectors(batch)
    """
    config = GeneratorConfig(
        count=count,
        string_type=StringType.FLOAT32_VECTOR,
        vector_dim=dimension,
        batch_size=batch_size
    )
    
    gen = StringGenerator(config)
    return gen


if __name__ == "__main__":
    # Example: Generate 1M strings with shared prefixes
    config = GeneratorConfig(
        count=1_000_000,
        string_type=StringType.ASCII,
        length=LengthConfig(
            avg=100,
            min=50,
            max=200,
            distribution=Distribution.NORMAL,
            std_dev=25
        ),
        prefix=PrefixConfig(
            enabled=True,
            min_shared=5,
            max_shared=20,
            share_probability=0.3,
            prefix_pool_size=50
        ),
        batch_size=50000,
        seed=42
    )
    
    gen = StringGenerator(config)
    
    total = 0
    for i, batch in enumerate(gen):
        total += len(batch)
        if i == 0:
            print(f"First batch size: {len(batch)}")
            print(f"Sample strings (first 3):")
            for j in range(min(3, len(batch))):
                s = batch[j]
                print(f"  [{j}] len={len(s)}, preview={s[:50]}...")
    
    print(f"Total strings generated: {total}")