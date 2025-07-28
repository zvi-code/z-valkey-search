#!/usr/bin/env python3
"""
Tags builder for Valkey keys, built on top of string_generator.py.
Generates comma-separated tag strings with configurable sharing properties.
"""

import numpy as np
from typing import Iterator, Optional, List, Dict, Set, Union
from dataclasses import dataclass
from enum import Enum
import random
from collections import defaultdict

from integration.utils.string_generator import (
    StringGenerator, GeneratorConfig, LengthConfig, PrefixConfig,
    Distribution, StringType
)


class TagSharingMode(Enum):
    """Modes for how tags are shared between keys"""
    UNIQUE = "unique"  # Each key has unique tags (1:1)
    SHARED_POOL = "shared_pool"  # Tags drawn from a shared pool
    PERFECT_OVERLAP = "perfect_overlap"  # All keys have all tags
    GROUP_BASED = "group_based"  # Keys in same group share tags


@dataclass
class TagDistribution:
    """Configuration for number of tags per key"""
    avg: int = 3
    min: int = 1
    max: int = 10
    distribution: Distribution = Distribution.UNIFORM


@dataclass
class TagSharingConfig:
    """Configuration for how tags are shared between keys"""
    mode: TagSharingMode = TagSharingMode.UNIQUE
    # For SHARED_POOL mode
    pool_size: Optional[int] = None  # Total unique tags in pool
    reuse_probability: float = 0.5  # Probability of reusing existing tag
    # For GROUP_BASED mode
    keys_per_group: Optional[int] = None  # Keys sharing same tag set
    tags_per_group: Optional[int] = None  # Unique tags per group


@dataclass
class TagsConfig:
    """Main configuration for tags generation"""
    num_keys: int
    tags_per_key: TagDistribution = None
    tag_length: LengthConfig = None
    tag_prefix: Optional[PrefixConfig] = None  # For prefix queries
    sharing: TagSharingConfig = None
    # String generation config
    tag_string_type: StringType = StringType.ALPHANUMERIC
    tag_charset: Optional[str] = None
    batch_size: int = 10000
    seed: Optional[int] = None


class TagsBuilder:
    """
    Builds comma-separated tag strings for Valkey keys.
    
    Example usage:
        # Unique tags per key
        config = TagsConfig(
            num_keys=1000,
            tags_per_key=TagDistribution(avg=3),
            tag_length=LengthConfig(avg=10, min=5, max=15),
            sharing=TagSharingConfig(mode=TagSharingMode.UNIQUE)
        )
        
        builder = TagsBuilder(config)
        for key_tags_batch in builder:
            # Each item is a comma-separated tag string
            pass
    """
    
    def __init__(self, config: TagsConfig):
        self.config = config
        self._validate_config()
        
        if config.seed:
            np.random.seed(config.seed)
            random.seed(config.seed)
        
        # Initialize tag generation strategy based on sharing mode
        self._tag_pool = []
        self._tag_generator = None
        self._group_tags = {}
        self._keys_generated = 0
        
        self._initialize_tag_generation()
    
    def _validate_config(self):
        """Validate configuration parameters"""
        if self.config.num_keys <= 0:
            raise ValueError("num_keys must be positive")
        
        # Set defaults if not provided
        if self.config.tags_per_key is None:
            self.config.tags_per_key = TagDistribution()
        
        if self.config.tag_length is None:
            self.config.tag_length = LengthConfig(avg=10, min=5, max=20)
        
        if self.config.sharing is None:
            self.config.sharing = TagSharingConfig()
        
        # Validate sharing config
        sharing = self.config.sharing
        if sharing.mode == TagSharingMode.SHARED_POOL:
            if sharing.pool_size is None:
                # Default: 10% of total possible tags
                sharing.pool_size = max(10, self.config.num_keys // 10)
        
        if sharing.mode == TagSharingMode.GROUP_BASED:
            if sharing.keys_per_group is None:
                sharing.keys_per_group = 100
            if sharing.tags_per_group is None:
                sharing.tags_per_group = 10
    
    def _initialize_tag_generation(self):
        """Initialize tag generation based on sharing mode"""
        sharing = self.config.sharing
        
        if sharing.mode == TagSharingMode.UNIQUE:
            # Each key gets unique tags - need num_keys * avg_tags_per_key tags
            total_tags = self.config.num_keys * self.config.tags_per_key.avg
            self._tag_generator = self._create_tag_generator(total_tags)
            
        elif sharing.mode == TagSharingMode.SHARED_POOL:
            # Pre-generate pool of tags
            self._tag_generator = self._create_tag_generator(sharing.pool_size)
            self._tag_pool = list(self._generate_all_tags(self._tag_generator))
            
        elif sharing.mode == TagSharingMode.PERFECT_OVERLAP:
            # Generate fixed set of tags once
            self._tag_generator = self._create_tag_generator(self.config.tags_per_key.avg)
            self._tag_pool = list(self._generate_all_tags(self._tag_generator))
            
        elif sharing.mode == TagSharingMode.GROUP_BASED:
            # Pre-generate tags for each group
            num_groups = (self.config.num_keys + sharing.keys_per_group - 1) // sharing.keys_per_group
            for group_id in range(num_groups):
                group_gen = self._create_tag_generator(sharing.tags_per_group)
                self._group_tags[group_id] = list(self._generate_all_tags(group_gen))
    
    def _create_tag_generator(self, count: int) -> StringGenerator:
        """Create a string generator for tags"""
        gen_config = GeneratorConfig(
            count=count,
            string_type=self.config.tag_string_type,
            length=self.config.tag_length,
            prefix=self.config.tag_prefix,
            charset=self.config.tag_charset,
            batch_size=min(count, 10000),
            seed=None  # Use current random state
        )
        return StringGenerator(gen_config)
    
    def _generate_all_tags(self, generator: StringGenerator) -> List[str]:
        """Generate all tags from a generator"""
        tags = []
        for batch in generator:
            for tag_bytes in batch:
                # Convert bytes to string
                if self.config.tag_string_type == StringType.FLOAT32_VECTOR:
                    # Keep as hex for float vectors
                    tag_str = tag_bytes.hex()
                else:
                    try:
                        tag_str = tag_bytes.decode('utf-8')
                    except:
                        tag_str = tag_bytes.hex()
                tags.append(tag_str)
        return tags
    
    def _get_tags_for_key(self, key_index: int) -> List[str]:
        """Get tags for a specific key based on sharing mode"""
        sharing = self.config.sharing
        
        # Determine number of tags for this key
        num_tags = self._sample_tag_count()
        
        if sharing.mode == TagSharingMode.UNIQUE:
            # Generate unique tags on the fly
            if self._tag_generator is None:
                self._tag_generator = self._create_tag_generator(num_tags)
            else:
                # Continue with existing generator
                pass
            
            tags = []
            generated = 0
            for batch in self._tag_generator:
                for tag_bytes in batch:
                    if self.config.tag_string_type == StringType.FLOAT32_VECTOR:
                        tag_str = tag_bytes.hex()
                    else:
                        try:
                            tag_str = tag_bytes.decode('utf-8')
                        except:
                            tag_str = tag_bytes.hex()
                    tags.append(tag_str)
                    generated += 1
                    if generated >= num_tags:
                        return tags
            return tags
            
        elif sharing.mode == TagSharingMode.SHARED_POOL:
            # Sample from pool with reuse probability
            tags = set()
            while len(tags) < num_tags and len(tags) < len(self._tag_pool):
                if random.random() < sharing.reuse_probability and len(tags) > 0:
                    # Reuse existing tag
                    tag = random.choice(list(tags))
                else:
                    # Pick from pool
                    tag = random.choice(self._tag_pool)
                tags.add(tag)
            return list(tags)
            
        elif sharing.mode == TagSharingMode.PERFECT_OVERLAP:
            # All keys get all tags
            return self._tag_pool[:num_tags]
            
        elif sharing.mode == TagSharingMode.GROUP_BASED:
            # Determine group for this key
            group_id = key_index // sharing.keys_per_group
            group_tags = self._group_tags.get(group_id, [])
            # Sample tags from group
            if len(group_tags) <= num_tags:
                return group_tags
            return random.sample(group_tags, num_tags)
    
    def _sample_tag_count(self) -> int:
        """Sample number of tags based on distribution"""
        dist = self.config.tags_per_key
        
        if dist.distribution == Distribution.UNIFORM:
            return random.randint(dist.min, dist.max)
        elif dist.distribution == Distribution.NORMAL:
            std_dev = (dist.max - dist.min) / 6
            count = int(np.random.normal(dist.avg, std_dev))
            return max(dist.min, min(dist.max, count))
        elif dist.distribution == Distribution.EXPONENTIAL:
            scale = dist.avg - dist.min
            count = int(np.random.exponential(scale) + dist.min)
            return max(dist.min, min(dist.max, count))
        else:
            # Default to average
            return dist.avg
    
    def __iter__(self) -> Iterator[List[str]]:
        """Iterator interface returning batches of tag strings"""
        keys_generated = 0
        
        # For UNIQUE mode, create a continuous generator
        if self.config.sharing.mode == TagSharingMode.UNIQUE:
            total_tags_needed = self.config.num_keys * self.config.tags_per_key.avg
            self._tag_generator = self._create_tag_generator(total_tags_needed * 2)  # Buffer
            self._tag_iterator = iter(self._tag_generator)
            self._tag_buffer = []
        
        while keys_generated < self.config.num_keys:
            batch_size = min(self.config.batch_size, self.config.num_keys - keys_generated)
            batch = []
            
            for i in range(batch_size):
                key_index = keys_generated + i
                
                if self.config.sharing.mode == TagSharingMode.UNIQUE:
                    # Get unique tags from continuous generation
                    tags = self._get_unique_tags_continuous()
                else:
                    tags = self._get_tags_for_key(key_index)
                
                # Join tags with comma
                tag_string = ','.join(tags)
                batch.append(tag_string)
            
            keys_generated += batch_size
            yield batch
    
    def _get_unique_tags_continuous(self) -> List[str]:
        """Get unique tags from continuous generator"""
        num_tags = self._sample_tag_count()
        tags = []
        
        # Fill buffer if needed
        while len(self._tag_buffer) < num_tags:
            try:
                batch = next(self._tag_iterator)
                for tag_bytes in batch:
                    if self.config.tag_string_type == StringType.FLOAT32_VECTOR:
                        tag_str = tag_bytes.hex()
                    else:
                        try:
                            tag_str = tag_bytes.decode('utf-8')
                        except:
                            tag_str = tag_bytes.hex()
                    self._tag_buffer.append(tag_str)
            except StopIteration:
                # Need more tags
                additional = num_tags * 100  # Generate in larger batches
                self._tag_generator = self._create_tag_generator(additional)
                self._tag_iterator = iter(self._tag_generator)
        
        # Take tags from buffer
        tags = self._tag_buffer[:num_tags]
        self._tag_buffer = self._tag_buffer[num_tags:]
        
        return tags