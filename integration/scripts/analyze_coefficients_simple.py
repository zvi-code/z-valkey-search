#!/usr/bin/env python3
"""
Simple coefficient analysis without sklearn dependency.
"""

import pandas as pd
import numpy as np
import json
import sys

def simple_linear_regression(x, y):
    """Simple linear regression using numpy"""
    n = len(x)
    x_mean = np.mean(x)
    y_mean = np.mean(y)
    
    # Calculate slope and intercept
    numerator = np.sum((x - x_mean) * (y - y_mean))
    denominator = np.sum((x - x_mean) ** 2)
    
    if denominator == 0:
        return 0, y_mean, 0
    
    slope = numerator / denominator
    intercept = y_mean - slope * x_mean
    
    # Calculate R²
    y_pred = slope * x + intercept
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - y_mean) ** 2)
    r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
    
    return slope, intercept, r2

def analyze_tag_coefficients(df):
    """Analyze tag-related coefficients"""
    tag_df = df[df['dimension'] == 'tag_count_impact'].copy()
    
    if len(tag_df) == 0:
        print("No tag dimension data found")
        return {}
    
    X = tag_df['unique_tags'].values
    y = tag_df['memory_used'].values
    
    slope, intercept, r2 = simple_linear_regression(X, y)
    
    print(f"\nTag Analysis:")
    print(f"  Base overhead (intercept): {intercept:,.0f} bytes")
    print(f"  Per unique tag: {slope:.2f} bytes")
    print(f"  R² score: {r2:.4f}")
    
    # Show data points
    print(f"\n  Data points:")
    for _, row in tag_df.iterrows():
        print(f"    {row['unique_tags']} tags -> {row['memory_used']:,} bytes")
    
    return {
        'base_overhead': intercept,
        'per_unique_tag': slope
    }

def analyze_vector_coefficients(df):
    """Analyze vector-related coefficients"""
    vector_df = df[df['dimension'] == 'vector_dimension_impact'].copy()
    
    if len(vector_df) == 0:
        print("\nNo vector dimension data found")
        return {}
    
    X = vector_df['vector_dim'].values
    y = vector_df['memory_used'].values
    
    slope, intercept, r2 = simple_linear_regression(X, y)
    
    print(f"\nVector Analysis:")
    print(f"  Base overhead: {intercept:,.0f} bytes")
    print(f"  Per dimension (total): {slope:.2f} bytes")
    print(f"  R² score: {r2:.4f}")
    
    # Calculate per-key per-dimension cost
    num_keys = vector_df['total_keys'].iloc[0]
    per_key_per_dim = slope / num_keys
    
    print(f"  Per key per dimension: {per_key_per_dim:.4f} bytes")
    print(f"  Expected per key per dim (4 bytes float): 4.0 bytes")
    print(f"  Overhead factor: {per_key_per_dim / 4:.2f}x")
    
    return {
        'vector_base_overhead': intercept,
        'per_vector_dim_total': slope,
        'per_vector_dim_per_key': per_key_per_dim
    }

def estimate_memory_formula(coefficients, num_keys, unique_tags, tag_length, vector_dim):
    """Create memory estimation formula"""
    base = coefficients.get('base_overhead', 0)
    per_tag = coefficients.get('per_unique_tag', 0)
    per_vec_dim = coefficients.get('per_vector_dim_per_key', 0)
    
    estimated = base + (unique_tags * per_tag) + (num_keys * vector_dim * per_vec_dim)
    
    return estimated

def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_coefficients_simple.py <results_csv>")
        sys.exit(1)
    
    # Load results
    df = pd.read_csv(sys.argv[1])
    
    print(f"Loaded {len(df)} measurement results")
    print(f"Dimensions tested: {df['dimension'].unique()}")
    
    # Analyze each dimension
    coefficients = {}
    
    tag_coefs = analyze_tag_coefficients(df)
    coefficients.update(tag_coefs)
    
    vector_coefs = analyze_vector_coefficients(df)
    coefficients.update(vector_coefs)
    
    # Save refined coefficients
    with open('refined_coefficients.json', 'w') as f:
        json.dump(coefficients, f, indent=2)
    
    print(f"\n\nRefined coefficients saved to refined_coefficients.json")
    
    # Example estimation
    if 'base_overhead' in coefficients and 'per_unique_tag' in coefficients:
        print("\nExample memory estimations:")
        test_cases = [
            (10000, 1000, 10, 128),
            (100000, 10000, 20, 256),
            (1000000, 50000, 15, 512)
        ]
        
        for num_keys, unique_tags, tag_length, vector_dim in test_cases:
            est = estimate_memory_formula(coefficients, num_keys, unique_tags, tag_length, vector_dim)
            print(f"  {num_keys:,} keys, {unique_tags:,} tags, {vector_dim}d vectors: {est/1024/1024:.1f} MB")

if __name__ == "__main__":
    main()