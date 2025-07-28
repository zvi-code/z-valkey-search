#!/usr/bin/env python3
"""
Analyze coefficient results and perform more sophisticated regression analysis.
"""

import pandas as pd
import numpy as np
import json
import sys
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
import seaborn as sns

def analyze_tag_coefficients(df):
    """Analyze tag-related coefficients with multiple regression"""
    tag_df = df[df['dimension'] == 'tag_count_impact'].copy()
    
    # Calculate base overhead (intercept)
    X = tag_df['unique_tags'].values.reshape(-1, 1)
    y = tag_df['memory_used'].values
    
    model = LinearRegression()
    model.fit(X, y)
    
    base_overhead = model.intercept_
    per_tag_coef = model.coef_[0]
    
    print(f"\nTag Analysis:")
    print(f"  Base overhead: {base_overhead:,.0f} bytes")
    print(f"  Per unique tag: {per_tag_coef:.2f} bytes")
    print(f"  R² score: {model.score(X, y):.4f}")
    
    # Plot the relationship
    plt.figure(figsize=(10, 6))
    plt.scatter(X, y, alpha=0.6)
    plt.plot(X, model.predict(X), 'r-', linewidth=2)
    plt.xlabel('Number of Unique Tags')
    plt.ylabel('Memory Used (bytes)')
    plt.title('Memory Usage vs Unique Tags')
    plt.grid(True, alpha=0.3)
    plt.savefig('tag_memory_analysis.png')
    plt.close()
    
    return {
        'base_overhead': base_overhead,
        'per_unique_tag': per_tag_coef
    }

def analyze_vector_coefficients(df):
    """Analyze vector-related coefficients"""
    vector_df = df[df['dimension'] == 'vector_dimension_impact'].copy()
    
    if len(vector_df) == 0:
        return {}
    
    X = vector_df['vector_dim'].values.reshape(-1, 1)
    y = vector_df['memory_used'].values
    
    model = LinearRegression()
    model.fit(X, y)
    
    base_overhead = model.intercept_
    per_dim_coef = model.coef_[0]
    
    print(f"\nVector Analysis:")
    print(f"  Base overhead: {base_overhead:,.0f} bytes")
    print(f"  Per dimension: {per_dim_coef:.2f} bytes per key")
    print(f"  R² score: {model.score(X, y):.4f}")
    
    # Calculate per-key per-dimension cost
    num_keys = vector_df['total_keys'].iloc[0]
    per_key_per_dim = per_dim_coef / num_keys
    
    print(f"  Per key per dimension: {per_key_per_dim:.4f} bytes")
    
    return {
        'vector_base_overhead': base_overhead,
        'per_vector_dim_total': per_dim_coef,
        'per_vector_dim_per_key': per_key_per_dim
    }

def analyze_combined_model(df):
    """Build a combined model considering all factors"""
    # Prepare features
    features = []
    feature_names = []
    
    if 'unique_tags' in df.columns:
        features.append(df['unique_tags'].values)
        feature_names.append('unique_tags')
    
    if 'avg_tag_length' in df.columns:
        features.append(df['avg_tag_length'].values)
        feature_names.append('avg_tag_length')
        
    if 'vector_dim' in df.columns:
        features.append(df['vector_dim'].values)
        feature_names.append('vector_dim')
        
    if 'num_numeric_fields' in df.columns:
        features.append(df['num_numeric_fields'].values)
        feature_names.append('num_numeric_fields')
    
    # Add interaction terms
    if 'total_keys' in df.columns and 'vector_dim' in df.columns:
        features.append(df['total_keys'].values * df['vector_dim'].values)
        feature_names.append('keys_x_vector_dim')
    
    if len(features) == 0:
        print("No features found for combined model")
        return {}
    
    X = np.column_stack(features)
    y = df['memory_used'].values
    
    model = LinearRegression()
    model.fit(X, y)
    
    print(f"\nCombined Model Analysis:")
    print(f"  Intercept: {model.intercept_:,.0f} bytes")
    print(f"  R² score: {model.score(X, y):.4f}")
    print(f"\n  Coefficients:")
    
    coefficients = {}
    for name, coef in zip(feature_names, model.coef_):
        print(f"    {name}: {coef:.4f}")
        coefficients[name] = coef
    
    return coefficients

def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_coefficients.py <results_csv>")
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
    
    # Combined model
    combined_coefs = analyze_combined_model(df)
    coefficients.update(combined_coefs)
    
    # Save refined coefficients
    with open('refined_coefficients.json', 'w') as f:
        json.dump(coefficients, f, indent=2)
    
    print(f"\n\nRefined coefficients saved to refined_coefficients.json")

if __name__ == "__main__":
    main()