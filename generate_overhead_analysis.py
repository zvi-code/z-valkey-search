#!/usr/bin/env python3
"""
Generate visualizations comparing Tag index memory usage vs raw data size
"""

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Configure matplotlib
plt.style.use('seaborn-v0_8-darkgrid')
plt.rcParams['figure.figsize'] = (20, 16)
plt.rcParams['font.size'] = 11

# Data from analysis
scenarios_data = {
    'Short Tags\n(15B avg)': {
        'raw_data_mb': 1150,
        'index_memory_mb': 4622,
        'keys': 10_000_000,
        'tags_per_key': 5,
        'tag_length': 15,
        'overhead_factor': 4.0
    },
    'Medium Tags\n(30B avg)': {
        'raw_data_mb': 3080,
        'index_memory_mb': 7508,
        'keys': 10_000_000,
        'tags_per_key': 10,
        'tag_length': 30,
        'overhead_factor': 2.4
    },
    'Long Tags\n(100B avg)': {
        'raw_data_mb': 20080,
        'index_memory_mb': 13924,
        'keys': 10_000_000,
        'tags_per_key': 20,
        'tag_length': 100,
        'overhead_factor': 0.7
    },
    'Best Case\n(4KB, sharing)': {
        'raw_data_mb': 410160,
        'index_memory_mb': 8252,
        'keys': 10_000_000,
        'tags_per_key': 10,
        'tag_length': 4096,
        'overhead_factor': 0.02
    },
    'Worst Case\n(4KB, no sharing)': {
        'raw_data_mb': 410160,
        'index_memory_mb': 58420,
        'keys': 10_000_000,
        'tags_per_key': 10,
        'tag_length': 4096,
        'overhead_factor': 0.14
    },
    'Many Short\n(100×10B)': {
        'raw_data_mb': 10080,
        'index_memory_mb': 33939,
        'keys': 10_000_000,
        'tags_per_key': 100,
        'tag_length': 10,
        'overhead_factor': 3.4
    }
}

# Create comprehensive visualization
fig, ((ax1, ax2), (ax3, ax4), (ax5, ax6)) = plt.subplots(3, 2, figsize=(20, 18))

# 1. Raw Data vs Index Memory Comparison
scenarios = list(scenarios_data.keys())
raw_data = [scenarios_data[s]['raw_data_mb'] for s in scenarios]
index_memory = [scenarios_data[s]['index_memory_mb'] for s in scenarios]

x = np.arange(len(scenarios))
width = 0.35

bars1 = ax1.bar(x - width/2, raw_data, width, label='Raw Data Size', color='lightblue', alpha=0.7)
bars2 = ax1.bar(x + width/2, index_memory, width, label='Index Memory', color='darkred', alpha=0.7)

ax1.set_ylabel('Memory Usage (MB)')
ax1.set_title('Raw Data Size vs Tag Index Memory Usage', fontsize=14)
ax1.set_xticks(x)
ax1.set_xticklabels(scenarios, rotation=45, ha='right')
ax1.legend()
ax1.set_yscale('log')

# Add value labels
for i, (raw, idx) in enumerate(zip(raw_data, index_memory)):
    if raw > idx:
        ax1.text(i - width/2, raw * 1.1, f'{raw:,.0f}', ha='center', fontsize=9)
        ax1.text(i + width/2, idx * 1.1, f'{idx:,.0f}', ha='center', fontsize=9)
    else:
        ax1.text(i - width/2, raw * 1.1, f'{raw:,.0f}', ha='center', fontsize=9)
        ax1.text(i + width/2, idx * 1.1, f'{idx:,.0f}', ha='center', fontsize=9)

# 2. Overhead Factor Analysis
overhead_factors = [scenarios_data[s]['overhead_factor'] for s in scenarios]
colors = ['red' if x > 2 else 'orange' if x > 1 else 'green' for x in overhead_factors]

bars = ax2.bar(scenarios, overhead_factors, color=colors, alpha=0.7)
ax2.axhline(y=1, color='black', linestyle='--', alpha=0.5, label='1x = Same as raw data')
ax2.set_ylabel('Overhead Factor (Index Memory / Raw Data)')
ax2.set_title('Memory Overhead Factor by Scenario', fontsize=14)
ax2.set_xticklabels(scenarios, rotation=45, ha='right')
ax2.legend()
ax2.set_yscale('log')

# Add value labels
for bar, factor in zip(bars, overhead_factors):
    height = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width()/2., height * 1.1,
             f'{factor:.2f}x', ha='center', va='bottom', fontweight='bold')

# 3. Tag Length vs Overhead Factor
tag_lengths = [scenarios_data[s]['tag_length'] for s in scenarios]
ax3.scatter(tag_lengths, overhead_factors, s=100, alpha=0.7, c=colors)

# Fit trend line
log_lengths = np.log(tag_lengths)
log_factors = np.log(overhead_factors)
z = np.polyfit(log_lengths, log_factors, 1)
p = np.poly1d(z)

x_trend = np.logspace(1, 4, 100)
y_trend = np.exp(p(np.log(x_trend)))
ax3.plot(x_trend, y_trend, '--', color='gray', alpha=0.8, label='Trend line')

ax3.set_xlabel('Average Tag Length (bytes)')
ax3.set_ylabel('Overhead Factor')
ax3.set_title('Overhead Factor vs Tag Length', fontsize=14)
ax3.set_xscale('log')
ax3.set_yscale('log')
ax3.grid(True, alpha=0.3)
ax3.legend()

# Add scenario labels
for i, scenario in enumerate(scenarios):
    ax3.annotate(scenario.split('\n')[0], (tag_lengths[i], overhead_factors[i]),
                xytext=(5, 5), textcoords='offset points', fontsize=9)

# 4. Memory Efficiency by Tag String Size per Key
tag_string_sizes = []
for s in scenarios:
    tags_per_key = scenarios_data[s]['tags_per_key']
    tag_length = scenarios_data[s]['tag_length']
    # Approximate: tags_per_key * tag_length + separators
    tag_string_size = tags_per_key * tag_length + max(0, tags_per_key - 1)
    tag_string_sizes.append(tag_string_size)

ax4.scatter(tag_string_sizes, overhead_factors, s=100, alpha=0.7, c=colors)
ax4.set_xlabel('Tag String Size per Key (bytes)')
ax4.set_ylabel('Overhead Factor')
ax4.set_title('Overhead Factor vs Tag String Size per Key', fontsize=14)
ax4.set_xscale('log')
ax4.set_yscale('log')
ax4.grid(True, alpha=0.3)

# Add 10KB limit line
ax4.axvline(x=10240, color='red', linestyle='--', alpha=0.7, label='Valkey 10KB limit')
ax4.legend()

# Add scenario labels
for i, scenario in enumerate(scenarios):
    ax4.annotate(scenario.split('\n')[0], (tag_string_sizes[i], overhead_factors[i]),
                xytext=(5, 5), textcoords='offset points', fontsize=9)

# 5. Memory Breakdown Stacked Chart
components = ['Raw Data', 'String Interning', 'Patricia Tree', 'Hash Maps', 'Other Overhead']

# Estimate component breakdown for each scenario
breakdown_data = []
for s in scenarios:
    raw = scenarios_data[s]['raw_data_mb']
    total = scenarios_data[s]['index_memory_mb']
    overhead = total - raw
    
    # Rough estimates based on analysis
    string_interning = overhead * 0.15
    patricia_tree = overhead * 0.45
    hash_maps = overhead * 0.35
    other = overhead * 0.05
    
    breakdown_data.append([raw, string_interning, patricia_tree, hash_maps, other])

breakdown_data = np.array(breakdown_data).T

bottom = np.zeros(len(scenarios))
colors_stack = ['lightblue', '#ff9999', '#66b3ff', '#99ff99', '#ffcc99']

for i, component in enumerate(components):
    ax5.bar(scenarios, breakdown_data[i], bottom=bottom, label=component, 
            color=colors_stack[i], alpha=0.8)
    bottom += breakdown_data[i]

ax5.set_ylabel('Memory Usage (MB)')
ax5.set_title('Memory Component Breakdown', fontsize=14)
ax5.set_xticklabels(scenarios, rotation=45, ha='right')
ax5.legend()
ax5.set_yscale('log')

# 6. Efficiency Regions Chart
tag_string_range = np.logspace(1, 5, 1000)  # 10 bytes to 100KB
overhead_curve = []

for size in tag_string_range:
    # Approximate overhead based on observed patterns
    if size < 100:
        # High overhead for small tags
        overhead = 5.0 * (100 / size) ** 0.5
    elif size < 1000:
        # Medium overhead  
        overhead = 3.0 * (1000 / size) ** 0.3
    else:
        # Low overhead for large tags
        overhead = 0.5 * (10000 / size) ** 0.1
    
    overhead_curve.append(max(0.01, overhead))

ax6.plot(tag_string_range, overhead_curve, 'b-', linewidth=3, label='Estimated Overhead')
ax6.fill_between(tag_string_range, overhead_curve, alpha=0.3)

# Mark efficiency regions
ax6.axvspan(10, 500, alpha=0.2, color='red', label='High Overhead (2-5x)')
ax6.axvspan(500, 5000, alpha=0.2, color='yellow', label='Medium Overhead (1-2x)')  
ax6.axvspan(5000, 50000, alpha=0.2, color='green', label='Low Overhead (<1x)')

# Mark 10KB limit
ax6.axvline(x=10240, color='red', linestyle='--', linewidth=2, label='Valkey 10KB limit')

ax6.set_xlabel('Tag String Size per Key (bytes)')
ax6.set_ylabel('Overhead Factor')
ax6.set_title('Memory Efficiency Regions', fontsize=14)
ax6.set_xscale('log')
ax6.set_yscale('log')
ax6.legend()
ax6.grid(True, alpha=0.3)

# Main title
fig.suptitle('Tag Index Memory Analysis: Index Memory vs Raw Data Size\n' +
             '10M Keys Across Various Tag Patterns', fontsize=16, y=0.98)

plt.tight_layout()
plt.savefig('/home/ubuntu/valkey-search/memory_overhead_analysis.png', dpi=300, bbox_inches='tight')

# Create a summary table figure
fig2, ax = plt.subplots(figsize=(16, 10))
ax.axis('tight')
ax.axis('off')

# Prepare table data
table_data = []
headers = ['Scenario', 'Keys', 'Tags/Key', 'Tag Len', 'Tag String/Key', 'Raw Data', 'Index Memory', 'Overhead']

for scenario in scenarios:
    data = scenarios_data[scenario]
    tags_per_key = data['tags_per_key']
    tag_length = data['tag_length']
    tag_string_size = tags_per_key * tag_length + max(0, tags_per_key - 1)
    
    row = [
        scenario.replace('\n', ' '),
        f"{data['keys']:,}",
        str(tags_per_key),
        f"{tag_length}B",
        f"{tag_string_size:,}B",
        f"{data['raw_data_mb']:,} MB",
        f"{data['index_memory_mb']:,} MB",
        f"{data['overhead_factor']:.2f}x"
    ]
    table_data.append(row)

# Create table
table = ax.table(cellText=table_data, colLabels=headers, cellLoc='center', loc='center')
table.auto_set_font_size(False)
table.set_fontsize(11)
table.scale(1.2, 2)

# Color code overhead column
for i in range(len(table_data)):
    overhead = float(table_data[i][-1].replace('x', ''))
    if overhead > 2:
        color = '#ffcccc'  # Light red
    elif overhead > 1:
        color = '#ffffcc'  # Light yellow
    else:
        color = '#ccffcc'  # Light green
    
    table[(i+1, len(headers)-1)].set_facecolor(color)

# Style header
for j in range(len(headers)):
    table[(0, j)].set_facecolor('#cccccc')
    table[(0, j)].set_text_props(weight='bold')

ax.set_title('Tag Index Memory Usage Summary\nComparing Index Memory vs Raw Data Size', 
             fontsize=16, pad=20)

plt.savefig('/home/ubuntu/valkey-search/memory_overhead_table.png', dpi=300, bbox_inches='tight')

print("Generated visualizations:")
print("1. memory_overhead_analysis.png - Comprehensive overhead analysis")  
print("2. memory_overhead_table.png - Summary table with color coding")