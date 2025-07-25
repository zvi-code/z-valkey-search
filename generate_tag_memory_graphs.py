#!/usr/bin/env python3
"""
Generate graphs showing Tag index memory usage across different dimensions
"""

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.gridspec import GridSpec

# Configure matplotlib for better output
plt.style.use('seaborn-v0_8-darkgrid')
plt.rcParams['figure.figsize'] = (16, 12)
plt.rcParams['font.size'] = 10

def calculate_memory_mb(num_keys, avg_tag_length, tags_per_key, prefix_sharing, keys_per_tag=None):
    """Calculate memory usage in MB based on parameters"""
    # Estimate unique counts based on sharing
    unique_tag_ratio = (1 - prefix_sharing) * 0.8
    
    # If keys_per_tag is specified, calculate unique tags differently
    if keys_per_tag:
        unique_tags = num_keys * tags_per_key / keys_per_tag
    else:
        unique_tags = num_keys * tags_per_key * unique_tag_ratio
    
    # Unique string ratio depends on overlap
    unique_string_ratio = 0.8 + (1 - prefix_sharing) * 0.18
    unique_strings = num_keys * unique_string_ratio
    
    # Memory components in bytes
    interned_keys_data = num_keys * 8  # avg 8 bytes per key
    interned_keys_overhead = num_keys * 40
    
    interned_tags_data = unique_strings * avg_tag_length
    interned_tags_overhead = unique_strings * 40
    
    tracked_map_entries = num_keys * 48
    taginfo_base = num_keys * 48
    tag_sets = num_keys * tags_per_key * 24
    
    patricia_leaf_nodes = unique_tags * 104
    patricia_values = unique_tags * (num_keys / unique_tags) * 24
    patricia_internal = unique_tags * 10  # rough estimate
    
    intern_store = (num_keys + unique_strings) * 32
    
    total_bytes = (
        interned_keys_data + interned_keys_overhead +
        interned_tags_data + interned_tags_overhead +
        tracked_map_entries + taginfo_base + tag_sets +
        patricia_leaf_nodes + patricia_values + patricia_internal +
        intern_store
    )
    
    return total_bytes / (1024 * 1024)  # Convert to MB

# Create figure with multiple subplots
fig = plt.figure(figsize=(20, 16))
gs = GridSpec(3, 3, figure=fig, hspace=0.3, wspace=0.3)

# 1. Impact of Tag Length
ax1 = fig.add_subplot(gs[0, 0])
tag_lengths = np.arange(10, 5000, 100)
memory_low_share = [calculate_memory_mb(10_000_000, tl, 10, 0.1) for tl in tag_lengths]
memory_med_share = [calculate_memory_mb(10_000_000, tl, 10, 0.5) for tl in tag_lengths]
memory_high_share = [calculate_memory_mb(10_000_000, tl, 10, 0.9) for tl in tag_lengths]

ax1.plot(tag_lengths, memory_low_share, 'r-', label='Low sharing (10%)', linewidth=2)
ax1.plot(tag_lengths, memory_med_share, 'b-', label='Medium sharing (50%)', linewidth=2)
ax1.plot(tag_lengths, memory_high_share, 'g-', label='High sharing (90%)', linewidth=2)
ax1.set_xlabel('Average Tag Length (bytes)')
ax1.set_ylabel('Memory Usage (MB)')
ax1.set_title('Memory vs Tag Length (10M keys, 10 tags/key)')
ax1.legend()
ax1.grid(True, alpha=0.3)

# 2. Impact of Tags per Key
ax2 = fig.add_subplot(gs[0, 1])
tags_per_key = np.arange(1, 101, 2)
memory_short = [calculate_memory_mb(10_000_000, 20, tpk, 0.5) for tpk in tags_per_key]
memory_med = [calculate_memory_mb(10_000_000, 100, tpk, 0.5) for tpk in tags_per_key]
memory_long = [calculate_memory_mb(10_000_000, 1000, tpk, 0.5) for tpk in tags_per_key]

ax2.plot(tags_per_key, memory_short, 'g-', label='Short tags (20B)', linewidth=2)
ax2.plot(tags_per_key, memory_med, 'b-', label='Medium tags (100B)', linewidth=2)
ax2.plot(tags_per_key, memory_long, 'r-', label='Long tags (1KB)', linewidth=2)
ax2.set_xlabel('Tags per Key')
ax2.set_ylabel('Memory Usage (MB)')
ax2.set_title('Memory vs Tags per Key (10M keys, 50% sharing)')
ax2.legend()
ax2.grid(True, alpha=0.3)

# 3. Impact of Prefix Sharing
ax3 = fig.add_subplot(gs[0, 2])
sharing_rates = np.arange(0, 1.01, 0.05)
memory_small = [calculate_memory_mb(10_000_000, 50, 5, sr) for sr in sharing_rates]
memory_medium = [calculate_memory_mb(10_000_000, 100, 10, sr) for sr in sharing_rates]
memory_large = [calculate_memory_mb(10_000_000, 500, 20, sr) for sr in sharing_rates]

ax3.plot(sharing_rates * 100, memory_small, 'g-', label='Small (50B, 5 tags)', linewidth=2)
ax3.plot(sharing_rates * 100, memory_medium, 'b-', label='Medium (100B, 10 tags)', linewidth=2)
ax3.plot(sharing_rates * 100, memory_large, 'r-', label='Large (500B, 20 tags)', linewidth=2)
ax3.set_xlabel('Prefix Sharing Rate (%)')
ax3.set_ylabel('Memory Usage (MB)')
ax3.set_title('Memory vs Prefix Sharing (10M keys)')
ax3.legend()
ax3.grid(True, alpha=0.3)

# 4. Extreme Cases Comparison
ax4 = fig.add_subplot(gs[1, :2])
scenarios = ['Best Case\n(4KB, 99% share)', 'Worst Case\n(4KB, 0% share)', 
             'Single Tag\n(4KB, 95% share)', 'Many Short\n(10B, 5% share)',
             'Typical\n(30B, 50% share)']
memory_values = [8252, 58420, 6398, 33939, 7508]
colors = ['green', 'red', 'blue', 'orange', 'gray']

bars = ax4.bar(scenarios, memory_values, color=colors, alpha=0.7)
ax4.set_ylabel('Memory Usage (MB)')
ax4.set_title('Memory Usage: Extreme Cases vs Typical (10M keys)')
ax4.set_ylim(0, max(memory_values) * 1.1)

# Add value labels on bars
for bar, value in zip(bars, memory_values):
    height = bar.get_height()
    ax4.text(bar.get_x() + bar.get_width()/2., height,
             f'{value:,.0f} MB', ha='center', va='bottom')

# 5. Memory per Key Analysis
ax5 = fig.add_subplot(gs[1, 2])
memory_per_key = [m / 10_000_000 * 1024 for m in memory_values]  # Convert to bytes per key
bars2 = ax5.bar(scenarios, memory_per_key, color=colors, alpha=0.7)
ax5.set_ylabel('Memory per Key (bytes)')
ax5.set_title('Memory per Key: Extreme Cases')
ax5.set_ylim(0, max(memory_per_key) * 1.1)

for bar, value in zip(bars2, memory_per_key):
    height = bar.get_height()
    ax5.text(bar.get_x() + bar.get_width()/2., height,
             f'{value:,.0f} B', ha='center', va='bottom', rotation=45)

# 6. 3D Surface Plot: Tag Length vs Sharing Rate
ax6 = fig.add_subplot(gs[2, 0], projection='3d')
tag_lens = np.linspace(10, 1000, 20)
share_rates = np.linspace(0, 1, 20)
TL, SR = np.meshgrid(tag_lens, share_rates)
Z = np.array([[calculate_memory_mb(10_000_000, tl, 10, sr) 
               for tl in tag_lens] for sr in share_rates])

surf = ax6.plot_surface(TL, SR * 100, Z, cmap='viridis', alpha=0.8)
ax6.set_xlabel('Tag Length (bytes)')
ax6.set_ylabel('Sharing Rate (%)')
ax6.set_zlabel('Memory (MB)')
ax6.set_title('Memory Usage Surface (10M keys, 10 tags/key)')
fig.colorbar(surf, ax=ax6, shrink=0.5, aspect=5)

# 7. Keys per Tag Impact
ax7 = fig.add_subplot(gs[2, 1])
keys_per_tag_values = [1, 10, 100, 1000, 10000, 100000, 1000000]
memory_1tag = [calculate_memory_mb(10_000_000, 100, 1, 0.5, kpt) for kpt in keys_per_tag_values]
memory_10tags = [calculate_memory_mb(10_000_000, 100, 10, 0.5, kpt) for kpt in keys_per_tag_values]

ax7.semilogx(keys_per_tag_values, memory_1tag, 'b-o', label='1 tag/key', linewidth=2)
ax7.semilogx(keys_per_tag_values, memory_10tags, 'r-o', label='10 tags/key', linewidth=2)
ax7.set_xlabel('Keys per Unique Tag (log scale)')
ax7.set_ylabel('Memory Usage (MB)')
ax7.set_title('Memory vs Tag Reuse (10M keys, 100B tags)')
ax7.legend()
ax7.grid(True, alpha=0.3)

# 8. Memory Breakdown Pie Chart
ax8 = fig.add_subplot(gs[2, 2])
# Typical scenario breakdown
components = ['Interned Keys', 'Interned Tags', 'Hash Maps', 'Tag Sets', 'Patricia Tree', 'Other']
sizes = [480, 630, 960, 2400, 2708, 330]  # MB from Scenario 2
colors_pie = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99', '#ff99cc', '#c2c2f0']

wedges, texts, autotexts = ax8.pie(sizes, labels=components, colors=colors_pie, 
                                    autopct='%1.1f%%', startangle=90)
ax8.set_title('Memory Breakdown: Typical Scenario')

# Main title
fig.suptitle('Tag Index Memory Usage Analysis - 10M Keys', fontsize=16, y=0.98)

# Save the figure
plt.tight_layout()
plt.savefig('/home/ubuntu/valkey-search/tag_memory_analysis.png', dpi=300, bbox_inches='tight')
plt.savefig('/home/ubuntu/valkey-search/tag_memory_analysis.pdf', bbox_inches='tight')

# Generate additional detailed analysis graphs
fig2, ((ax9, ax10), (ax11, ax12)) = plt.subplots(2, 2, figsize=(16, 12))

# 9. Scaling with Number of Keys
ax9.set_title('Memory Scaling with Database Size')
key_counts = np.logspace(3, 8, 20)  # 1K to 100M keys
for tag_len, color in [(20, 'g'), (100, 'b'), (1000, 'r')]:
    memory = [calculate_memory_mb(n, tag_len, 10, 0.5) for n in key_counts]
    ax9.loglog(key_counts, memory, f'{color}-', label=f'{tag_len}B tags', linewidth=2)
ax9.set_xlabel('Number of Keys')
ax9.set_ylabel('Memory Usage (MB)')
ax9.legend()
ax9.grid(True, alpha=0.3)

# 10. Extreme Tag Lengths
ax10.set_title('Memory with Extreme Tag Lengths')
extreme_lengths = [10, 50, 100, 500, 1000, 2000, 4096, 8192]
sharing_scenarios = {'No sharing (0%)': 0.0, 'Low (25%)': 0.25, 
                    'Medium (50%)': 0.5, 'High (75%)': 0.75, 'Max (95%)': 0.95}
for label, sharing in sharing_scenarios.items():
    memory = [calculate_memory_mb(1_000_000, tl, 5, sharing) for tl in extreme_lengths]
    ax10.semilogy(extreme_lengths, memory, '-o', label=label, linewidth=2)
ax10.set_xlabel('Tag Length (bytes)')
ax10.set_ylabel('Memory Usage (MB, log scale)')
ax10.legend()
ax10.grid(True, alpha=0.3)

# 11. Cost Analysis (Memory per Document)
ax11.set_title('Memory Cost per Document')
tag_configs = [(5, 20), (10, 50), (20, 100), (50, 200), (100, 500)]
sharing_rates = np.arange(0, 1.01, 0.1)
for tags, length in tag_configs:
    cost_per_doc = [calculate_memory_mb(1_000_000, length, tags, sr) / 1000 
                    for sr in sharing_rates]  # KB per doc
    ax11.plot(sharing_rates * 100, cost_per_doc, '-o', 
              label=f'{tags} tags @ {length}B', linewidth=2)
ax11.set_xlabel('Prefix Sharing Rate (%)')
ax11.set_ylabel('Memory per Document (KB)')
ax11.legend()
ax11.grid(True, alpha=0.3)

# 12. Optimization Opportunities
ax12.set_title('Memory Optimization Potential')
categories = ['Baseline', 'String\nInterning', '+Prefix\nSharing', '+Tag\nDedup', 'Optimal']
memory_saved = [0, 15, 35, 45, 55]  # Cumulative % saved
remaining = [100, 85, 65, 55, 45]

x = np.arange(len(categories))
width = 0.6

p1 = ax12.bar(x, remaining, width, label='Memory Used', color='lightcoral')
p2 = ax12.bar(x, memory_saved, width, bottom=remaining, label='Memory Saved', color='lightgreen')

ax12.set_ylabel('Memory Usage (%)')
ax12.set_xticks(x)
ax12.set_xticklabels(categories)
ax12.legend()
ax12.set_ylim(0, 110)

# Add percentage labels
for i, (r, s) in enumerate(zip(remaining, memory_saved)):
    if s > 0:
        ax12.text(i, r + s/2, f'{s}%', ha='center', va='center')

fig2.suptitle('Tag Index Memory Analysis - Detailed Views', fontsize=14)
plt.tight_layout()
plt.savefig('/home/ubuntu/valkey-search/tag_memory_detailed.png', dpi=300, bbox_inches='tight')

print("Graphs generated successfully!")
print("- tag_memory_analysis.png: Main analysis with 8 graphs")
print("- tag_memory_detailed.png: Additional detailed views")