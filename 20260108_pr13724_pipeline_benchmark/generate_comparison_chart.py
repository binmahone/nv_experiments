#!/usr/bin/env python3
"""
Generate comparison chart for Baseline vs PR-13724 vs PR-14090.
"""

import matplotlib.pyplot as plt
import numpy as np

def main():
    configs = ['Small', 'Medium', 'Large', 'XLarge', 'XXLarge']
    
    # Query Total Time (seconds) - from benchmark results
    baseline_times = [11.3, 25.1, 59.1, 99.3, 132.9]
    pr13724_times = [11.7, 22.8, 43.5, 55.7, 75.5]
    pr14090_times = [11.7, 23.8, 38.5, 58.4, 67.3]
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = np.arange(len(configs))
    width = 0.25
    
    bars1 = ax.bar(x - width, baseline_times, width, label='Baseline', 
                   color='#4a90d9', edgecolor='black')
    bars2 = ax.bar(x, pr13724_times, width, label='PR-13724', 
                   color='#50c878', edgecolor='black')
    bars3 = ax.bar(x + width, pr14090_times, width, label='PR-14090', 
                   color='#ff7f50', edgecolor='black')
    
    ax.set_xlabel('Configuration', fontsize=12)
    ax.set_ylabel('Query Total Time (seconds)', fontsize=12)
    ax.set_title('RAPIDS Shuffle Writer Performance Comparison', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(configs)
    ax.legend(loc='upper left')
    
    # Add value labels on bars
    for bars in [bars1, bars2, bars3]:
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax.annotate(f'{height:.1f}s',
                           xy=(bar.get_x() + bar.get_width() / 2, height),
                           xytext=(0, 3),
                           textcoords="offset points",
                           ha='center', va='bottom', fontsize=8)
    
    ax.set_ylim(0, max(baseline_times) * 1.15)
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    
    output_path = '/home/hongbin/experiments/20260108_pr13724_pipeline_benchmark/comparison_chart.png'
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"Chart saved to: {output_path}")

if __name__ == "__main__":
    main()
