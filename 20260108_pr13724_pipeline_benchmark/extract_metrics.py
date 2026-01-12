#!/usr/bin/env python3
"""
Extract metrics from Spark event logs for PR-13724 vs Baseline comparison.

Metrics extracted:
1. Mapper stage time (Stage with shuffle write)
2. GpuScan parquet - output columnar batches (average per mapper task)
3. GpuColumnarExchange - shuffle bytes written (average per mapper task)
"""

import json
import sys
import os
from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np

# Config name mapping
CONFIG_NAMES = {
    '16p16g': 'Small',
    '16p32g': 'Medium',
    '16p48g': 'Large',
    '16p64g': 'XLarge',
    '16p80g': 'XXLarge'
}

def safe_int(value):
    """Safely convert value to int, handling strings and None"""
    if value is None:
        return 0
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return int(value)

def extract_metrics(event_log_path):
    """Extract required metrics from event log."""
    
    # Store results
    stage_times = {}  # stage_id -> duration_ms
    stage_task_counts = defaultdict(int)  # stage_id -> task count
    stage_shuffle_write = defaultdict(int)  # stage_id -> shuffle bytes written
    
    # Metrics by stage
    metrics_by_stage = defaultdict(lambda: defaultdict(int))
    
    # Track mapper stages (stages that have shuffle write)
    mapper_stages = set()
    
    # First pass: identify stages and collect basic info
    with open(event_log_path, 'r') as f:
        for line in f:
            try:
                event = json.loads(line.strip())
                event_type = event.get('Event', '')
                
                # Track stage info from StageCompleted
                if event_type == 'SparkListenerStageCompleted':
                    stage_info = event.get('Stage Info', {})
                    stage_id = stage_info.get('Stage ID')
                    
                    # Get stage duration
                    submission_time = safe_int(stage_info.get('Submission Time', 0))
                    completion_time = safe_int(stage_info.get('Completion Time', 0))
                    if completion_time > submission_time:
                        stage_times[stage_id] = completion_time - submission_time
                    
                    # Extract accumulables (metrics)
                    accumulables = stage_info.get('Accumulables', [])
                    for acc in accumulables:
                        name = acc.get('Name', '')
                        value = safe_int(acc.get('Value', 0))
                        
                        # Check for shuffle write (indicates mapper stage)
                        if 'shuffle bytes written' in name.lower():
                            mapper_stages.add(stage_id)
                            metrics_by_stage[stage_id]['shuffle_bytes_written'] += value
                        
                        # GpuScan output columnar batches
                        if 'output columnar batches' in name.lower():
                            metrics_by_stage[stage_id]['output_columnar_batches'] += value
                        
                        # output rows
                        if name.lower() == 'output rows':
                            metrics_by_stage[stage_id]['output_rows'] += value
                        
                        # Number of tasks
                        if 'number of tasks' in name.lower():
                            metrics_by_stage[stage_id]['num_tasks'] = value
                
                # Count tasks per stage
                elif event_type == 'SparkListenerTaskEnd':
                    stage_id = event.get('Stage ID')
                    task_end_reason = event.get('Task End Reason', {})
                    if task_end_reason.get('Reason') == 'Success':
                        stage_task_counts[stage_id] += 1
                        
            except json.JSONDecodeError:
                continue
            except Exception as e:
                continue
    
    return {
        'stage_times': stage_times,
        'stage_task_counts': dict(stage_task_counts),
        'mapper_stages': mapper_stages,
        'metrics_by_stage': dict(metrics_by_stage)
    }

def format_bytes(bytes_val):
    """Format bytes to human readable string."""
    if bytes_val >= 1024**3:
        return f"{bytes_val / 1024**3:.2f} GB"
    elif bytes_val >= 1024**2:
        return f"{bytes_val / 1024**2:.2f} MB"
    elif bytes_val >= 1024:
        return f"{bytes_val / 1024:.2f} KB"
    return f"{bytes_val} B"

def analyze_event_log(event_log_path, test_name):
    """Analyze a single event log and return summary."""
    if not os.path.exists(event_log_path):
        return None
    
    data = extract_metrics(event_log_path)
    
    # Find the timed run mapper stages (skip warmup - usually stage 4)
    # Timed runs are typically stage 8, 12, 16 (or similar pattern)
    mapper_stages = sorted(data['mapper_stages'])
    
    # Skip first mapper stage (warmup), use the rest (timed runs)
    timed_mapper_stages = [s for s in mapper_stages if s >= 4][1:]  # Skip warmup
    
    results = {
        'test_name': test_name,
        'mapper_stages': [],
        'total_mapper_time_ms': 0,
        'total_batches': 0,
        'total_shuffle_bytes': 0,
        'total_tasks': 0
    }
    
    for stage_id in timed_mapper_stages[:3]:  # Take up to 3 timed runs
        stage_time = data['stage_times'].get(stage_id, 0)
        metrics = data['metrics_by_stage'].get(stage_id, {})
        task_count = data['stage_task_counts'].get(stage_id, 0) or metrics.get('num_tasks', 16)
        
        batches = metrics.get('output_columnar_batches', 0)
        shuffle_bytes = metrics.get('shuffle_bytes_written', 0)
        
        results['mapper_stages'].append({
            'stage_id': stage_id,
            'time_ms': stage_time,
            'tasks': task_count,
            'batches': batches,
            'shuffle_bytes': shuffle_bytes,
            'avg_batches_per_task': batches / task_count if task_count > 0 else 0,
            'avg_shuffle_bytes_per_task': shuffle_bytes / task_count if task_count > 0 else 0
        })
        
        results['total_mapper_time_ms'] += stage_time
        results['total_batches'] += batches
        results['total_shuffle_bytes'] += shuffle_bytes
        results['total_tasks'] += task_count
    
    # Calculate averages
    num_runs = len(results['mapper_stages'])
    if num_runs > 0:
        results['avg_mapper_time_ms'] = results['total_mapper_time_ms'] / num_runs
        results['avg_batches_per_task'] = results['total_batches'] / results['total_tasks'] if results['total_tasks'] > 0 else 0
        results['avg_shuffle_bytes_per_task'] = results['total_shuffle_bytes'] / results['total_tasks'] if results['total_tasks'] > 0 else 0
    
    return results

def generate_chart(all_results, output_path):
    """Generate comparison chart."""
    configs = ['Small', 'Medium', 'Large', 'XLarge', 'XXLarge']
    config_keys = ['16p16g', '16p32g', '16p48g', '16p64g', '16p80g']
    
    # Extract data
    pr13724_times = []
    baseline_times = []
    
    for config_key in config_keys:
        pr13724_key = f"phase2_{config_key}"
        baseline_key = f"baseline_{config_key}"
        
        pr13724_time = all_results.get(pr13724_key, {}).get('avg_mapper_time_ms', 0) / 1000  # to seconds
        baseline_time = all_results.get(baseline_key, {}).get('avg_mapper_time_ms', 0) / 1000
        
        pr13724_times.append(pr13724_time)
        baseline_times.append(baseline_time)
    
    # Create figure
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = np.arange(len(configs))
    width = 0.35
    
    bars1 = ax.bar(x - width/2, baseline_times, width, label='Baseline', color='#4a90d9', edgecolor='black')
    bars2 = ax.bar(x + width/2, pr13724_times, width, label='PR-13724', color='#50c878', edgecolor='black')
    
    ax.set_xlabel('Configuration', fontsize=12)
    ax.set_ylabel('Mapper Stage Time (seconds)', fontsize=12)
    ax.set_title('RAPIDS Shuffle Writer Performance: PR-13724 vs Baseline', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(configs)
    ax.legend(loc='upper left')
    
    # Add value labels on bars
    for bar in bars1:
        height = bar.get_height()
        if height > 0:
            ax.annotate(f'{height:.1f}s',
                       xy=(bar.get_x() + bar.get_width() / 2, height),
                       xytext=(0, 3),
                       textcoords="offset points",
                       ha='center', va='bottom', fontsize=9)
    
    for bar in bars2:
        height = bar.get_height()
        if height > 0:
            ax.annotate(f'{height:.1f}s',
                       xy=(bar.get_x() + bar.get_width() / 2, height),
                       xytext=(0, 3),
                       textcoords="offset points",
                       ha='center', va='bottom', fontsize=9)
    
    # Add improvement annotations
    for i, (b_time, p_time) in enumerate(zip(baseline_times, pr13724_times)):
        if b_time > 0 and p_time > 0:
            improvement = (b_time - p_time) / b_time * 100
            if improvement > 0:
                ax.annotate(f'+{improvement:.0f}%',
                           xy=(x[i], max(b_time, p_time) + 5),
                           ha='center', va='bottom', fontsize=10, color='green', fontweight='bold')
            else:
                ax.annotate(f'{improvement:.0f}%',
                           xy=(x[i], max(b_time, p_time) + 5),
                           ha='center', va='bottom', fontsize=10, color='red')
    
    ax.set_ylim(0, max(max(baseline_times), max(pr13724_times)) * 1.2)
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"Chart saved to: {output_path}")

def main():
    # Test mapping: (jar, config) -> app_id
    # Based on batch test output
    test_mapping = {
        ('phase2', '16p16g'): 'local-1767962226928',
        ('phase2', '16p32g'): 'local-1767962348975',
        ('phase2', '16p48g'): 'local-1767962509186',
        ('phase2', '16p64g'): 'local-1767962778706',
        ('phase2', '16p80g'): 'local-1767963115800',
        ('baseline', '16p16g'): 'local-1767963501159',
        ('baseline', '16p32g'): 'local-1767963611011',
        ('baseline', '16p48g'): 'local-1767963774200',
        ('baseline', '16p64g'): 'local-1767964079766',
        ('baseline', '16p80g'): 'local-1767964573608',
    }
    
    event_log_dir = '/tmp/spark-events'
    
    # Analyze each test
    all_results = {}
    
    print("=" * 100)
    print("METRICS COMPARISON: PR-13724 vs Baseline")
    print("=" * 100)
    print()
    
    for (jar, config), app_id in test_mapping.items():
        test_name = f"{jar}_{config}"
        event_log_path = os.path.join(event_log_dir, app_id)
        
        results = analyze_event_log(event_log_path, test_name)
        if results:
            all_results[test_name] = results
    
    # Print comparison table
    configs = ['16p16g', '16p32g', '16p48g', '16p64g', '16p80g']
    
    print("=" * 100)
    print("MAPPER STAGE TIME (avg of 3 timed runs)")
    print("=" * 100)
    print(f"{'Config':<12} {'PR-13724 (ms)':<18} {'Baseline (ms)':<18} {'Diff':<15} {'Improvement':<15}")
    print("-" * 100)
    
    for config in configs:
        phase2_key = f"phase2_{config}"
        baseline_key = f"baseline_{config}"
        
        phase2_time = all_results.get(phase2_key, {}).get('avg_mapper_time_ms', 0)
        baseline_time = all_results.get(baseline_key, {}).get('avg_mapper_time_ms', 0)
        
        config_name = CONFIG_NAMES.get(config, config)
        
        if phase2_time and baseline_time:
            diff = phase2_time - baseline_time
            improvement = (baseline_time - phase2_time) / baseline_time * 100
            print(f"{config_name:<12} {phase2_time:<18.0f} {baseline_time:<18.0f} {diff:<+15.0f} {improvement:<+14.1f}%")
        else:
            print(f"{config_name:<12} {'N/A':<18} {'N/A':<18} {'N/A':<15} {'N/A':<15}")
    
    print()
    print("=" * 100)
    print("OUTPUT COLUMNAR BATCHES (avg per mapper task)")
    print("=" * 100)
    print(f"{'Config':<12} {'PR-13724':<15} {'Baseline':<15} {'Diff':<15}")
    print("-" * 100)
    
    for config in configs:
        phase2_key = f"phase2_{config}"
        baseline_key = f"baseline_{config}"
        
        phase2_batches = all_results.get(phase2_key, {}).get('avg_batches_per_task', 0)
        baseline_batches = all_results.get(baseline_key, {}).get('avg_batches_per_task', 0)
        
        config_name = CONFIG_NAMES.get(config, config)
        
        if phase2_batches or baseline_batches:
            diff = phase2_batches - baseline_batches
            print(f"{config_name:<12} {phase2_batches:<15.1f} {baseline_batches:<15.1f} {diff:<+15.1f}")
        else:
            print(f"{config_name:<12} {'N/A':<15} {'N/A':<15} {'N/A':<15}")
    
    print()
    print("=" * 100)
    print("SHUFFLE BYTES WRITTEN (avg per mapper task)")
    print("=" * 100)
    print(f"{'Config':<12} {'PR-13724':<20} {'Baseline':<20} {'Ratio':<15}")
    print("-" * 100)
    
    for config in configs:
        phase2_key = f"phase2_{config}"
        baseline_key = f"baseline_{config}"
        
        phase2_bytes = all_results.get(phase2_key, {}).get('avg_shuffle_bytes_per_task', 0)
        baseline_bytes = all_results.get(baseline_key, {}).get('avg_shuffle_bytes_per_task', 0)
        
        config_name = CONFIG_NAMES.get(config, config)
        
        if phase2_bytes or baseline_bytes:
            ratio = phase2_bytes / baseline_bytes if baseline_bytes > 0 else 0
            print(f"{config_name:<12} {format_bytes(phase2_bytes):<20} {format_bytes(baseline_bytes):<20} {ratio:<15.2f}x")
        else:
            print(f"{config_name:<12} {'N/A':<20} {'N/A':<20} {'N/A':<15}")
    
    print()
    print("=" * 100)
    
    # Generate chart
    chart_path = '/home/hongbin/experiments/20260108_pr13724_pipeline_benchmark/benchmark_results.png'
    generate_chart(all_results, chart_path)

if __name__ == "__main__":
    main()
