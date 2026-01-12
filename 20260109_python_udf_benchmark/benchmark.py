#!/usr/bin/env python3
"""
Benchmark: Embedded Python UDF vs Cross-Process Python UDF

This experiment compares two approaches for executing Python UDFs:
1. Embedded (DuckDB-style): Python interpreter embedded in the same process
2. Cross-Process (Spark-style): Separate Python worker process with IPC

The goal is to demonstrate the performance difference between these two approaches.
"""

import time
import multiprocessing as mp
import numpy as np
import pyarrow as pa
from typing import Callable, List, Tuple
import os
import sys

# Try to import duckdb, will be used for embedded comparison
try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False
    print("Warning: DuckDB not installed. Will only run cross-process simulation.")


# =============================================================================
# Cross-Process Approach (Simulating Spark's Architecture)
# =============================================================================

def worker_process(conn, udf_func_name: str):
    """
    Worker process that receives data via pipe, executes UDF, and returns results.
    This simulates Spark's Python worker process.
    """
    import pickle
    
    # Define available UDFs
    udf_registry = {
        'simple_add': lambda x: x + 1,
        'multiply': lambda x: x * 2,
        'complex_math': lambda x: np.sin(x) * np.cos(x) + np.sqrt(np.abs(x)),
    }
    
    udf_func = udf_registry[udf_func_name]
    
    while True:
        try:
            # Receive data (simulating Arrow deserialization)
            data = conn.recv()
            if data is None:
                break
            
            # Execute UDF
            result = udf_func(data)
            
            # Send result back (simulating Arrow serialization)
            conn.send(result)
        except EOFError:
            break
    
    conn.close()


class CrossProcessUDFExecutor:
    """
    Simulates Spark's cross-process UDF execution model.
    Data is sent to a worker process via pipe, processed, and returned.
    """
    
    def __init__(self, udf_name: str):
        self.udf_name = udf_name
        self.parent_conn, self.child_conn = mp.Pipe()
        self.worker = mp.Process(target=worker_process, args=(self.child_conn, udf_name))
        self.worker.start()
    
    def execute(self, data: np.ndarray) -> np.ndarray:
        """Execute UDF on data via cross-process communication."""
        # Send data to worker (simulates Arrow serialization + network transfer)
        self.parent_conn.send(data)
        
        # Receive result (simulates Arrow deserialization)
        result = self.parent_conn.recv()
        return result
    
    def close(self):
        """Shutdown the worker process."""
        self.parent_conn.send(None)
        self.worker.join()
        self.parent_conn.close()


# =============================================================================
# Embedded Approach (Simulating DuckDB's Architecture)
# =============================================================================

class EmbeddedUDFExecutor:
    """
    Simulates DuckDB's embedded Python UDF execution model.
    Python function is called directly in the same process.
    """
    
    def __init__(self, udf_name: str):
        self.udf_registry = {
            'simple_add': lambda x: x + 1,
            'multiply': lambda x: x * 2,
            'complex_math': lambda x: np.sin(x) * np.cos(x) + np.sqrt(np.abs(x)),
        }
        self.udf_func = self.udf_registry[udf_name]
    
    def execute(self, data: np.ndarray) -> np.ndarray:
        """Execute UDF directly in the same process."""
        return self.udf_func(data)
    
    def close(self):
        pass


# =============================================================================
# DuckDB Native UDF (Actual Implementation)
# =============================================================================

def run_duckdb_native_udf(data: np.ndarray, udf_name: str, iterations: int) -> Tuple[float, float]:
    """
    Run UDF using DuckDB's native Python UDF support.
    Returns (total_time, avg_time_per_iteration)
    """
    if not HAS_DUCKDB:
        return (0.0, 0.0)
    
    conn = duckdb.connect()
    
    # Register UDF based on name
    if udf_name == 'simple_add':
        def udf_func(x):
            return x + 1
        conn.create_function('my_udf', udf_func, ['DOUBLE'], 'DOUBLE')
    elif udf_name == 'multiply':
        def udf_func(x):
            return x * 2
        conn.create_function('my_udf', udf_func, ['DOUBLE'], 'DOUBLE')
    elif udf_name == 'complex_math':
        def udf_func(x):
            import math
            return math.sin(x) * math.cos(x) + math.sqrt(abs(x))
        conn.create_function('my_udf', udf_func, ['DOUBLE'], 'DOUBLE')
    
    # Create table with data
    conn.execute("CREATE TABLE test_data AS SELECT unnest(?) as value", [data.tolist()])
    
    # Warm up
    conn.execute("SELECT my_udf(value) FROM test_data").fetchall()
    
    # Benchmark
    start = time.perf_counter()
    for _ in range(iterations):
        conn.execute("SELECT my_udf(value) FROM test_data").fetchall()
    end = time.perf_counter()
    
    total_time = end - start
    avg_time = total_time / iterations
    
    conn.close()
    return (total_time, avg_time)


def run_duckdb_vectorized_udf(data: np.ndarray, udf_name: str, iterations: int) -> Tuple[float, float]:
    """
    Run UDF using DuckDB's vectorized (Arrow) Python UDF support.
    Returns (total_time, avg_time_per_iteration)
    """
    if not HAS_DUCKDB:
        return (0.0, 0.0)
    
    conn = duckdb.connect()
    
    # Register vectorized UDF
    if udf_name == 'simple_add':
        def udf_func(x):
            return pa.compute.add(x, 1)
        conn.create_function('my_udf', udf_func, [duckdb.typing.DOUBLE], duckdb.typing.DOUBLE,
                           type='arrow')
    elif udf_name == 'multiply':
        def udf_func(x):
            return pa.compute.multiply(x, 2)
        conn.create_function('my_udf', udf_func, [duckdb.typing.DOUBLE], duckdb.typing.DOUBLE,
                           type='arrow')
    elif udf_name == 'complex_math':
        def udf_func(x):
            # Arrow compute for vectorized math
            arr = x.to_numpy()
            result = np.sin(arr) * np.cos(arr) + np.sqrt(np.abs(arr))
            return pa.array(result)
        conn.create_function('my_udf', udf_func, [duckdb.typing.DOUBLE], duckdb.typing.DOUBLE,
                           type='arrow')
    
    # Create table with data
    conn.execute("CREATE TABLE test_data AS SELECT unnest(?) as value", [data.tolist()])
    
    # Warm up
    conn.execute("SELECT my_udf(value) FROM test_data").fetchall()
    
    # Benchmark
    start = time.perf_counter()
    for _ in range(iterations):
        conn.execute("SELECT my_udf(value) FROM test_data").fetchall()
    end = time.perf_counter()
    
    total_time = end - start
    avg_time = total_time / iterations
    
    conn.close()
    return (total_time, avg_time)


# =============================================================================
# Benchmark Runner
# =============================================================================

def run_benchmark(
    data_sizes: List[int],
    batch_counts: List[int],
    udf_names: List[str],
    iterations: int = 10
):
    """
    Run comprehensive benchmark comparing different UDF execution approaches.
    """
    results = []
    
    for udf_name in udf_names:
        print(f"\n{'='*60}")
        print(f"UDF: {udf_name}")
        print(f"{'='*60}")
        
        for data_size in data_sizes:
            for batch_count in batch_counts:
                print(f"\nData size: {data_size:,} rows, Batches: {batch_count}")
                print("-" * 50)
                
                # Generate test data
                data = np.random.rand(data_size).astype(np.float64)
                batch_size = data_size // batch_count
                batches = [data[i:i+batch_size] for i in range(0, data_size, batch_size)]
                
                # 1. Cross-Process (Spark-style)
                cross_proc_executor = CrossProcessUDFExecutor(udf_name)
                
                # Warm up
                for batch in batches[:min(3, len(batches))]:
                    cross_proc_executor.execute(batch)
                
                start = time.perf_counter()
                for _ in range(iterations):
                    for batch in batches:
                        cross_proc_executor.execute(batch)
                end = time.perf_counter()
                cross_proc_time = (end - start) / iterations
                cross_proc_executor.close()
                
                # 2. Embedded (DuckDB-style simulation)
                embedded_executor = EmbeddedUDFExecutor(udf_name)
                
                # Warm up
                for batch in batches[:min(3, len(batches))]:
                    embedded_executor.execute(batch)
                
                start = time.perf_counter()
                for _ in range(iterations):
                    for batch in batches:
                        embedded_executor.execute(batch)
                end = time.perf_counter()
                embedded_time = (end - start) / iterations
                embedded_executor.close()
                
                # 3. DuckDB Native UDF (row-by-row)
                duckdb_native_total, duckdb_native_avg = run_duckdb_native_udf(
                    data, udf_name, iterations
                )
                
                # 4. DuckDB Vectorized UDF (Arrow)
                duckdb_vec_total, duckdb_vec_avg = run_duckdb_vectorized_udf(
                    data, udf_name, iterations
                )
                
                # Calculate speedups
                if cross_proc_time > 0:
                    embedded_speedup = cross_proc_time / embedded_time
                else:
                    embedded_speedup = 0
                
                # Print results
                print(f"  Cross-Process (Spark-style):   {cross_proc_time*1000:8.2f} ms")
                print(f"  Embedded (direct call):        {embedded_time*1000:8.2f} ms  "
                      f"({embedded_speedup:.1f}x faster)")
                
                if HAS_DUCKDB:
                    if duckdb_native_avg > 0:
                        duckdb_native_speedup = cross_proc_time / duckdb_native_avg
                        print(f"  DuckDB Native UDF:             {duckdb_native_avg*1000:8.2f} ms  "
                              f"({duckdb_native_speedup:.1f}x vs cross-proc)")
                    if duckdb_vec_avg > 0:
                        duckdb_vec_speedup = cross_proc_time / duckdb_vec_avg
                        print(f"  DuckDB Vectorized UDF:         {duckdb_vec_avg*1000:8.2f} ms  "
                              f"({duckdb_vec_speedup:.1f}x vs cross-proc)")
                
                results.append({
                    'udf_name': udf_name,
                    'data_size': data_size,
                    'batch_count': batch_count,
                    'cross_proc_ms': cross_proc_time * 1000,
                    'embedded_ms': embedded_time * 1000,
                    'embedded_speedup': embedded_speedup,
                    'duckdb_native_ms': duckdb_native_avg * 1000 if HAS_DUCKDB else 0,
                    'duckdb_vec_ms': duckdb_vec_avg * 1000 if HAS_DUCKDB else 0,
                })
    
    return results


def print_summary(results: List[dict]):
    """Print a summary table of results."""
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"{'UDF':<15} {'Size':>10} {'Batches':>8} {'Cross-Proc':>12} {'Embedded':>12} {'Speedup':>10}")
    print("-" * 80)
    
    for r in results:
        print(f"{r['udf_name']:<15} {r['data_size']:>10,} {r['batch_count']:>8} "
              f"{r['cross_proc_ms']:>10.2f}ms {r['embedded_ms']:>10.2f}ms "
              f"{r['embedded_speedup']:>9.1f}x")


def main():
    print("=" * 80)
    print("Python UDF Execution Model Benchmark")
    print("Comparing: Cross-Process (Spark) vs Embedded (DuckDB)")
    print("=" * 80)
    print(f"Python version: {sys.version}")
    print(f"NumPy version: {np.__version__}")
    print(f"DuckDB available: {HAS_DUCKDB}")
    if HAS_DUCKDB:
        print(f"DuckDB version: {duckdb.__version__}")
    print()
    
    # Benchmark parameters
    data_sizes = [10_000, 100_000, 1_000_000]
    batch_counts = [1, 10, 100]  # Number of batches to split data into
    udf_names = ['simple_add', 'multiply', 'complex_math']
    iterations = 5
    
    results = run_benchmark(data_sizes, batch_counts, udf_names, iterations)
    print_summary(results)
    
    print("\n" + "=" * 80)
    print("KEY INSIGHTS:")
    print("=" * 80)
    print("""
1. Cross-process overhead: Each batch transfer involves:
   - Serialization (Python -> pickle/Arrow)
   - IPC transfer (pipe/socket)
   - Deserialization (Arrow -> Python)
   - Result transfer back

2. Embedded approach eliminates:
   - Process startup cost (amortized but still present)
   - IPC latency
   - Serialization/deserialization overhead

3. Impact is most visible with:
   - Many small batches (high IPC overhead per batch)
   - Simple UDFs (computation time << transfer time)
   - Low-latency requirements

4. For GPU workloads (spark-rapids), additional considerations:
   - GPU memory transfer: GPU -> CPU -> Python -> CPU -> GPU
   - Embedded approach could enable zero-copy via __cuda_array_interface__
""")


if __name__ == "__main__":
    main()
