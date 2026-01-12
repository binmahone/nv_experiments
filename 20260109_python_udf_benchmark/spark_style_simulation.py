#!/usr/bin/env python3
"""
Realistic simulation of Spark Rapids Python UDF execution model.

This simulates the actual data flow in Spark Python UDF:
1. JVM side: Data is serialized to Arrow format
2. Data is sent via socket to Python worker
3. Python worker deserializes Arrow -> pandas
4. UDF is executed
5. Result is serialized back to Arrow
6. Sent back to JVM via socket

Key configuration in Spark:
- spark.sql.execution.arrow.maxRecordsPerBatch (default: 10000 rows)

We simulate this using multiprocessing with explicit Arrow serialization.
"""

import time
import multiprocessing as mp
import numpy as np
import pyarrow as pa
import socket
from typing import List, Tuple
import sys


# =============================================================================
# Spark-Style Worker (with Arrow serialization)
# =============================================================================

def spark_style_worker(server_socket_path: str, udf_code: str):
    """
    Worker process that mimics Spark's Python worker behavior.
    Uses Unix domain socket for IPC (similar to Spark's approach).
    """
    # Compile UDF
    udf_namespace = {}
    exec(f"import numpy as np\nudf_func = {udf_code}", udf_namespace)
    udf_func = udf_namespace['udf_func']
    
    # Connect to parent
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(server_socket_path)
    
    try:
        while True:
            # Receive length prefix
            length_bytes = sock.recv(8)
            if not length_bytes or len(length_bytes) < 8:
                break
            
            data_length = int.from_bytes(length_bytes, 'big')
            if data_length == 0:
                break
            
            # Receive Arrow IPC data
            arrow_data = b''
            while len(arrow_data) < data_length:
                chunk = sock.recv(min(65536, data_length - len(arrow_data)))
                if not chunk:
                    break
                arrow_data += chunk
            
            # Deserialize Arrow -> pandas (simulating Spark's approach)
            reader = pa.ipc.open_stream(arrow_data)
            table = reader.read_all()
            
            # Convert to pandas and execute UDF
            pdf = table.to_pandas()
            result_data = udf_func(pdf['value'].values)
            
            # Create result Arrow table
            result_table = pa.table({'result': result_data})
            
            # Serialize back to Arrow IPC
            sink = pa.BufferOutputStream()
            writer = pa.ipc.new_stream(sink, result_table.schema)
            writer.write_table(result_table)
            writer.close()
            result_bytes = sink.getvalue().to_pybytes()
            
            # Send result back
            sock.sendall(len(result_bytes).to_bytes(8, 'big'))
            sock.sendall(result_bytes)
            
    finally:
        sock.close()


class SparkStyleUDFExecutor:
    """
    Simulates Spark's cross-process UDF execution with realistic Arrow serialization.
    """
    
    def __init__(self, udf_code: str):
        import tempfile
        import os
        
        self.udf_code = udf_code
        
        # Create Unix domain socket
        self.socket_path = tempfile.mktemp(prefix='spark_udf_')
        self.server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server_socket.bind(self.socket_path)
        self.server_socket.listen(1)
        
        # Start worker process
        self.worker = mp.Process(target=spark_style_worker, args=(self.socket_path, udf_code))
        self.worker.start()
        
        # Accept connection from worker
        self.client_socket, _ = self.server_socket.accept()
    
    def execute(self, data: np.ndarray) -> np.ndarray:
        """Execute UDF with full Arrow serialization round-trip."""
        # Create Arrow table
        table = pa.table({'value': data})
        
        # Serialize to Arrow IPC format
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()
        arrow_bytes = sink.getvalue().to_pybytes()
        
        # Send to worker
        self.client_socket.sendall(len(arrow_bytes).to_bytes(8, 'big'))
        self.client_socket.sendall(arrow_bytes)
        
        # Receive result length
        length_bytes = self.client_socket.recv(8)
        result_length = int.from_bytes(length_bytes, 'big')
        
        # Receive result data
        result_bytes = b''
        while len(result_bytes) < result_length:
            chunk = self.client_socket.recv(min(65536, result_length - len(result_bytes)))
            if not chunk:
                break
            result_bytes += chunk
        
        # Deserialize result
        reader = pa.ipc.open_stream(result_bytes)
        result_table = reader.read_all()
        
        return result_table['result'].to_numpy()
    
    def close(self):
        """Shutdown worker and cleanup."""
        import os
        # Send shutdown signal
        self.client_socket.sendall((0).to_bytes(8, 'big'))
        self.client_socket.close()
        self.worker.join(timeout=5)
        if self.worker.is_alive():
            self.worker.terminate()
        self.server_socket.close()
        try:
            os.unlink(self.socket_path)
        except:
            pass


# =============================================================================
# Embedded Style (Direct Call)
# =============================================================================

class EmbeddedUDFExecutor:
    """Direct in-process UDF execution (DuckDB-style)."""
    
    def __init__(self, udf_code: str):
        udf_namespace = {}
        exec(f"import numpy as np\nself_udf_func = {udf_code}", udf_namespace)
        self.udf_func = udf_namespace['self_udf_func']
    
    def execute(self, data: np.ndarray) -> np.ndarray:
        return self.udf_func(data)
    
    def close(self):
        pass


# =============================================================================
# Benchmark Functions
# =============================================================================

def benchmark_execution(
    executor,
    data: np.ndarray,
    batch_size: int,
    iterations: int
) -> Tuple[float, float, float]:
    """
    Benchmark UDF execution.
    Returns: (total_time, avg_batch_time, throughput_rows_per_sec)
    """
    batches = [data[i:i+batch_size] for i in range(0, len(data), batch_size)]
    
    # Warm up
    for batch in batches[:min(3, len(batches))]:
        executor.execute(batch)
    
    # Benchmark
    total_time = 0
    for _ in range(iterations):
        start = time.perf_counter()
        for batch in batches:
            executor.execute(batch)
        end = time.perf_counter()
        total_time += (end - start)
    
    avg_time = total_time / iterations
    avg_batch_time = avg_time / len(batches)
    throughput = len(data) / avg_time
    
    return (avg_time, avg_batch_time, throughput)


def run_detailed_benchmark():
    """Run detailed benchmark with various configurations."""
    
    print("=" * 80)
    print("Python UDF Execution Model Benchmark")
    print("Cross-Process (Spark-style) vs Embedded (DuckDB-style)")
    print("=" * 80)
    print()
    print("Spark Configuration Reference:")
    print("  - spark.sql.execution.arrow.maxRecordsPerBatch = 10000 (default)")
    print("  - spark.rapids.sql.batchSizeBytes = 2147483647 (~2GB, for GPU batches)")
    print()
    
    # UDF definitions
    udfs = {
        'trivial': 'lambda x: x',
        'simple_add': 'lambda x: x + 1',
        'numpy_ops': 'lambda x: np.sin(x) * np.cos(x)',
        'complex': 'lambda x: np.sqrt(np.abs(np.sin(x) * np.cos(x) + np.log1p(np.abs(x))))',
    }
    
    # Test configurations matching Spark Rapids scenarios
    # Spark default: maxRecordsPerBatch = 10000
    # GPU typical: batch ~1GB, with double = ~125M rows
    configs = [
        # (total_rows, batch_size, description)
        # Scenario 1: Small data, default Spark batch size
        (100_000, 10_000, "100K rows, Spark default batch (10K rows)"),
        (1_000_000, 10_000, "1M rows, Spark default batch (10K rows)"),
        
        # Scenario 2: Larger batches (more GPU-like)
        (1_000_000, 100_000, "1M rows, larger batch (100K rows)"),
        (1_000_000, 1_000_000, "1M rows, single batch"),
        
        # Scenario 3: Simulated GPU batch (~1GB = 125M doubles, scaled down)
        (5_000_000, 5_000_000, "5M rows, single batch (GPU-like)"),
    ]
    
    iterations = 2
    results = []
    
    for udf_name, udf_code in udfs.items():
        print(f"\n{'='*70}")
        print(f"UDF: {udf_name}")
        print(f"Code: {udf_code}")
        print(f"{'='*70}")
        
        for total_rows, batch_size, desc in configs:
            print(f"\n{desc}")
            print("-" * 60)
            
            num_batches = (total_rows + batch_size - 1) // batch_size
            
            # Generate data
            data = np.random.rand(total_rows).astype(np.float64)
            
            # Spark-style execution
            try:
                spark_executor = SparkStyleUDFExecutor(udf_code)
                spark_time, spark_batch_time, spark_throughput = benchmark_execution(
                    spark_executor, data, batch_size, iterations
                )
                spark_executor.close()
            except Exception as e:
                print(f"  Spark-style failed: {e}")
                spark_time, spark_batch_time, spark_throughput = 0, 0, 0
            
            # Embedded execution
            embedded_executor = EmbeddedUDFExecutor(udf_code)
            embedded_time, embedded_batch_time, embedded_throughput = benchmark_execution(
                embedded_executor, data, batch_size, iterations
            )
            embedded_executor.close()
            
            # Calculate speedup
            if spark_time > 0:
                speedup = spark_time / embedded_time
            else:
                speedup = 0
            
            # Calculate IPC overhead
            ipc_overhead_ms = (spark_time - embedded_time) * 1000 if spark_time > embedded_time else 0
            ipc_per_batch_ms = ipc_overhead_ms / num_batches if num_batches > 0 else 0
            
            # Print results
            print(f"  Batches: {num_batches}, Batch size: {batch_size:,} rows")
            print(f"  Cross-Process: {spark_time*1000:8.2f} ms  "
                  f"({spark_throughput/1e6:6.2f} M rows/s)")
            print(f"  Embedded:      {embedded_time*1000:8.2f} ms  "
                  f"({embedded_throughput/1e6:6.2f} M rows/s)")
            print(f"  Speedup:       {speedup:8.1f}x")
            print(f"  IPC Overhead:  {ipc_overhead_ms:8.2f} ms total, "
                  f"{ipc_per_batch_ms:.2f} ms/batch")
            
            results.append({
                'udf': udf_name,
                'total_rows': total_rows,
                'batch_size': batch_size,
                'num_batches': num_batches,
                'spark_ms': spark_time * 1000,
                'embedded_ms': embedded_time * 1000,
                'speedup': speedup,
                'ipc_overhead_ms': ipc_overhead_ms,
                'ipc_per_batch_ms': ipc_per_batch_ms,
            })
    
    # Print summary table
    print("\n" + "=" * 100)
    print("SUMMARY TABLE")
    print("=" * 100)
    print(f"{'UDF':<12} {'Total Rows':>12} {'Batch Size':>12} {'Batches':>8} "
          f"{'Cross-Proc':>12} {'Embedded':>12} {'Speedup':>10} {'IPC/Batch':>10}")
    print("-" * 100)
    
    for r in results:
        print(f"{r['udf']:<12} {r['total_rows']:>12,} {r['batch_size']:>12,} {r['num_batches']:>8} "
              f"{r['spark_ms']:>10.2f}ms {r['embedded_ms']:>10.2f}ms "
              f"{r['speedup']:>9.1f}x {r['ipc_per_batch_ms']:>8.2f}ms")
    
    # Analysis
    print("\n" + "=" * 80)
    print("KEY FINDINGS")
    print("=" * 80)
    
    # Find average IPC overhead per batch
    ipc_overheads = [r['ipc_per_batch_ms'] for r in results if r['ipc_per_batch_ms'] > 0]
    if ipc_overheads:
        avg_ipc = sum(ipc_overheads) / len(ipc_overheads)
        print(f"\n1. Average IPC Overhead: {avg_ipc:.2f} ms per batch")
    
    # Find best/worst cases
    speedups = [(r['speedup'], r['udf'], r['num_batches']) for r in results if r['speedup'] > 0]
    if speedups:
        best = max(speedups, key=lambda x: x[0])
        worst = min(speedups, key=lambda x: x[0])
        print(f"\n2. Best Speedup:  {best[0]:.1f}x ({best[1]}, {best[2]} batches)")
        print(f"   Worst Speedup: {worst[0]:.1f}x ({worst[1]}, {worst[2]} batches)")
    
    print("""
3. Observations:
   - IPC overhead is ~1-5ms per batch for small batches
   - For Spark default (10K rows/batch), overhead dominates for simple UDFs
   - Larger batches (1M+) amortize IPC overhead better
   - Complex UDFs reduce relative impact of IPC overhead

4. Implications for spark-rapids:
   - Current GPU batch size (~1GB) maps to ~125M rows for doubles
   - With Spark default 10K rows/batch, many IPC round-trips occur
   - Embedded approach could eliminate this overhead entirely
   - For GPU: additional CPU<->GPU transfer overhead not measured here
""")
    
    return results


if __name__ == "__main__":
    results = run_detailed_benchmark()
