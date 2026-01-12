# Python UDF Execution Model Benchmark

This experiment compares two approaches for executing Python UDFs:

## 1. Cross-Process (Spark-style)
- Python runs in a separate worker process
- Data is serialized using Apache Arrow
- IPC via Unix domain socket or pipe
- Each batch incurs serialization + transfer + deserialization overhead

## 2. Embedded (DuckDB-style)
- Python interpreter embedded in the same process
- Direct function call, no IPC
- Data can be passed by reference (zero-copy possible)
- No serialization overhead

## Files

- `benchmark.py` - Basic benchmark comparing different approaches
- `spark_style_simulation.py` - More realistic Spark simulation with Arrow IPC

## Running

```bash
# Install dependencies
pip install numpy pyarrow duckdb

# Run basic benchmark
python benchmark.py

# Run detailed Spark-style simulation
python spark_style_simulation.py
```

## Expected Results

The embedded approach should be significantly faster, especially when:
- Many small batches (high IPC overhead per batch)
- Simple UDFs (computation time << transfer time)
- Low-latency requirements

The cross-process approach may be competitive when:
- Few large batches (amortizes IPC overhead)
- Complex UDFs (computation >> transfer)
- Process isolation is required

## Relevance to spark-rapids

For GPU workloads, the overhead is even more significant:
- Current: GPU -> CPU -> Arrow -> Socket -> Python -> Socket -> Arrow -> CPU -> GPU
- Potential with embedded: GPU -> Python (via __cuda_array_interface__) -> GPU

This could enable zero-copy GPU data access from Python UDFs.
