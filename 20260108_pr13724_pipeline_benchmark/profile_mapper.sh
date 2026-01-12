#!/bin/bash
set -e

SPARK_HOME="/home/hongbin/develop/spark-3.3.2-bin-hadoop3-official"
JAR_PATH="/home/hongbin/experiments/20260108_pr13724_pipeline_benchmark/jars/rapids-4-spark_2.12-phase2.jar"
DATA_PATH="/home/hongbin/experiments/20260108_pr13724_pipeline_benchmark/data/16gb_shuffle_data"
PARALLELISM=16
ASPROF="/home/hongbin/develop/async-profiler-3.0-linux-x64/bin/asprof"
OUTPUT_DIR="/home/hongbin/experiments/20260108_pr13724_pipeline_benchmark/profiles"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "$OUTPUT_DIR"

echo "Starting spark-shell in background..."

# Create a Scala script file
cat << 'SCALA_EOF' > /tmp/benchmark_script.scala
import org.apache.hadoop.fs.{FileSystem, Path}

val inputPath = "/home/hongbin/experiments/20260108_pr13724_pipeline_benchmark/data/16gb_shuffle_data"
val outputDir = "/tmp/benchmark_output"
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

def deleteOutputDir(): Unit = {
  val path = new Path(outputDir)
  if (fs.exists(path)) {
    fs.delete(path, true)
  }
}

val inputDf = spark.read.parquet(inputPath)

// Signal file to indicate benchmark is starting
new java.io.File("/tmp/benchmark_starting").createNewFile()

println("=== Starting mapper stage (shuffle write) ===")
deleteOutputDir()
val start = System.nanoTime()
inputDf.repartition(200).write.mode("overwrite").parquet(outputDir)
val elapsed = (System.nanoTime() - start) / 1000000
println(s"=== Mapper stage completed in ${elapsed}ms ===")

// Signal completion
new java.io.File("/tmp/benchmark_done").createNewFile()

Thread.sleep(3000)
System.exit(0)
SCALA_EOF

# Clean up signal files
rm -f /tmp/benchmark_starting /tmp/benchmark_done

# Start spark-shell in background
$SPARK_HOME/bin/spark-shell \
    --master "local[$PARALLELISM]" \
    --driver-memory 32g \
    --conf spark.app.name="profile_phase2_16p16g" \
    --conf spark.eventLog.enabled=true \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.sql.enabled=true \
    --conf spark.sql.adaptive.enabled=false \
    --conf spark.rapids.sql.explain=NOT_ON_GPU \
    --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark332.RapidsShuffleManager \
    --conf spark.rapids.shuffle.mode=MULTITHREADED \
    --conf spark.rapids.shuffle.multiThreaded.writer.threads=$PARALLELISM \
    --conf spark.rapids.shuffle.multiThreaded.reader.threads=$PARALLELISM \
    --conf spark.rapids.sql.concurrentGpuTasks=$PARALLELISM \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.default.parallelism=$PARALLELISM \
    --conf spark.sql.parquet.aggregatePushdown=false \
    --conf spark.rapids.shuffle.multithreaded.skipMerge=false \
    --jars "$JAR_PATH" \
    -i /tmp/benchmark_script.scala > /tmp/spark_profile_output.log 2>&1 &

SPARK_PID=$!
echo "Spark PID: $SPARK_PID"

# Wait for benchmark to start
echo "Waiting for benchmark to start..."
while [ ! -f /tmp/benchmark_starting ]; do
    sleep 0.5
    # Check if spark process is still running
    if ! kill -0 $SPARK_PID 2>/dev/null; then
        echo "ERROR: Spark process died"
        cat /tmp/spark_profile_output.log
        exit 1
    fi
done

echo "Benchmark started, starting profiler..."
sleep 2  # Give mapper stage a moment to start

# Start profiling for 20 seconds (should capture most of the mapper stage)
PROFILE_FILE="$OUTPUT_DIR/phase2_16p16g_mapper_${TIMESTAMP}"
echo "Collecting wall clock profile for 20 seconds..."
$ASPROF -d 20 -e wall -i 5ms -t -f "${PROFILE_FILE}.jfr" $SPARK_PID

echo "Converting to HTML..."
java -jar /home/hongbin/develop/async-profiler-3.0-linux-x64/converter.jar \
    --threads -o html "${PROFILE_FILE}.jfr" "${PROFILE_FILE}.html" 2>/dev/null || \
java -cp /home/hongbin/develop/async-profiler-3.0-linux-x64/lib/converter.jar \
    jfr2flame --threads -o "${PROFILE_FILE}.html" "${PROFILE_FILE}.jfr" 2>/dev/null || \
echo "Note: JFR file saved, convert manually if needed"

# Wait for benchmark to complete
echo "Waiting for benchmark to complete..."
wait $SPARK_PID 2>/dev/null || true

echo ""
echo "=== Profile saved to: ${PROFILE_FILE}.jfr ==="
echo "=== HTML (if converted): ${PROFILE_FILE}.html ==="
echo ""
echo "Spark output:"
grep -E "Starting mapper|completed in" /tmp/spark_profile_output.log || cat /tmp/spark_profile_output.log | tail -20
