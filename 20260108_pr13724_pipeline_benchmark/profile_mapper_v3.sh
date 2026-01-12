#!/bin/bash
set -e

SPARK_HOME="/home/hongbin/develop/spark-3.3.2-bin-hadoop3-official"
JAR_PATH="/home/hongbin/experiments/20260108_pr13724_pipeline_benchmark/jars/rapids-4-spark_2.12-phase2.jar"
ASPROF="/home/hongbin/develop/async-profiler-3.0-linux-x64/bin/asprof"
OUTPUT_DIR="/home/hongbin/experiments/20260108_pr13724_pipeline_benchmark/profiles"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
PROFILE_FILE="$OUTPUT_DIR/mapper_phase2_${TIMESTAMP}"

mkdir -p "$OUTPUT_DIR"

echo "Starting spark-shell in background..."

$SPARK_HOME/bin/spark-shell \
    --master "local[16]" \
    --driver-memory 32g \
    --conf spark.app.name="profile_phase2_mapper_$TIMESTAMP" \
    --conf spark.eventLog.enabled=true \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.sql.enabled=true \
    --conf spark.sql.adaptive.enabled=false \
    --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark332.RapidsShuffleManager \
    --conf spark.rapids.shuffle.mode=MULTITHREADED \
    --conf spark.rapids.shuffle.multiThreaded.writer.threads=16 \
    --conf spark.rapids.shuffle.multiThreaded.reader.threads=16 \
    --conf spark.rapids.sql.concurrentGpuTasks=16 \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.default.parallelism=16 \
    --conf spark.sql.parquet.aggregatePushdown=false \
    --conf spark.rapids.shuffle.multithreaded.skipMerge=false \
    --jars "$JAR_PATH" \
    << 'SCALA_EOF' &
import org.apache.hadoop.fs.{FileSystem, Path}

val inputPath = "/home/hongbin/experiments/20260108_pr13724_pipeline_benchmark/data/16gb_shuffle_data"
val outputDir = "/tmp/benchmark_output"
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

def deleteOutputDir(): Unit = {
  val path = new Path(outputDir)
  if (fs.exists(path)) fs.delete(path, true)
}

val inputDf = spark.read.parquet(inputPath)

// Signal ready
new java.io.File("/tmp/spark_ready").createNewFile()

// Wait for profiler to attach
println("=== Waiting for profiler to attach ===")
while (!new java.io.File("/tmp/profiler_attached").exists()) {
  Thread.sleep(500)
}

println("=== Starting mapper stage ===")
deleteOutputDir()
val start = System.nanoTime()
inputDf.repartition(200).write.mode("overwrite").parquet(outputDir)
val elapsed = (System.nanoTime() - start) / 1000000
println(s"=== Mapper stage completed in ${elapsed}ms ===")

// Signal done
new java.io.File("/tmp/spark_done").createNewFile()
Thread.sleep(3000)
System.exit(0)
SCALA_EOF

SPARK_PID=$!
echo "Spark shell PID: $SPARK_PID"

# Clean up signal files
rm -f /tmp/spark_ready /tmp/profiler_attached /tmp/spark_done

# Wait for spark to be ready
echo "Waiting for Spark to be ready..."
while [ ! -f /tmp/spark_ready ]; do
    sleep 1
    if ! kill -0 $SPARK_PID 2>/dev/null; then
        echo "ERROR: Spark died"
        exit 1
    fi
done

# Find the actual Java process (spark-shell spawns a child Java process)
JAVA_PID=$(pgrep -P $SPARK_PID java 2>/dev/null || echo $SPARK_PID)
echo "Java PID to profile: $JAVA_PID"

# Start profiler
echo "Starting async-profiler for 25 seconds..."
$ASPROF -d 25 -e wall -i 5ms -t -f "${PROFILE_FILE}.jfr" $JAVA_PID &
PROF_PID=$!

sleep 2
# Signal spark to start
touch /tmp/profiler_attached

# Wait for profiler
wait $PROF_PID
echo "Profiler finished"

# Wait for spark
wait $SPARK_PID 2>/dev/null || true

echo ""
echo "Converting JFR to HTML..."
if [ -f "${PROFILE_FILE}.jfr" ]; then
    java -jar /home/hongbin/develop/async-profiler-3.0-linux-x64/lib/converter.jar \
        --threads -o flamegraph "${PROFILE_FILE}.jfr" "${PROFILE_FILE}.html" 2>&1 && \
    echo "=== Profile saved: ${PROFILE_FILE}.html ===" || \
    echo "=== JFR saved: ${PROFILE_FILE}.jfr (convert manually) ==="
    ls -la "${PROFILE_FILE}"*
fi
