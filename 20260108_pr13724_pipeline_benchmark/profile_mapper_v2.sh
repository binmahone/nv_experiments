#!/bin/bash
set -e

SPARK_HOME="/home/hongbin/develop/spark-3.3.2-bin-hadoop3-official"
JAR_PATH="/home/hongbin/experiments/20260108_pr13724_pipeline_benchmark/jars/rapids-4-spark_2.12-phase2.jar"
ASPROF="/home/hongbin/develop/async-profiler-3.0-linux-x64/bin/asprof"
OUTPUT_DIR="/home/hongbin/experiments/20260108_pr13724_pipeline_benchmark/profiles"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "$OUTPUT_DIR"

# Clean up
rm -f /tmp/benchmark_starting /tmp/benchmark_done

echo "Starting spark-shell..."

$SPARK_HOME/bin/spark-shell \
    --master "local[16]" \
    --driver-memory 32g \
    --conf spark.app.name="profile_phase2_mapper" \
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
    << 'SCALA_EOF'
import org.apache.hadoop.fs.{FileSystem, Path}

val inputPath = "/home/hongbin/experiments/20260108_pr13724_pipeline_benchmark/data/16gb_shuffle_data"
val outputDir = "/tmp/benchmark_output"
val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

def deleteOutputDir(): Unit = {
  val path = new Path(outputDir)
  if (fs.exists(path)) fs.delete(path, true)
}

val inputDf = spark.read.parquet(inputPath)

// Get my own PID
val pid = ProcessHandle.current().pid()
println(s"\n=== Spark PID: $pid ===")
println("=== Will start profiling in 3 seconds, then run mapper stage ===\n")

// Start profiler in background
import sys.process._
val profilerCmd = Seq("bash", "-c", s"""
  sleep 3
  /home/hongbin/develop/async-profiler-3.0-linux-x64/bin/asprof \
    -d 25 -e wall -i 5ms -t \
    -f /home/hongbin/experiments/20260108_pr13724_pipeline_benchmark/profiles/mapper_${pid}.jfr \
    $pid
""")
val profilerProcess = profilerCmd.run()

Thread.sleep(5000)  // Wait for profiler to attach

println("=== Starting mapper stage ===")
deleteOutputDir()
val start = System.nanoTime()
inputDf.repartition(200).write.mode("overwrite").parquet(outputDir)
val elapsed = (System.nanoTime() - start) / 1000000
println(s"=== Mapper stage completed in ${elapsed}ms ===")

// Wait for profiler to finish
Thread.sleep(5000)
println(s"\n=== Profile saved to: /home/hongbin/experiments/20260108_pr13724_pipeline_benchmark/profiles/mapper_${pid}.jfr ===")

:quit
SCALA_EOF

echo ""
echo "Converting JFR to HTML..."
LATEST_JFR=$(ls -t $OUTPUT_DIR/mapper_*.jfr 2>/dev/null | head -1)
if [ -n "$LATEST_JFR" ]; then
    HTML_FILE="${LATEST_JFR%.jfr}.html"
    java -jar /home/hongbin/develop/async-profiler-3.0-linux-x64/lib/converter.jar \
        --threads -o flamegraph "$LATEST_JFR" "$HTML_FILE" 2>/dev/null && \
    echo "HTML saved to: $HTML_FILE" || \
    echo "JFR saved to: $LATEST_JFR (convert manually)"
fi
