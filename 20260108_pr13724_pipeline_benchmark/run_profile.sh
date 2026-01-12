#!/bin/bash
# Profiling script - strictly follows run_benchmark.sh configurations
# Captures wall-clock flame graph for mapper stage (Stage 1)
#
# Usage:
#   ./run_profile.sh <jar> <config>
#
# Examples:
#   ./run_profile.sh baseline 16p16g
#   ./run_profile.sh phase2 16p16g

set -e

SPARK_HOME=/disk2/develop/spark-3.3.2-bin-hadoop3-official
EXPERIMENT_DIR=/home/hongbin/experiments/20260108_pr13724_pipeline_benchmark
DATA_DIR="$EXPERIMENT_DIR/data"
PROFILE_DIR="$EXPERIMENT_DIR/profiles"
BASE_FILE_DIR="$DATA_DIR/base_50mb.parquet"
ASYNC_PROFILER_HOME=/home/hongbin/develop/async-profiler-3.0-linux-x64

# JAR mapping
declare -A JAR_MAP
JAR_MAP["baseline"]="$EXPERIMENT_DIR/jars/rapids-4-spark_2.12-baseline-e95ea4c7d.jar"
JAR_MAP["pr13724"]="$EXPERIMENT_DIR/jars/rapids-4-spark_2.12-pr13724.jar"
JAR_MAP["phase2"]="$EXPERIMENT_DIR/jars/rapids-4-spark_2.12-phase2.jar"
JAR_MAP["phase3"]="$EXPERIMENT_DIR/jars/rapids-4-spark_2.12-phase3.jar"

# Config mapping - exact same as run_benchmark.sh
declare -A CONFIG_PARALLELISM
declare -A CONFIG_DATA_DIR
declare -A CONFIG_NUM_SYMLINKS
declare -A CONFIG_DISPLAY_NAME

CONFIG_PARALLELISM["1p1g"]=1
CONFIG_DATA_DIR["1p1g"]="1gb_shuffle_data"
CONFIG_NUM_SYMLINKS["1p1g"]=240
CONFIG_DISPLAY_NAME["1p1g"]="1 parallelism + 1GB shuffle"

CONFIG_PARALLELISM["1p3g"]=1
CONFIG_DATA_DIR["1p3g"]="3gb_shuffle_data"
CONFIG_NUM_SYMLINKS["1p3g"]=720
CONFIG_DISPLAY_NAME["1p3g"]="1 parallelism + 3GB shuffle"

CONFIG_PARALLELISM["2p2g"]=2
CONFIG_DATA_DIR["2p2g"]="2gb_shuffle_data"
CONFIG_NUM_SYMLINKS["2p2g"]=480
CONFIG_DISPLAY_NAME["2p2g"]="2 parallelism + 2GB shuffle"

CONFIG_PARALLELISM["2p6g"]=2
CONFIG_DATA_DIR["2p6g"]="6gb_shuffle_data"
CONFIG_NUM_SYMLINKS["2p6g"]=1440
CONFIG_DISPLAY_NAME["2p6g"]="2 parallelism + 6GB shuffle"

CONFIG_PARALLELISM["4p4g"]=4
CONFIG_DATA_DIR["4p4g"]="4gb_shuffle_data"
CONFIG_NUM_SYMLINKS["4p4g"]=960
CONFIG_DISPLAY_NAME["4p4g"]="4 parallelism + 4GB shuffle"

CONFIG_PARALLELISM["4p12g"]=4
CONFIG_DATA_DIR["4p12g"]="12gb_shuffle_data"
CONFIG_NUM_SYMLINKS["4p12g"]=2880
CONFIG_DISPLAY_NAME["4p12g"]="4 parallelism + 12GB shuffle"

CONFIG_PARALLELISM["16p16g"]=16
CONFIG_DATA_DIR["16p16g"]="16gb_shuffle_data"
CONFIG_NUM_SYMLINKS["16p16g"]=3840
CONFIG_DISPLAY_NAME["16p16g"]="16 parallelism + 16GB shuffle"

usage() {
    echo "Usage: $0 <jar> <config>"
    echo ""
    echo "JAR options: baseline, pr13724, phase2, phase3"
    echo "Config options: 1p1g, 1p3g, 2p2g, 2p6g, 4p4g, 4p12g, 16p16g"
    exit 1
}

if [ $# -ne 2 ]; then
    usage
fi

JAR_NAME=$1
CONFIG_NAME=$2

if [ -z "${JAR_MAP[$JAR_NAME]}" ]; then
    echo "ERROR: Invalid jar name: $JAR_NAME"
    exit 1
fi

if [ -z "${CONFIG_PARALLELISM[$CONFIG_NAME]}" ]; then
    echo "ERROR: Invalid config name: $CONFIG_NAME"
    exit 1
fi

JAR_PATH="${JAR_MAP[$JAR_NAME]}"
PARALLELISM="${CONFIG_PARALLELISM[$CONFIG_NAME]}"
DATA_DIR_NAME="${CONFIG_DATA_DIR[$CONFIG_NAME]}"
NUM_SYMLINKS="${CONFIG_NUM_SYMLINKS[$CONFIG_NAME]}"
DISPLAY_NAME="${CONFIG_DISPLAY_NAME[$CONFIG_NAME]}"
DATA_PATH="$DATA_DIR/$DATA_DIR_NAME"

if [ ! -f "$JAR_PATH" ]; then
    echo "ERROR: JAR not found: $JAR_PATH"
    exit 1
fi

if [ ! -d "$DATA_PATH" ]; then
    echo "ERROR: Data directory not found: $DATA_PATH"
    echo "Please run ./run_benchmark.sh $JAR_NAME $CONFIG_NAME first to create data"
    exit 1
fi

mkdir -p $PROFILE_DIR /tmp/spark-events

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
APP_NAME="Profile_${JAR_NAME}_${CONFIG_NAME}_${TIMESTAMP}"
JFR_FILE="$PROFILE_DIR/${JAR_NAME}_${CONFIG_NAME}_${TIMESTAMP}.jfr"
HTML_FILE="$PROFILE_DIR/${JAR_NAME}_${CONFIG_NAME}_${TIMESTAMP}_wall.html"

echo "========================================"
echo "Profiling: $APP_NAME"
echo "JAR: $JAR_PATH"
echo "Config: $DISPLAY_NAME"
echo "Data: $DATA_PATH"
echo "Parallelism: $PARALLELISM"
echo "========================================"

# Run spark-shell with profiling - exact same config as run_benchmark.sh
$SPARK_HOME/bin/spark-shell \
    --master 'local[16]' \
    --driver-memory 50G \
    --name "$APP_NAME" \
    --conf spark.default.parallelism=$PARALLELISM \
    --conf spark.driver.maxResultSize=2GB \
    --conf spark.executor.memory=12G \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.sql.enabled=true \
    --conf spark.rapids.sql.batchSizeBytes=1073741824 \
    --conf spark.rapids.sql.concurrentGpuTasks=2 \
    --conf spark.sql.files.maxPartitionBytes=20gb \
    --conf spark.sql.files.openCostInBytes=0 \
    --conf spark.sql.parquet.aggregatePushdown=false \
    --conf spark.rapids.memory.host.spillStorageSize=45G \
    --conf spark.rapids.memory.host.partialFileBufferMemoryThreshold=0.5 \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.shuffle.manager=com.nvidia.spark.rapids.spark332.RapidsShuffleManager \
    --conf spark.rapids.shuffle.mode=MULTITHREADED \
    --conf spark.rapids.shuffle.multiThreaded.writer.threads=16 \
    --conf spark.rapids.shuffle.multiThreaded.reader.threads=16 \
    --conf spark.rapids.shuffle.multithreaded.skipMerge=false \
    --conf spark.rapids.sql.metrics.level=DEBUG \
    --conf spark.shuffle.service.enabled=false \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=file:///tmp/spark-events \
    --conf spark.driver.extraClassPath=$JAR_PATH \
    --jars $JAR_PATH \
    << SCALA_EOF

import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.lang.management.ManagementFactory

val asyncProfilerHome = "$ASYNC_PROFILER_HOME"
val jfrFile = "$JFR_FILE"
val pid = ManagementFactory.getRuntimeMXBean().getName().split("@")(0)

println(s"JVM PID: \$pid")

val inputDf = spark.read.parquet("$DATA_PATH")
val outputDir = "/tmp/benchmark_output"

val numPartitions = inputDf.rdd.getNumPartitions
println(s"Input partitions (mapper tasks): \$numPartitions")

def deleteOutputDir(): Unit = {
  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val path = new Path(outputDir)
  if (fs.exists(path)) {
    fs.delete(path, true)
  }
}

// Warmup
println("Warmup run...")
deleteOutputDir()
inputDf.repartition(200).write.parquet(outputDir)

// GC and wait
System.gc()
Thread.sleep(3000)

// Start profiler
println("Starting async-profiler (wall clock mode)...")
val startCmd = s"\$asyncProfilerHome/bin/asprof start -e wall -i 100us -f \$jfrFile \$pid"
println(s"Command: \$startCmd")
val startResult = sys.process.Process(Seq("bash", "-c", startCmd)).!!
println(s"Profiler start result: \$startResult")

// Delete and run with profiling
deleteOutputDir()
println("Running profiled iteration...")
val start = System.nanoTime()
inputDf.repartition(200).write.parquet(outputDir)
val elapsed = (System.nanoTime() - start) / 1000000
println(s"Profiled run completed in \${elapsed}ms")

// Stop profiler
println("Stopping async-profiler...")
val stopCmd = s"\$asyncProfilerHome/bin/asprof stop \$pid"
val stopResult = sys.process.Process(Seq("bash", "-c", stopCmd)).!!
println(s"Profiler stop result: \$stopResult")

println(s"JFR file saved to: \$jfrFile")

:quit
SCALA_EOF

echo ""
echo "Converting JFR to HTML flame graph..."
java -cp $ASYNC_PROFILER_HOME/lib/converter.jar \
    jfr2flame --threads --total "$JFR_FILE" "$HTML_FILE"

echo ""
echo "Creating symlink in async-profiler directory..."
ln -sf "$HTML_FILE" "$ASYNC_PROFILER_HOME/${JAR_NAME}_${CONFIG_NAME}_wall.html"

echo ""
echo "========================================"
echo "Profiling Complete!"
echo "========================================"
echo "JFR: $JFR_FILE"
echo "HTML: $HTML_FILE"
echo "Symlink: $ASYNC_PROFILER_HOME/${JAR_NAME}_${CONFIG_NAME}_wall.html"
