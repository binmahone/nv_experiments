#!/bin/bash
# Unified benchmark script for PR #13724 pipeline optimization
# Supports switching between jars and parallelism/data configurations
#
# Usage:
#   ./run_benchmark.sh <jar> <config>
#
# JAR options:
#   baseline  - baseline-e95ea4c7d.jar
#   pr13724   - pr13724.jar  
#   phase2    - phase2.jar
#
# Config options:
#   1p1g  - 1 parallelism + ~1GB shuffle
#   1p3g  - 1 parallelism + ~3GB shuffle
#   2p2g  - 2 parallelism + ~2GB shuffle
#   2p6g  - 2 parallelism + ~6GB shuffle
#
# Examples:
#   ./run_benchmark.sh baseline 1p1g
#   ./run_benchmark.sh pr13724 2p6g
#   ./run_benchmark.sh phase2 1p3g

set -e

SPARK_HOME=/disk2/develop/spark-3.3.2-bin-hadoop3-official
EXPERIMENT_DIR=/home/hongbin/experiments/20260108_pr13724_pipeline_benchmark
DATA_DIR="$EXPERIMENT_DIR/data"
RESULT_DIR="$EXPERIMENT_DIR/results"
BASE_FILE_DIR="$DATA_DIR/base_50mb.parquet"

# JAR mapping
declare -A JAR_MAP
JAR_MAP["baseline"]="$EXPERIMENT_DIR/jars/rapids-4-spark_2.12-baseline-e95ea4c7d.jar"
JAR_MAP["pr13724"]="$EXPERIMENT_DIR/jars/rapids-4-spark_2.12-pr13724.jar"
JAR_MAP["phase2"]="/home/hongbin/code/spark-rapids3-2/dist/target/rapids-4-spark_2.12-26.02.0-SNAPSHOT-cuda12.jar"
JAR_MAP["phase3"]="$EXPERIMENT_DIR/jars/rapids-4-spark_2.12-phase3.jar"

# Config mapping: parallelism, shuffle_size_name, num_symlinks, data_dir_suffix
# Approx: 50MB parquet * N symlinks -> raw_size -> ~1/12 compression -> shuffle_size
# 1GB shuffle: ~12GB raw -> 240 symlinks
# 3GB shuffle: ~36GB raw -> 720 symlinks  
# 2GB shuffle: ~24GB raw -> 480 symlinks (existing)
# 6GB shuffle: ~72GB raw -> 1440 symlinks (existing)
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

CONFIG_PARALLELISM["16p32g"]=16
CONFIG_DATA_DIR["16p32g"]="32gb_shuffle_data"
CONFIG_NUM_SYMLINKS["16p32g"]=7680
CONFIG_DISPLAY_NAME["16p32g"]="16 parallelism + 32GB shuffle"

CONFIG_PARALLELISM["16p48g"]=16
CONFIG_DATA_DIR["16p48g"]="48gb_shuffle_data"
CONFIG_NUM_SYMLINKS["16p48g"]=11520
CONFIG_DISPLAY_NAME["16p48g"]="16 parallelism + 48GB shuffle"

CONFIG_PARALLELISM["16p64g"]=16
CONFIG_DATA_DIR["16p64g"]="64gb_shuffle_data"
CONFIG_NUM_SYMLINKS["16p64g"]=15360
CONFIG_DISPLAY_NAME["16p64g"]="16 parallelism + 64GB shuffle"

CONFIG_PARALLELISM["16p80g"]=16
CONFIG_DATA_DIR["16p80g"]="80gb_shuffle_data"
CONFIG_NUM_SYMLINKS["16p80g"]=19200
CONFIG_DISPLAY_NAME["16p80g"]="16 parallelism + 80GB shuffle"

usage() {
    echo "Usage: $0 <jar> <config>"
    echo ""
    echo "JAR options:"
    echo "  baseline  - baseline-e95ea4c7d.jar"
    echo "  pr13724   - pr13724.jar"
    echo "  phase2    - phase2.jar"
    echo ""
    echo "Config options:"
    echo "  1p1g  - 1 parallelism + ~1GB shuffle (240 symlinks)"
    echo "  1p3g  - 1 parallelism + ~3GB shuffle (720 symlinks)"
    echo "  2p2g  - 2 parallelism + ~2GB shuffle (480 symlinks)"
    echo "  2p6g  - 2 parallelism + ~6GB shuffle (1440 symlinks)"
    echo "  4p4g  - 4 parallelism + ~4GB shuffle (960 symlinks)"
    echo "  4p12g - 4 parallelism + ~12GB shuffle (2880 symlinks)"
    echo ""
    echo "Examples:"
    echo "  $0 baseline 1p1g"
    echo "  $0 pr13724 2p6g"
    echo "  $0 phase2 1p3g"
    exit 1
}

# Parse arguments
if [ $# -ne 2 ]; then
    usage
fi

JAR_NAME=$1
CONFIG_NAME=$2

# Validate jar name
if [ -z "${JAR_MAP[$JAR_NAME]}" ]; then
    echo "ERROR: Invalid jar name: $JAR_NAME"
    echo "Valid options: baseline, pr13724, phase2"
    exit 1
fi

# Validate config name
if [ -z "${CONFIG_PARALLELISM[$CONFIG_NAME]}" ]; then
    echo "ERROR: Invalid config name: $CONFIG_NAME"
    echo "Valid options: 1p1g, 1p3g, 2p2g, 2p6g"
    exit 1
fi

JAR_PATH="${JAR_MAP[$JAR_NAME]}"
PARALLELISM="${CONFIG_PARALLELISM[$CONFIG_NAME]}"
DATA_DIR_NAME="${CONFIG_DATA_DIR[$CONFIG_NAME]}"
NUM_SYMLINKS="${CONFIG_NUM_SYMLINKS[$CONFIG_NAME]}"
DISPLAY_NAME="${CONFIG_DISPLAY_NAME[$CONFIG_NAME]}"
DATA_PATH="$DATA_DIR/$DATA_DIR_NAME"

# Check jar exists
if [ ! -f "$JAR_PATH" ]; then
    echo "ERROR: JAR not found: $JAR_PATH"
    exit 1
fi

mkdir -p $DATA_DIR $RESULT_DIR /tmp/spark-events

# Generate base data if needed
generate_base_data() {
    if [ -d "$BASE_FILE_DIR" ] && [ -f "$BASE_FILE_DIR"/*.snappy.parquet 2>/dev/null ]; then
        echo "Base 50MB parquet already exists."
        return 0
    fi
    
    echo "Generating base 50MB parquet file..."
    $SPARK_HOME/bin/spark-shell \
        --master 'local[4]' \
        --driver-memory 8G \
        --name "DataGen_Base50MB" \
        --conf spark.plugins=com.nvidia.spark.SQLPlugin \
        --conf spark.rapids.sql.enabled=true \
        --conf spark.eventLog.enabled=true \
        --conf spark.eventLog.dir=file:///tmp/spark-events \
        --conf spark.driver.extraClassPath=$JAR_PATH \
        --jars $JAR_PATH \
        << 'SCALA_EOF'

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

val targetBytes = 50L * 1024L * 1024L
val bytesPerRow = 100L
val numRows = targetBytes / bytesPerRow

println(s"Generating $numRows rows for 50MB base data...")

val df = spark.range(0, numRows, 1, 1)
  .withColumn("col1", (col("id") * 13 + 7).cast("long"))
  .withColumn("col2", (col("id") * 17 + 11).cast("long"))
  .withColumn("col3", (col("id") * 23 + 13).cast("long"))
  .withColumn("str_col", concat(lit("row_data_padding_"), col("id").cast("string")))

df.write.mode(SaveMode.Overwrite)
  .parquet("/home/hongbin/experiments/20260108_pr13724_pipeline_benchmark/data/base_50mb.parquet")

println(s"Base data generated successfully.")

:quit
SCALA_EOF
}

# Create symlinked dataset if needed
create_symlinks() {
    local target_dir=$1
    local num_links=$2
    
    if [ -d "$target_dir" ]; then
        local existing_count=$(ls "$target_dir"/*.parquet 2>/dev/null | wc -l)
        if [ "$existing_count" -eq "$num_links" ]; then
            echo "Data directory $target_dir already has $num_links symlinks."
            return 0
        fi
        echo "Recreating $target_dir with $num_links symlinks..."
        rm -rf "$target_dir"
    fi
    
    # Find base parquet file
    local base_parquet=$(find "$BASE_FILE_DIR" -name "*.snappy.parquet" -type f | head -1)
    if [ -z "$base_parquet" ]; then
        echo "ERROR: No parquet file found in $BASE_FILE_DIR"
        exit 1
    fi
    
    echo "Creating $num_links symlinks in $target_dir..."
    mkdir -p "$target_dir"
    for i in $(seq 1 $num_links); do
        ln -sf "$base_parquet" "$target_dir/part-$(printf '%05d' $i).snappy.parquet"
    done
    echo "Created $num_links symlinks."
}

# Run benchmark
run_benchmark() {
    local APP_NAME="PR13724_${JAR_NAME}_${CONFIG_NAME}_$(date +%H%M%S)"
    local RESULT_FILE="$RESULT_DIR/${APP_NAME}.txt"
    
    echo ""
    echo "========================================"
    echo "Running: $APP_NAME"
    echo "JAR: $JAR_PATH"
    echo "Config: $DISPLAY_NAME"
    echo "Data: $DATA_PATH"
    echo "Parallelism: $PARALLELISM"
    echo "========================================"
    
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
        --conf spark.rapids.memory.pinnedPool.size=8G \
        --conf spark.rapids.sql.metrics.level=DEBUG \
        --conf spark.shuffle.service.enabled=false \
        --conf spark.eventLog.enabled=true \
        --conf spark.eventLog.dir=file:///tmp/spark-events \
        --conf spark.driver.extraClassPath=$JAR_PATH \
        --jars $JAR_PATH \
        << SCALA_EOF

import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}

val inputDf = spark.read.parquet("$DATA_PATH")
val outputDir = "/tmp/benchmark_output"

val numPartitions = inputDf.rdd.getNumPartitions
println(s"Input partitions (mapper tasks): \$numPartitions")
val rowCount = inputDf.count()
println(s"Row count: \$rowCount")

// Helper to delete output dir
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

// Timed runs (3 times)
val numRuns = 3
val times = new scala.collection.mutable.ArrayBuffer[Long]()

(1 to numRuns).foreach { i =>
  deleteOutputDir()
  System.gc()
  Thread.sleep(2000)
  
  val start = System.nanoTime()
  inputDf.repartition(200).write.parquet(outputDir)
  val elapsed = (System.nanoTime() - start) / 1000000
  times += elapsed
  println(s"Timed run \$i: \${elapsed}ms")
}

val avgTime = times.sum / times.length
val minTime = times.min
val maxTime = times.max
val allRuns = times.mkString(", ")

println("")
println("==========================================")
println(s"Results for $APP_NAME")
println("==========================================")
println(s"Config: $DISPLAY_NAME")
println(s"Average time: \${avgTime}ms")
println(s"Min time: \${minTime}ms")
println(s"Max time: \${maxTime}ms")
println(s"All runs: \${allRuns}ms")

val pw = new java.io.PrintWriter("$RESULT_FILE")
pw.println(s"JAR: $JAR_PATH")
pw.println(s"Config: $DISPLAY_NAME")
pw.println(s"Parallelism: $PARALLELISM")
pw.println(s"Average time: \${avgTime}ms")
pw.println(s"Min time: \${minTime}ms")
pw.println(s"Max time: \${maxTime}ms")
pw.println(s"All runs: \${allRuns}ms")
pw.close()

:quit
SCALA_EOF

    echo ""
    echo "Result saved to: $RESULT_FILE"
}

# Main execution
echo "========================================"
echo "Pipeline Benchmark for PR #13724"
echo "========================================"
echo "JAR: $JAR_NAME -> $JAR_PATH"
echo "Config: $CONFIG_NAME ($DISPLAY_NAME)"
echo ""

# Prepare data
generate_base_data
create_symlinks "$DATA_PATH" "$NUM_SYMLINKS"

# Run benchmark
run_benchmark

echo ""
echo "========================================"
echo "Benchmark Complete!"
echo "========================================"
