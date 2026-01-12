#!/bin/bash
set -e

SPARK_HOME="/home/hongbin/develop/spark-3.3.2-bin-hadoop3-official"
JAR_PATH="/home/hongbin/experiments/20260108_pr13724_pipeline_benchmark/jars/rapids-4-spark_2.12-phase2.jar"
DATA_PATH="/home/hongbin/experiments/20260108_pr13724_pipeline_benchmark/data/16gb_shuffle_data"
PARALLELISM=16
APP_NAME="phase2_16p16g_skipMerge_false_memThreshold_0"

echo "Running phase2 with skipMerge=false and partialFileBufferMemoryThreshold=0.001"
echo "Config: 16 parallelism, 16GB shuffle data"

$SPARK_HOME/bin/spark-shell \
    --master "local[$PARALLELISM]" \
    --driver-memory 32g \
    --conf spark.app.name="$APP_NAME" \
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
    --conf spark.rapids.memory.host.partialFileBufferMemoryThreshold=0.001 \
    --jars "$JAR_PATH" \
    << 'SCALA_EOF'
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

// Warmup
println("Warmup run...")
deleteOutputDir()
inputDf.repartition(200).write.mode("overwrite").parquet(outputDir)

// Timed runs
val numRuns = 3
val times = new scala.collection.mutable.ArrayBuffer[Long]()

(1 to numRuns).foreach { i =>
  System.gc()
  Thread.sleep(2000)
  deleteOutputDir()
  val start = System.nanoTime()
  inputDf.repartition(200).write.mode("overwrite").parquet(outputDir)
  val elapsed = (System.nanoTime() - start) / 1000000
  times += elapsed
  println(s"Timed run $i: ${elapsed}ms")
}

val avg = times.sum / times.length
println(s"\nResults: phase2_16p16g_skipMerge_false_memThreshold_0")
println(s"Times: ${times.mkString(", ")}ms")
println(s"Average time: ${avg}ms")

:quit
SCALA_EOF
