#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "Building Arrow Socket Benchmark..."

# Compile Java with Arrow libraries
ARROW_CP="arrow_libs/arrow-vector-15.0.0.jar:arrow_libs/arrow-memory-core-15.0.0.jar:arrow_libs/arrow-memory-unsafe-15.0.0.jar:arrow_libs/arrow-format-15.0.0.jar:arrow_libs/flatbuffers-java-23.5.26.jar:arrow_libs/slf4j-api-1.7.36.jar:arrow_libs/slf4j-simple-1.7.36.jar"

mkdir -p classes

echo "Compiling Java code..."
javac -cp "$ARROW_CP" -d classes java/EmbeddedPythonUDF.java java/ArrowSocketBenchmark.java

echo "Build complete!"
echo ""
echo "To run the benchmark:"
echo "  ./run_arrow_benchmark.sh"
