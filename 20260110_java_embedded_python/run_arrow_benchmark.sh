#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "Running Arrow Socket Benchmark..."
echo ""

# Set up classpath
ARROW_CP="arrow_libs/arrow-vector-15.0.0.jar:arrow_libs/arrow-memory-core-15.0.0.jar:arrow_libs/arrow-memory-unsafe-15.0.0.jar:arrow_libs/arrow-format-15.0.0.jar:arrow_libs/flatbuffers-java-23.5.26.jar:arrow_libs/jackson-databind-2.15.2.jar:arrow_libs/jackson-core-2.15.2.jar:arrow_libs/jackson-annotations-2.15.2.jar:arrow_libs/slf4j-api-1.7.36.jar:arrow_libs/slf4j-simple-1.7.36.jar"

# Find Python library for LD_PRELOAD
PYTHON_LIB=$(python3 -c "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))")/libpython3.11.so

# Check Java version
JAVA_VER=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
JAVA_OPTS=""
if [ "$JAVA_VER" -ge 9 ] 2>/dev/null; then
    JAVA_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED"
fi

# Run benchmark
LD_PRELOAD="$PYTHON_LIB" \
LD_LIBRARY_PATH="lib:$LD_LIBRARY_PATH" \
java -cp "classes:$ARROW_CP" \
    -Djava.library.path=lib \
    -Darrow.memory.debug.allocator=false \
    $JAVA_OPTS \
    ArrowSocketBenchmark
