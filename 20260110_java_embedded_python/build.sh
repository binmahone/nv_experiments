#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "Building Java + JNI Embedded Python UDF"
echo "=========================================="

# Check dependencies
echo "Checking dependencies..."

if ! command -v cmake &> /dev/null; then
    echo "ERROR: cmake not found. Please install cmake."
    exit 1
fi

if ! command -v javac &> /dev/null; then
    echo "ERROR: javac not found. Please install JDK."
    exit 1
fi

if ! python3 -c "import numpy" &> /dev/null; then
    echo "ERROR: numpy not found. Please install: pip install numpy"
    exit 1
fi

# Find JAVA_HOME
if [ -z "$JAVA_HOME" ]; then
    JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
    echo "Auto-detected JAVA_HOME: $JAVA_HOME"
fi

# Build C++ library
echo ""
echo "Building C++ native library..."
mkdir -p cpp/build
cd cpp/build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
cd ../..

# Verify library was built
if [ ! -f "lib/libembedded_python_udf.so" ]; then
    echo "ERROR: Native library not built successfully"
    exit 1
fi
echo "Native library built: lib/libembedded_python_udf.so"

# Compile Java
echo ""
echo "Compiling Java code..."
mkdir -p classes
javac -d classes java/EmbeddedPythonUDF.java
echo "Java compiled to: classes/"

echo ""
echo "=========================================="
echo "Build completed successfully!"
echo "=========================================="
echo ""
echo "To run the benchmark:"
echo "  cd $SCRIPT_DIR"
echo "  ./run_benchmark.sh"
