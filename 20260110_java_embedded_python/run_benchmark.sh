#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if built
if [ ! -f "lib/libembedded_python_udf.so" ]; then
    echo "Native library not found. Running build first..."
    ./build.sh
fi

if [ ! -d "classes" ]; then
    echo "Java classes not found. Running build first..."
    ./build.sh
fi

echo "Running benchmark..."
echo ""

# Set library path
export LD_LIBRARY_PATH="$SCRIPT_DIR/lib:/home/hongbin/anaconda3/lib:$LD_LIBRARY_PATH"

# Set Python home for embedded interpreter
export PYTHONHOME="/home/hongbin/anaconda3"
export PYTHONPATH="/home/hongbin/anaconda3/lib/python3.11:/home/hongbin/anaconda3/lib/python3.11/site-packages"

# IMPORTANT: Preload libpython to expose symbols needed by numpy
# This is required because numpy's C extensions need Python symbols
export LD_PRELOAD="/home/hongbin/anaconda3/lib/libpython3.11.so"

# Run Java benchmark
java -Djava.library.path="$SCRIPT_DIR/lib" -cp classes EmbeddedPythonUDF
