#!/bin/bash
# Batch benchmark script - run multiple jar/config combinations
#
# Usage:
#   ./run_batch.sh                        # Run all combinations
#   ./run_batch.sh baseline,pr13724 1p1g,2p2g  # Run specific combinations
#
# Examples:
#   ./run_batch.sh baseline,pr13724 2p2g,2p6g   # Compare baseline vs pr13724 on 2p configs
#   ./run_batch.sh phase2 1p1g,1p3g,2p2g,2p6g   # Run phase2 on all configs
#   ./run_batch.sh baseline,pr13724,phase2 2p2g # Compare all 3 jars on 2p2g

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHMARK_SCRIPT="$SCRIPT_DIR/run_benchmark.sh"

ALL_JARS="baseline,pr13724,phase2"
ALL_CONFIGS="1p1g,1p3g,2p2g,2p6g"

usage() {
    echo "Usage: $0 [jars] [configs]"
    echo ""
    echo "Arguments (optional):"
    echo "  jars    - Comma-separated list of jars: baseline,pr13724,phase2"
    echo "  configs - Comma-separated list of configs: 1p1g,1p3g,2p2g,2p6g"
    echo ""
    echo "If no arguments provided, runs all combinations."
    echo ""
    echo "Examples:"
    echo "  $0                                    # All combinations"
    echo "  $0 baseline,pr13724 2p2g,2p6g         # Specific combinations"
    echo "  $0 phase2 1p1g,1p3g,2p2g,2p6g         # One jar, all configs"
    exit 1
}

if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
    usage
fi

# Parse arguments
JARS=${1:-$ALL_JARS}
CONFIGS=${2:-$ALL_CONFIGS}

# Convert to arrays
IFS=',' read -ra JAR_ARRAY <<< "$JARS"
IFS=',' read -ra CONFIG_ARRAY <<< "$CONFIGS"

echo "========================================"
echo "Batch Benchmark for PR #13724"
echo "========================================"
echo "Jars: ${JAR_ARRAY[*]}"
echo "Configs: ${CONFIG_ARRAY[*]}"
echo "Total runs: $((${#JAR_ARRAY[@]} * ${#CONFIG_ARRAY[@]}))"
echo "========================================"
echo ""

# Track results
declare -a RESULTS

run_count=0
total_runs=$((${#JAR_ARRAY[@]} * ${#CONFIG_ARRAY[@]}))

for jar in "${JAR_ARRAY[@]}"; do
    for config in "${CONFIG_ARRAY[@]}"; do
        run_count=$((run_count + 1))
        echo ""
        echo "========================================"
        echo "[$run_count/$total_runs] Running: $jar + $config"
        echo "========================================"
        
        if "$BENCHMARK_SCRIPT" "$jar" "$config"; then
            RESULTS+=("$jar + $config: SUCCESS")
        else
            RESULTS+=("$jar + $config: FAILED")
        fi
    done
done

# Print summary
echo ""
echo "========================================"
echo "Batch Benchmark Summary"
echo "========================================"
for result in "${RESULTS[@]}"; do
    echo "  $result"
done
echo ""

# Show all result files
echo "Result files:"
ls -lt "$SCRIPT_DIR/results/"*.txt 2>/dev/null | head -20
