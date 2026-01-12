#!/bin/bash
# Batch benchmark script for phase2, baseline, phase3
# Tests: 16p16g, 16p32g, 16p48g, 16p64g, 16p80g for each jar
# Total: 15 sessions

set -e

# Use second GPU (GPU 1)
export CUDA_VISIBLE_DEVICES=1
echo "Using GPU: $CUDA_VISIBLE_DEVICES"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/batch_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/batch_test_${TIMESTAMP}.log"

mkdir -p "$LOG_DIR"

# Test matrix
JARS=("phase2" "baseline" "phase3")
CONFIGS=("16p16g" "16p32g" "16p48g" "16p64g" "16p80g")

# Summary file
SUMMARY_FILE="$LOG_DIR/summary_${TIMESTAMP}.txt"

echo "========================================" | tee "$LOG_FILE"
echo "Batch Benchmark Started: $(date)" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "Test Matrix:" | tee -a "$LOG_FILE"
echo "  JARs: ${JARS[*]}" | tee -a "$LOG_FILE"
echo "  Configs: ${CONFIGS[*]}" | tee -a "$LOG_FILE"
echo "  Total tests: $((${#JARS[@]} * ${#CONFIGS[@]}))" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Initialize summary
echo "========================================" > "$SUMMARY_FILE"
echo "Benchmark Summary - $(date)" >> "$SUMMARY_FILE"
echo "========================================" >> "$SUMMARY_FILE"
echo "" >> "$SUMMARY_FILE"
printf "%-10s %-10s %10s %10s %10s %12s\n" "JAR" "CONFIG" "Run1(ms)" "Run2(ms)" "Run3(ms)" "Avg(ms)" >> "$SUMMARY_FILE"
echo "------------------------------------------------------------------------" >> "$SUMMARY_FILE"

TEST_NUM=0
TOTAL_TESTS=$((${#JARS[@]} * ${#CONFIGS[@]}))

for jar in "${JARS[@]}"; do
    for config in "${CONFIGS[@]}"; do
        TEST_NUM=$((TEST_NUM + 1))
        
        echo "" | tee -a "$LOG_FILE"
        echo "========================================" | tee -a "$LOG_FILE"
        echo "Test $TEST_NUM/$TOTAL_TESTS: $jar + $config" | tee -a "$LOG_FILE"
        echo "Started: $(date)" | tee -a "$LOG_FILE"
        echo "========================================" | tee -a "$LOG_FILE"
        
        # Run benchmark
        START_TIME=$(date +%s)
        
        if "$SCRIPT_DIR/run_benchmark.sh" "$jar" "$config" 2>&1 | tee -a "$LOG_FILE"; then
            END_TIME=$(date +%s)
            DURATION=$((END_TIME - START_TIME))
            
            # Extract results from latest result file
            RESULT_FILE=$(ls -t "$SCRIPT_DIR/results/PR13724_${jar}_${config}"*.txt 2>/dev/null | head -1)
            
            if [ -f "$RESULT_FILE" ]; then
                AVG_TIME=$(grep "Average time:" "$RESULT_FILE" | awk '{print $3}' | sed 's/ms//')
                ALL_RUNS=$(grep "All runs:" "$RESULT_FILE" | cut -d: -f2 | sed 's/ms//g')
                RUN1=$(echo "$ALL_RUNS" | cut -d, -f1 | tr -d ' ')
                RUN2=$(echo "$ALL_RUNS" | cut -d, -f2 | tr -d ' ')
                RUN3=$(echo "$ALL_RUNS" | cut -d, -f3 | tr -d ' ')
                
                printf "%-10s %-10s %10s %10s %10s %12s\n" "$jar" "$config" "$RUN1" "$RUN2" "$RUN3" "$AVG_TIME" >> "$SUMMARY_FILE"
                
                echo "" | tee -a "$LOG_FILE"
                echo "✓ Completed: $jar + $config (${DURATION}s total, avg=${AVG_TIME}ms)" | tee -a "$LOG_FILE"
            else
                echo "✗ Result file not found for $jar + $config" | tee -a "$LOG_FILE"
                printf "%-10s %-10s %10s %10s %10s %12s\n" "$jar" "$config" "ERROR" "ERROR" "ERROR" "ERROR" >> "$SUMMARY_FILE"
            fi
        else
            echo "✗ FAILED: $jar + $config" | tee -a "$LOG_FILE"
            printf "%-10s %-10s %10s %10s %10s %12s\n" "$jar" "$config" "FAILED" "FAILED" "FAILED" "FAILED" >> "$SUMMARY_FILE"
        fi
        
        # Pause between tests for clear monitoring boundaries
        echo "Pausing 10 seconds before next test..." | tee -a "$LOG_FILE"
        sleep 10
    done
done

echo "" >> "$SUMMARY_FILE"
echo "========================================" >> "$SUMMARY_FILE"
echo "Completed: $(date)" >> "$SUMMARY_FILE"
echo "========================================" >> "$SUMMARY_FILE"

echo "" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
echo "All Tests Completed: $(date)" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "Summary saved to: $SUMMARY_FILE" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Print summary
cat "$SUMMARY_FILE"
