#!/bin/bash

# Script to run ruff checks on PyBIRD AI codebase
# Saves results to ruff_check_results.txt

OUTPUT_FILE="ruff_check_results.txt"

echo "Running ruff checks on PyBIRD AI codebase..." > "$OUTPUT_FILE"
echo "Generated on: $(date)" >> "$OUTPUT_FILE"
echo "=========================================" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

# Function to run ruff check and capture output
run_ruff_check() {
    local path="$1"
    echo "Checking: $path" >> "$OUTPUT_FILE"
    echo "-------------------" >> "$OUTPUT_FILE"
    uv run ruff check "$path" >> "$OUTPUT_FILE" 2>&1
    echo "" >> "$OUTPUT_FILE"
}

# Run all the ruff checks
run_ruff_check "pybirdai/utils/"
run_ruff_check "pybirdai/entry_points/"
run_ruff_check "pybirdai/process_steps/"

echo "Ruff checks completed. Results saved to: $OUTPUT_FILE"
echo "=========================================" >> "$OUTPUT_FILE"
echo "End of ruff check results" >> "$OUTPUT_FILE"
