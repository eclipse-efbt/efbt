#!/bin/bash
# Script to run black formatting and pylint checks on PyBIRD AI codebase
# Saves results to code_quality_results.txt

OUTPUT_FILE="code_quality_results.txt"

echo "Running code quality checks on PyBIRD AI codebase..." > "$OUTPUT_FILE"
echo "=========================================" >> "$OUTPUT_FILE"
echo "" >> "$OUTPUT_FILE"

# Function to run black formatting check
run_black_check() {
    local path="$1"
    echo "Checking formatting with Black: $path" >> "$OUTPUT_FILE"
    echo "-----------------------------------" >> "$OUTPUT_FILE"
    uv run black --check --diff "$path" >> "$OUTPUT_FILE" 2>&1
    echo "" >> "$OUTPUT_FILE"
}

# Function to run pylint check and capture output
run_pylint_check() {
    local path="$1"
    echo "Running pylint on: $path" >> "$OUTPUT_FILE"
    echo "-----------------------------------" >> "$OUTPUT_FILE"
    uv run pylint "$path" >> "$OUTPUT_FILE" 2>&1
    echo "" >> "$OUTPUT_FILE"
}

# Run black formatting checks
echo "=== BLACK FORMATTING CHECKS ===" >> "$OUTPUT_FILE"
run_black_check "pybirdai/utils/"
run_black_check "pybirdai/entry_points/"
run_black_check "pybirdai/process_steps/"

echo "" >> "$OUTPUT_FILE"
echo "=== PYLINT CHECKS ===" >> "$OUTPUT_FILE"

# Run pylint checks
run_pylint_check "pybirdai/utils/"
run_pylint_check "pybirdai/entry_points/"
run_pylint_check "pybirdai/process_steps/"

echo "End of code quality check results" >> "$OUTPUT_FILE"
echo "Results saved to: $OUTPUT_FILE"