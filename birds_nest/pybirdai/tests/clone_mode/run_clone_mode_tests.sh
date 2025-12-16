#!/bin/bash
# Clone Mode Test Runner
# Copyright (c) 2025 Arfa Digital Consulting
#
# This script runs the clone mode tests and generates a report.
#
# Usage:
#   ./run_clone_mode_tests.sh           # Run all tests
#   ./run_clone_mode_tests.sh --quick   # Skip slow tests
#   ./run_clone_mode_tests.sh --dpm     # Run only DPM tests
#   ./run_clone_mode_tests.sh --main    # Run only Main workflow tests
#   ./run_clone_mode_tests.sh --ancrdt  # Run only ANCRDT tests

set -e

# Get the directory of this script
# Path: birds_nest/pybirdai/tests/clone_mode/run_clone_mode_tests.sh
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIRDS_NEST_DIR="$(dirname "$(dirname "$(dirname "$SCRIPT_DIR")")")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Parse arguments
PYTEST_ARGS="-v"
TEST_FILTER=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --quick)
            PYTEST_ARGS="$PYTEST_ARGS -m 'not slow'"
            shift
            ;;
        --dpm)
            TEST_FILTER="::TestDPMWorkflow"
            shift
            ;;
        --main)
            TEST_FILTER="::TestMainWorkflow"
            shift
            ;;
        --ancrdt)
            TEST_FILTER="::TestANCRDTWorkflow"
            shift
            ;;
        --basic)
            TEST_FILTER="::TestCloneModeBasic"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--quick] [--dpm|--main|--ancrdt|--basic]"
            exit 1
            ;;
    esac
done

echo "========================================"
echo "Clone Mode Test Suite"
echo "========================================"
echo ""
echo "Test directory: $SCRIPT_DIR"
echo "Birds Nest directory: $BIRDS_NEST_DIR"
echo ""

# Change to birds_nest directory
cd "$BIRDS_NEST_DIR"

# Check if virtual environment exists
if [ -d ".venv" ]; then
    echo "Using virtual environment..."
    source .venv/bin/activate
fi

# Backup current database if it exists
if [ -f "db.sqlite3" ]; then
    echo "Backing up current database..."
    cp db.sqlite3 db.sqlite3.pre_test_backup
fi

# Run tests
echo ""
echo "Running tests..."
echo "----------------------------------------"

TEST_PATH="pybirdai/tests/clone_mode/test_clone_mode.py${TEST_FILTER}"

if command -v uv &> /dev/null; then
    # Use uv if available
    uv run python -m pytest "$TEST_PATH" $PYTEST_ARGS
    TEST_RESULT=$?
else
    # Fall back to direct pytest
    python -m pytest "$TEST_PATH" $PYTEST_ARGS
    TEST_RESULT=$?
fi

# Restore database backup
if [ -f "db.sqlite3.pre_test_backup" ]; then
    echo ""
    echo "Restoring original database..."
    mv db.sqlite3.pre_test_backup db.sqlite3
fi

# Print result
echo ""
echo "========================================"
if [ $TEST_RESULT -eq 0 ]; then
    echo -e "${GREEN}ALL TESTS PASSED${NC}"
else
    echo -e "${RED}SOME TESTS FAILED${NC}"
fi
echo "========================================"

exit $TEST_RESULT
