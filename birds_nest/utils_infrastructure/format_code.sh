#!/bin/bash
# Script to format all Python code with Black

echo "Formatting Python code with Black..."

# Format specific directories
uv run black pybirdai/utils/
uv run black pybirdai/entry_points/
uv run black pybirdai/process_steps/

echo "Code formatting complete!"