"""
Database and Performance Constants for SDD Model Import Operations

This module defines constants used for importing data from websites/technical exports
to SDD model, particularly for bulk_create operations to optimize performance.
"""

# Batch sizes for bulk_create operations
# These values balance memory usage, database parameter limits, and performance.
# Based on profiling results, a batch size of 50,000 provides optimal performance
# for the BIRD data import pipeline.

# Default batch size for most model bulk_create operations
# This value works well for models with typical field counts (5-20 fields)
# and provides good performance while staying within database limits.
BULK_CREATE_BATCH_SIZE_DEFAULT = 50000

# Specific batch size for ordinate items (kept separate for clarity)
# Ordinate items are one of the largest data volumes in BIRD imports
BULK_CREATE_BATCH_SIZE_ORDINATE_ITEMS = 50000

# Note: For databases with strict parameter limits (e.g., SQLite with 999 parameter limit),
# consider using dynamic batch size calculation based on model field count.
# See pybirdai/utils/clone_mode/import_from_metadata_export.py for an example.
