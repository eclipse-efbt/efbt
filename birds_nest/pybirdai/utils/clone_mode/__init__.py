# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#
"""
Clone Mode utilities for PyBIRD AI.

This module provides functionality for:
- Importing BIRD data from CSV exports (import_from_metadata_export)
- Managing process metadata for workflow state (process_metadata)
- Export functionality with IDs (export_with_ids)

Submodules:
- importer/: CSV import utilities and column mappings
- metadata/: Process metadata management (workflow status, validation, restoration)
"""

# Main public API
from .import_from_metadata_export import (
    CSVDataImporter,
    import_bird_data_from_csv_export,
    import_bird_data_from_csv_export_ordered,
)

from .process_metadata import (
    # Main functions
    generate_process_metadata,
    save_process_metadata,
    load_process_metadata,
    get_restart_point,
    validate_metadata_parsing_only,
    restore_workflow_states,
    verify_environment_state,
    # Constants
    SCHEMA_VERSION,
    WORKFLOW_STEP_COUNTS,
    COMPLETION_STEPS,
)

__all__ = [
    # Import functionality
    'CSVDataImporter',
    'import_bird_data_from_csv_export',
    'import_bird_data_from_csv_export_ordered',
    # Process metadata
    'generate_process_metadata',
    'save_process_metadata',
    'load_process_metadata',
    'get_restart_point',
    'validate_metadata_parsing_only',
    'restore_workflow_states',
    'verify_environment_state',
    # Constants
    'SCHEMA_VERSION',
    'WORKFLOW_STEP_COUNTS',
    'COMPLETION_STEPS',
]
